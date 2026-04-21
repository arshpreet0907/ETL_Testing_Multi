"""
utils/comparator.py
--------------------
Purpose : Accept two PySpark DataFrames (expected/source and actual/target),
          join them on primary key columns, and produce an exploded diff DataFrame
          where each row represents a single cell-level difference.

The output DataFrame is schema-compatible with utils/reporter.py — pass it
directly to generate_report() without any transformation.

Output schema
-------------
    primary_key_value : str   — PK value(s); composite PKs joined with "|"
    column_name       : str   — column name that differs
    expected_value    : str   — value from `source_df` (transformed source)
    actual_value      : str   — value from `target_df`
    diff_type         : str   — MISSING_IN_TARGET | EXTRA_IN_TARGET | VALUE_MISMATCH

Difference types
----------------
    MISSING_IN_TARGET — Record exists in source but not in target
    EXTRA_IN_TARGET   — Record exists in target but not in source
    VALUE_MISMATCH    — Record exists in both but values differ

Null handling (per spec)
------------------------
    null  == null        → MATCH   (both sides absent — not a difference)
    null  == "null"      → MISMATCH (transform wrote literal string "null")
    ""    == null        → MISMATCH (empty string is not null)

Normalisation before compare
-----------------------------
    - Strip leading/trailing whitespace from all string values
    - Lowercase boolean literals ("True", "TRUE", "False" → "true"/"false")

Comparison Strategies
---------------------
Three interchangeable strategies are provided.  Switch by uncommenting the
desired return line in compare_dataframes().

    Strategy A — _compare_full_outer()
        Full outer join on all columns.  Simple, guaranteed correct, no hash
        collision risk.  Slowest for wide tables.
        Best for: ≤100K rows or narrow tables (≤20 columns).

    Strategy B — _compare_hash()   ⭐ Recommended for large tables
        Two-phase hash comparison.
        Phase 1: slim PK + xxhash64 join (tiny shuffle).  Identifies
                 missing, extra, and mismatched rows via hash comparison.
                 For PASS case (0 diffs): STOP HERE — no Phase 2.
        Phase 2: collects mismatched rows to the driver and compares
                 column-by-column in Python.  No Spark joins/shuffles.
                 Time: ~3-5s regardless of dataset size.
        10–20× less shuffle data.  Handles 10M+ rows in 4 GB.
        Collision risk: ~1 in 370K runs at 10M rows (effectively zero).
        Best for: >100K rows or wide tables (>30 columns).

    Strategy C — _compare_anti_inner()
        Decomposed into left_anti + inner joins.  All three join types
        support broadcast (unlike full outer).  No hash collision risk.
        Best for: Medium tables (100K–1M rows) where broadcast fits in memory.

Usage
-----
    from utils.comparator import compare_dataframes

    diff_df, source_count, target_count, matched_count, total_diffs = compare_dataframes(
        source_df=source_dataframe,
        target_df=target_dataframe,
        primary_key_cols=["ORDER_ID"],
        compare_cols=["ORDER_STATUS", "IS_ACTIVE_FLAG", "UNIT_PRICE_USD"],
    )
"""

import logging
import time
from typing import List, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType, StructField

logger = logging.getLogger(__name__)

# Prefixes used to disambiguate source / target column names after the join
_SRC_PREFIX = "src_"
_TGT_PREFIX = "tgt_"

# Reusable diff output schema
_DIFF_SCHEMA = StructType([
    StructField("primary_key_value", StringType(), True),
    StructField("column_name",        StringType(), True),
    StructField("expected_value",     StringType(), True),
    StructField("actual_value",       StringType(), True),
    StructField("diff_type",          StringType(), True),
])

# Phase 2 always uses driver-side collection (no Spark joins).
#
# Why no Spark fallback?
# ----------------------
# Phase 1 (hash join) identifies HOW MANY rows differ but not WHICH COLUMNS.
# Phase 2 must retrieve the actual values for those rows to build the diff
# report.  Two options exist:
#
#   A. Spark joins (old) — inner join + explode + union + cache + count.
#      Fixed ~30-40s overhead from query planning, task scheduling, and
#      serialization — even for 1 mismatched row.
#
#   B. Collect to driver (current) — .filter().collect() on cached DFs,
#      compare in Python, return spark.createDataFrame(results).
#      Time proportional to mismatches: ~3s for 1 row, ~5s for 10K rows.
#
# The only limit on (B) is driver memory.  With 4 GB driver heap:
#   - 10K rows × 20 cols × 50 bytes ≈ 10 MB   → trivial
#   - 100K rows                      ≈ 100 MB  → fine
#   - 500K rows                      ≈ 500 MB  → still safe
#
# Since this is an ETL *validation* tool (expected diffs ≈ 0; a run with
# 500K+ mismatches means the ETL is fundamentally broken), the collected
# path handles every realistic scenario.  No threshold or Spark fallback
# is needed.


# =========================================================================== #
# Numeric precision auto-detection                                             #
# =========================================================================== #

# Default rounding precision for float/double columns when neither side
# carries a DecimalType with an explicit scale.  6 decimal places is
# generous enough for most ETL monetary / metric data while still
# eliminating IEEE-754 representation noise (e.g. 128129.02000000002).
_DEFAULT_DOUBLE_PRECISION = 6


def _build_precision_map(
    source_schema,
    target_schema,
    compare_cols: List[str],
) -> dict:
    """
    Auto-detect rounding precision for each numeric compare column.

    Inspects the PySpark schemas of *both* the source and target DataFrames
    and returns a ``{col_name: scale}`` dict used by ``_normalise_df`` to
    round floating-point values before casting to string.

    Rules
    -----
    1. If either side is ``DecimalType`` → use its ``.scale``
       (e.g. DECIMAL(12,2) → 2).  If *both* sides are Decimal, use the
       **max** scale so no side loses precision.
    2. If neither side is Decimal but at least one is ``FloatType`` or
       ``DoubleType`` → use ``_DEFAULT_DOUBLE_PRECISION`` (6).
    3. Integer-only columns are omitted from the map (no rounding needed).

    This is a metadata-only operation — zero Spark actions, negligible cost.
    """
    from pyspark.sql.types import (
        DecimalType, FloatType, DoubleType,
        IntegerType, LongType, ShortType, ByteType,
    )

    src_fields = {f.name: f.dataType for f in source_schema.fields}
    tgt_fields = {f.name: f.dataType for f in target_schema.fields}

    precision_map: dict = {}

    for col in compare_cols:
        src_dt = src_fields.get(col)
        tgt_dt = tgt_fields.get(col)

        src_is_decimal = isinstance(src_dt, DecimalType)
        tgt_is_decimal = isinstance(tgt_dt, DecimalType)
        src_is_float   = isinstance(src_dt, (FloatType, DoubleType))
        tgt_is_float   = isinstance(tgt_dt, (FloatType, DoubleType))

        if src_is_decimal and tgt_is_decimal:
            # Both Decimal — use the larger scale so neither side truncates
            precision_map[col] = max(src_dt.scale, tgt_dt.scale)
        elif src_is_decimal:
            # Source is Decimal, target is float/double or another type
            precision_map[col] = src_dt.scale
        elif tgt_is_decimal:
            # Target is Decimal, source is float/double or another type
            precision_map[col] = tgt_dt.scale
        elif src_is_float or tgt_is_float:
            # Neither side is Decimal but at least one is float/double
            precision_map[col] = _DEFAULT_DOUBLE_PRECISION
        # else: integer-only or string — no rounding entry needed

    if precision_map:
        logger.info(
            "Auto-detected numeric precision map for %d column(s): %s",
            len(precision_map), precision_map,
        )

    return precision_map


# =========================================================================== #
# Public entry point                                                           #
# =========================================================================== #

def compare_dataframes(
    source_df: DataFrame,
    target_df: DataFrame,
    primary_key_cols: List[str],
    compare_cols: List[str],
) -> Tuple[DataFrame, int, int, int, int]:
    """
    Compare two DataFrames column-by-column after joining on primary key(s).

    Shared preprocessing (lowercase + normalise) is applied here, then the
    comparison is delegated to one of three interchangeable strategy functions.
    Uncomment the desired strategy in the dispatch section below.

    Parameters
    ----------
    source_df : pyspark.sql.DataFrame
        The *expected* data — typically source_enriched loaded from CSV.
        All columns are treated as strings (inferSchema must be False at read time).
    target_df : pyspark.sql.DataFrame
        The *actual* data — typically target_actual loaded from CSV.
    primary_key_cols : list of str
        Column name(s) used as the join key.  Must exist in both DataFrames.
        Column names are matched case-insensitively.
    compare_cols : list of str
        Non-PK columns to compare.  Must exist in both DataFrames.

    Returns
    -------
    diff_df : pyspark.sql.DataFrame
        Exploded diff; schema: primary_key_value, column_name,
        expected_value, actual_value, diff_type.
        Empty DataFrame (same schema) when there are no differences.
    source_row_count : int
    target_row_count : int
    matched_row_count : int
    total_diff_count : int
        Total number of differences (missing + extra + value mismatches)
    """
    # ------------------------------------------------------------------ #
    # Shared preprocessing — identical for all strategies                 #
    # ------------------------------------------------------------------ #
    logger.info("Normalizing column names to lowercase")
    primary_key_cols = [pk.lower() for pk in primary_key_cols]
    compare_cols     = [col.lower() for col in compare_cols]

    source_df = source_df.select(
        [F.col(c).alias(c.lower()) for c in source_df.columns]
    )
    target_df = target_df.select(
        [F.col(c).alias(c.lower()) for c in target_df.columns]
    )

    all_cols = primary_key_cols + compare_cols

    # ── Auto-detect numeric rounding precision from both schemas ───────
    # Must happen AFTER lowercase aliasing (schema field names are now lower)
    # but BEFORE _normalise_df casts values to strings.
    precision_map = _build_precision_map(
        source_df.schema, target_df.schema, compare_cols,
    )

    source_norm = _normalise_df(source_df, all_cols, precision_map)
    target_norm = _normalise_df(target_df, all_cols, precision_map)

    source_norm = source_norm.cache()
    target_norm = target_norm.cache()
    # ── Force cache materialisation ───────────────────────────────────
    # Without this, lazy caching leads to eviction under memory pressure.
    logger.info("Materialising normalised DataFrames...")
    t_cache = time.time()
    _src_n = source_norm.count()
    _tgt_n = target_norm.count()
    logger.info(
        "Normalised: source=%d rows, target=%d rows (%.2fs)",
        _src_n, _tgt_n, time.time() - t_cache,
    )

    # [CACHING] Unpersist input DFs after normalization
    if source_df.is_cached:
        source_df.unpersist()
    if target_df.is_cached:
        target_df.unpersist()

    # ── Strategy dispatch (uncomment one) ─────────────────────────────
    # Normalised DFs are cached and stable (input caches freed above).
    # The finally block unpersists them after the strategy has materialised
    # [SERVERLESS] Unpersist disabled — uncomment for dedicated cluster
    # its diff_df (.cache() + .count()), so there is no memory leak.
    try:
        # Strategy A: Full outer join — simple, correct, slow for wide/large tables
        # return _compare_full_outer(source_norm, target_norm, primary_key_cols, compare_cols)

        # Strategy B: Two-phase hash — fast, handles 10M+ rows, tiny collision risk
        return _compare_hash(source_norm, target_norm, primary_key_cols, compare_cols)

        # Strategy C: Anti + inner joins — middle ground, supports broadcast
        # return _compare_anti_inner(source_norm, target_norm, primary_key_cols, compare_cols)
    finally:
        source_norm.unpersist()
        target_norm.unpersist()


# =========================================================================== #
# Strategy A — Full Outer Join (simple, guaranteed correct)                    #
# =========================================================================== #

def _compare_full_outer(
    source_norm: DataFrame,
    target_norm: DataFrame,
    pk_cols: List[str],
    compare_cols: List[str],
) -> Tuple[DataFrame, int, int, int, int]:
    """
    Strategy A: Full outer join on all columns.

    Pros : Simple, guaranteed correct, no hash collision risk.
    Cons : Shuffles ALL columns — slowest for wide tables.
           Spark CANNOT broadcast for full outer joins (always SortMergeJoin).
    Best for: ≤100K rows or narrow tables (≤20 columns).

    Spark actions: 1 (join materialisation + combined counts)
    """
    all_cols = pk_cols + compare_cols

    # ── Prefix + full outer join ──────────────────────────────────────
    source_prefixed = _prefix_df(source_norm, _SRC_PREFIX, all_cols)
    target_prefixed = _prefix_df(target_norm, _TGT_PREFIX, all_cols)

    join_condition = _build_join_condition(pk_cols)
    joined = source_prefixed.join(target_prefixed, on=join_condition, how="full")
    joined = joined.cache()

    # ── Presence flags ────────────────────────────────────────────────
    first_pk = pk_cols[0]
    src_pk_col = f"{_SRC_PREFIX}{first_pk}"
    tgt_pk_col = f"{_TGT_PREFIX}{first_pk}"

    joined = joined.withColumn(
        "_in_source", F.col(src_pk_col).isNotNull()
    ).withColumn(
        "_in_target", F.col(tgt_pk_col).isNotNull()
    )

    # ── Single aggregation for all counts ─────────────────────────────
    _in_src  = F.col("_in_source")
    _in_tgt  = F.col("_in_target")
    _in_both = _in_src & _in_tgt

    any_mismatch_expr = F.lit(False)
    for col in compare_cols:
        any_mismatch_expr = any_mismatch_expr | ~F.col(
            f"{_SRC_PREFIX}{col}"
        ).eqNullSafe(F.col(f"{_TGT_PREFIX}{col}"))

    _counts = joined.select(
        F.count(F.when(_in_src & ~_in_tgt, 1)).alias("missing"),
        F.count(F.when(~_in_src & _in_tgt, 1)).alias("extra"),
        F.count(F.when(_in_both, 1)).alias("both"),
        F.count(F.when(_in_both & any_mismatch_expr, 1)).alias("value_mismatch"),
    ).first()

    missing_count        = _counts["missing"]
    extra_count          = _counts["extra"]
    in_both_count        = _counts["both"]
    value_mismatch_count = _counts["value_mismatch"]
    matched_row_count    = in_both_count - value_mismatch_count

    # Derive source/target counts from join (no separate .count() calls)
    source_row_count = missing_count + in_both_count
    target_row_count = extra_count + in_both_count

    logger.info(
        "Comparing %d source rows against %d target rows on PK=%s",
        source_row_count, target_row_count, pk_cols,
    )
    logger.info(
        "Records: %d in source only | %d in target only | %d in both",
        missing_count, extra_count, in_both_count,
    )
    logger.info(
        "Matched rows: %d | Value mismatches: %d",
        matched_row_count, value_mismatch_count,
    )

    # ── Short-circuit if PASS ─────────────────────────────────────────
    if missing_count == 0 and extra_count == 0 and value_mismatch_count == 0:
        logger.info("No differences found — skipping diff DataFrame creation")
        joined.unpersist()
        return _empty_diff_df(), source_row_count, target_row_count, matched_row_count, 0

    # ── Lazy filters for three categories ─────────────────────────────
    missing_in_target = joined.filter(F.col("_in_source") & ~F.col("_in_target"))
    extra_in_target   = joined.filter(~F.col("_in_source") & F.col("_in_target"))
    in_both           = joined.filter(F.col("_in_source") & F.col("_in_target"))

    # Compute mismatch flags in a single select()
    flag_exprs = [
        (~F.col(f"{_SRC_PREFIX}{col}").eqNullSafe(
            F.col(f"{_TGT_PREFIX}{col}")
        )).alias(f"mismatch_{col}")
        for col in compare_cols
    ]
    mismatch_cols = [f"mismatch_{col}" for col in compare_cols]
    in_both = in_both.select("*", *flag_exprs)

    any_mismatch = F.lit(False)
    for flag in mismatch_cols:
        any_mismatch = any_mismatch | F.col(flag)

    value_mismatches = in_both.filter(any_mismatch)

    # ── Explode differences ───────────────────────────────────────────
    logger.info(
        "Creating diff report for %d missing, %d extra, %d value mismatches",
        missing_count, extra_count, value_mismatch_count,
    )

    diff_missing = _explode_missing_records(
        missing_in_target, pk_cols, compare_cols, "MISSING_IN_TARGET", missing_count,
    )
    diff_extra = _explode_missing_records(
        extra_in_target, pk_cols, compare_cols, "EXTRA_IN_TARGET", extra_count,
    )
    diff_values = _explode_differences(
        mismatched_rows=value_mismatches,
        primary_key_cols=pk_cols,
        compare_cols=compare_cols,
        mismatch_cols=mismatch_cols,
        value_mismatch_count=value_mismatch_count,
    )

    diff_df = diff_missing.union(diff_extra).union(diff_values)
    total_diff_count = missing_count + extra_count + value_mismatch_count

    # [STRATEGY A] Re-enable caching
    diff_df = diff_df.coalesce(2).cache()
    diff_df.count()  # force materialisation

    logger.info("Total differences to report: %d", total_diff_count)
    joined.unpersist()

    return diff_df, source_row_count, target_row_count, matched_row_count, total_diff_count


# =========================================================================== #
# Strategy B — Two-Phase Hash Comparison (fast, recommended for large tables)  #
# =========================================================================== #

def _compare_hash(
    source_norm: DataFrame,
    target_norm: DataFrame,
    pk_cols: List[str],
    compare_cols: List[str],
) -> Tuple[DataFrame, int, int, int, int]:
    """
    Strategy B: Two-phase hash comparison.

    Phase 1: Full outer join on PK + xxhash64 only (tiny shuffle).
             Single aggregation gives missing, extra, hash-mismatch, matched.
             For PASS case (0 diffs): STOP HERE.

    Phase 2: Collect mismatched rows to the driver, compare column-by-column
             in Python.  No Spark joins or shuffles.
             Time: ~3-5s regardless of dataset size.

    Pros : 10–20× less shuffle data.  Handles 10M+ rows in 4 GB.
    Cons : xxhash64 collision probability ~1 in 370,000 runs at 10M rows.
           A collision would cause a real mismatch to be silently skipped.
    Best for: Large tables (>100K rows) or wide tables (>30 columns).

    Spark actions: 1 (PASS) or 2 (FAIL with value mismatches)
    """

    all_cols = pk_cols + compare_cols

    # ══════════════════════════════════════════════════════════════════
    # Phase 1 — Slim PK + hash join
    # ══════════════════════════════════════════════════════════════════
    logger.info("phase 1 slim pk + hash join")
    start = time.time()
    # Compute xxhash64 over all compare columns on each normalised DF
    compare_col_refs = [F.col(c) for c in compare_cols]
    source_hashed = source_norm.withColumn("_row_hash", F.xxhash64(*compare_col_refs))
    target_hashed = target_norm.withColumn("_row_hash", F.xxhash64(*compare_col_refs))

    # Select only PK + hash (slim DFs — dramatically less shuffle data)
    slim_cols = pk_cols + ["_row_hash"]
    source_slim = _prefix_df(source_hashed.select(*slim_cols), _SRC_PREFIX, slim_cols)
    target_slim = _prefix_df(target_hashed.select(*slim_cols), _TGT_PREFIX, slim_cols)

    join_condition = _build_join_condition(pk_cols)
    slim_joined = source_slim.join(target_slim, on=join_condition, how="full")
    slim_joined = slim_joined.cache()

    # ── Presence flags + single aggregation ───────────────────────────
    first_pk = pk_cols[0]
    _in_src = F.col(f"{_SRC_PREFIX}{first_pk}").isNotNull()
    _in_tgt = F.col(f"{_TGT_PREFIX}{first_pk}").isNotNull()
    _in_both = _in_src & _in_tgt
    _hash_mismatch = ~F.col(f"{_SRC_PREFIX}_row_hash").eqNullSafe(
        F.col(f"{_TGT_PREFIX}_row_hash")
    )

    _counts = slim_joined.select(
        F.count(F.when(_in_src & ~_in_tgt, 1)).alias("missing"),
        F.count(F.when(~_in_src & _in_tgt, 1)).alias("extra"),
        F.count(F.when(_in_both, 1)).alias("both"),
        F.count(F.when(_in_both & _hash_mismatch, 1)).alias("hash_mismatch"),
    ).first()

    missing_count       = _counts["missing"]
    extra_count         = _counts["extra"]
    in_both_count       = _counts["both"]
    hash_mismatch_count = _counts["hash_mismatch"]
    matched_row_count   = in_both_count - hash_mismatch_count

    # Derive source/target counts from join
    source_row_count = missing_count + in_both_count
    target_row_count = extra_count + in_both_count

    logger.info(
        "Comparing %d source rows against %d target rows on PK=%s",
        source_row_count, target_row_count, pk_cols,
    )
    logger.info(
        "Records: %d in source only | %d in target only | %d in both",
        missing_count, extra_count, in_both_count,
    )
    logger.info(
        "Matched rows (hash): %d | Hash mismatches: %d",
        matched_row_count, hash_mismatch_count,
    )
    end = time.time() - start
    logger.info("phase 1 comparison time: %0.2fs", end)

    # ── Short-circuit if PASS ─────────────────────────────────────────
    if missing_count == 0 and extra_count == 0 and hash_mismatch_count == 0:
        logger.info("No differences found (hash PASS) — skipping detail phase")
        slim_joined.unpersist()
        return _empty_diff_df(), source_row_count, target_row_count, matched_row_count, 0

    total_diff_count = missing_count + extra_count + hash_mismatch_count

    # ══════════════════════════════════════════════════════════════════
    # Phase 2 — Collect mismatched rows, compare on driver
    # ══════════════════════════════════════════════════════════════════
    #
    # Phase 1 tells us HOW MANY rows differ (via hash), but not WHICH
    # COLUMNS or WHAT VALUES.  Phase 2 retrieves the actual data for
    # mismatched PKs and compares column-by-column in Python.
    #
    # All work happens on the driver — no Spark joins, shuffles, or
    # multi-stage DAG planning.  Time: ~3-5s (cache scan + Python dict
    # compare) regardless of dataset size.

    start = time.time()
    logger.info(
        "Phase 2 (collected): %d total diffs — driver-side comparison "
        "(%d missing, %d extra, %d value mismatches)",
        total_diff_count, missing_count, extra_count, hash_mismatch_count,
    )
    diff_df = _phase2_collected(
        source_norm, target_norm, slim_joined,
        pk_cols, compare_cols,
        missing_count, extra_count, hash_mismatch_count,
        _in_src, _in_tgt, _in_both, _hash_mismatch,
    )
    # Coalesce to 1 partition — the diff DF was created from driver-side
    # Python data (spark.createDataFrame), so it's tiny.  Without coalesce,
    # Spark parallelises it across spark.default.parallelism partitions,
    # each needing a Python worker.  Under memory pressure this causes
    # "Python worker failed to connect back" SocketTimeoutExceptions.
    diff_df = diff_df.coalesce(1).cache()

    logger.info("Total differences to report: %d", total_diff_count)
    slim_joined.unpersist()

    end = time.time() - start
    logger.info("Phase 2 time: %.2fs", end)
    return diff_df, source_row_count, target_row_count, matched_row_count, total_diff_count


# =========================================================================== #
# Strategy C — Anti + Inner Joins (middle ground, supports broadcast)          #
# =========================================================================== #

def _compare_anti_inner(
    source_norm: DataFrame,
    target_norm: DataFrame,
    pk_cols: List[str],
    compare_cols: List[str],
) -> Tuple[DataFrame, int, int, int, int]:
    """
    Strategy C: Decomposed anti + inner joins.

    Replaces 1 full outer join (no broadcast possible) with 3 simpler joins
    that all support broadcast:
        left_anti  — source PKs not in target (missing)
        left_anti  — target PKs not in source (extra)
        inner      — both sides, compare values

    Pros : No hash collision risk.  Supports broadcast for all joins.
    Cons : 3 joins instead of 1.  For the inner join, one full DF is
           broadcast — requires sufficient driver/executor memory.
    Best for: Medium tables (100K–1M rows) where broadcast fits in memory.

    Spark actions: 3 (missing count, extra count, inner join aggregation)
    """
    all_cols = pk_cols + compare_cols

    # ── Anti joins for missing / extra (broadcast PK-only for efficiency) ──
    target_pks = target_norm.select(*pk_cols)
    source_pks = source_norm.select(*pk_cols)

    missing_raw = source_norm.join(
        F.broadcast(target_pks), on=pk_cols, how="left_anti",
    )
    extra_raw = target_norm.join(
        F.broadcast(source_pks), on=pk_cols, how="left_anti",
    )

    # ── Inner join for value comparison (prefixed to disambiguate) ─────
    source_prefixed = _prefix_df(source_norm, _SRC_PREFIX, all_cols)
    target_prefixed = _prefix_df(target_norm, _TGT_PREFIX, all_cols)

    join_condition = _build_join_condition(pk_cols)
    in_both = source_prefixed.join(
        target_prefixed, on=join_condition, how="inner",
    )
    in_both = in_both.cache()

    # ── Counts ────────────────────────────────────────────────────────
    missing_count = missing_raw.count()
    extra_count   = extra_raw.count()

    any_mismatch_expr = F.lit(False)
    for col in compare_cols:
        any_mismatch_expr = any_mismatch_expr | ~F.col(
            f"{_SRC_PREFIX}{col}"
        ).eqNullSafe(F.col(f"{_TGT_PREFIX}{col}"))

    _counts = in_both.select(
        F.count(F.lit(1)).alias("both"),
        F.count(F.when(any_mismatch_expr, 1)).alias("value_mismatch"),
    ).first()

    in_both_count        = _counts["both"]
    value_mismatch_count = _counts["value_mismatch"]
    matched_row_count    = in_both_count - value_mismatch_count

    # Derive source/target counts
    source_row_count = missing_count + in_both_count
    target_row_count = extra_count + in_both_count

    logger.info(
        "Comparing %d source rows against %d target rows on PK=%s",
        source_row_count, target_row_count, pk_cols,
    )
    logger.info(
        "Records: %d in source only | %d in target only | %d in both",
        missing_count, extra_count, in_both_count,
    )
    logger.info(
        "Matched rows: %d | Value mismatches: %d",
        matched_row_count, value_mismatch_count,
    )

    # ── Short-circuit if PASS ─────────────────────────────────────────
    if missing_count == 0 and extra_count == 0 and value_mismatch_count == 0:
        logger.info("No differences found — skipping diff DataFrame creation")
        in_both.unpersist()
        return _empty_diff_df(), source_row_count, target_row_count, matched_row_count, 0

    # ── Explode missing / extra ───────────────────────────────────────
    logger.info(
        "Creating diff report for %d missing, %d extra, %d value mismatches",
        missing_count, extra_count, value_mismatch_count,
    )

    # Anti join results have unprefixed columns — prefix before explode
    missing_prefixed = _prefix_df(missing_raw, _SRC_PREFIX, all_cols)
    diff_missing = _explode_missing_records(
        missing_prefixed, pk_cols, compare_cols, "MISSING_IN_TARGET", missing_count,
    )

    extra_prefixed = _prefix_df(extra_raw, _TGT_PREFIX, all_cols)
    diff_extra = _explode_missing_records(
        extra_prefixed, pk_cols, compare_cols, "EXTRA_IN_TARGET", extra_count,
    )

    # ── Value mismatches from inner join ──────────────────────────────
    if value_mismatch_count > 0:
        flag_exprs = [
            (~F.col(f"{_SRC_PREFIX}{col}").eqNullSafe(
                F.col(f"{_TGT_PREFIX}{col}")
            )).alias(f"mismatch_{col}")
            for col in compare_cols
        ]
        mismatch_flag_cols = [f"mismatch_{col}" for col in compare_cols]
        in_both_flagged = in_both.select("*", *flag_exprs)

        any_mismatch = F.lit(False)
        for flag in mismatch_flag_cols:
            any_mismatch = any_mismatch | F.col(flag)

        value_mismatches = in_both_flagged.filter(any_mismatch)

        diff_values = _explode_differences(
            mismatched_rows=value_mismatches,
            primary_key_cols=pk_cols,
            compare_cols=compare_cols,
            mismatch_cols=mismatch_flag_cols,
            value_mismatch_count=value_mismatch_count,
        )
    else:
        diff_values = _empty_diff_df()

    diff_df = diff_missing.union(diff_extra).union(diff_values)
    total_diff_count = missing_count + extra_count + value_mismatch_count

    diff_df = diff_df.coalesce(2).cache()
    diff_df.count()  # force materialisation

    logger.info("Total differences to report: %d", total_diff_count)
    in_both.unpersist()

    return diff_df, source_row_count, target_row_count, matched_row_count, total_diff_count


# =========================================================================== #
# Phase 2 helper — Driver-side collected comparison (fast path)               #
# =========================================================================== #

def _phase2_collected(
    source_norm: DataFrame,
    target_norm: DataFrame,
    slim_joined: DataFrame,
    pk_cols: List[str],
    compare_cols: List[str],
    missing_count: int,
    extra_count: int,
    hash_mismatch_count: int,
    in_src_expr,
    in_tgt_expr,
    in_both_expr,
    hash_mismatch_expr,
) -> DataFrame:
    """
    Phase 2 — collect mismatched rows to the driver, compare in Python.

    Why Phase 2 exists
    ------------------
    Phase 1 uses xxhash64 to detect WHICH rows differ, but a hash is a
    single number — it cannot tell you which columns changed or what the
    old/new values are.  Phase 2 retrieves the actual row data for the
    mismatched PKs and compares column-by-column to build the diff report.

    Why collect instead of Spark joins?
    ------------------------------------
    A Spark-based Phase 2 (inner join → mismatch flags → array → explode →
    filter → union → cache → count) has ~30-40s of fixed overhead from
    query planning, task scheduling, and serialization — even for 1 row.

    The collected approach:
      1. Collects diff PK values from the cached slim_joined (~instant).
      2. For value mismatches: filters source_norm/target_norm by PK and
         collects to the driver (one cache scan per side, ~2-3s each).
      3. Compares column-by-column in pure Python (~microseconds).
      4. Returns a DataFrame created from the Python results (~instant).

    Time: ~3-5s for any realistic diff count.

    Memory safety: with 4 GB driver heap, safely handles 500K+ diff rows.
    In practice ETL validation rarely produces more than a few thousand
    diffs (a run with 500K+ mismatches means the ETL is fundamentally broken).
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    diffs: list = []

    # ── Missing in target (from slim_joined cache — instant) ──────────
    if missing_count > 0:
        missing_rows = slim_joined.filter(
            in_src_expr & ~in_tgt_expr
        ).select(
            *[F.col(f"{_SRC_PREFIX}{pk}").alias(pk) for pk in pk_cols]
        ).collect()
        for row in missing_rows:
            pk_val = "|".join(
                str(row[pk]) if row[pk] is not None else "" for pk in pk_cols
            )
            diffs.append((
                pk_val, "<ENTIRE_ROW>",
                "<PRESENT_IN_SOURCE>", "<MISSING_IN_TARGET>",
                "MISSING_IN_TARGET",
            ))

    # ── Extra in target (from slim_joined cache — instant) ────────────
    if extra_count > 0:
        extra_rows = slim_joined.filter(
            ~in_src_expr & in_tgt_expr
        ).select(
            *[F.col(f"{_TGT_PREFIX}{pk}").alias(pk) for pk in pk_cols]
        ).collect()
        for row in extra_rows:
            pk_val = "|".join(
                str(row[pk]) if row[pk] is not None else "" for pk in pk_cols
            )
            diffs.append((
                pk_val, "<ENTIRE_ROW>",
                "<EXTRA_IN_TARGET>", "<PRESENT_IN_TARGET>",
                "EXTRA_IN_TARGET",
            ))

    # ── Value mismatches (collect from cache + Python compare) ────────
    if hash_mismatch_count > 0:
        # Step 1: collect mismatched PK values from slim_joined (cached)
        mismatch_pk_rows = slim_joined.filter(
            in_both_expr & hash_mismatch_expr
        ).select(
            *[F.col(f"{_SRC_PREFIX}{pk}").alias(pk) for pk in pk_cols]
        ).collect()
        logger.info(
            "Collected %d mismatched PK values for driver-side comparison",
            len(mismatch_pk_rows),
        )

        # Step 2: collect source/target rows for mismatched PKs
        # Use broadcast join for both single and composite PK — avoids full
        # cache scan that .isin() triggers (scans every partition regardless).
        # Broadcast join lets Spark use BroadcastHashJoin, which is proportional
        # to the mismatch count rather than the full dataset size.
        all_cols = pk_cols + compare_cols
        if len(pk_cols) == 1:
            pk = pk_cols[0]
            pk_values = [row[pk] for row in mismatch_pk_rows]
            pk_df = spark.createDataFrame([(v,) for v in pk_values], [pk])
            src_rows = source_norm.join(
                F.broadcast(pk_df), on=pk, how="inner"
            ).select(*all_cols).collect()
            tgt_rows = target_norm.join(
                F.broadcast(pk_df), on=pk, how="inner"
            ).select(*all_cols).collect()
        else:
            # Composite PK — broadcast-join with tiny PK DataFrame
            pk_data = [
                {pk: row[pk] for pk in pk_cols}
                for row in mismatch_pk_rows
            ]
            pk_df = spark.createDataFrame(pk_data)
            src_rows = source_norm.join(
                F.broadcast(pk_df), on=pk_cols, how="inner"
            ).select(*all_cols).collect()
            tgt_rows = target_norm.join(
                F.broadcast(pk_df), on=pk_cols, how="inner"
            ).select(*all_cols).collect()

        # Step 3: build lookup dicts keyed by PK string
        def _make_pk_key(row_data):
            return "|".join(
                str(row_data[pk]) if row_data[pk] is not None else ""
                for pk in pk_cols
            )

        src_dict = {_make_pk_key(r): r.asDict() for r in src_rows}
        tgt_dict = {_make_pk_key(r): r.asDict() for r in tgt_rows}

        # Step 4: column-by-column comparison in Python
        for pk_str, src_data in src_dict.items():
            tgt_data = tgt_dict.get(pk_str)
            if tgt_data is None:
                continue  # should not happen for value mismatches
            for col in compare_cols:
                sv = src_data.get(col)
                tv = tgt_data.get(col)
                # null-safe: None == None → match (same as eqNullSafe)
                if sv is None and tv is None:
                    continue
                if sv != tv:
                    diffs.append((pk_str, col, sv, tv, "VALUE_MISMATCH"))

    # ── Create diff DataFrame from Python results ─────────────────────
    logger.info("Driver-side comparison produced %d diff rows", len(diffs))
    if diffs:
        diff_df = spark.createDataFrame(diffs, schema=_DIFF_SCHEMA).coalesce(1)
    else:
        diff_df = _empty_diff_df()

    return diff_df


# =========================================================================== #
# Shared helpers — new                                                         #
# =========================================================================== #

def _prefix_df(df: DataFrame, prefix: str, cols: List[str]) -> DataFrame:
    """
    Select *cols* from *df* and rename each with *prefix*.

    Example: _prefix_df(df, "src_", ["pk", "col1"]) → columns: src_pk, src_col1
    """
    return df.select([F.col(c).alias(f"{prefix}{c}") for c in cols])


def _empty_diff_df() -> DataFrame:
    """Return an empty DataFrame with the standard diff schema."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    return spark.createDataFrame([], _DIFF_SCHEMA)


# =========================================================================== #
# Shared helpers — unchanged                                                   #
# =========================================================================== #

def _normalise_df(df: DataFrame, cols: List[str], precision_map: dict = None) -> DataFrame:
    """
    Apply pre-comparison normalisation to selected columns:
      - Cast to string and trim whitespace
      - Normalize numeric columns based on schema type (5.2 == 5.20)
      - Round float/double columns to auto-detected precision to eliminate
        IEEE-754 representation noise (e.g. 128129.02 vs 128129.02000000002)
      - Lowercase boolean literals (true/false)

    Numeric normalization is only applied to columns with numeric schema types
    (IntegerType, LongType, FloatType, DoubleType, DecimalType) to avoid
    issues with alphanumeric columns.

    Parameters
    ----------
    precision_map : dict, optional
        {col_name: scale} mapping from ``_build_precision_map()``.
        Columns present in this map are rounded to *scale* decimal places
        after casting to double, before the final string cast.
        Columns absent from the map are normalised without rounding
        (integer types, string types, etc.).
    """
    from pyspark.sql.types import (
        IntegerType, LongType, FloatType, DoubleType,
        DecimalType, ShortType, ByteType
    )

    if precision_map is None:
        precision_map = {}

    # Identify numeric columns by schema type
    numeric_types = (IntegerType, LongType, FloatType, DoubleType,
                     DecimalType, ShortType, ByteType)
    # Separate integer-family (no decimals) from fractional numeric types
    integer_types = (IntegerType, LongType, ShortType, ByteType)

    numeric_cols = [
        field.name
        for field in df.schema.fields
        if field.name in cols and isinstance(field.dataType, numeric_types)
    ]
    integer_cols = {
        field.name
        for field in df.schema.fields
        if field.name in cols and isinstance(field.dataType, integer_types)
    }

    logger.info("Detected %d numeric columns for normalization: %s",
                len(numeric_cols), numeric_cols)
    if precision_map:
        applicable = {c: s for c, s in precision_map.items() if c in numeric_cols}
        if applicable:
            logger.info("Rounding applied to %d column(s): %s", len(applicable), applicable)

    # Build all normalised column expressions in one select()
    # Previously: a for-loop with df = df.withColumn(...) per column → N plan nodes.
    # Now: one select() with all expressions inline → 1 plan node.
    passthrough = [F.col(c) for c in df.columns if c not in cols]
    norm_exprs = []
    for col in cols:
        if col in numeric_cols:
            scale = precision_map.get(col)
            if scale is not None and col not in integer_cols:
                # Float / double / decimal with auto-detected precision →
                # round to *scale* decimal places before string cast.
                # This eliminates IEEE-754 noise like 128129.02000000002.
                norm_exprs.append(
                    F.when(F.col(col).isNull(), F.lit(None).cast(StringType()))
                    .otherwise(
                        F.round(F.col(col).cast("double"), scale).cast(StringType())
                    )
                    .alias(col)
                )
            else:
                # Integer type or no precision entry → cast without rounding
                norm_exprs.append(
                    F.when(F.col(col).isNull(), F.lit(None).cast(StringType()))
                    .otherwise(F.col(col).cast("double").cast(StringType()))
                    .alias(col)
                )
        else:
            norm_exprs.append(
                F.when(F.col(col).isNull(), F.lit(None).cast(StringType()))
                .otherwise(
                    F.regexp_replace(
                        F.trim(F.col(col).cast(StringType())),
                        r"(?i)^(true|false)$",
                        F.lower(F.col(col).cast(StringType())),
                    )
                )
                .alias(col)
            )
    return df.select(*passthrough, *norm_exprs)


def _build_join_condition(primary_key_cols: List[str]):
    """Build a null-safe join condition across all PK columns."""
    condition = None
    for col in primary_key_cols:
        src = F.col(f"{_SRC_PREFIX}{col}").cast(StringType())
        tgt = F.col(f"{_TGT_PREFIX}{col}").cast(StringType())
        clause = src.eqNullSafe(tgt)
        condition = clause if condition is None else condition & clause
    return condition


def _explode_missing_records(
    df: DataFrame,
    primary_key_cols: List[str],
    compare_cols: List[str],
    diff_type: str,
    row_count: int,
) -> DataFrame:
    """
    Convert missing/extra records into diff format.
    Creates ONE row per missing/extra record (not one per column).

    For missing in target: expected_value = '<ALL_COLUMNS>', actual_value = '<MISSING>'
    For extra in target: expected_value = '<EXTRA>', actual_value = '<ALL_COLUMNS>'

    row_count parameter replaces the df.rdd.isEmpty() call that was
    here previously. rdd.isEmpty() forces a full RDD scan on a DF that was
    already counted by the caller — passing the count avoids that extra action.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType as ST

    spark = SparkSession.getActiveSession()

    # Use caller-supplied count instead of rdd.isEmpty()
    if row_count == 0:
        return spark.createDataFrame([], _DIFF_SCHEMA)

    schema = StructType([
        StructField("primary_key_value", ST(), True),
        StructField("column_name", ST(), True),
        StructField("expected_value", ST(), True),
        StructField("actual_value", ST(), True),
        StructField("diff_type", ST(), True),
    ])

    # Determine which prefix to use based on diff_type
    prefix = _SRC_PREFIX if diff_type == "MISSING_IN_TARGET" else _TGT_PREFIX
    pk_concat = F.concat_ws("|", *[F.col(f"{prefix}{k}") for k in primary_key_cols])

    # Create single row per missing/extra record instead of one per column
    if diff_type == "MISSING_IN_TARGET":
        result = df.select(
            pk_concat.alias("primary_key_value"),
            F.lit("<ENTIRE_ROW>").alias("column_name"),
            F.lit("<PRESENT_IN_SOURCE>").alias("expected_value"),
            F.lit("<MISSING_IN_TARGET>").alias("actual_value"),
            F.lit(diff_type).alias("diff_type"),
        )
    else:  # EXTRA_IN_TARGET
        result = df.select(
            pk_concat.alias("primary_key_value"),
            F.lit("<ENTIRE_ROW>").alias("column_name"),
            F.lit("<EXTRA_IN_TARGET>").alias("expected_value"),
            F.lit("<PRESENT_IN_TARGET>").alias("actual_value"),
            F.lit(diff_type).alias("diff_type"),
        )

    return result


def _explode_differences(
    mismatched_rows: DataFrame,
    primary_key_cols: List[str],
    compare_cols: List[str],
    mismatch_cols: List[str],
    value_mismatch_count: int,  # passed in from caller — avoids rdd.isEmpty()
) -> DataFrame:
    """
    Convert wide-format mismatch rows into one record per differing cell.

    Returns a DataFrame with schema:
        primary_key_value | column_name | expected_value | actual_value | diff_type
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType as ST

    spark = SparkSession.getActiveSession()
    # Use caller-supplied count instead of rdd.isEmpty()
    if value_mismatch_count == 0:
        return spark.createDataFrame([], _DIFF_SCHEMA)

    schema = StructType([
        StructField("primary_key_value", ST(), True),
        StructField("column_name", ST(), True),
        StructField("expected_value", ST(), True),
        StructField("actual_value", ST(), True),
        StructField("diff_type", ST(), True),
    ])

    # Build a list of struct expressions: one per compare_col
    # Each struct is (pk_value, col_name, expected, actual, is_mismatch)
    struct_exprs = []
    pk_concat = F.concat_ws("|", *[F.col(f"{_SRC_PREFIX}{k}") for k in primary_key_cols])

    for col, flag in zip(compare_cols, mismatch_cols):
        struct_exprs.append(
            F.struct(
                pk_concat.alias("primary_key_value"),
                F.lit(col).alias("column_name"),
                F.col(f"{_SRC_PREFIX}{col}").alias("expected_value"),
                F.col(f"{_TGT_PREFIX}{col}").alias("actual_value"),
                F.col(flag).alias("is_mismatch"),
                F.lit("VALUE_MISMATCH").alias("diff_type"),
            )
        )

    # Combine all structs into one array column, then explode
    all_diffs = (
        mismatched_rows
        .withColumn("_diffs", F.array(*struct_exprs))
        .withColumn("_diff", F.explode(F.col("_diffs")))
        .filter(F.col("_diff.is_mismatch"))
        .select(
            F.col("_diff.primary_key_value"),
            F.col("_diff.column_name"),
            F.col("_diff.expected_value"),
            F.col("_diff.actual_value"),
            F.col("_diff.diff_type"),
        )
    )

    return all_diffs
