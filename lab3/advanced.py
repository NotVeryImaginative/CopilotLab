import typing
from pyspark.sql import DataFrame

def get_prod_table(self, selected_nomination_info: DataFrame, re_run_events: DataFrame = None, handler: typing.Any = None):
    # """
    # Build final product nomination information and enrich selected nominations with product-level flags and
    # aggregated brand/category lists.
    #
    # This function performs a sequence of validations, narrow projections and joins to produce:
    #  - final_nomination_prod_info: a combined DataFrame of featured / brand / category product associations
    #  - selected_nomination_info: the input selected nominations DataFrame augmented with brand/category and flags
    #  - nominations_brand_cat_df: one-row-per-parent-event aggregation of brands and categories
    #
    # High-level behavior
    #  - Validates that inputs are Spark DataFrames and that required column-mapping attributes exist on self.
    #  - Filters self.nomination_prod_table for the configured current week, allowed event types and non-null product level.
    #  - Joins filtered nomination-product records to the provided selected_nomination_info to produce a deduplicated
    #    line-item <-> product-group set.
    #  - Builds three product sets by joining a narrow projection of prod_table:
    #      * Featured products (fp): join on fis_week and prod_group -> flagged with column 'fp'
    #      * Brand products (brand): join on fis_week, brand, l10_code -> flagged with column 'brand'
    #      * Category products (cat): join on fis_week and prod_merch_l10_code -> flagged with column 'cat'
    #  - Safely outer-joins these sets on a minimal keyset to form final_nomination_prod_info.
    #  - Builds brand_cat_info (dominant brand / category per parent event) and a prod_flag indicating product presence.
    #  - Left-joins brand_cat_info and prod_flag into selected_nomination_info and calls self.get_fop_windows in a
    #    defensive manner.
    #  - Optionally joins re_run_events when re_run conditions are met.
    #  - Optionally persists interim and final tables if self.dynamic_tables contains names.
    #  - Produces nominations_brand_cat_df by aggregating brand/cat lists per parent event.
    #
    # Parameters
    #  - self: object
    #      Expected attributes (must be present on self):
    #        * spark: SparkSession (optional; a session will be created if missing)
    #        * current_week: value used to filter nomination_prod_table[FIS week column]
    #        * current_date: datetime-like (used in re_run handling)
    #        * re_run: 'true'/'false' string controlling conditional re_run behavior
    #        * re_run_day: int (weekday index) controlling when to apply re_run_events
    #        * nomination_prod_table: Spark DataFrame
    #        * prod_table: Spark DataFrame
    #        * nomination_prod_table_columns: dict mapping logical names -> actual column names
    #        * prod_table_columns: dict mapping logical names -> actual column names
    #        * nomination_table_columns: dict mapping logical names -> actual column names
    #        * event_table_columns: dict mapping logical names -> actual column names
    #        * dynamic_tables: dict of optional table names for persistence (keys used: 'final_nomination_prod_info',
    #          'interim_nominations_table', 'final_nominations_table')
    #        * get_fop_windows: callable that accepts and returns a DataFrame (optional)
    #  - selected_nomination_info: pyspark.sql.DataFrame
    #      Narrow selection of nomination-level columns is used; expected columns include at least:
    #        * parent_event_code (name resolved via event_table_columns mapping)
    #        * line_item_id
    #        * nomination_start_week / event_start_week (used for week joins if present)
    #        * segmentation descriptor column (name resolved via nomination_table_columns['seg_desc_list'])
    #  - re_run_events: pyspark.sql.DataFrame or None
    #      Optional DataFrame with parent_event_code and re_run flag used when self.re_run is enabled.
    #  - handler: typing.Any
    #      Present for API compatibility but not used by this implementation.
    #
    # Required mapping keys (examples of logical keys expected in the mapping dicts)
    #  - nomination_prod_table_columns must include: 'fis_week_id', 'event_type_code', 'prod_level_code', 'nomination_code', 'event_start_week'
    #  - prod_table_columns must include: 'prod_group_code' and typically 'brand_name', 'prod_merch_l10_code', 'prod_merch_l10_desc', 'fis_week_id', 'prod_desc'
    #  - nomination_table_columns must include: 'nomination_code', 'seg_desc_list'
    #  - event_table_columns must include: 'parent_event_code'
    #
    # Returns
    #  - tuple(final_nomination_prod_info, selected_nomination_info, nominations_brand_cat_df)
    #    * final_nomination_prod_info: pyspark.sql.DataFrame (combined featured/brand/category product associations).
    #      May be an empty DataFrame on error or best-effort result when joins fail. If dynamic_tables['final_nomination_prod_info']
    #      is provided the DataFrame will be persisted and re-read from the catalog.
    #    * selected_nomination_info: pyspark.sql.DataFrame (input selected_nomination_info augmented with brand/category
    #      columns and a 'prod_found_flag' column set to 1 when a matching product exists, otherwise 0). Persisted if
    #      dynamic_tables contains interim/final names.
    #    * nominations_brand_cat_df: pyspark.sql.DataFrame with columns (parent_event_code, brands, categories[, nomination_code])
    #      where brands/categories are comma-separated aggregated lists per parent event. Falls back to an empty DataFrame on failure.
    #
    # Side effects
    #  - May write Spark SQL tables if self.dynamic_tables contains names for 'final_nomination_prod_info',
    #    'interim_nominations_table', or 'final_nominations_table'.
    #  - Calls self.get_fop_windows(selected_nomination_info) if available; errors in that call are logged and suppressed.
    #  - Logs informational and error messages using self.log if present, otherwise prints to stdout.
    #
    # Error handling
    #  - Raises ValueError if provided inputs are not Spark DataFrames.
    #  - Raises KeyError if required mapping keys are missing.
    #  - Raises RuntimeError if required mapping attributes are absent on self.
    #  - Most runtime join/aggregation/persisting errors are caught, logged, and the function continues returning a best-effort result.
    #
    # Notes and implementation hints
    #  - The function intentionally projects narrow subsets of columns before joins to reduce shuffle and memory pressure.
    #  - All joins that can benefit from broadcasting use F.broadcast where appropriate.
    #  - The function accepts slightly flexible mapping dictionaries — as long as the required logical keys are present and
    #    actual column names match the DataFrame schemas the function will operate correctly.
    #  - If you adapt mappings or input DataFrames, ensure the keys listed under "Required mapping keys" are present and
    #    that column names referenced in selected_nomination_info match ones referenced by the passed mappings.
    #
    # Example (conceptual)
    #  - Provide self with properly populated mapping dicts, self.nomination_prod_table and self.prod_table DataFrames,
    #    and call get_prod_table(self, selected_nomination_info, re_run_events=None). The result will be a 3-tuple of DataFrames.
    # """
    """
    Build and return final_nomination_prod_info and selected_nomination_info.

    The function:
    - validates inputs and required mappings,
    - filters nomination_prod_table for current week and allowed event types,
    - joins with selected nomination info,
    - builds featured / brand / category product sets using narrow projections,
    - outer-joins them to produce final_nomination_prod_info,
    - persists interim/final tables if dynamic_tables entries are present,
    - performs downstream joins/flags and returns the final DataFrames.

    Returns:
        (final_nomination_prod_info: DataFrame or None, selected_nomination_info: DataFrame)
    """
    # safe logger
    logger = getattr(self, "log", None)
    def log_info(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    from pyspark.sql import functions as F
    from pyspark.sql import Window
    from pyspark.sql import SparkSession
    from pyspark.sql.utils import AnalysisException

    spark = getattr(self, "spark", None) or SparkSession.builder.getOrCreate()

    # --- helpers ---
    def ensure_df(obj, name):
        if obj is None or not hasattr(obj, "schema"):
            raise ValueError(f"{name} must be a Spark DataFrame")

    def get_col(mapping, key, context):
        if not isinstance(mapping, dict):
            raise ValueError(f"{context} mapping is missing or invalid")
        if key not in mapping:
            raise KeyError(f"Required key '{key}' not found in {context} mapping")
        return mapping[key]

    def safe_select(df: DataFrame, cols: typing.List[str]) -> DataFrame:
        # project only existing columns
        existing = [c for c in cols if c in df.columns]
        return df.select(*existing)

    # validate inputs
    ensure_df(selected_nomination_info, "selected_nomination_info")
    if re_run_events is not None:
        ensure_df(re_run_events, "re_run_events")

    log_info("get_prod_table: starts")

    # resolve column names from self mappings (raises clear errors if missing)
    try:
        nom_prod_cols_map = self.nomination_prod_table_columns
        prod_cols_map = self.prod_table_columns
        nom_table_cols_map = self.nomination_table_columns
        event_table_cols_map = self.event_table_columns
    except Exception as e:
        raise RuntimeError("Missing required column mapping attributes on self") from e

    # minimum required keys (adjust/add if your mappings differ)
    required_keys = {
        "nomination_prod_table": ["fis_week_id", "event_type_code", "prod_level_code", "nomination_code", "event_start_week"],
        "prod_table": ["prod_group_code", "brand_name", "prod_merch_l10_code", "prod_merch_l10_desc", "fis_week_id", "prod_desc"],
        "nomination_table": ["nomination_code", "seg_desc_list"],
        "event_table": ["parent_event_code"]
    }
    # validate required mapping keys exist
    for ctx, keys in required_keys.items():
        mapping = {
            "nomination_prod_table": nom_prod_cols_map,
            "prod_table": prod_cols_map,
            "nomination_table": nom_table_cols_map,
            "event_table": event_table_cols_map
        }[ctx]
        for k in keys:
            if k not in mapping:
                raise KeyError(f"missing mapping key '{k}' in {ctx} mappings")

    # alias DataFrames to avoid column collisions and keep narrow projections
    nomination_prod_table = getattr(self, "nomination_prod_table", None)
    prod_table = getattr(self, "prod_table", None)
    ensure_df(nomination_prod_table, "self.nomination_prod_table")
    ensure_df(prod_table, "self.prod_table")

    # --- Filter nomination_prod_table for current context ---
    try:
        fis_week_col = get_col(nom_prod_cols_map, "fis_week_id", "nomination_prod_table")
        event_type_col = get_col(nom_prod_cols_map, "event_type_code", "nomination_prod_table")
        prod_level_col = get_col(nom_prod_cols_map, "prod_level_code", "nomination_prod_table")
        nomination_code_col = get_col(nom_prod_cols_map, "nomination_code", "nomination_prod_table")
    except Exception as e:
        raise

    filtered_nom_prod = nomination_prod_table \
        .where(F.col(fis_week_col) == getattr(self, "current_week", None)) \
        .where(F.col(event_type_col).isin(['AAA'])) \
        .where(F.col(prod_level_col).isNotNull())

    # join with selected nomination info - pick only needed columns from both sides
    nom_sel_join_key = get_col(nom_table_cols_map, "nomination_code", "nomination_table")
    # project narrow
    left_cols = [prod_level_col, nomination_code_col]
    right_cols = [event_table_cols_map['parent_event_code'], 'line_item_id', nom_table_cols_map['seg_desc_list'], 'nomination_start_week', 'event_start_week']
    # keep only existing right cols
    right_cols = [c for c in right_cols if c in selected_nomination_info.columns]

    nomination_prod_info = filtered_nom_prod.select(*[c for c in left_cols if c in filtered_nom_prod.columns]) \
        .join(F.broadcast(selected_nomination_info.select(*right_cols, nom_sel_join_key) if nom_sel_join_key in selected_nomination_info.columns else selected_nomination_info),
              on=nom_sel_join_key,
              how='inner') \
        .withColumnRenamed(prod_level_col, prod_cols_map['prod_group_code'])

    # deduplicate line items
    dedup_cols = [
        event_table_cols_map['parent_event_code'],
        'line_item_id',
        nom_table_cols_map['seg_desc_list'],
        prod_cols_map['prod_group_code']
    ]
    dedup_cols = [c for c in dedup_cols if c is not None]
    selected_lineitem_prod = nomination_prod_info.dropDuplicates(dedup_cols)

    # prepare narrow product master
    prod_select_cols = [
        prod_cols_map['prod_group_code'],
        prod_cols_map.get('brand_name', 'brand_name'),
        prod_cols_map.get('prod_merch_l10_code', 'prod_merch_l10_code'),
        prod_cols_map.get('prod_merch_l10_desc', 'prod_merch_l10_desc'),
        prod_cols_map.get('fis_week_id', 'fis_week_id'),
        prod_cols_map.get('prod_desc', 'prod_desc')
    ]
    tmp_prod = prod_table.select(*[c for c in prod_select_cols if c in prod_table.columns]).dropDuplicates()

    # define safe join helper: join expr built from column names present in both sides
    def join_on_expr(left_df, right_df, left_col, right_col):
        if left_col not in left_df.columns or right_col not in right_df.columns:
            raise KeyError(f"Join columns not present: {left_col} or {right_col}")
        return left_df[left_col] == right_df[right_col]

    # FEATURED PRODUCTS: join tmp_prod with selected_lineitem_prod on fis_week / prod_group
    try:
        cond_fp = (
            join_on_expr(tmp_prod, selected_lineitem_prod, prod_cols_map.get('fis_week_id', 'fis_week_id'),
                         'nomination_start_week')
            & join_on_expr(tmp_prod, selected_lineitem_prod, prod_cols_map['prod_group_code'],
                           prod_cols_map['prod_group_code'])
        )
        sel_li_prod_alias = F.broadcast(selected_lineitem_prod)
        fp_df = tmp_prod.join(sel_li_prod_alias, on=cond_fp, how='inner') \
            .withColumn('fp', F.lit(True)) \
            .select(
                event_table_cols_map['parent_event_code'],
                'line_item_id',
                nom_table_cols_map['seg_desc_list'],
                prod_cols_map['prod_group_code'],
                F.col(prod_cols_map.get('prod_desc', 'prod_desc')).alias('product_desc'),
                'fp',
                'nomination_start_week'
            ).distinct()
    except Exception as e:
        log_info(f"Featured products build failed: {e}")
        fp_df = spark.createDataFrame([], schema=tmp_prod.limit(0).schema)  # empty

    # BRAND PRODUCTS
    try:
        # prepare small right side of join from selected_lineitem_prod
        brand_right = selected_lineitem_prod.select(
            event_table_cols_map['parent_event_code'],
            'line_item_id',
            nom_table_cols_map['seg_desc_list'],
            prod_cols_map.get('prod_merch_l10_code', 'prod_merch_l10_code'),
            prod_cols_map.get('brand_name', 'brand_name'),
            'nomination_start_week'
        ).distinct()
        cond_brand = (
            join_on_expr(tmp_prod, brand_right, prod_cols_map.get('fis_week_id', 'fis_week_id'), 'nomination_start_week')
            & (tmp_prod[prod_cols_map.get('brand_name', 'brand_name')] == brand_right[prod_cols_map.get('brand_name', 'brand_name')])
            & (tmp_prod[prod_cols_map.get('prod_merch_l10_code', 'prod_merch_l10_code')] == brand_right[prod_cols_map.get('prod_merch_l10_code', 'prod_merch_l10_code')])
        )
        brand_df = tmp_prod.join(F.broadcast(brand_right), on=cond_brand, how='inner') \
            .withColumn('brand', F.lit(True)) \
            .select(
                event_table_cols_map['parent_event_code'],
                'line_item_id',
                nom_table_cols_map['seg_desc_list'],
                prod_cols_map['prod_group_code'],
                F.col('brand_name').alias('brand_desc'),
                'brand',
                'nomination_start_week'
            ).distinct()
    except Exception as e:
        log_info(f"Brand products build failed: {e}")
        brand_df = spark.createDataFrame([], schema=tmp_prod.limit(0).schema)

    # CATEGORY PRODUCTS
    try:
        df_line_item_info = selected_lineitem_prod.select(
            event_table_cols_map['parent_event_code'],
            'line_item_id',
            nom_table_cols_map['seg_desc_list'],
            F.col(prod_cols_map.get('prod_merch_l10_code', 'prod_merch_l10_code')).alias('prod_merch_l10_code'),
            F.col('nomination_start_week').alias('fis_week_id')
        ).distinct()
        cat_df = tmp_prod.join(F.broadcast(df_line_item_info), on=['fis_week_id', 'prod_merch_l10_code'], how='inner') \
            .withColumn('cat', F.lit(True)) \
            .select(
                event_table_cols_map['parent_event_code'],
                'line_item_id',
                nom_table_cols_map['seg_desc_list'],
                prod_cols_map['prod_group_code'],
                F.col('prod_merch_l10_desc').alias('cat_desc'),
                'cat',
                F.col('fis_week_id').alias('nomination_start_week')
            ).distinct()
    except Exception as e:
        log_info(f"Category products build failed: {e}")
        cat_df = spark.createDataFrame([], schema=tmp_prod.limit(0).schema)

    # Combine the product sets using outer joins on the narrow key set
    join_keys = [
        event_table_cols_map['parent_event_code'],
        'line_item_id',
        nom_table_cols_map['seg_desc_list'],
        prod_cols_map['prod_group_code'],
        'nomination_start_week'
    ]
    # Ensure keys exist in DFs before joining; fallback to intersection
    def safe_outer_join(left: DataFrame, right: DataFrame, keys: typing.List[str]) -> DataFrame:
        common_keys = [k for k in keys if k in left.columns or k in right.columns]
        if not common_keys:
            # nothing to join on, return left unioned with right columns via crossJoin limited (avoid cross join). Return left.
            return left
        return left.join(F.broadcast(right), on=common_keys, how='outer')

    try:
        fp_brand = safe_outer_join(fp_df, brand_df, join_keys)
        final_nomination_prod_info = safe_outer_join(fp_brand, cat_df, join_keys)
    except Exception as e:
        log_info(f"Combining product frames failed: {e}")
        final_nomination_prod_info = fp_df  # best effort

    # persist final_nomination_prod_info if requested
    try:
        final_table_name = self.dynamic_tables.get('final_nomination_prod_info')
        if final_table_name:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {final_table_name}")
            except Exception:
                pass
            final_nomination_prod_info.write.mode("overwrite").saveAsTable(final_table_name)
            final_nomination_prod_info = spark.table(final_table_name)
    except Exception as e:
        log_info(f"Persisting final_nomination_prod_info failed: {e}")

    # Build brand_cat_info and prod_flag as in original flow but keep narrow projections
    try:
        prod_cols = [c for c in prod_cols_map.values() if c in prod_table.columns]
        brand_cat_info = nomination_prod_info.select(
            event_table_cols_map['parent_event_code'],
            prod_cols_map['prod_group_code'],
            'event_start_week'
        ).dropDuplicates().join(
            prod_table.select(*prod_cols),
            how='inner',
            on=[
                prod_table[prod_cols_map['fis_week_id']] == nomination_prod_info['event_start_week'],
                prod_table[prod_cols_map['prod_group_code']] == nomination_prod_info[prod_colsMap['prod_group_code']]
            ]
        ).select(
            event_table_cols_map['parent_event_code'],
            prod_cols_map.get('brand_name', 'brand_name'),
            prod_cols_map.get('prod_merch_l10_code', 'prod_merch_l10_code'),
            prod_cols_map.get('prod_merch_l10_desc', 'prod_merch_l10_desc')
        ).distinct()

        win_mode = Window.partitionBy(event_table_cols_map['parent_event_code']).orderBy(F.desc('l10_count'))
        brand_cat_info = brand_cat_info.groupBy(
            event_table_cols_map['parent_event_code'],
            prod_cols_map.get('brand_name', 'brand_name')
        ).agg(F.count(prod_cols_map.get('brand_name', 'brand_name')).alias('l10_count')) \
         .withColumn('rank', F.row_number().over(win_mode)) \
         .where(F.col('rank') == 1).drop('rank', 'l10_count')
    except Exception as e:
        log_info(f"brand_cat_info build failed: {e}")
        brand_cat_info = spark.createDataFrame([], schema=final_nomination_prod_info.limit(0).schema)

    prod_flag = nomination_prod_info.select(
        event_table_cols_map['parent_event_code'],
        'line_item_id'
    ).distinct().withColumn('prod_found_flag', F.lit(1))

    # Join brand_cat_info and prod_flag into selected_nomination_info (narrow right only columns)
    try:
        selected_nom_cols = selected_nomination_info.columns
        left_on = [event_table_cols_map['parent_event_code']]
        # perform left joins
        selected_nomination_info = selected_nomination_info.join(F.broadcast(brand_cat_info), on=left_on, how='left') \
            .join(F.broadcast(prod_flag), on=[event_table_cols_map['parent_event_code'], 'line_item_id'], how='left') \
            .fillna({'prod_found_flag': 0})
    except Exception as e:
        log_info(f"Joining brand_cat_info/prod_flag failed: {e}")

    # call downstream steps in a defensive way
    try:
        selected_nomination_info = self.get_fop_windows(selected_nomination_info)
    except Exception as e:
        log_info(f"get_fop_windows failed: {e}")

    # re_run handling
    try:
        if getattr(self, 're_run', 'false') == 'true' and getattr(self, 'current_date', None) is not None:
            if int(self.current_date.weekday()) == int(getattr(self, 're_run_day', 0)):
                if re_run_events is not None:
                    try:
                        selected_nomination_info = selected_nomination_info.join(
                            F.broadcast(re_run_events.select(event_table_cols_map['parent_event_code'], 're_run').distinct()),
                            on=event_table_cols_map['parent_event_code'],
                            how='left'
                        )
                    except Exception as e:
                        log_info(f"Joining re_run_events failed: {e}")
    except Exception as e:
        log_info(f"re_run handling failed: {e}")

    # persist interim and final nomination tables if requested
    try:
        interim_name = self.dynamic_tables.get('interim_nominations_table')
        if interim_name:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {interim_name}")
            except Exception:
                pass
            selected_nomination_info.write.mode("overwrite").saveAsTable(interim_name)
            selected_nomination_info = spark.table(interim_name)
    except Exception as e:
        log_info(f"Persisting interim nominations failed: {e}")

    try:
        final_name = self.dynamic_tables.get('final_nominations_table')
        if final_name:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {final_name}")
            except Exception:
                pass
            selected_nomination_info.write.mode("overwrite").saveAsTable(final_name)
            selected_nomination_info = spark.table(final_name)
    except Exception as e:
        log_info(f"Persisting final nominations failed: {e}")

    # --- NEW: build nominations dataframe with aggregated brands and categories ---
    try:
        # parent key
        parent_key = event_table_cols_map['parent_event_code']

        # ensure brand/category column names exist (fallback to mapping names)
        brand_col = prod_cols_map.get('brand_name', 'brand_name')
        cat_col = prod_cols_map.get('prod_merch_l10_desc', 'prod_merch_l10_desc')

        # aggregate brand_cat_info into one row per nomination with comma-separated lists
        if 'brand_cat_info' in locals() and brand_cat_info is not None:
            brand_cat_agg = brand_cat_info.groupBy(parent_key).agg(
                F.array_join(F.collect_set(F.col(brand_col)), ', ').alias('brands'),
                F.array_join(F.collect_set(F.col(cat_col)), ', ').alias('categories')
            )
        else:
            # empty schema safe fallback
            brand_cat_agg = spark.createDataFrame([], schema='parent_event_code string, brands string, categories string')

        # base nominations (distinct parent_event_code) from selected_nomination_info
        noms_base = selected_nomination_info.select(parent_key).dropDuplicates()

        # left join aggregated brand/category info and fill missing with blank
        nominations_brand_cat_df = noms_base.join(
            F.broadcast(brand_cat_agg),
            on=parent_key,
            how='left'
        ).fillna({'brands': '', 'categories': ''})

        # optional: bring through other nomination-level columns if present (e.g., nomination_code)
        if 'nomination_code' in selected_nomination_info.columns:
            noms_extra = selected_nomination_info.select(parent_key, 'nomination_code').dropDuplicates([parent_key])
            nominations_brand_cat_df = nominations_brand_cat_df.join(noms_extra, on=parent_key, how='left')
    except Exception as e:
        log_info(f"Building nominations_brand_cat_df failed: {e}")
        # fallback to minimal empty DataFrame with expected columns
        nominations_brand_cat_df = spark.createDataFrame([], schema='parent_event_code string, brands string, categories string')

    log_info("get_prod_table: ends")
    # return final nomination products, selected nominations and the new nominations -> brands/categories DF
    return final_nomination_prod_info, selected_nomination_info, nominations_brand_cat_df
