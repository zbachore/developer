# Databricks notebook source
from pyspark.sql.functions import lit, current_timestamp

def merge_into_stage(source_df, primary_key, source_table, target_table):
    """
    Merges data from a source DataFrame into a Delta target table using audit columns.

    This function performs the following steps:
    1. Adds audit columns (`is_deleted`, `row_inserted_time`, `row_updated_time`) to the source data.
    2. Identifies new or updated records by comparing with the source data with the target table.
    3. Performs an upsert (MERGE) into the target Delta table.
    4. Identifies deleted rows (present in target but not in source) and:
        - Inserts them into a deleted rows audit table. Expects the deleted_rows table to be present.
        - Physically deletes them from the target table.

    Parameters:
        source_df (DataFrame): Dataframe with curated source data that is ready to be pushed to target.
        primary_key (str): The column used as a unique identifier for matching records.
        source_table (str): The source table name in 'catalog.schema.table' format.
        target_table (str): The target table name in 'catalog.schema.table' format.

    Raises:
        Exception: If any error occurs during processing.
    """

    try:
        # Note: Multi-table transactions are not supported as of now. The try block is used here only to catch exceptions here.
        # Step 1: Get current timestamp once
        ts = spark.sql("SELECT current_timestamp() AS now").collect()[0]["now"]
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # need to convert to string for sql.
    

        source_df_with_audit = (
            source_df
            .withColumn('is_deleted', lit(0))
            .withColumn('row_inserted_time', lit(ts))
            .withColumn('row_updated_time', lit(ts))
        )

        # Create source temp view for EXCEPT logic based on source_df (audit columns are not added yet)
        table_name = source_table.split(".")[-1]
        source_view = f"{table_name}_view"
        source_df.createOrReplaceTempView(source_view)

        # Get common columns to use in the EXCEPT logic (original columns only)
        target_columns = spark.table(target_table).columns
        # common_columns = list(set(source_df.columns) & set(target_columns))
        common_columns = [col for col in target_columns if col in source_df.columns]
        common_columns_list = ", ".join(common_columns)
        common_columns_list_for_deletes = ", ".join([f"t.{col}" for col in common_columns])
        # print(common_columns_list_for_deletes) # troubleshooting


        # Identify new and updated rows
        merge_view = f"{table_name}_merge_view" # we want to name the view after the table to avoid name collisions
        spark.sql(f"""
            SELECT {common_columns_list} FROM {source_view}
            EXCEPT
            SELECT {common_columns_list} FROM {target_table}
        """).createOrReplaceTempView(merge_view)

        # Add audit fields to merge_view in preparation to merge to target table
        merge_df = spark.table(merge_view)
        merge_df_with_audit = merge_df
        merge_df_with_audit = (
            merge_df
            .withColumn('is_deleted', lit(0))
            .withColumn('row_inserted_time', lit(ts))
            .withColumn('row_updated_time', lit(ts))
        ) 
        merge_df_with_audit.createOrReplaceTempView(merge_view)

        # Get columns for insert logic
        insert_columns = merge_df_with_audit.columns
        insert_values = [f"source.{col}" for col in insert_columns]

        excluded = {primary_key, 'is_deleted', 'row_inserted_time', 'row_updated_time'}
        updatable_columns = [col for col in insert_columns if col not in excluded]

        update_set_clause = ",\n  ".join(
            [f"target.{col} = source.{col}" for col in updatable_columns] + [
                "target.is_deleted = 0",
                f"target.row_updated_time = cast('{ts_str}' AS TIMESTAMP)"
            ]
        )

        # Run final MERGE
        print(f"Merging {merge_view} into {target_table}.")
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {merge_view} AS source
        ON source.{primary_key} = target.{primary_key}
        WHEN MATCHED THEN
        UPDATE SET
        {update_set_clause}
        WHEN NOT MATCHED THEN
        INSERT ({", ".join(insert_columns)})
        VALUES ({", ".join(insert_values)})
        """
        spark.sql(merge_sql)

        # Delete Handling

        # Identify deleted rows - that are in target but not in source
        deleted_df_without_all_columns = spark.sql(f"""
            SELECT {common_columns_list_for_deletes}, 1 AS is_deleted, t.row_inserted_time  -- keep existing row_inserted_time value
            FROM {target_table} t
            LEFT ANTI JOIN {source_table} s
            ON t.{primary_key} = s.{primary_key}
        """)
        deleted_df = deleted_df_without_all_columns.withColumn('row_updated_time', lit(ts))
        # print(deleted_df.columns) # troubleshooting

        # Check if any rows were deleted
        if not deleted_df.take(1):
            print(f"No deleted rows found for {target_table}. Skipping deletion sync.")
        else:
            # Step 3: Create temp view for deleted rows
            deleted_rows_view = f"{table_name}_deleted_rows_view"
            deleted_df.createOrReplaceTempView(deleted_rows_view)
            print(f"Deleting rows from {target_table}.")

            # Step 4: Create deleted_rows tables in advance.
            deleted_rows_table = f"{target_table}_deleted_rows"
            # spark.sql(f"""
            #     CREATE TABLE IF NOT EXISTS {deleted_rows_table}
            #     USING DELTA
            #     AS SELECT * FROM {target_table}
            #     WHERE 1 = 0
            # """)
            # spark.sql(f"""select * from {deleted_rows_view}""").display() # troubleshooting
            # Step 5: Insert only new deleted rows into deleted table
            print(f"Inserting deleted rows into {deleted_rows_table}.")
            spark.sql(f"""
                INSERT INTO {deleted_rows_table}
                SELECT  d.*
                FROM {deleted_rows_view} d
            """)

            print(f"Deleting deleted rows from {target_table}.")
            spark.sql(f"""
                MERGE INTO {target_table} t
                USING {deleted_rows_table} d 
                ON t.{primary_key} = d.{primary_key}
                WHEN MATCHED THEN
                DELETE
            
                      """)
    except Exception as e:
        print(f"Error during merge_to_stage: {e}")
        raise

# COMMAND ----------


