# Databricks notebook source
dbutils.fs.rm('dbfs:/user/hive/warehouse/customers_target', True)
spark.sql('drop table if exists customers_target')

# COMMAND ----------

from pyspark.sql.functions import col, pandas_udf, split, concat_ws, lit, lower, current_timestamp
import random
import pandas as pd
from datetime import datetime

# Get the current timestamp for the filename
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# Number of unique customers
num_customers = 10

# List of common first and last names
first_names = ["John", "Jane", "Michael", "Emily", "David", "Emma", "Chris", "Olivia", "Daniel", "Sophia"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]

# Lists of street names, cities, and ZIP codes
street_names = ["Maple St", "Oak St", "Pine St", "Cedar St", "Elm St", "Washington Ave", "2nd Ave", "3rd St", "4th St", "5th Ave"]
cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
zip_codes = ["10001", "90001", "60601", "77001", "85001", "19101", "78201", "92101", "75201", "95101"]

# Function to generate a random customer name
def random_customer_name():
    return f"{random.choice(first_names)} {random.choice(last_names)}"

# Function to generate a random address
def random_address():
    street_number = random.randint(100, 9999)
    return f"{street_number} {random.choice(street_names)}, {random.choice(cities)}, {random.choice(zip_codes)}"

# Pandas UDF to apply the random_customer_name function to a DataFrame
@pandas_udf("string")
def generate_customer_names(id_series: pd.Series) -> pd.Series:
    return pd.Series([random_customer_name() for _ in id_series])

# Pandas UDF to apply the random_address function to a DataFrame
@pandas_udf("string")
def generate_addresses(id_series: pd.Series) -> pd.Series:
    return pd.Series([random_address() for _ in id_series])

# Function to generate random phone numbers
def random_phone_number():
    return f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

# Generate the customer data
customer_df = (
    spark.range(num_customers)
    .withColumn("customer_id", col("id").cast("int"))
    .withColumn("customer_name", generate_customer_names(col("id")))
    .withColumn("first_name", split(col("customer_name"), " ").getItem(0))
    .withColumn("last_name", split(col("customer_name"), " ").getItem(1))
    .withColumn("email", concat_ws("@", concat_ws(".", lower(col("first_name")), lower(col("last_name"))), concat_ws(".", lower(col("last_name")), lit("com"))))
    .withColumn("phone_number", lit(random_phone_number()))  # For simplicity, this will be the same number
    .withColumn("address", generate_addresses(col("id")))
    .withColumn("timestamp", current_timestamp())
    .drop("customer_name")  # Remove intermediate columns
    .drop("id")
)

customerdf = customer_df.dropDuplicates(["customer_id", "first_name", "last_name"])

# Define the Delta table output path
delta_output_path = f"dbfs:/mnt/data/customers/customers_{timestamp}"

# Create an empty Delta table with CDF enabled BEFORE inserting any data
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS customers
    (
        customer_id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone_number STRING,
        address STRING,
        timestamp TIMESTAMP
    )
    USING delta
    LOCATION '{delta_output_path}'
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Insert the customer data with schema merging enabled
customerdf.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("customers")

print(f"Customer data saved to Delta table at {delta_output_path}")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customers_target AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   current_timestamp() AS inserted_time,
# MAGIC   current_timestamp() AS updated_time,
# MAGIC   'inserted' AS operation,
# MAGIC   false AS is_deleted
# MAGIC FROM customers;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC update customers 
# MAGIC SET last_name = 'Bachore'
# MAGIC where last_name = 'Davis'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from customers 
# MAGIC where last_name in ('Rodriguez')

# COMMAND ----------

# MAGIC %sql 
# MAGIC RESTORE TABLE customers_target TO VERSION AS OF 0;

# COMMAND ----------

spark.sql("""
insert into customers(customer_id, first_name, last_name, email, phone_number, address, `timestamp`)
values
(10, 'Mulu', 'Bachore', 'zbela@gmail.com', '+1-555-555-5555', '9800 Garden Road, Columbia, MD', current_timestamp()),
(11, 'Zena', 'Bachore', 'zbela@gmail.com', '+1-555-555-5555', '9800 Garden Road, Columbia, MD', current_timestamp()),
(12, 'Yonas', 'Bachore', 'zbela@gmail.com', '+1-555-555-5555', '9800 Garden Road, Columbia, MD', current_timestamp())
""")

# COMMAND ----------

from datetime import datetime

# Assign the current timestamp to job_beginning_time
job_beginning_now = datetime.now()

# Convert job_beginning_time to a string in the desired format
job_beginning_time = job_beginning_now.strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# Get the schema of the customers table
schema = spark.sql("DESCRIBE TABLE customers").collect()

# Exclude the 'timestamp' column
# excluded_columns = {'timestamp'}
column_list = [row['col_name'] for row in schema]

# Print the final list of columns
# print(column_list)

# # Capture the current timestamp
# job_beginning_time = spark.sql("SELECT current_timestamp() AS job_beginning_time").collect()[0]['job_beginning_time']
# # print(job_beginning_time)

# Load the customers table into a DataFrame
customers_df = spark.table("customers").select(*column_list)

# Load the customers_target table into a DataFrame
customers_target_df = spark.table("customers_target").select(*column_list, "inserted_time", "updated_time", "is_deleted", "operation")

# Identify rows to update
update_condition = " OR ".join([f"source.{col} != target.{col}" for col in column_list if col not in ['customer_id']])
update_df = customers_df.alias("source").join(customers_target_df.alias("target"), "customer_id") \
    .filter(update_condition)

# Identify rows to insert
insert_df = customers_df.alias("source").join(customers_target_df.alias("target"), "customer_id", "left_anti")

# Identify rows to flag as deleted
delete_df = customers_target_df.alias("target").join(customers_df.alias("source"), "customer_id", "left_anti")

# Update rows
update_df = (update_df
             .withColumn("updated_time", lit(job_beginning_time))
             .withColumn("is_deleted", lit(False)))
update_expr = {
    f"target.{col}": f"source.{col}" 
    for col in column_list 
    if col not in {'customer_id'}
}

update_expr["target.inserted_time"] = "target.inserted_time" # do not change existing inserted_time value.
# update_expr["target.updated_time"] = "source.updated_time"
update_expr["target.updated_time"] = job_beginning_time
update_expr["target.is_deleted"] = 'false'
update_expr["target.operation"] = 'updated'

# Assuming update_df is already defined
update_df = update_df.selectExpr(
    *[f"source.{col} AS {col}" for col in column_list],
    "inserted_time",
    "updated_time",
    "is_deleted"
).withColumn("operation", lit("updated"))

update_df.createOrReplaceTempView("update_view")

spark.sql("""
MERGE INTO customers_target AS target
USING update_view AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET *
""")

# Insert rows
insert_df = insert_df.withColumn("inserted_time", lit(job_beginning_time)) \
                     .withColumn("updated_time", lit(job_beginning_time)) \
                     .withColumn("is_deleted", lit(False)) \
                     .withColumn("operation", lit("inserted"))

insert_df.selectExpr(*[f"source.{col} AS {col}" for col in column_list], "inserted_time", "updated_time", "is_deleted", "operation") \
         .createOrReplaceTempView("insert_view")

spark.sql("""
MERGE INTO customers_target AS target
USING insert_view AS source
ON target.customer_id = source.customer_id
WHEN NOT MATCHED BY TARGET THEN
  INSERT *
""")

# Flag rows as deleted and create delete_view
delete_df = (delete_df.withColumn("updated_time", lit(job_beginning_time))
                     .withColumn("is_deleted", lit(True))
                     .withColumn("operation", lit("deleted")))

# Ensure all columns in delete_view match customers_target schema
delete_df.selectExpr(
    *[f"{col}" for col in column_list],  # Customer-specific columns
    "inserted_time",                     # Existing column
    "updated_time",                      # Updated column
    "is_deleted",
    "operation"                         # New column
).createOrReplaceTempView("delete_view")

# Perform the MERGE operation to flag rows as deleted
# Use the string in the SQL query
spark.sql(f"""
MERGE INTO customers_target AS target
USING delete_view AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND target.is_deleted = false THEN
  UPDATE SET
    target.updated_time = '{job_beginning_time}',
    target.is_deleted = true,
    target.operation = 'deleted'
""")

# COMMAND ----------

display(spark.sql(f"""
select * from customers_target
where updated_time >= '{job_beginning_time}'
"""))

# COMMAND ----------


