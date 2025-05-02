# Databricks notebook source
# List all files in the Silver layer to verify

dbutils.fs.ls("/mnt/silver")

# COMMAND ----------

# Define the folder path for all CSV files in the Bronze layer
bronze_folder_path = "/mnt/bronze/"

# Read all CSV files from the folder into separate DataFrames
df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{bronze_folder_path}Customer.csv")
df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{bronze_folder_path}Inventory.csv")
df3 = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{bronze_folder_path}Product.csv")
df4 = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{bronze_folder_path}Sales.csv")
df5 = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{bronze_folder_path}Stores.csv")

# Display each DataFrame as a separate table
display(df1)  # Display table 1
display(df2)  # Display table 2
display(df3)  # Display table 3
display(df4)  # Display table 4
display(df5)  # Display table 5

# COMMAND ----------

from pyspark.sql.functions import to_date, col

# Assuming sale_date is currently a string or timestamp
df4 = df4.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

# If you want to check the result
display(df4)


# COMMAND ----------

# Function to convert to snake_case
def to_snake_case(name):
    return name.lower().replace(' ', '_').replace('-', '_')

# Rename all columns to snake_case
df5 = df5.toDF(*[to_snake_case(col_name) for col_name in df5.columns])

# Display the table nicely
display(df5)


# COMMAND ----------

# Joining the DataFrames based on the provided relationships
retail_df = df4.alias("df4") \
    .join(df1.alias("df1"), df4.customer_id == df1.customer_id, "inner") \
    .join(df2.alias("df2"), df4.product_id == df2.product_id, "inner") \
    .join(df3.alias("df3"), df4.product_id == df3.product_id, "inner") \
    .join(df5.alias("df5"), df4.store_id == df5.store_id, "inner")

# Select relevant columns, including additional ones you want to keep
retail_df = retail_df.select(
    # Columns from Sales table (df4)
    "df4.sale_id", "df4.quantity_sold", "df4.unit_price", "df4.payment_method", "df4.discount_applied", "df4.total_price", "df4.sale_date",  # Additional column from Sales table
    
    # Columns from Customer table (df1)
    "df1.customer_name", "df1.gender", "df1.loyalty_points", "df1.age", "df1.email", "df1.phone_number", 
    "df1.address", "df1.city", "df1.state", "df1.country",  # Additional columns from Customer table
    
    # Columns from Product table (df3)
    "df3.product_name", "df3.product_id", "df3.category", "df3.brand", "df3.cost_price", "df3.selling_price",
    "df3.supplier_id",  # Additional column from Product table
    
    # Columns from Stores table (df5)
    "df5.store_name", "df5.store_id", "df5.location", "df5.manager_name", "df5.opening_date", "df5.store_type",  # Additional columns from Stores table
    
    # Columns from Inventory table (df2)
    "df2.inventory_id", "df2.stock_quantity", "df2.last_restock_date", "df2.supplier_name" # Additional columns from Inventory table
)

# Write the resulting DataFrame to the Silver container
retail_df.write.mode("overwrite").parquet("/mnt/silver/Retail_Dataset.parquet")


display(retail_df)


# COMMAND ----------

target_path = "/mnt/silver/Retail_Dataset.parquet"

def clean_and_rename_parquet(target_path, new_name):
    # List all files in the target path
    files = dbutils.fs.ls(target_path)
    
    # Iterate through the files to remove unwanted files like _SUCCESS, .committed, .started
    for file in files:
        if file.name.startswith("_SUCCESS") or file.name.startswith("_committed") or file.name.startswith("_started"):
            dbutils.fs.rm(file.path, True)  # Remove unwanted files
            print(f"Removed {file.name}")
    
    # Now, find the part- file and rename it
    for file in files:
        if file.name.startswith("part-") and file.name.endswith(".parquet"):
            # Get the source path for the part file
            source_path = file.path
            
            # Define the destination path with the new name
            destination_path = target_path + "/" + new_name
            
            # Move (rename) the file
            dbutils.fs.mv(source_path, destination_path)
            
            # Print confirmation
            print(f"Renamed {file.name} to {new_name}")
            break  # Exit loop after renaming (since there should only be one part file)


# Call the function to clean up and rename the file
clean_and_rename_parquet(target_path, "Retail_Dataset.parquet")
