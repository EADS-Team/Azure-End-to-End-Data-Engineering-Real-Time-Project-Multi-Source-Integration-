# Databricks notebook source
# List all files in the gold layer to verify

dbutils.fs.ls("/mnt/gold")

# COMMAND ----------

# Define the path to the Retail_Dataset.parquet in the Silver container
silver_path = "dbfs:/mnt/silver/Retail_Dataset.parquet"

# Read the Parquet file from the Silver container into a DataFrame
silver_df = spark.read.parquet(silver_path)

# Display the DataFrame (optional, for verification)
display(silver_df)


# COMMAND ----------

# Define facts (numerical measures)
facts_df = silver_df.select(
    "sale_id", "quantity_sold", "unit_price", "total_price", 
    "discount_applied", "payment_method", "sale_date"
)

# Define dimensions (categorical attributes)
customer_dim_df = silver_df.select(
    "customer_name", "gender", "loyalty_points", "email", 
    "phone_number", "address", "city", "state", "country"
)

product_dim_df = silver_df.select(
    "product_id", "product_name", "category", "brand", "cost_price", "selling_price"
)

store_dim_df = silver_df.select(
    "store_id", "store_name", "location", "manager_name", "opening_date", "store_type"
)

inventory_dim_df = silver_df.select(
    "inventory_id", "stock_quantity", "last_restock_date", "supplier_name"
)

# Function to remove all files in a folder (if any exist)
def remove_existing_files(folder_path):
    # List files in the folder
    files = dbutils.fs.ls(folder_path)
    
    # Remove all files in the folder
    for file in files:
        dbutils.fs.rm(file.path, True)
        print(f"Removed {file.name}")

# Define the path for the Gold container
gold_folder = "dbfs:/mnt/gold/"

# First, remove all existing files in the Gold container
remove_existing_files(gold_folder)

# Write the Facts to the Gold container
facts_df.write.mode("overwrite").parquet(gold_folder + "fact_table.parquet")

# Write the Dimensions to the Gold container
customer_dim_df.write.mode("overwrite").parquet(gold_folder + "dim_customer.parquet")
product_dim_df.write.mode("overwrite").parquet(gold_folder + "dim_product.parquet")
store_dim_df.write.mode("overwrite").parquet(gold_folder + "dim_store.parquet")
inventory_dim_df.write.mode("overwrite").parquet(gold_folder + "dim_inventory.parquet")

# Verify the files are written in the Gold container
dbutils.fs.ls(gold_folder)

# COMMAND ----------

# Function to remove unwanted files and rename the final part file
def remove_and_rename_files(folder_path, new_name):
    # List files in the folder
    files = dbutils.fs.ls(folder_path)
    
    # Remove _SUCCESS, _committed, _started, and part files
    for file in files:
        if file.name.startswith(('_SUCCESS', '_committed', '_started')):
            dbutils.fs.rm(file.path, True)
            print(f"Removed {file.name}")
    
    # List files again to find the final part file (which starts with "part-")
    files = dbutils.fs.ls(folder_path)
    for file in files:
        if file.name.startswith("part-"):
            source_path = file.path
            destination_path = folder_path + "/" + new_name
            dbutils.fs.mv(source_path, destination_path)
            print(f"Renamed {file.name} to {new_name}")

# Define the path for the Gold container
gold_folder = "dbfs:/mnt/gold/"

# Remove unwanted files and rename the fact table file
remove_and_rename_files(gold_folder + "fact_table.parquet", "fact_table.parquet")

# Remove unwanted files and rename the customer dimension file
remove_and_rename_files(gold_folder + "dim_customer.parquet", "dim_customer.parquet")

# Remove unwanted files and rename the product dimension file
remove_and_rename_files(gold_folder + "dim_product.parquet", "dim_product.parquet")

# Remove unwanted files and rename the store dimension file
remove_and_rename_files(gold_folder + "dim_store.parquet", "dim_store.parquet")

# Remove unwanted files and rename the inventory dimension file
remove_and_rename_files(gold_folder + "dim_inventory.parquet", "dim_inventory.parquet")

# Verify the files are renamed and the unwanted files are removed
dbutils.fs.ls(gold_folder)