# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "94406eca-e065-4111-b470-a2b78f10b93d",
# META       "default_lakehouse_name": "LH_02",
# META       "default_lakehouse_workspace_id": "15e5d01d-3433-4715-84cb-11f798721e0f",
# META       "known_lakehouses": [
# META         {
# META           "id": "94406eca-e065-4111-b470-a2b78f10b93d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# table_name = "sales"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# Read the new sales data

df = spark.read.format("csv").option("header", "true").load("Files/data_sales/*.csv")

## Add month and year columns

df1 = df.withColumn("Year",  year(col("OrderDate"))+1).withColumn("Month", month(col("OrderDate")))

# Derive FirstName and LastName columns

df2 = df1.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

# Filter and reorder columns
df3 = df2["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "EmailAddress", "Item", "Quantity", "UnitPrice", "TaxAmount"]

# Load the data into a table

df3.write.format("delta").mode("overwrite").saveAsTable(table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
