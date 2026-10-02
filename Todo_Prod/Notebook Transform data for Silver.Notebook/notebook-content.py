# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "84f4b6fa-e71e-485c-8112-ba011ad3f65e",
# META       "default_lakehouse_name": "LH_Medallion",
# META       "default_lakehouse_workspace_id": "15e5d01d-3433-4715-84cb-11f798721e0f",
# META       "known_lakehouses": [
# META         {
# META           "id": "84f4b6fa-e71e-485c-8112-ba011ad3f65e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Transform data and load to silver Delta table


# CELL ********************

from pyspark.sql.types import *

# Create the schema for the table

orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()  ),
    StructField("SalesOrderLineNumber", IntegerType() ),
    StructField("OrderDate",  DateType()),
    StructField("CustomerName",  StringType()),
    StructField("Email",  StringType()),
    StructField("Item",  StringType()),
    StructField("Quantity",  IntegerType()),
    StructField("UnitPrice", FloatType()  ),
    StructField("Tax", FloatType() ),
])

# Import all files from bronze folder of lakehouse

df = spark.read.format("csv").option("header", "false").schema(orderSchema).load("Files/bronze/*.csv")

# Display the first 10 rows of the dataframe to preview the data

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name    

# Add columns IsFlagged, CreatedTS and ModifiedTS

df = df.withColumn("FileName", input_file_name()) \
    .withColumn("IsFlagged", when (col("OrderDate") < '2019-08-01', True).otherwise(False))\
    .withColumn("CreatedTS", current_timestamp()).withColumn("ModifiedTS", current_timestamp())


#Update CustomerName to "Unknown" if CustomerName null or empity

df = df.withColumn("CustomerName", when((col("CustomerName").isNull() | (col("CustomerName")=="")), lit("Unknown")).otherwise(col("CustomerName")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Definig schema for the sales_silver table in the database using Delta Lake format


# CELL ********************

#Define the schema for the sales_silver table

from pyspark.sql.types import *

from delta.tables import *

DeltaTable.createIfNotExists(spark) \
    .tableName("LH_Medallion.dbo.sales_silver")\
    .addColumn("SalesOrderNumber", StringType()) \
    .addColumn("SalesOrderLineNumber", StringType()) \
    .addColumn("OrderDate", StringType()) \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("Item", StringType()) \
    .addColumn("Quantity", StringType()) \
    .addColumn("UnitPrice", StringType()) \
    .addColumn("Tax", StringType()) \
    .addColumn("FileName", StringType()) \
    .addColumn("IsFlagged", StringType()) \
    .addColumn("CreatedTS", StringType()) \
    .addColumn("ModifiedTS", StringType()) \
    .execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Performing an upsert operation on a Delta table

# CELL ********************

# Update existing records and insert new ones based on a condition defined by the columns SalesOrderNumber, OrderDate, CustomerName, and Item.

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, 'Tables/dbo/sales_silver')

dfUpdates = df

deltaTable.alias('silver') \
    .merge(
        dfUpdates.alias('updates'),
        'silver.SalesOrderNumber = updates.SalesOrderNumber and silver.OrderDate = updates.OrderDate and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item'
    ) \
    .whenMatchedUpdate(set= 
        {

        }
    ) \
    .whenNotMatchedBySourceUpdate (set={}) \
    .whenNotMatchedInsert(values=
        {
            "SalesOrderNumber": "updates.SalesOrderNumber",
            "SalesOrderLineNumber": "updates.SalesOrderLineNumber",            
            "OrderDate": "updates.OrderDate",
            "CustomerName": "updates.CustomerName",
            "Email": "updates.Email",
            "Item": "updates.Item",
            "Quantity": "updates.Quantity",
            "UnitPrice": "updates.UnitPrice",
            "Tax": "updates.Tax",
            "FileName": "updates.FileName",
            "IsFlagged": "updates.IsFlagged",
            "CreatedTS": "updates.CreatedTS",
            "ModifiedTS": "updates.ModifiedTS",

        }
    ) \
    .execute()

    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
