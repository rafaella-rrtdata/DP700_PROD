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

# #### 1. Load data to the dataframe as a starting point to create the gold layer

# CELL ********************

# Load data to the dataframe as a starting point to create the gold layer

df = spark.read.table("LH_Medallion.dbo.sales_silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2. Define the schema for the dimdate_gold tabla

# CELL ********************

from pyspark.sql.types import *
from delta.tables import * 

#Define the schema for the dimdate_gold tabla

DeltaTable.createIfNotExists(spark) \
    .tableName("LH_Medallion.dbo.dimdate_gold") \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn ("Year", IntegerType()) \
    .addColumn("mmmyyy", StringType()) \
    .addColumn("yyyymm", StringType()) \
    .execute()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3. Create dataframe for dimDate_gold

# CELL ********************

from pyspark.sql.functions import col, dayofmonth, month, year, date_format
    
# Create dataframe for dimDate_gold
    
dfdimDate_gold = df.dropDuplicates(["OrderDate"]).select(col("OrderDate"), \
        dayofmonth("OrderDate").alias("Day"), \
        month("OrderDate").alias("Month"), \
        year("OrderDate").alias("Year"), \
        date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"), \
        date_format(col("OrderDate"), "yyyyMM").alias("yyyymm"), \
    ).orderBy("OrderDate")

# Display the first 10 rows of the dataframe to preview your data

display(dfdimDate_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4. Create customer_gold dimension delta table

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *

# Create customer_gold dimension delta table

DeltaTable.createIfNotExists(spark) \
.tableName("LH_Medallion.dbo.dimcustomer_gold") \
.addColumn("CustomerName", StringType()) \
.addColumn("Email", StringType()) \
.addColumn("First", StringType()) \
.addColumn("Last", StringType()) \
.addColumn("CustomerID", LongType()) \
.execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, split

# Create customer_silver dataframe

df_dimCustomer_silver = df.dropDuplicates(["CustomerName", "Email"]).select(col("CustomerName"), col("Email")) \
    .withColumn("First", split(col("CustomerName"), " ").getItem(0)) \
    .withColumn("Last", split(col("CustomerName"), " ").getItem(1))

# Display the firt 10 rows of the dataframe to preview your data

display(df_dimCustomer_silver.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id , col, when, coalesce, max, lit

dfdimCustomer_temp = spark.read.table("LH_Medallion.dbo.dimcustomer_gold")

MAXCustomerID = dfdimCustomer_temp.select(coalesce(max(col("CustomerID")), lit(0)).alias("MAXCustomerID")).first()[0]

# display(MAXCustomerID)

dfdimCustomer_gold = df_dimCustomer_silver.join(
    dfdimCustomer_temp, 
    (df_dimCustomer_silver.CustomerName == dfdimCustomer_temp.CustomerName)  
    & (df_dimCustomer_silver.Email == dfdimCustomer_temp.Email)
    , "left_anti"
)

dfdimCustomer_gold = dfdimCustomer_gold.withColumn("CustomerID", monotonically_increasing_id() + MAXCustomerID + 1)

# Display the first 10 rows of the dataframe to preview the data

display(dfdimCustomer_gold.head(10))

# Here you’re cleaning and transforming customer data (dfdimCustomer_silver) by performing a left anti join to exclude duplicates 
# that already exist in the dimCustomer_gold table, and then generating unique CustomerID values using the monotonically_increasing_id() function.



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Now you’ll ensure that your customer table remains up-to-date as new data comes in. In a new code block, paste and run the following:

from delta.tables import *

deltaTable = DeltaTable.forPath(spark , 'Tables/dbo/dimcustomer_gold')

dfUpdates = dfdimCustomer_gold

deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.CustomerName = updates.CustomerName AND gold.Email = updates.Email'
        ) \
        .whenMatchedUpdate(set={}) \
        .whenNotMatchedInsert(values =
            {
                "CustomerName": "updates.CustomerName",
                "Email": "updates.Email",
                "First": "updates.First",
                "Last": "updates.Last",
                "CustomerID": "updates.CustomerID"
            }
        ) \
        .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 8. Create product dimension

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *

DeltaTable.createIfNotExists(spark) \
    .tableName("LH_Medallion.dbo.dimproduct_gold") \
    .addColumn("ItemName", StringType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("ItemInfo", StringType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 9. Create the product_silver dataframe.

# CELL ********************

from pyspark.sql.functions import col, split, lit, when

#create product_silver dataframe

dfdimProduct_silver = df.dropDuplicates(["Item"]).select(col("Item")) \
    .withColumn("ItemName", split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo", 
        when(
            (
                split(col("Item"), ", ").getItem(1).isNull()
                | (split(col("Item"), ", ").getItem(1)=="")
            )
            ,lit("")
        )
        .otherwise(split(col("Item"), ", ").getItem(1))
    ) 

    # Display the first 10 rows of the dataframe to preview your data

display(dfdimProduct_silver.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 10. Create IDs for dimProduct_gold table

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id, col, lit, max, coalesce

#dfdimProduct_temp = dfdimProduct_silver

dfdimProduct_temp = spark.read.table("LH_Medallion.dbo.dimProduct_gold")

MAXProductID = dfdimProduct_temp.select(coalesce(max(col("ItemID")), lit(0)).alias("MAXItemID")).first()[0]

df_dimProduct_gold = dfdimProduct_silver.join(
    dfdimProduct_temp
    , (dfdimProduct_silver.ItemName == dfdimProduct_temp.ItemName)
    & (dfdimProduct_silver.ItemInfo == dfdimProduct_temp.ItemInfo)
    , "left_anti"
)

df_dimProduct_gold = df_dimProduct_gold.withColumn("ItemID", monotonically_increasing_id() + MAXProductID + 1)

# Display the first 10 rows of the dataframe to preview your data

display(dfdimCustomer_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 11.  To ensure that  product table remains up-to-date as new data comes in

# CELL ********************

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/dbo/dimproduct_gold')
            
dfUpdates = df_dimProduct_gold
            
deltaTable.alias('gold') \
  .merge(
        dfUpdates.alias('updates'),
        'gold.ItemName = updates.ItemName AND gold.ItemInfo = updates.ItemInfo'
        ) \
        .whenMatchedUpdate(set =
        {
               
        }
        ) \
        .whenNotMatchedInsert(values =
         {
          "ItemName": "updates.ItemName",
          "ItemInfo": "updates.ItemInfo",
          "ItemID": "updates.ItemID"
          }
          ) \
          .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 12. create the fact table

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *
    
DeltaTable.createIfNotExists(spark) \
    .tableName("LH_Medallion.dbo.factsales_gold") \
    .addColumn("CustomerID", LongType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 13.  Create a new dataframe to combine sales data with customer and product information include customer ID, item ID, order date, quantity, unit price, and tax

# CELL ********************

from pyspark.sql.functions import col
    
dfdimCustomer_temp = spark.read.table("LH_Medallion.dbo.dimCustomer_gold")
dfdimProduct_temp = spark.read.table("LH_Medallion.dbo.dimProduct_gold")
    
df = df.withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo",when((split(col("Item"), ", ").getItem(1).isNull() | (split(col("Item"), ", ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ", ").getItem(1))) \
    
    
# Create Sales_gold dataframe
    
dffactSales_gold = df.alias("df1").join(dfdimCustomer_temp.alias("df2"),(df.CustomerName == dfdimCustomer_temp.CustomerName) & (df.Email == dfdimCustomer_temp.Email), "left") \
        .join(dfdimProduct_temp.alias("df3"),(df.ItemName == dfdimProduct_temp.ItemName) & (df.ItemInfo == dfdimProduct_temp.ItemInfo), "left") \
    .select(col("df2.CustomerID") \
        , col("df3.ItemID") \
        , col("df1.OrderDate") \
        , col("df1.Quantity") \
        , col("df1.UnitPrice") \
        , col("df1.Tax") \
    ).orderBy(col("df1.OrderDate"), col("df2.CustomerID"), col("df3.ItemID"))
    
# Display the first 10 rows of the dataframe to preview your data
    
display(dffactSales_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 14. nsure that sales data remains up-to-date

# CELL ********************

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/dbo/factsales_gold')
    
dfUpdates = dffactSales_gold
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.OrderDate = updates.OrderDate AND gold.CustomerID = updates.CustomerID AND gold.ItemID = updates.ItemID'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "CustomerID": "updates.CustomerID",
      "ItemID": "updates.ItemID",
      "OrderDate": "updates.OrderDate",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
