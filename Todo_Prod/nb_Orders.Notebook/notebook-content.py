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

df= spark.read.format("csv").option("header", "true").load("Files/orders/*.csv")
display(df.limit(100))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Exploring the data
### Filter

customers = df['CustomerName', 'Email']

print(customers.count())
print(customers.distinct().count())

display(customers.distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Another way to select the columns

roadred_customers = df.select("CustomerName", "Email").where(df['Item']=='Road-250 Red, 52')

customers = df.select("CustomerName", "Email")

print(customers.count())
print(customers.distinct().count())

print(roadred_customers.count())
print(roadred_customers.distinct().count())

display(roadred_customers.distinct())



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Agregate

productSales = df.select("Item", "Quantity").groupBy("Item").sum("Quantity")


display(productSales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Using sql functions

from pyspark.sql.functions import * 

yearSales = df.select(year(col("OrderDate")).alias("Year")).groupBy("Year").count().orderBy("Year")

display(yearSales)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Transformaciones para el dataframe final

from pyspark.sql.functions import *

# Create Year and Month columns

df= df.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))


#Create new FirstName and LastName fields

df = df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

# Select and reorder columns

prepared_df = df["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "Email", "Item", "Quantity", "UnitPrice", "Tax"]

#display the five first orders

display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Save transformed data

prepared_df.write.mode('overwrite').format("delta").saveAsTable("dbo.sales_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Running a SQL cell
# MAGIC 
# MAGIC SELECT 
# MAGIC     YEAR(OrderDate) as OrderYear,
# MAGIC     sum(UnitPrice * Quantity + Tax) as GrossRevenue
# MAGIC from sales_orders
# MAGIC group by year(OrderDate)
# MAGIC order by OrderYear;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from sales_orders

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
