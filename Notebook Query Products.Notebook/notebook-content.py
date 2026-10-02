# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5cfa9d05-eec7-48a5-91ae-74004b1f55e5",
# META       "default_lakehouse_name": "LH_01",
# META       "default_lakehouse_workspace_id": "4556cc13-7701-4e97-9168-95ccce590897",
# META       "known_lakehouses": [
# META         {
# META           "id": "5cfa9d05-eec7-48a5-91ae-74004b1f55e5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_01.dbo.products LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
