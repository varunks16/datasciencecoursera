# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fafaa516-f1cd-4935-8a83-509e1200f86a",
# META       "default_lakehouse_name": "Fabric_POC_DBU_LH",
# META       "default_lakehouse_workspace_id": "99016610-439d-4c98-8eb5-0de3ad218be6",
# META       "known_lakehouses": [
# META         {
# META           "id": "fafaa516-f1cd-4935-8a83-509e1200f86a"
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

from pyspark.sql import DataFrame

# Read table from Lakehouse (schema-enabled, three-part name)
# In Fabric, the database/catalog for the default Lakehouse is usually the Lakehouse name itself.
# The fully-qualified table name should therefore be:
#   Fabric_POC_DBU_LH.UDM_FINANCE_DATAPRODUCTS.`DP_PROFIT_LOSS.zBtcf2AT`
# Note: the last segment contains a dot, so we quote it with backticks.

df_pl: DataFrame = spark.read.table("Fabric_POC_DBU_LH.UDM_FINANCE_DATAPRODUCTS.`DP_PROFIT_LOSS.zBtcf2AT`")

# Take a sample of 10 rows
sample_df = df_pl.limit(10)

display(sample_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame

# Read table from Lakehouse (same as previous cell)
df_pl: DataFrame = spark.read.table(
    "Fabric_POC_DBU_LH.UDM_FINANCE_DATAPRODUCTS.`DP_PROFIT_LOSS.zBtcf2AT`"
)

# Set the transaction ID you want to filter on
target_transaction_id = "15433919255425885624"

# IMPORTANT: Replace 'TRANSACTION_ID' with the actual column name that holds the transaction ID
filtered_df = (
    df_pl
    .filter(df_pl["profit_loss_id"] == target_transaction_id)
    .limit(1)  # return only one transaction
)

display(filtered_df["customer_id"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame

# Read table from Lakehouse (schema-enabled, three-part name)
df_pl: DataFrame = spark.read.table(
    "Fabric_POC_DBU_LH.UDM_FINANCE_DATAPRODUCTS.`DP_PROFIT_LOSS.zBtcf2AT`"
)

# Set the transaction ID you want to filter on
target_transaction_id = "15433919255425885624"

# Filter to a single transaction (adjust column name if needed)
filtered_df = (
    df_pl
    .filter(df_pl["profit_loss_id"] == target_transaction_id)
    .limit(1)  # ensure only one transaction
)

# Create / overwrite GOLD Delta table with this single transaction
# Lakehouse name: Fabric_POC_DBU_LH
# Schema: gold
# Table: dp_profit_loss
filtered_df.write.format("delta").mode("overwrite").saveAsTable(
    "Fabric_POC_DBU_LH.gold.dp_profit_loss"
)

# Optional: verify what was written
display(spark.read.table("Fabric_POC_DBU_LH.gold.dp_profit_loss"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
