# Databricks notebook source
# MAGIC %run "/Workspace/optum/connectors"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/optum/generic"

# COMMAND ----------

claims_df = read_bronze_json("Claims")

# COMMAND ----------

df_row_columns(claims_df)

# COMMAND ----------

check_missing_values(claims_df,claims_df.columns)

# COMMAND ----------

display(claims_df.limit(5))

# COMMAND ----------

display(claims_df.limit(5))

# COMMAND ----------

claims_df = drop_columns(claims_df,['_id'])

# COMMAND ----------

check_string_as_nan(claims_df)

# COMMAND ----------

claims.count()
claims.groupby('Claim_Or_Rejected')[['claim_amount']].sum()
claims.groupBy('Claim_Or_Rejected').count().show()
claims.select("*").show(6)

# COMMAND ----------

claims_df = claims_df.replace("NaN",None)
claims_df = claims_df.fillna({"Claim_Or_Rejected":"N"})
claims_df = claims_df.withColumn("claim_amount", claims_df['claim_amount'].cast("integer"))
claims_df = claims_df.withColumn("claim_date", claims_df['claim_date'].cast("date"))

# COMMAND ----------

check_duplicates(claims_df,claims_df.columns)

# COMMAND ----------

display(claims_df)

# COMMAND ----------

claims_df.select("*").filter(col("SUB_ID").isin(["SUBID10022","SUBID10049"])).show(6)

# COMMAND ----------

write2silver(claims_df,"claims_S.csv")