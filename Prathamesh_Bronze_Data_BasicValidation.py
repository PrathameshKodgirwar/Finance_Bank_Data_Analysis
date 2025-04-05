#!/usr/bin/env python
# coding: utf-8

# ## Prathamesh_Bronze_Data_BasicValidation
# 
# New notebook

# In[1]:


# Welcome to your new notebook
# Load data from OneLake (Bronze Layer):
df_transaction = spark.read.format("parquet").load("abfss://FabricTrainingWorkspace@onelake.dfs.fabric.microsoft.com/LH_Finance_Project_Prathamesh_Kodgirwar.Lakehouse/Files/Bronze/transaction_data")
df_customer = spark.read.format("parquet").load("abfss://FabricTrainingWorkspace@onelake.dfs.fabric.microsoft.com/LH_Finance_Project_Prathamesh_Kodgirwar.Lakehouse/Files/Bronze/customer_data")
df_bank = spark.read.format("parquet").load("abfss://FabricTrainingWorkspace@onelake.dfs.fabric.microsoft.com/LH_Finance_Project_Prathamesh_Kodgirwar.Lakehouse/Files/Bronze/bank_data")

# Displaying sample records:
df_transaction.show(5)
df_customer.show(5)
df_bank.show(5)


# In[2]:


df_transaction.printSchema()
df_customer.printSchema()
df_bank.printSchema()


# **Transaction Table: Schema_Analysis**
# -formatting the columns as per accurate data types
# 

# In[5]:


from pyspark.sql.functions import col, to_date

df_transaction = df_transaction.withColumn("Transaction_ID", col("Transaction_ID").cast("string")) \
    .withColumn("Customer_ID", col("Customer_ID").cast("int")) \
    .withColumn("Account_Type", col("Account_Type").cast("string")) \
    .withColumn("Total_Balance", col("Total_Balance").cast("double")) \
    .withColumn("Transaction_Amount", col("Transaction_Amount").cast("double")) \
    .withColumn("Investment_Amount", col("Investment_Amount").cast("double")) \
    .withColumn("Investment_Type", col("Investment_Type").cast("string")) \
    .withColumn("Transaction_Date", to_date(col("Transaction_Date"), "yyyy-MM-dd"))

df_transaction.printSchema()


# **Customer Table: Schema_Analysis**
# -formatting the columns as per accurate data types

# In[6]:


df_customer = df_customer.withColumn("Customer_ID", col("Customer_ID").cast("int")) \
    .withColumn("Age", col("Age").cast("int")) \
    .withColumn("Customer_Type", col("Customer_Type").cast("string")) \
    .withColumn("City", col("City").cast("string")) \
    .withColumn("Region", col("Region").cast("string")) \
    .withColumn("Bank_Name", col("Bank_Name").cast("string")) \
    .withColumn("Branch_ID", col("Branch_ID").cast("int"))

df_customer.printSchema()


# **Bank Table: Schema_Analysis**
# -formatting the columns as per accurate data types

# In[7]:


df_bank = df_bank.withColumn("Branch_ID", col("Branch_ID").cast("int")) \
    .withColumn("City", col("City").cast("string")) \
    .withColumn("Region", col("Region").cast("string")) \
    .withColumn("Firm_Revenue", col("Firm_Revenue").cast("double")) \
    .withColumn("Expenses", col("Expenses").cast("double")) \
    .withColumn("Profit_Margin", col("Profit_Margin").cast("double"))

df_bank.printSchema()


# **Checking for redundancy in data.**

# In[10]:


df_transaction.groupBy(df_transaction.columns).count().filter("count > 1").show()
df_customer.groupBy(df_customer.columns).count().filter("count > 1").show()
df_bank.groupBy(df_bank.columns).count().filter("count > 1").show()


# **Since there is no redundancy, lets check for null values.**

# In[11]:


from pyspark.sql.functions import col, sum

df_transaction.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_transaction.columns]).show()
df_customer.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_customer.columns]).show()
df_bank.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_bank.columns]).show()


# **Since the data contains some missing values** ->
# **Age** : contains 500 rows of null values. ->
# **City** : Containing 500 rows of null values. ->
# Customer_type : Containing 500 rows of null values. -> 
# TotalRows : 10000
# 

# In[13]:


#Replacing Age null rows with avg_age:

from pyspark.sql.functions import mean

avg_age = df_customer.select(mean(col("Age"))).collect()[0][0]  # Calculating mean age
df_customer = df_customer.fillna({"Age": avg_age})  # Replace NULLs with average age


# In[14]:


from pyspark.sql.functions import col

most_common_type = df_customer.groupBy("Customer_Type").count().orderBy(col("count").desc()).first()[0]
df_customer = df_customer.fillna({"Customer_Type": most_common_type})

most_common_city = df_customer.groupBy("City").count().orderBy(col("count").desc()).first()[0]
df_customer = df_customer.fillna({"City": most_common_city})


# In[15]:


df_customer.select(
    sum(col("Age").isNull().cast("int")).alias("Age_Null"),
    sum(col("Customer_Type").isNull().cast("int")).alias("Customer_Type_Null"),
    sum(col("City").isNull().cast("int")).alias("City_Null")
).show()


# 
# Similarly in **Bank Table** -> 
# Firm_revenue contains 50 null value rows.
# 

# In[17]:


avg_firm_revenue = df_bank.select(mean(col("Firm_Revenue"))).collect()[0][0]
# Fill NULL values with the average:
df_bank = df_bank.fillna({"Firm_Revenue": avg_firm_revenue})


# In[18]:


df_transaction.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_transaction.columns]).show()
df_customer.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_customer.columns]).show()
df_bank.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_bank.columns]).show()


# Now **Basic validation is completed**, Dataset is cleaned and ready to be transformed into silver.

# In[20]:


df_transaction.write.format("delta").mode("overwrite").save("abfss://FabricTrainingWorkspace@onelake.dfs.fabric.microsoft.com/LH_Finance_Project_Prathamesh_Kodgirwar.Lakehouse/Files/Silver/transaction_data_cleaned")
df_customer.write.format("delta").mode("overwrite").save("abfss://FabricTrainingWorkspace@onelake.dfs.fabric.microsoft.com/LH_Finance_Project_Prathamesh_Kodgirwar.Lakehouse/Files/Silver/customer_data_cleaned")
df_bank.write.format("delta").mode("overwrite").save("abfss://FabricTrainingWorkspace@onelake.dfs.fabric.microsoft.com/LH_Finance_Project_Prathamesh_Kodgirwar.Lakehouse/Files/Silver/bank_data_cleaned")

