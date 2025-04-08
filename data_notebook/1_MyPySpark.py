# Databricks notebook source
# MAGIC %md
# MAGIC **Check Path **System****

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading CSV**

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", True) \
    .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading JSON**

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df_json = spark.read.format("json") \
    .option("header", "true") \
    .option("multiLine", False) \
    .load("/FileStore/tables/drivers.json")

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Definition**

# COMMAND ----------


df_json = spark.read.format('json').option('inferSchema',True)\
                    .option('header',True)\
                    .option('multiLine',False)\
                    .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DDL SCHEMA**

# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10)).display()  

# COMMAND ----------


my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 

                ''' 

# COMMAND ----------

display(
    df.filter(
        (col('Outlet_Size').isNull()) & 
        (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))
    )
)


# COMMAND ----------

df = spark.read.format('csv')\
            .schema(my_ddl_schema)\
            .option('header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv') 

# COMMAND ----------

df.withColumnRenamed('Item_Identifier' , 'ugot').display()

# COMMAND ----------

df.display()

# COMMAND ----------

 

# COMMAND ----------

# MAGIC %md
# MAGIC StructType() Schema

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *  

# COMMAND ----------


my_strct_schema = StructType([
    StructField('Item_Identifier', StringType(), True),
    StructField('Item_Weight', StringType(), True),
    StructField('Item_Fat_Content', StringType(), True),
    StructField('Item_Visibility', StringType(), True),
    StructField('Item_MRP', StringType(), True),
    StructField('Outlet_Identifier', StringType(), True),
    StructField('Outlet_Establishment_Year', StringType(), True),
    StructField('Outlet_Size', StringType(), True),
    StructField('Outlet_Location_Type', StringType(), True),
    StructField('Outlet_Type', StringType(), True),
    StructField('Item_Outlet_Sales', StringType(), True)
])

# COMMAND ----------

df = spark.read.format('csv') \
    .schema(my_strct_schema) \
    .option('header', True) \
    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(col('Item_Identifier') , col('Item_Weight') , col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC FILTER

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Filter Only

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Filter / Conditions with && 

# COMMAND ----------

df.filter( (col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10)  ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a new Column with new value (Constants)

# COMMAND ----------

df = df.withColumn('flag',lit("new")) 

# COMMAND ----------


df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC New column with Multiply 

# COMMAND ----------

df.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Search And Replace Value

# COMMAND ----------

df.withColumn('Item_Fat_Content', regexp_replace('Item_Fat_Content', 'Regular', 'Reg'))\
  .withColumn('Item_Fat_Content', regexp_replace('Item_Fat_Content', 'Low Fat', 'LF')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting

# COMMAND ----------

df.filter(col('Item_Weight').isNotNull())\
.sort(col('Item_Weight').asc()).display()

# COMMAND ----------

 df.sort(col('Item_Weight').desc(), col('Item_Visibility').asc()).display()
# df.sort(['Item_Weight','Item_Visibility'],ascending = [0,0]).display()

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping of Column

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping of multiple columns

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Removing of duplicate of Data Frames

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove duplicates on specific Columns

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove duplciate in Dataframe as well

# COMMAND ----------


df.distinct().display()

# COMMAND ----------


data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)


# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------


data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Union by Name

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC String Functions
# MAGIC Initcap()

# COMMAND ----------

df.select(upper('Item_Type').alias('upper_Item_Type')).display()

# COMMAND ----------



# COMMAND ----------


df = df.withColumn('curr_date',current_date())

df.display()
     

# COMMAND ----------

df.withColumn('curr_date' , date_add('curr_date' ,  -7)).display()


# COMMAND ----------

df = df.withColumn('datediff',datediff('week_after','curr_date'))

df.display()

# COMMAND ----------


df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))

df.display()

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Item_Weight']).display()

# COMMAND ----------

df.withColumn('Outlet_Type', split('Outlet_Type', ' ')).display()

# COMMAND ----------

df_new = df.select(col('Outlet_Location_Type') , col('Outlet_Type'))

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new = df_new.withColumn('Outlet_Type' , split('Outlet_Type' , ' '))

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new.withColumn('Outlet_Type' , explode('Outlet_Type')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df_new = df.groupBy('Item_Type' , 'Outlet_Size').agg(sum('Item_MRP').alias('TOTAL_MRP'))

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new.groupBy('Outlet_Size').agg(avg('TOTAL_MRP').alias('Total_Avr')).display()


# COMMAND ----------

df.groupBy('Item_Type' , 'Outlet_Size').agg(sum('Item_MRP'), avg('Item_MRP')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('IsExpensive',
              when((col('veg_flag') == 'Veg') & (col('Item_MRP') < 100), 'inexpensive')
              .otherwise('expensive')).display()

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.join(df2 , df1['dept_id'] == df2['dept_id'], 'inner').display()

# COMMAND ----------

df1.join(df2 , df1['dept_id'] == df2['dept_id'], 'LEFT').display()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

df1.join(df2 , df1['dept_id'] == df2['dept_id'], 'anti').display()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('Rank' , rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('DenseRank' , dense_rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Sum Per Row

# COMMAND ----------

df.withColumn('dum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()
     

# COMMAND ----------

# MAGIC %md
# MAGIC Sum Per Item Type

# COMMAND ----------


df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Sum per row downwards

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Get total value of ITEMP MRP and save it to total sum , value data will be some for all column

# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()
   

# COMMAND ----------

# MAGIC %md
# MAGIC USER DEFINE FUNCTION

# COMMAND ----------

def my_func(x):
    return x*x 

# COMMAND ----------

my_udf = udf(my_func)

# COMMAND ----------

df.withColumn('mynewcol',my_udf('Item_MRP')).display()