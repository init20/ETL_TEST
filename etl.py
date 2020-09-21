# -*- coding: utf-8 -*-
import sys
import urllib, json
import pandas as pd
import numpy as np
from azure.storage.blob import BlobClient
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import explode, count, col, upper, lit, when, size, split, regexp_replace, trim, max, sum, first
from pyspark.sql.types import *
from pyspark.sql.readwriter import DataFrameWriter

spark = SparkSession.builder.appName('ETL').getOrCreate()
sc = spark.sparkContext

def json_to_productsDF():
    url = "https://etlwebapp.azurewebsites.net/api/products"
    response = urllib.urlopen(url)
    data = json.loads(response.read())
    json_object = json.dumps(data)
    df = spark.read.json(sc.parallelize([json_object]))
    df2 = df.withColumn("value", explode("results")).select("value")
    df3 = df2.select("value.items").withColumn("exploded", explode("items")).\
    select("exploded.aisle","exploded.department","exploded.product_name")
    return df3

def order_dow_clean(df, days):
    for i in range(len(days_name)):
        df = df.withColumn("ORDER_DOW", when((col("ORDER_DOW")==str(days_name[i])), lit((i))).otherwise(col("ORDER_DOW")))
    return df

def split_order(df, df_aux, maxim):
    for i in range(maxim):
        df2 = df.withColumn("ORDER_FOR_PRODUCT", split(col("ORDER_DETAIL"), "~")[i])
        df_aux = df2.union(df_aux)
    df_aux2 = df_aux.filter(col("ORDER_FOR_PRODUCT").isNotNull())
    return df_aux2

def select_union(df):
    return df.select("ORDER_ID","product_name","QTY","aisle","department","mom","single","pet_friendly","complete_mystery").\
    withColumnRenamed("product_name","PRODUCT")

def quantile(df):
    x = df.toPandas()
    x['first']= np.percentile(x['QTY'], 25)
    x['second']= np.percentile(x['QTY'], 50)
    x['third']= np.percentile(x['QTY'], 75)
    x = x[['first', 'second' ,'third']].head(1)
    return spark.createDataFrame(x)

def number_for_day(df):
    return df.withColumn("DAY", when((col("ORDER_DOW")==0), lit("Sunday")).\
    when((col("ORDER_DOW")==1), lit("Monday")).when((col("ORDER_DOW")==2), lit("Tuesday")).when((col("ORDER_DOW")==3), \
    lit("Wednesday")).when((col("ORDER_DOW")==4), lit("Thursday")).when((col("ORDER_DOW")==5), lit("Friday")).otherwise("Saturday"))

def get_quantiles(df):
    control = False
    for i in range(7):
        day = df.filter(col("ORDER_DOW")==int(i)).select("QTY").withColumn("QTY", col("QTY").cast(IntegerType()))
        if (day.count() > 0):
            df = quantile(day)
            df2 = df.withColumn("ORDER_DOW", lit(int(i)))
            if (not control):
                quantiles = df2.limit(0)
                control = True
        quantiles = quantiles.union(df2)

products = json_to_productsDF()
##count and order by department
dep_grouped = products.groupBy("department").agg(count("product_name").alias("product_count")).orderBy(col("product_count").desc())
##MOM
mom = products.filter((col("department")=="dairy eggs") | (col("department")=="bakery") | (col("department")=="household") | (col("department")=="babies")).\
select("product_name").withColumn("mom", lit(1))

##SINGLE
single = products.filter((col("department")=="canned goods") | (col("department")=="meat seafood") | (col("department")=="alcohol") \
| (col("department")=="beverages")).select("product_name").withColumn("single", lit(1))

##PET FRIENDLY
pet_friendly = products.filter((col("department")=="canned goods") | (col("department")=="pets") | (col("department")=="frozen")).\
select("product_name").withColumn("pet_friendly", lit(1))

##JOIN
prod_mom = products.join(mom,["product_name"],"left")
prod_single = prod_mom.join(single,["product_name"],"left")
prod_pet_fr = prod_single.join(pet_friendly,["product_name"],"left").na.fill(0)

##COMPLETE MYSTERY
final_products = prod_pet_fr.withColumn("complete_mystery", when((\
(col("mom")==0) & (col("single")==0) & (col("pet_friendly")==0)), lit(1)).otherwise(lit(0)))

##WRITE TABLE... IF TABLE EXISTS, SAVE LEFANTI IN OTHER NEW
'''
table_list=spark.sql("""show tables in orderdb""")
table_exists = table_list.filter(col("tableName")=="catalog").count()
if (table_exists==1):
    final_new_products = spark.read.table("orderdb.catalog").join(final_products,["product_name"], "leftanti")
    ##SAVE NEW PRODUCTS
    final_new_products2 = DataFrameWriter(final_new_products)
    final_new_products2.jdbc(url=url, table= "NEW_CATALOG", mode ="overwrite", properties = properties)
else:
    final_new_products2 = DataFrameWriter(final_new_products)
    final_new_products2.jdbc(url=url, table= "CATALOG", mode ="overwrite", properties = properties)
'''
##READ CSV
sales = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', quote='"', delimiter=',').\
option("encoding", "ISO-8859-1").load('/user/mpineda/0*')

##TO FIX
##1.- ORDER_DOW ---> Tuesday =2 and Friday = 5 cases (DONE)
##2.- ORDER_HOUR_OF_DAY ---> Appers 24 hour... Change for 0 hour (Done)
##3.- NAME PRODUCTS, DIFFERENT CASES (Done)
days_name = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]

sales2 = order_dow_clean(sales, days_name)
sales3 = sales2.withColumn("ORDER_HOUR_OF_DAY", when((col("ORDER_HOUR_OF_DAY")==24), lit(0)).otherwise(col("ORDER_HOUR_OF_DAY")))
sales4 = sales3.withColumn("COUNT_PRODUCTS", size(split(col("ORDER_DETAIL"),"~")))

df_aux = sales4.withColumn("ORDER_FOR_PRODUCT", lit(None)).limit(0)
maxim = sales4.select("COUNT_PRODUCTS").orderBy(col("COUNT_PRODUCTS").desc()).collect()[0][0]

##FINAL HEADBOARD
head = sales4.select("ORDER_ID","USER_ID","ORDER_NUMBER","ORDER_DOW","ORDER_HOUR_OF_DAY","DAYS_SINCE_PRIOR_ORDER")

##FROM HERE I START TO WORKING WITH THE DETAILS (ORDER_ID, PRODUCTS, QTY)
details = split_order(sales4, df_aux, maxim).select("ORDER_ID","ORDER_FOR_PRODUCT").orderBy(col("ORDER_ID").asc()).persist()
details2 = details.withColumn("PRODUCT", split(col("ORDER_FOR_PRODUCT"), "\|")[0]).\
withColumn("QTY", split(col("ORDER_FOR_PRODUCT"), "\|")[2])
details2 = details2.withColumn("QTY", when((col("QTY").isNull()), 1).otherwise(col("QTY")))
details3 = details2.withColumn("QTY", regexp_replace('QTY', '"', '')).drop("ORDER_FOR_PRODUCT")
details4 = details3.withColumn("PRODUCT", regexp_replace('PRODUCT', '"', ''))
details5 = details4.withColumn("PRODUCT", regexp_replace('PRODUCT', "\\\\", ""))

final_products2 = final_products.withColumn("PRODUCT", regexp_replace("product_name", '"', ''))
final_products3 = final_products2.withColumn("PRODUCT", regexp_replace("PRODUCT", "\\\\", ""))

joined = details5.join(final_products3,["PRODUCT"],"inner")
##1051 Diff
left_det = details5.join(final_products3,["PRODUCT"],"leftanti").withColumn("PRODUCT", regexp_replace('PRODUCT', '[^a-zA-Z0-9]', ''))

final_products4 = final_products3.withColumn("PRODUCT_ORIGEN", col("PRODUCT")).withColumn("PRODUCT", regexp_replace('PRODUCT', '[^a-zA-Z0-9]', ''))
final_products5 = final_products4.withColumn("PRODUCT", split("PRODUCT", ",")[0])
join2 = left_det.join(final_products5,["PRODUCT"],"inner").dropDuplicates(["PRODUCT","ORDER_ID","QTY"])
##86 Diff
left_det2 = left_det.join(final_products5,["PRODUCT"],"leftanti")

final_products6 = final_products5.withColumn("PRODUCT", split("product_name",",")[0])
final_products7 = final_products6.withColumn("PRODUCT", regexp_replace('PRODUCT', '[^a-zA-Z0-9]', ''))
final_products8 = final_products7.groupBy("PRODUCT").agg(first("product_name").alias("product_name"), first("aisle").alias("aisle"), first("department").alias("department"),\
first("mom").alias("mom"), first("single").alias("single"), first("pet_friendly").alias("pet_friendly"), first("complete_mystery").alias("complete_mystery"))

join3 = left_det2.join(final_products8,["PRODUCT"],"inner")

df_join1 = select_union(joined)
df_join2 = select_union(join2)
df_join3 = select_union(join3)

det_united = df_join1.union(df_join2).union(df_join3)
##REPAIR CASE complete_mystery
det_united2 = det_united.withColumn("complete_mystery", when(( (col("mom")==1) | (col("single")==1) | (col("pet_friendly")==1)), 0).otherwise(col("complete_mystery")))
##DETAIL READY
join_head_det = head.join(det_united2, ["ORDER_ID"], "inner")
##BEHAVIUR CLIENT
client = join_head_det.withColumn("mom", col("QTY")*col("mom")).withColumn("single", col("QTY")*col("single")).withColumn("pet_friendly", col("QTY")*col("pet_friendly"))
client1 = client.groupBy("ORDER_ID","USER_ID").agg(sum("QTY").alias("QTY"), sum("mom").alias("mom"), sum("single").alias("single"), sum("pet_friendly").alias("pet_friendly"))
client2 = client1.withColumn("is_mom", when((col("mom")/col("qty") > 0.5 ), 1).otherwise(0)).withColumn("is_single", when((col("single")/col("qty") > 0.6 ), 1).otherwise(0)).\
withColumn("is_pet_friendly", when((col("pet_friendly")/col("qty") > 0.3 ), 1).otherwise(0))
client3 = client2.withColumn("complete_mystery", when(( (col("is_mom")==0) & (col("is_single")==0) & (col("is_pet_friendly")==0) ), 1).otherwise(0))
client_behaviur = client3.select("USER_ID","is_mom","is_single","is_pet_friendly","complete_mystery")

##SEGMENTATION CLIENT
join_head_det_agg = join_head_det.groupBy("USER_ID","ORDER_ID","ORDER_DOW","DAYS_SINCE_PRIOR_ORDER").agg(sum("QTY").alias("QTY"))

##GET QUANTILES
quantiles = get_quantiles(join_head_det_agg)
client_quantiles = join_head_det_agg.join(quantiles,["ORDER_DOW"],"inner")
client_quantiles2 = client_quantiles.withColumn("segmentation", when(((col("DAYS_SINCE_PRIOR_ORDER") <= 7) & ( col("QTY") > col("third"))), lit("You've Got a Friend in Me")).\
when(( ((col("DAYS_SINCE_PRIOR_ORDER")  >= 10) & (col("DAYS_SINCE_PRIOR_ORDER") <= 19)) & (col("QTY") > col("second")) ), lit("Baby come Back")).\
when(( (col("DAYS_SINCE_PRIOR_ORDER")  > 20) & (col("QTY") > col("first")) ), lit("Special Offers")).otherwise("Unrated"))
##FINAL CLIENTS
client_final = client_behaviur.join(client_quantiles2.select("USER_ID","segmentation"),["USER_ID"],"inner").\
withColumnRenamed("is_mom","MOM").withColumnRenamed("is_single","SINGLE").withColumnRenamed("is_pet_friendly","PET_FRIENDLY").\
withColumnRenamed("complete_mystery","COMPLETE_MYSTERY").withColumnRenamed("segmentation","SEGMENTATION")
##FINAL DETAILS
det_final = det_united.select("ORDER_ID","PRODUCT","aisle","department","QTY").orderBy(col("ORDER_ID").asc()).withColumn("QTY", col("QTY").cast(IntegerType())).\
withColumnRenamed("aisle","AISLE").withColumnRenamed("department","DEPARTMENT")
##HEAD FINAL
head_final = head.join(det_final.groupBy("ORDER_ID").agg(sum("QTY").cast(IntegerType()).alias("TOTAL_QTY")),["ORDER_ID"],"inner").orderBy(col("ORDER_ID").asc()).\
withColumn("ORDER_DOW", col("ORDER_DOW").cast(IntegerType())).withColumn("DAYS_SINCE_PRIOR_ORDER", col("DAYS_SINCE_PRIOR_ORDER").cast(IntegerType()))

client_final.show(10,False)
##ORDER_DETAIL
det_final.show(10,False)
##ORDER HEADER
head_final.show(10,False)

##DASHBOARD
##The mix of aisles with more products vs products sold

aisle_prod = final_products.groupBy("aisle").agg(count("product_name").alias("counted_products"))
aisle_prod2 = det_final.groupBy("aisle").agg(sum("QTY").alias("counted_products"))

##The days of the week with the highest number of orders processed
days_order = head_final.groupBy("ORDER_DOW").agg(count("ORDER_ID").alias("orders_processed"))
days_order2 = days_order.orderBy(col("orders_processed").desc())
days_order3 = number_for_day(days_order2).select("DAY","orders_processed")

##View the number of orders per day and hour
orders_for_day_hour = head_final.groupBy("ORDER_DOW","ORDER_HOUR_OF_DAY").agg(count("ORDER_ID").alias("orders_processed"))
orders_for_day_hour2 = number_for_day(orders_for_day_hour)

##Client Segmentation overview (for the Marketing and Customer LoyaltyDepartments)
loyalty = client_final.groupBy("SEGMENTATION").agg(count("USER_ID").alias("USERS"),sum("MOM").alias("MOM"), \
sum("SINGLE").alias("SINGLE"), sum("PET_FRIENDLY").alias("PET_FRIENDLY"), sum("COMPLETE_MYSTERY").alias("COMPLETE_MYSTERY"))

marketing = client_final.agg(sum("MOM").alias("MOM"), sum("SINGLE").alias("SINGLE"), sum("PET_FRIENDLY").alias("PET_FRIENDLY"), sum("COMPLETE_MYSTERY").alias("COMPLETE_MYSTERY"))
##MOM
ms = client_final.filter((col("MOM")==1) & (col("SINGLE")==1))
mp = client_final.filter((col("MOM")==1) & (col("PET_FRIENDLY")==1))
##SINGLE
sp = client_final.filter((col("SINGLE")==1) & (col("PET_FRIENDLY")==1))
marketing2 = marketing.withColumn("MOM_SINGLE", lit(ms.count())).withColumn("MOM_PET_FRIENDLY", lit(mp.count())).withColumn("SINGLE_PET_FRIENDLY", lit(sp.count()))
marketing3 = marketing2.withColumn("MOM", col("MOM")-col("MOM_PET_FRIENDLY")).withColumn("SINGLE", col("SINGLE")-col("SINGLE_PET_FRIENDLY")).withColumn("PET_FRIENDLY", col("PET_FRIENDLY")-col("MOM_PET_FRIENDLY")-col("SINGLE_PET_FRIENDLY"))

print("ALL OK")

"""
##CONECTION AND SAVE... PENDING
jdbcHostname = "orderservers.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "orderdb"
properties = {"user" : "etlguest","password" : "Etltest_2020" }

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
head_final1 = DataFrameWriter(head_final)
head_final1.jdbc(url=url, table= "ORDER_HEADER", mode ="overwrite", properties = properties)

det_final1 = DataFrameWriter(det_final)
det_final1.jdbc(url=url, table= "ORDER_DETAIL", mode ="overwrite", properties = properties)

client_final1 = DataFrameWriter(client_final)
client_final1.jdbc(url=url, table= "CLIENT", mode ="overwrite", properties = properties)

"""
