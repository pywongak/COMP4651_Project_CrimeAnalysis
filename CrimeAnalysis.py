# Databricks notebook source
# MAGIC %md # Import some useful computing libraries

# COMMAND ----------

from __future__ import print_function

from datetime import datetime, date, timedelta
import csv
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import udf

# COMMAND ----------

# MAGIC %md # Useful commands

# COMMAND ----------

# Close all figures to release memory
plt.close("all")

# COMMAND ----------

# MAGIC %md # Define some constants here

# COMMAND ----------

# Interval we are interested
STARTTIME = datetime(2014, 1, 1, 0, 0, 0)
ENDTIME = datetime(2017, 5, 1, 0, 0, 0)

# The following constants are derviated from the above constants
STARTDATE = STARTTIME.date()
ENDDATE = ENDTIME.date()

# COMMAND ----------

# MAGIC %md # Get Data that stored in S3

# COMMAND ----------

ACCESSY_KEY_ID = u"AKIAI7XH3QZ3MFK54IKQ"
SECERET_ACCESS_KEY = u"Vte/7rTdng469SgbsKkkSTNFH1Tij3CGonVU4hhI"

mounts_list = [
{'bucket':'comp4651aaron-us/', 'mount_folder':'/mnt/comp4651'}
]

print("This cell contains a secret")

# COMMAND ----------

for mount_point in mounts_list:
  bucket = mount_point['bucket']
  mount_folder = mount_point['mount_folder']
  try:
    dbutils.fs.unmount(mount_folder)
  except:
    pass
  finally: #If MOUNT_FOLDER does not exist
    import urllib
    dbutils.fs.mount("s3a://"+ urllib.quote_plus(ACCESSY_KEY_ID) + ":" + urllib.quote_plus(SECERET_ACCESS_KEY) + "@" + bucket,mount_folder)

# COMMAND ----------

# MAGIC %fs ls /mnt/comp4651

# COMMAND ----------

# MAGIC %md Import csv file into raw dataframes

# COMMAND ----------

datafileMC = '/mnt/comp4651/Crime.fixed.csv'
datafileSF = '/mnt/comp4651/Police_Department_Incidents.csv'

dfMCraw = spark.read.csv(datafileMC, header=True, inferSchema=True)
dfSFraw = spark.read.csv(datafileSF, header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md Let's take a look of the original columns and a few rows in the raw dataframes

# COMMAND ----------

print("MC")
dfMCraw.printSchema()
print(dfMCraw.take(2))

print("SF")
dfSFraw.printSchema()
print(dfSFraw.take(2))

# COMMAND ----------

catemapFileMC = 'dbfs:/mnt/comp4651/mtCrime.csv'
catemapFileSF = 'dbfs:/mnt/comp4651/sfCrime.csv'

def loadCatemapMC(csvfile):
  from HTMLParser import HTMLParser
  htmlparser = HTMLParser()
  
  schema = StructType([
    StructField("Class_Description", StringType(), False),
    StructField("count", IntegerType(), False),
    StructField("New_Class", StringType(), False),
  ])
  
  df = spark.read.csv(csvfile, header=True, schema=schema)
  pairs = df.select("Class_Description", "New_Class").collect()
  
  catemap = {htmlparser.unescape(s["Class_Description"]): htmlparser.unescape(s["New_Class"]) for s in pairs}
  return catemap

def loadCatemapSF(csvfile):
  from HTMLParser import HTMLParser
  htmlparser = HTMLParser()
  
  schema = StructType([
      StructField("Category", StringType(), False),
      StructField("Descript", StringType(), False),
      StructField("count", IntegerType(), False),
      StructField("New_Class", StringType(), False),
    ])
  
  df = spark.read.csv(csvfile, header=True, schema=schema)
  pairs = df.select("Category", "Descript", "New_Class").collect()
  catemap = {(htmlparser.unescape(s["Category"]), htmlparser.unescape(s["Descript"])): htmlparser.unescape(s["New_Class"]) for s in pairs}
  return catemap

catemapMC = loadCatemapMC(catemapFileMC)
catemapSF = loadCatemapSF(catemapFileSF)

# COMMAND ----------

# Find un-mapped category
# MC
setA_MC = set(dfMCraw.rdd.map(lambda s: s["Class Description"]).distinct().collect())
setB_MC = set(catemapMC.keys())
print(setA_MC.difference(setB_MC), sep='\n')
print(setB_MC.difference(setA_MC), sep='\n')

# COMMAND ----------

# Find un-mapped category
# SF
setA_SF = set(dfSFraw.rdd.map(lambda s: (s["Category"], s["Descript"])).distinct().collect())
setB_SF = set(catemapSF.keys())
print(setA_SF.difference(setB_SF), sep='\n')
print(setB_SF.difference(setA_SF), sep='\n')

# COMMAND ----------

# MAGIC %md Selecting the interested columns and transfer the raw dataframes to the normalized dataframes

# COMMAND ----------

schema = StructType([
    StructField("Id", IntegerType(), False),
    StructField("DataSource", StringType(), False),
    StructField("Category", StringType(), True),
    StructField("Datetime", TimestampType(), False),
    StructField("Hour", IntegerType(), False),
    StructField("Month", IntegerType(), False),
    StructField("Year", IntegerType(), False),
    StructField("DayOfWeek", IntegerType(), False),
    StructField("MonthDelta", IntegerType(), False),
    StructField("TimeCategory", StringType(), False),
  ])

CrimeRow = Row(
  "Id",
  "DataSource",
  "Category",
  "Datetime",
  "Hour",
  "Month",
  "Year",
  "DayOfWeek",
  "MonthDelta",
  "TimeCategory",
)

def toMonthDelta(dt):
  return (dt.year - STARTTIME.year) * 12 + (dt.month - STARTTIME.month)

def toTimeCategory(dt):
  hour = dt.hour
  if 6 <= hour <= 11:
    tc = "Morning"
  elif 12 <= hour <= 17:
    tc = "Afternoon"
  elif 18 <= hour <= 23:
    tc = "Night"
  else:
    tc = "Evening"
  return tc

# COMMAND ----------

def schemaNormalizerMC(s):
  rowId = s["Incident ID"]
  datasource = "MC"
  old_category = s["Class Description"]
  category = catemapMC.get(old_category, None)
  dt = datetime.strptime(s["Dispatch Date / Time"], '%m/%d/%Y %I:%M:%S %p')
  (hour, month, year) = (dt.hour, dt.month, dt.year)
  dayofweek = dt.isoweekday()
  monthDelta = toMonthDelta(dt)
  tc = toTimeCategory(dt)
  return CrimeRow(rowId, datasource, category, dt, hour, month, year, dayofweek, monthDelta, tc)

dfMC = spark.createDataFrame(dfMCraw.rdd.map(schemaNormalizerMC), schema)

# COMMAND ----------

def schemaNormalizerSF(s):
  rowId = s["IncidntNum"]
  datasource = "SF"
  old_category = s["Category"]
  old_descript = s["Descript"]
  category = catemapSF.get((old_category, old_descript), None)
  dt = datetime.strptime("{} {}".format(s["Date"], s["Time"]) , '%m/%d/%Y %H:%M')
  (hour, month, year) = (dt.hour, dt.month, dt.year)
  dayofweek = dt.isoweekday()
  monthDelta = toMonthDelta(dt)
  tc = toTimeCategory(dt)
  return CrimeRow(rowId, datasource, category, dt, hour, month, year, dayofweek, monthDelta, tc)

dfSF = spark.createDataFrame(dfSFraw.rdd.map(schemaNormalizerSF), schema)

# COMMAND ----------

# MAGIC %md Filter the normalized dataframes between 1st, Jan 2014 and 1st, May 2017

# COMMAND ----------

# Filter dataframe and cache the resultant dataframe
def filterDataFrame(df):
  df = df.filter((df["Datetime"] >= STARTTIME) & (df["Datetime"] < ENDTIME) 
                 & (df["Category"] != "NON-CRIMINAL")
                 & (df["Category"] != "AIDED CASE")
                 & (df["Category"] != "SUSPICIOUS OCC")
                 & (df["Category"] != "TRAFFIC VIOLATION")
                 & (df["Category"] != "POL INFORMATION")
                 & (df["Category"] != "OTHER OFFENSES")
                 & (df["Category"] != "MISSING PERSON")
                )
  return df

dfMC = filterDataFrame(dfMC).cache()
dfSF = filterDataFrame(dfSF).cache()

# COMMAND ----------

# Actually this cell is used to test the above code is 100% working
print("After filtering, there are {} rows in MC and {} rows in SF".format(dfMC.count(), dfSF.count()))

# COMMAND ----------


print("Categories are:")
print("\n".join([s["Category"] for s in dfMC.union(dfSF).select("Category").distinct().collect()]))

# COMMAND ----------

# MAGIC %md Take a look of the first and the last row of each dataframe in a table

# COMMAND ----------

def firstAndLastRows(df):
  first = df.orderBy("Datetime", ascending=True).limit(1)
  last = df.orderBy("Datetime", ascending=False).limit(1)
  return first.union(last)

display(firstAndLastRows(dfMC).union(firstAndLastRows(dfSF)).collect())

# COMMAND ----------

from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import udf
from pyspark.sql.types import *

def calYearDelta(year):
  return year - 2014

udfYearDelta = udf(calYearDelta, IntegerType())

catIndexer = StringIndexer(inputCol="Category", outputCol="categoryIndex")
trainedCatIndexer = catIndexer.fit(dfSF.union(dfMC))
indexedSF = trainedCatIndexer.transform(dfSF)
indexedMC = trainedCatIndexer.transform(dfMC)
indexedMC = indexedMC.withColumn("YearDelta", udfYearDelta("Year"))
indexedSF = indexedSF.withColumn("YearDelta", udfYearDelta("Year"))
display(indexedMC.limit(3).union(indexedSF.limit(3)))


# COMMAND ----------

indexedMC.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
def monthCountMap(cat, count):
  i = 0
  while 100*i<=count:
    i += 1
  return cat+" - "+ str(i)

udfMonthCountMap = udf(monthCountMap, StringType())
fpMonthMC = indexedMC.groupBy("MonthDelta","Category").count().withColumn("countMonthMap", udfMonthCountMap("Category", "count")).select("MonthDelta", "countMonthMap").groupBy("MonthDelta").agg(collect_list("countMonthMap").alias("catCount"))


# COMMAND ----------

fpMonthSF = indexedSF.groupBy("MonthDelta","Category").count().withColumn("countMonthMap", udfMonthCountMap("Category", "count")).select("MonthDelta", "countMonthMap").groupBy("MonthDelta").agg(collect_list("countMonthMap").alias("catCount"))

# COMMAND ----------

display(fpMonthMC)
display(fpMonthSF)

# COMMAND ----------

#for x in fpMonthMC:
  #print(x.catCount)
fpMonthMC2 = fpMonthMC.rdd.map(lambda row: row.catCount)#[x.catCount for x in fpMonthMC]


# COMMAND ----------

fpMonthSF2 = fpMonthSF.rdd.map(lambda row: row.catCount)#[x.catCount for x in fpMonthMC]

# COMMAND ----------

from pyspark.mllib.fpm import FPGrowth

modelMonthMC = FPGrowth.train(fpMonthMC2, minSupport=0.7, numPartitions=5)
resultMonthMC = modelMonthMC.freqItemsets()


# COMMAND ----------


modelMonthSF = FPGrowth.train(fpMonthMC2, minSupport=0.7, numPartitions=5)
resultMonthSF = modelMonthSF.freqItemsets()


# COMMAND ----------

monthlyFreqItemsMC = resultMonthMC.collect()

# COMMAND ----------

monthlyFreqItemsSF = resultMonthSF.collect()

# COMMAND ----------

print(monthlyFreqItemsSF)

# COMMAND ----------

monthlyFreqItemsSortMC1 = sorted(monthlyFreqItemsMC, key=lambda x: len(x.items), reverse=True)

# COMMAND ----------

monthlyFreqItemsSortSF1 = sorted(monthlyFreqItemsSF, key=lambda x: len(x.items), reverse=True)

# COMMAND ----------

resultMonthFreqMC1 = []
for i in monthlyFreqItemsSortMC1:
  included = False
  for j in resultMonthFreqMC1:
    if set(i.items).issubset(set(j.items)):
      included = True
  if not included: 
    resultMonthFreqMC1.append(i)


# COMMAND ----------

resultMonthFreqSF1 = []
for i in monthlyFreqItemsSortSF1:
  included = False
  for j in resultMonthFreqSF1:
    if set(i.items).issubset(set(j.items)):
      included = True
  if not included: 
    resultMonthFreqSF1.append(i)


# COMMAND ----------

print(resultMonthFreqSF1)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["Month", "DayOfWeek", "Hour"],#, "YearDelta"],
    outputCol="features")

transformedMC = assembler.transform(indexedMC)
transformedSF = assembler.transform(indexedSF)
display(transformedMC.limit(3))

# COMMAND ----------

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel

prepMC = transformedMC.rdd.map(lambda row: LabeledPoint(row.categoryIndex, Vectors.dense(row.features)))
naiveBayesMC = NaiveBayes.train(prepMC)
prepSF = transformedSF.rdd.map(lambda row: LabeledPoint(row.categoryIndex, Vectors.dense(row.features)))
naiveBayesSF = NaiveBayes.train(prepSF)

# COMMAND ----------

for i in xrange(0,24):
  print(naiveBayesMC.predict(Vectors.dense([2,8,i])))

# COMMAND ----------

transformedSF.filter(transformedSF.categoryIndex == 6).select("Category", "categoryIndex").take(5)

# COMMAND ----------

import pyspark.sql

print("transformedMC.corr", transformedMC.corr("categoryIndex","timeCategoryIndex"))
print("transformedMC.corr", transformedMC.corr("categoryIndex","Month"))
print("transformedMC.corr", transformedMC.corr("categoryIndex","Year"))
print("transformedMC.corr", transformedMC.corr("categoryIndex","DayOfWeek"))
print("transformedMC.cov", transformedMC.cov("categoryIndex","timeCategoryIndex"))
print("transformedMC.cov", transformedMC.cov("categoryIndex","Month"))
print("transformedMC.cov", transformedMC.cov("categoryIndex","Year"))
print("transformedMC.cov", transformedMC.cov("categoryIndex","DayOfWeek"))
print("transformedSF.corr", transformedSF.corr("categoryIndex","timeCategoryIndex"))
print("transformedSF.corr", transformedSF.corr("categoryIndex","Month"))
print("transformedSF.corr", transformedSF.corr("categoryIndex","Year"))
print("transformedSF.corr", transformedSF.corr("categoryIndex","DayOfWeek"))
print("transformedSF.cov", transformedSF.cov("categoryIndex","timeCategoryIndex"))
print("transformedSF.cov", transformedSF.cov("categoryIndex","Month"))
print("transformedSF.cov", transformedSF.cov("categoryIndex","Year"))
print("transformedSF.cov", transformedSF.cov("categoryIndex","DayOfWeek"))
#display(transformedMC.crosstab("categoryIndex","timeCategoryIndex"))
#display(transformedMC.crosstab("categoryIndex","Month"))
#display(transformedMC.crosstab("categoryIndex","Year"))
#display(transformedMC.crosstab("categoryIndex","DayOfWeek"))
#freqItems
#display(transformedMC.freqItems(["categoryIndex","timeCategoryIndex", "Month", "Year", "DayOfWeek"]))
#print(transformedMC.freqItems("categoryIndex","Month"))
#print(transformedMC.freqItems("categoryIndex","Year"))
#print(transformedMC.freqItems("categoryIndex","DayOfWeek"))

# COMMAND ----------

# MAGIC %md Table of top 20 category for the MC.

# COMMAND ----------

display(transformedMC.groupBy("Category").count().orderBy("count", ascending=False).limit(20).collect())

# COMMAND ----------

display(transformedSF.groupBy("Category").count().orderBy("count", ascending=False).limit(20).collect())
