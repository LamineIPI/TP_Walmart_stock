#!/usr/bin/env python
# coding: utf-8

# Importation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Instantiation
spark = SparkSession.builder.master("local").appName("FolksDF").getOrCreate()


# Q1 : Importation des fichiers
Walmart = spark.read.option("header",'true')\
                    .csv("Input/walmart_stock.csv")
Walmart.show(5)


# Q2 : afficher les colonnes d'un data frame
Walmart.columns


# Q3 : le schéma
Walmart.printSchema() # nullable = true : autorise les valeurs nulles


# Q4 : Create a new dataframe with a column called HV_Ratio that is the ratio of the High Price versus volume of stock traded for a day

Walmart2 = Walmart.withColumn("HV_Ratio", F.col("High")/F.col("Volume"))
Walmart2.head() # juste la premiere ligne
Walmart2.show(4) # les 4 premieres lignes


# Q5 : What day had the Peak High in Price?

# SQL
Walmart2.createOrReplaceTempView("WalmartSQL") # transformation du data frame en table !!!!
#spark.sql("""select Date from WalmartSQL order by High desc""").first() # solution 1
spark.sql("""select Date from WalmartSQL order by High desc limit 1""").show() # solution 2

# DSL donc en spark
# Walmart2.orderBy(F.col("High").desc()).select(F.col("Date")).head() # solution 1
Walmart2.select(F.col("Date"))\
        .orderBy(F.col("High").desc())\
        .head() # solution 1 bis ( ressemble au SQL)


# Q6 : What is the mean of the Close column?

# SQL
spark.sql("""select mean(Close) as Moyenne from WalmartSQL""").show()

# DSL
# Walmart2.select("Close") \
#        .summary("mean") \
#        .show()  # solution 1

Walmart2.agg(F.mean("Close").alias("Moyenne")).show() # solution 2 (on peut changer mean par avg)


# Q 7 : What is the max and min of the Volume column?
# SQL
spark.sql("""select max(Volume), min(Volume) from WalmartSQL""").show()

# DSL
Walmart2.agg(F.max("Volume"),F.min("Volume"))\
        .show()


# Q 8 : How many days was the Close lower than 60 dollars?
# SQL
spark.sql("""select count(Date) from WalmartSQL where Close < '60' """).show()

# DSL
#Walmart2.filter(F.col("Close") < '60') \
#        .agg(F.count("Date")) \ # faire l'aggregation apres le filter
#        .show()  # solution 1

Walmart2.filter(F.col("Close") < '60')\
        .count()   # solution 2


# Q 10 : What percentage of the time was the High greater than 80 dollars ?(In other words, (Number of Days High>80)/(Total Days in the dataset))
# SQL
spark.sql(""" select round((select count(*) from WalmartSQL
                where High>='80')/(count(*)) * 100, 2 ) as Percentage  from WalmartSQL """).show()

# DSL
Temp = Walmart2.filter(F.col("High")>'80')\
               .agg(F.count("*").alias("Comptage"))\
               .collect()[0][0] 
Walmart2.agg(F.round((Temp/F.count("*")*100),2).alias("Percentage"))\
        .show()


# Q 11 : What is the max High per year?
# SQL
spark.sql(""" select max(High) as max_High, substr(Date,1,4) as Annee
                                           from WalmartSQL group by Annee """).show() 

# DSL
Walmart2.groupBy(F.year("Date").alias("Année"))\
         .agg(F.max("High").alias("Max_High"))\
         .sort(F.year("Date"))\
         .show()


# Q12- What is the average Close for each Calendar Month? In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months.
# SQL
spark.sql("""select month(Date) as Month, mean(Close) as Mean_Close 
                        from WalmartSQL group by Month order by Month""").show()

# DSL
Walmart2.groupBy(F.month("Date").alias("Month"))\
        .agg(F.mean(F.col("Close")).alias("Mean_Close"))\
        .orderBy("Month")\
        .show()

