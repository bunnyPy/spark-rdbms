from pyspark import SparkContext
sc = SparkContext(appName = "test")
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
sqlContext.sql("use default")
from pyspark.sql import Row
import json


dataframe_mysql = sqlContext.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/bunnydb",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "discounts",
    user="root",
    password="root").load()
print (dataframe_mysql)

dataframe_mysql.write.format("json").save("/mysql/discounts")

