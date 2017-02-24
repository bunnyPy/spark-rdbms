from pyspark import SparkContext
sc = SparkContext(appName = "test")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
sqlContext.sql("use default")

from pyspark.sql import Row
import json


# write mysql data from djangodb database table name auth_user into hdfs


#read json file
df=sqlContext.read.json("/oracle/*.json")
df.show()
df.printSchema()
df.write.jdbc(url="jdbc:mysql://localhost:3306/bunnydb?rewriteBatchedStatements=true&user=root&password=root", table = "oraclebatch", mode="append")
