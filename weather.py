import urllib2
wiki ="https://api.darksky.net/forecast/9b341e2d7bbd90ed10eaf590cabcc688/28.6466773,76.813073"
page = urllib2.urlopen(wiki)
from bs4 import BeautifulSoup
soup = BeautifulSoup(page)


from pyspark.sql import SQLContext
sqlContext=SQLContext(sc)


json=sc.parallelize(soup.p.string)
json
ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:475
df=sqlContext.read.json(json)

from pyspark.sql.functions import explode

df1.select(explode("icon")).select("col").show()
df = pd.DataFrame.from_dict(weather['daily'][0],orient='index').transpose()
