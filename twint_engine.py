import twint
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import *

conf = twint.Config()
conf.Search = "Mumbai"
conf.Hide_output = True
conf.Pandas = True
conf.Limit = 10

tweets = twint.run.Search(conf)

columns = twint.storage.panda.Tweets_df.columns

df = twint.storage.panda.Tweets_df[['hashtags', 'tweet']]

sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')
spark = SparkSession(sc)
hasht = sc.parallelize(df['hashtags'])
hasht = htags.reduce(lambda x, y: x+y)
thasht = spark.createDataFrame(hasht, StringType()).createTempView('data')
spark.sql('SELECT value, count(value) As tag_count from data group by value order by tag_count DESC LIMIT 5').show()
sc.stop()
