#!/usr/bin/env python

import os  
import json  
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:1.6.0 pyspark-shell'  
#    Spark
from pyspark import SparkContext  
from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils  
from tweet_parser.tweet import Tweet

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")  
sc.setLogLevel("WARN")  
ssc = StreamingContext(sc, 10)

kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 'Spark-Streaming', {'tweets':1})
parsed = kvs.map(lambda v: json.loads(v[1]))
keys = parsed.map(lambda tweet: tweet.keys())
values = parsed.map(lambda tweet: tweet.values())
keys.pprint()
values.pprint()
# text_counts = parsed.map(lambda tweet: (tweet['text'],1)).reduceByKey(lambda x,y: x + y)
# text_counts.pprint()

# author_counts = parsed.map(lambda tweet: (tweet['user']['screen_name'],1)).reduceByKey(lambda x,y: x + y)
# author_counts.pprint()
# parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()  
# authors_dstream = parsed.map(lambda tweet: tweet['user']['screen_name']) 
# author_counts = authors_dstream.countByValue()  
# author_counts.pprint() 

# author_counts_sorted_dstream = author_counts.transform((lambda foo:foo.sortBy(lambda x:( -x[1]))))
# author_counts_sorted_dstream.pprint()

# top_five_authors = author_counts_sorted_dstream.transform(lambda rdd:sc.parallelize(rdd.take(5)))
# top_five_authors.pprint() 

# filtered_authors = author_counts.filter(lambda x:x[1]>1 or x[0].lower().startswith('rm'))

# filtered_authors.transform(lambda rdd:rdd.sortBy(lambda x:-x[1])).pprint()

ssc.start()  
ssc.awaitTermination() 