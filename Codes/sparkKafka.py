#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os  
import re
import json  
import matplotlib.pyplot as plt
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:1.6.0 pyspark-shell'  
#    Spark
from pyspark import SparkContext  
from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils  
from tweet_parser.tweet import Tweet

sc = SparkContext(appName="CS523FinalProject")  
sc.setLogLevel("WARN")  
ssc = StreamingContext(sc, 60)

kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 'Spark-Streaming', {'tweets':1})
parsed = kvs.map(lambda v: json.loads(v[1]))


# keys = parsed.map(lambda tweet: tweet.keys())
# keys.pprint()
# values = parsed.map(lambda tweet: tweet.values())
# values.pprint()   
# entities = parsed.map(lambda t: t['entities'])
# entities.pprint()

# Count the langs
langsCount = parsed.map(lambda tweet: tweet.get('lang')) \
    .filter(lambda s: s != None) \
    .map(lambda s: (s,1)) \
    .reduceByKey(lambda x,y: x + y) 

langsCount.pprint()

# data = {
# 'bins': langsCount[0][:-1],
# 'freq': langsCount[1]
# }
# plt.bar(data['bins'], data['freq'], width=2000)
# plt.title('Histogram of \'balance\'')

# Count the hashtags
hashtags = parsed.filter(lambda t: t.get('lang') == 'en') \
    .map(lambda tweet: tweet.get('entities')) \
    .filter(lambda e: e != None) \
    .map(lambda e: e.get('hashtags')) \
    .flatMap(lambda a: a[:]) \
    .filter(lambda d: d.get('text').encode('utf-8').isalpha()) \
    .map(lambda d: d.get('text').encode('utf-8')) \
    .map(lambda s: (s,1)) \
    .reduceByKey(lambda x,y: x + y) 
hashtags.pprint()




# text_counts = parsed.map(lambda tweet: (tweet.get('text'),1)).reduceByKey(lambda x,y: x + y)
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