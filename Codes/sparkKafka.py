#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os  
import re
import json  
import pandas as pd
import matplotlib.pyplot as plt
import plotly as py
import plotly.graph_objs as go

from pyspark import SparkContext  
from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils  
from pyspark.sql import Row, SQLContext
from pyspark.sql import HiveContext
from tweet_parser.tweet import Tweet

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def getHiveContextInstance(sparkContext):
    if ('hiveContextSingletonInstance' not in globals()):
        globals()['hiveContextSingletonInstance'] = HiveContext(sparkContext)
    return globals()['hiveContextSingletonInstance']


def getLangsCount(tweets):
    # Count the langs
    langsCount = tweets.map(lambda tweet: tweet.get('lang')) \
        .filter(lambda s: s != None) \
        .map(lambda s: (s,1)) \
        .reduceByKey(lambda x,y: x + y) 

    langsCount.pprint()

    return langsCount

def storeLangsToHive(tweets, rdd):
    # Get the singleton instance of SparkSession
    sqlContext = getHiveContextInstance(rdd.context)
    # hiveContext = getHiveContextInstance(rdd.context)

    # Convert RDD[String, Integer] to RDD[Row] to DataFrame
    rowRdd = rdd.map(lambda t: Row(lang=t[0], count=t[1]))
    df_langscount = sqlContext.createDataFrame(rowRdd)

    # sqlContext.registerDataFrameAsTable(df_langscount, "langscount")
    df_langscount.write.mode('append').saveAsTable("langscount")
    df2 = sqlContext.sql("select lang, sum(count) as cnt \
        from langscount group by lang order by cnt desc")
    df3 = df2.toPandas()
    data = go.Data([go.Bar(x=df3['lang'],y=df3['cnt'])])
    layout = go.Layout(xaxis=dict(autorange=True))
    fig = go.Figure(data=data, layout=layout)
    py.offline.plot(fig, filename="/home/jason/bdt/assignments/cs523BDT/DataVirtual/langscount_every_10_sec.html")
    sqlContext.sql("select lang, sum(count) as cnt \
        from langscount group by lang order by cnt desc limit 5").show()

def getHashtags(tweets):
    # Count the hashtags
    hashtags = parsed.filter(lambda t: t.get('lang') == 'en') \
        .map(lambda tweet: tweet.get('entities')) \
        .filter(lambda e: e != None) \
        .map(lambda e: e.get('hashtags')) \
        .flatMap(lambda a: a[:]) \
        .filter(lambda d: all(ord(c) < 128 for c in d.get('text').encode('utf-8'))) \
        .map(lambda d: d.get('text').encode('utf-8')) \
        .map(lambda s: (s,1)) \
        .reduceByKey(lambda x,y: x + y) 
    hashtags.pprint()
    ## We can also use 
    ## .filter(lambda d: d.get('text').encode('utf-8').isalpha()) 
    ## if we want to only count the englist words

    return hashtags

def storeHashtagsToHive(tweets, rdd):
    # Get the singleton instance of SparkSession
    sqlContext = getHiveContextInstance(rdd.context)

    # Convert RDD[String, Integer] to RDD[Row] to DataFrame
    rowRdd = rdd.map(lambda t: Row(hashtags=t[0], count=t[1]))
    df_langscount = sqlContext.createDataFrame(rowRdd)
    df_langscount.write.mode('append').saveAsTable("hashtags")
    df2 = sqlContext.sql("select hashtags, sum(count) as cnt \
        from hashtags group by lang order by cnt desc")
    df3 = df2.toPandas()
    data = go.Data([go.Bar(x=df3['hashtags'],y=df3['cnt'])])
    layout = go.Layout(xaxis=dict(autorange=True))
    fig = go.Figure(data=data, layout=layout)
    py.offline.plot(fig, filename="/home/jason/bdt/assignments/cs523BDT/DataVirtual/hashtags_every_10_sec.html")
    sqlContext.sql("select hashtags, sum(count) as cnt \
        from hashtags group by lang order by cnt desc limit 5").show()


def getSourcesCount(tweets):
    p = r'.+>(.+?)<.+'
    # Count the sources
    sourcesCount = tweets.map(lambda t: t.get('source')) \
          .filter(lambda s: s != None) \
          .map(lambda s: re.findall(p, s)[0]) \
          .map(lambda s: (s,1)) \
          .reduceByKey(lambda x,y: x + y)
    
    sourcesCount.pprint()

    return sourcesCount

def storeSourceToHive(tweets, rdd):
    # Get the singleton instance of SparkSession
    sqlContext = getHiveContextInstance(rdd.context)

    # Convert RDD[String, Integer] to RDD[Row] to DataFrame
    rowRdd = rdd.map(lambda t: Row(source=t[0], count=t[1]))
    df_langscount = sqlContext.createDataFrame(rowRdd)
    df_langscount.write.mode('append').saveAsTable("source")
    df2 = sqlContext.sql("select source, sum(count) as cnt \
        from source group by lang order by cnt desc")
    df3 = df2.toPandas()
    data = go.Data([go.Bar(x=df3['source'],y=df3['cnt'])])
    layout = go.Layout(xaxis=dict(autorange=True))
    fig = go.Figure(data=data, layout=layout)
    py.offline.plot(fig, filename="/home/jason/bdt/assignments/cs523BDT/DataVirtual/source_every_10_sec.html")
    sqlContext.sql("select hashtags, sum(count) as cnt \
        from source group by lang order by cnt desc limit 5").show()

if __name__ == "__main__":

    sc = SparkContext(appName="CS523FinalProject")  
    sc.setLogLevel("ERROR")
    sc.setSystemProperty("hive.metastore.uris", "")
    ssc = StreamingContext(sc, 10)

    kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 'Spark-Streaming', {'tweets':1})
    parsed = kvs.map(lambda v: json.loads(v[1]))

    langscount = getLangsCount(parsed)
    langscount.foreachRDD(storeLangsToHive)
    
    hashtags = getHashtags(parsed)
    hashtags.foreachRDD(storeHashtagsToHive)

    sources = getSourcesCount(parsed)
    sources.foreachRDD(storeSourceToHive)

    ssc.start()  
    ssc.awaitTermination() 
