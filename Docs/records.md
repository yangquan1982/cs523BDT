
## Use system tools to configure zookeeper and kafka
https://www.cnblogs.com/vipzhou/p/7235625.html

### Start zookeeper and kafka
Entry the kafka install dir:
```
sudo bin/zookeeper-server-start.sh config/zookeeper.properties
sudo bin/kafka-server-start.sh config/server.properties
```

## Prepare python environment
```
pip install pyspark
pip install kafka-python
pip install python-twitter
pip install tweepy
```

## Test kafka and spring streaming work together
```
/usr/lib/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0–8_2.11:1.6.0 ~/bdt/assignments/cs523BDT/Codes/sparkKafka.py
```
There are two problems, one is package org.apache.spark:spark-streaming-kafka-0–8_2.11:1.6.0 cannot be found, manually download it, then put it down to .m2 relevant directory--according to the error message; the other is the Spark version lower than 2.1.0 doesn't support python 3.6, so I have to reinstall anaconda2.


## Hive 
In order to easily proceed the data visualization, we need to store the spark streaming data into hive table and then use spark sql and data frame.

### How to config hive to use mysql
https://dzone.com/articles/how-configure-mysql-metastore

## Spark SQL
Since we use spark dataframe to store the data into hive, we can use the same sqlContext to proceed spark SQL on hive.

## Plotly
As for the data visualization, we use plotly to do it. Although, you need to notice that plotly has two modes, online and offline. We choose the offline mode to make it work quicker.
If you want to use online mode,you need to apply a Plot.ly account and the plot will be upload to you online account.

