
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
There are two problems, one is package org.apache.spark:spark-streaming-kafka-0–8_2.11:1.6.0 cannot be found, manully download it, then put it down to .m2 relevent directory--acroding to the error message; the other is the Spark version lowwer than 2.1.0 doesn't support python 3.6, so I have to reinstall anaconda2.


## Hive Table Design
In order to easily proceed the data virtualization, we need to store the spark streaming data into hive table and then use spark sql and data frame.
the first version of the hive table would be as follows:
    Table name: tweets.
    Columns: id, userId, source, lang, timestamp

### How to config hive to use mysql
https://dzone.com/articles/how-configure-mysql-metastore


