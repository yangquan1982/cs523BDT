# cs523BDT
cs523 Big Data Technology Final Project.

## Describe
The basic idea of the project is to get data from twitter, deal with it with spark stream, proceed some simple analysis, then store the result to hive, then extract data and group by using spark SQL. Finally, virtualization the final result with Plotly dynimaticlly.

## Environment 

All the development work is done on Ubuntu 16.04. We use zookepper, kafka, spark, hive, plotly to intergrate the whole project, which all are running under pseudo mode. The main programe is writtern by python.

### Requirments
- Anaconda2(python 2.7.14)
- Zookeeper 3.4.5
- Kafka 2.11-1.0.0
- Spark 1.6.0
- Plotly 2.5.0

## How it work
Firstly, you need to be sure all the components are correctly installed and started, mainly focus on kafka and hive.

### Get the data
Under the project directory, run the getData.py script:
``` 
$ python Code/getData.py
```
This script will use twitter real-time streaming API to get real-time tweets, the filter condition is set to "AI". It will proceed the tweets into kafka while writing to a local json file.

If you're offline, you also can use a tweets file to generate data in Kafka. For example, we already have a json file in Data directory as tweets_AI.json, use the order to push the data to Kafka:
```
$ cat Data/tweets_AI.json | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic tweets
```
### The main programe
The main programe sparkKafka.py need to initialize by spark:
```
$  $SPARK_HOME/spark-submit --packages org.apache.spark:spark-streaming-kafka-0–8_2.11:1.6.0 Codes/sparkKafka.py
```
When it's running, it gets data from kafka and parse data to count hashtags, you will see some simple result in the terminal window. Since the spark streaming batch interval is set to 10s, the interval analysis result is stored to hive for further analysis. In the mean time, we extract data from hive and proceed further analysis using spark SQL, then virtualizing the final result by Plotly. In our main programe, the spark SQL will analysis the history data, and update the data virtualization every 10s with the latest data.