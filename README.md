# Spark AdHoc Kafka Datasource

This is a datasource implementation for quick query in Kafka with Spark. 
You can control the parallelism of data fetching from kafka, and is not limited by 
the original size of kafka partitions.  It is useful especially when you just want 
to filter some data from kafka sometimes and it's not a daily job. It saves you 
a lot of time since the traditional way is consuming kafka and write the data to HDFS/ES first.

## Requirements

This library requires Spark 2.4+ (tested) and Kafka 0.10.0+. 
Some older versions of Spark may work too but they are not officially supported.


## Liking 

You can link against this library in your program at the following coordinates:

## Scala 2.11

```sql
groupId: tech.mlsql
artifactId: spark-adhoc-kafka_2.11
version: 0.1.2

```

## Usage

Spark DataFrame:

```scala
val df = spark
      .read
      .format("org.apache.spark.sql.kafka010.AdHocKafkaSourceProvider")
      .option("kafka.bootstrap.servers","127.0.0.1:9200")
      .option( "startingOffsets", "earliest")
      .option( "endingOffsets" , "latest")                          
      .option("multiplyFactor","2") // the parallelism is  multiplyFactor * num of kafka partitions
      .option("maxSizePerPartition","100000") // the parallelism is  total records/ maxSizePerPartition  
      .option("subscribe", topic)
      .load().selectExpr("CAST(value AS STRING)")

```

The priority of multiplyFactor is higher then maxSizePerPartition.

MLSQL:

```sql
load adHocKafka.`topic` where 
kafka.bootstrap.servers="127.0.0.1:9200"
and multiplyFactor="2" 
as table1;

select count(*) from table1 where value like "%yes%" as output;

```


With Spark AdHoc Kafka, you can use startingOffsets/endingOffsets to restrict the range and speed up the query. 
But in most case, we hope we can query the data within a specific time interval.


Spark DataFrame:

```scala
val df = spark
      .read
      .format("org.apache.spark.sql.kafka010.AdHocKafkaSourceProvider")
      .option("kafka.bootstrap.servers","127.0.0.1:9200")
      .option("multiplyFactor","2") // the parallelism is  multiplyFactor * num of kafka partitions
      .option("maxSizePerPartition","100000") // the parallelism is  total records/ maxSizePerPartition
      .option("timeFormat","yyyyMMdd") 
      .option("startingTime","20170101") 
      .option("endingTime","20180101")   
      .option("subscribe", topic)
      .load().selectExpr("CAST(value AS STRING)")

```

MLSQL:

```sql
load adHocKafka.`topic` where 

kafka.bootstrap.servers="127.0.0.1:9200"

and multiplyFactor="2" 

and timeFormat="yyyyMMdd"
and startingTime="20170101"
and endingTime="20180101"

as table1;

select count(*) from table1 where value like "%yes%" as output;
```


