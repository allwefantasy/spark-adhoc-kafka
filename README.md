# Spark AdHoc Kafka Datasource

This is a datasource implementation for quick querying Kafka with Spark. It's very quick if you just
filter data.

## Requirements

This library requires Spark 2.4+ (tested). Some older versions of Spark may work too but they are not officially supported.

## Liking 

You can link against this library in your program at the following coordinates:

## Scala 2.11

```sql
groupId: tech.mlsql
artifactId: spark-adhoc-kafka_2.11
version: 0.1.0
```

## Usage

Spark DataFrame:

```scala
val df = spark
      .read
      .format("org.apache.spark.sql.kafka010.AdHocKafkaSourceProvider")
      .option("kafka.bootstrap.servers","127.0.0.1:9200")
      .option("multiplyFactor","2") // the parallelism is  multiplyFactor * num of kafka partitions
      .option("maxSizePerPartition","100000") // the parallelism is  total records/ maxSizePerPartition  
      .option("subscribe", topic).load().selectExpr("CAST(value AS STRING)")

```

The priority of multiplyFactor is higher then maxSizePerPartition.

MLSQL:

```sql
load adHocKafka.`topic` where 
kafka.bootstrap.servers="127.0.0.1:9200"
and multiplyFactor="2" 
as table1;

select * from table1 where value like %yes% as output;
```