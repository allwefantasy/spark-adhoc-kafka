package org.apache.spark.sql.kafka010

import java.{util => ju}

import org.apache.spark.SparkContext

/**
  * 2019-06-25 WilliamZhu(allwefantasy@gmail.com)
  */
class AdHocKafkaSourceRDD(
                           sc: SparkContext,
                           executorKafkaParams: ju.Map[String, Object],
                           offsetRanges: Seq[KafkaSourceRDDOffsetRange],
                           pollTimeoutMs: Long,
                           failOnDataLoss: Boolean,
                           reuseKafkaConsumer: Boolean) extends KafkaSourceRDD(
  sc: SparkContext,
  executorKafkaParams: ju.Map[String, Object],
  offsetRanges: Seq[KafkaSourceRDDOffsetRange],
  pollTimeoutMs: Long,
  failOnDataLoss: Boolean,
  reuseKafkaConsumer: Boolean) {
  

}
