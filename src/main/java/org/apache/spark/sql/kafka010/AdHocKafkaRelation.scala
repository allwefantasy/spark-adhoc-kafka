package org.apache.spark.sql.kafka010

import java.util.UUID

import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.unsafe.types.UTF8String


class AdHocKafkaRelation(
                          override val sqlContext: SQLContext,
                          strategy: ConsumerStrategy,
                          sourceOptions: Map[String, String],
                          specifiedKafkaParams: Map[String, String],
                          failOnDataLoss: Boolean,
                          startingOffsets: KafkaOffsetRangeLimit,
                          endingOffsets: KafkaOffsetRangeLimit)
  extends BaseRelation with TableScan with Logging {
  assert(startingOffsets != LatestOffsetRangeLimit,
    "Starting offset not allowed to be set to latest offsets.")
  assert(endingOffsets != EarliestOffsetRangeLimit,
    "Ending offset not allowed to be set to earliest offsets.")

  private val pollTimeoutMs = sourceOptions.getOrElse(
    "kafkaConsumer.pollTimeoutMs",
    (sqlContext.sparkContext.conf.getTimeAsSeconds(
      "spark.network.timeout",
      "120s") * 1000L).toString
  ).toLong

  override def schema: StructType = KafkaOffsetReader.kafkaSchema

  override def buildScan(): RDD[Row] = {
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = s"spark-kafka-relation-${UUID.randomUUID}"

    val kafkaOffsetReader = new KafkaOffsetReader(
      strategy,
      KafkaSourceProvider.kafkaParamsForDriver(specifiedKafkaParams),
      sourceOptions,
      driverGroupIdPrefix = s"$uniqueGroupId-driver")

    // Leverage the KafkaReader to obtain the relevant partition offsets
    val (fromPartitionOffsets, untilPartitionOffsets) = {
      try {
        (getPartitionOffsets(kafkaOffsetReader, startingOffsets),
          getPartitionOffsets(kafkaOffsetReader, endingOffsets))
      } finally {
        kafkaOffsetReader.close()
      }
    }

    // Obtain topicPartitions in both from and until partition offset, ignoring
    // topic partitions that were added and/or deleted between the two above calls.
    if (fromPartitionOffsets.keySet != untilPartitionOffsets.keySet) {
      implicit val topicOrdering: Ordering[TopicPartition] = Ordering.by(t => t.topic())
      val fromTopics = fromPartitionOffsets.keySet.toList.sorted.mkString(",")
      val untilTopics = untilPartitionOffsets.keySet.toList.sorted.mkString(",")
      throw new IllegalStateException("different topic partitions " +
        s"for starting offsets topics[${fromTopics}] and " +
        s"ending offsets topics[${untilTopics}]")
    }

    val multiplyFactor = sourceOptions.getOrElse(AdHocKafkaSourceProvider.MULTIPLY_FACTOR, "-1").toLong
    val maxSizePerPartition = sourceOptions.getOrElse(AdHocKafkaSourceProvider.MAX_SIZE_PER_PARTITION, "-1").toLong

    def generateOffsetRange(fromOffset: Long, untilOffset: Long): Seq[(Long, Long)] = {

      val defafultValue = Seq((fromOffset, untilOffset))


      if (multiplyFactor != -1) {
        val step = (untilOffset - fromOffset) / multiplyFactor
        if (step == 0) return defafultValue
        // when 
        (fromOffset until untilOffset by step) map { i =>
          (i, Math.min(i + step, untilOffset))
        }
      } else if (maxSizePerPartition != -1) {
        val maxRange = untilOffset - fromOffset
        val step = Math.max(maxRange, maxSizePerPartition)

        if (step == 0) return defafultValue

        (fromOffset until untilOffset by step) map { i =>
          (i, Math.min(i + step, untilOffset))
        }
      } else {
        defafultValue
      }

    }

    // Calculate offset ranges
    val offsetRanges = untilPartitionOffsets.keySet.flatMap { tp =>
      val fromOffset = fromPartitionOffsets.get(tp).getOrElse {
        // This should not happen since topicPartitions contains all partitions not in
        // fromPartitionOffsets
        throw new IllegalStateException(s"$tp doesn't have a from offset")
      }
      val untilOffset = untilPartitionOffsets(tp)

      generateOffsetRange(fromOffset, untilOffset).map { offset =>
        KafkaSourceRDDOffsetRange(tp, offset._1, offset._2, None)
      }

    }.toArray

    logInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))

    // Create an RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val executorKafkaParams =
      KafkaSourceProvider.kafkaParamsForExecutors(specifiedKafkaParams, uniqueGroupId)
    val rdd = new KafkaSourceRDD(
      sqlContext.sparkContext, executorKafkaParams, offsetRanges,
      pollTimeoutMs, failOnDataLoss, reuseKafkaConsumer = false).map { cr =>
      InternalRow(
        cr.key,
        cr.value,
        UTF8String.fromString(cr.topic),
        cr.partition,
        cr.offset,
        DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(cr.timestamp)),
        cr.timestampType.id)
    }
    sqlContext.internalCreateDataFrame(rdd.setName("kafka"), schema).rdd
  }

  private def getPartitionOffsets(
                                   kafkaReader: KafkaOffsetReader,
                                   kafkaOffsets: KafkaOffsetRangeLimit): Map[TopicPartition, Long] = {
    def validateTopicPartitions(partitions: Set[TopicPartition],
                                partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
      assert(partitions == partitionOffsets.keySet,
        "If startingOffsets contains specific offsets, you must specify all TopicPartitions.\n" +
          "Use -1 for latest, -2 for earliest, if you don't care.\n" +
          s"Specified: ${partitionOffsets.keySet} Assigned: ${partitions}")
      logDebug(s"Partitions assigned to consumer: $partitions. Seeking to $partitionOffsets")
      kafkaReader.fetchSpecificOffsets(partitionOffsets, (data) => {}).partitionToOffsets
    }

    val partitions = kafkaReader.fetchTopicPartitions()
    // Obtain TopicPartition offsets with late binding support
    kafkaOffsets match {
      case EarliestOffsetRangeLimit => kafkaReader.fetchEarliestOffsets()
      case LatestOffsetRangeLimit => kafkaReader.fetchLatestOffsets(None)
      case SpecificOffsetRangeLimit(partitionOffsets) =>
        validateTopicPartitions(partitions, partitionOffsets)
    }
  }

  override def toString: String =
    s"KafkaRelation(strategy=$strategy, start=$startingOffsets, end=$endingOffsets)"
}