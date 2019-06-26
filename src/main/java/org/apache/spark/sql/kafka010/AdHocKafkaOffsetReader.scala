package org.apache.spark.sql.kafka010


import java.{util => ju}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.util.{ThreadUtils, UninterruptibleThread}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * @author dongbin@guahao.com
  *         19-6-26 上午12:38
  */
class AdHocKafkaOffsetReader(
                              consumerStrategy: ConsumerStrategy,
                              driverKafkaParams: ju.Map[String, Object],
                              readerOptions: Map[String, String],
                              driverGroupIdPrefix: String)
  extends KafkaOffsetReader(consumerStrategy, driverKafkaParams, readerOptions, driverGroupIdPrefix) {

  def fetchStartingOffsetsByTime(timeString: String, timeFormat: String) = {
    getLogTimestampOffsets(convertTimestamp(timeString, timeFormat), true)
  }

  def fetchEndingOffsetsByTime(timeString: String, timeFormat: String) = {
    getLogTimestampOffsets(convertTimestamp(timeString, timeFormat), false)
  }

  def convertTimestamp(timeString: String, timeFormat: String): java.lang.Long = {
    DateTime.parse(timeString, DateTimeFormat.forPattern(timeFormat)).toDate.getTime
  }


  private def getLogTimestampOffsets(timestamp: java.lang.Long, defaultBeginning: Boolean) = runUninterruptibly {
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)

    val (successfulOffsetsForTimes, unsuccessfulOffsetsForTimes) =
      consumer.offsetsForTimes(partitions.asScala.map(_ -> timestamp).toMap.asJava).asScala.partition(_._2 != null)

    val success: Map[TopicPartition, Long] = successfulOffsetsForTimes.map {
      case (topicPartition, offsetAndTimestamp) => topicPartition -> offsetAndTimestamp.offset
    }.toMap

    val topicPartitions = unsuccessfulOffsetsForTimes.keySet.toSeq

    var offsets = consumer.beginningOffsets(topicPartitions.asJava)

    if (!defaultBeginning) {
      offsets = consumer.endOffsets(topicPartitions.asJava)
    }

    val unsuccess: Map[TopicPartition, Long] = topicPartitions.map { topicPartition =>
      val logOffset = offsets.get(topicPartition).toLong
      topicPartition -> logOffset
    }.toMap
    success ++ unsuccess
  }


  def fetchRecentNumOffsets(recent: Long) = runUninterruptibly {
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    val beginningOffsets = consumer.beginningOffsets(partitions).asScala

    consumer.endOffsets(partitions).asScala.map {
      case (topicPartition, endingOffset) => topicPartition -> {
        val targetOffsets = (endingOffset - recent)
        if (beginningOffsets.get(topicPartition).get > targetOffsets) beginningOffsets.get(topicPartition).get.toLong else targetOffsets.toLong
      }
    }.toMap
  }


  private def runUninterruptibly[T](body: => T): T = {
    if (!Thread.currentThread.isInstanceOf[UninterruptibleThread]) {
      val future = Future {
        body
      }(execContext)
      ThreadUtils.awaitResult(future, Duration.Inf)
    } else {
      body
    }
  }
}
