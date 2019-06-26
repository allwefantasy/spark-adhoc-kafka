package tech.mlsql.test.datasource

import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.kafka010.{KafkaTest, KafkaTestUtils}
import org.apache.spark.sql.test.SharedSQLContext
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

/**
  * 2019-06-25 WilliamZhu(allwefantasy@gmail.com)
  */
class AdhocKafkaSuite extends QueryTest with SharedSQLContext with KafkaTest {

  private val topicId = new AtomicInteger(0)

  private var testUtils: KafkaTestUtils = _

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def assignString(topic: String, partitions: Iterable[Int]): String = {
    JsonUtils.partitions(partitions.map(p => new TopicPartition(topic, p)))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  private def createDF(
                        topic: String,
                        withOptions: Map[String, String] = Map.empty[String, String],
                        brokerAddress: Option[String] = None) = {
    val df = spark
      .read
      .format("org.apache.spark.sql.kafka010.AdHocKafkaSourceProvider")
      .option("kafka.bootstrap.servers",
        brokerAddress.getOrElse(testUtils.brokerAddress))
      .option("subscribe", topic)
    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load().selectExpr("CAST(value AS STRING)")
  }


  test("explicit earliest to latest offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Specify explicit earliest and latest offset values
    var df = createDF(topic,
      withOptions = Map(
        "startingOffsets" -> "earliest",
        "endingOffsets" -> "latest",
        "multiplyFactor" -> "2")
    )
    assert(df.rdd.partitions.size == 5)
    assert(df.count() == 21)

    // Specify explicit earliest and latest offset values
    df = createDF(topic,
      withOptions = Map(
        "startingOffsets" -> "earliest",
        "endingOffsets" -> "latest",
        "multiplyFactor" -> "3")
    )
    assert(df.rdd.partitions.size == 9)
    assert(df.count() == 21)


  }

  test("fetch kafka data by time interval") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Specify explicit earliest and latest offset values
    Thread.sleep(3000)
    val time = DateTime.now()

    testUtils.sendMessages(topic, Array("21"), Some(0))
    testUtils.sendMessages(topic, Array("22"), Some(1))
    testUtils.sendMessages(topic, Array("23", "24"), Some(2))

    var df = createDF(topic,
      withOptions = Map(
        "startingOffsets" -> "earliest",
        "endingOffsets" -> "latest",
        "multiplyFactor" -> "2",
        "timeFormat" -> "yyyyMMdd HHmmss S",
        "startingTime" -> time.toString("yyyyMMdd HHmmss S")
      )
    )
    assert(df.rdd.partitions.size == 4)
    assert(df.count() == 4)

    testUtils.sendMessages(topic, Array("25"), Some(0))
    Thread.sleep(1000)
    val time2 = DateTime.now()

    df = createDF(topic,
      withOptions = Map(
        "multiplyFactor" -> "2",
        "timeFormat" -> "yyyyMMdd HHmmss S",
        "startingTime" -> time.toString("yyyyMMdd HHmmss S"),
        "endingTime" -> time2.toString("yyyyMMdd HHmmss S")
      )
    )
    assert(df.rdd.partitions.size == 5)
    assert(df.count() == 5)

    val time3 = DateTime.now()
    Thread.sleep(1000)
    testUtils.sendMessages(topic, Array("26", "27"), Some(0))

    df = createDF(topic,
      withOptions = Map(
        "multiplyFactor" -> "2",
        "timeFormat" -> "yyyyMMdd HHmmss S",
        "startingTime" -> time.toString("yyyyMMdd HHmmss S"),
        "endingTime" -> time3.toString("yyyyMMdd HHmmss S")
      )
    )

    assert(df.rdd.partitions.size == 5)
    assert(df.count() == 5)

  }

}

object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * Read TopicPartitions from json string
    */
  def partitions(str: String): Array[TopicPartition] = {
    try {
      Serialization.read[Map[String, Seq[Int]]](str).flatMap { case (topic, parts) =>
        parts.map { part =>
          new TopicPartition(topic, part)
        }
      }.toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":[0,1],"topicB":[0,1]}, got $str""")
    }
  }

  /**
    * Write TopicPartitions as json string
    */
  def partitions(partitions: Iterable[TopicPartition]): String = {
    val result = new HashMap[String, List[Int]]
    partitions.foreach { tp =>
      val parts: List[Int] = result.getOrElse(tp.topic, Nil)
      result += tp.topic -> (tp.partition :: parts)
    }
    Serialization.write(result)
  }

  /**
    * Read per-TopicPartition offsets from json string
    */
  def partitionOffsets(str: String): Map[TopicPartition, Long] = {
    try {
      Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partOffsets) =>
        partOffsets.map { case (part, offset) =>
          new TopicPartition(topic, part) -> offset
        }
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
  }

  /**
    * Write per-TopicPartition offsets as json string
    */
  def partitionOffsets(partitionOffsets: Map[TopicPartition, Long]): String = {
    val result = new HashMap[String, HashMap[Int, Long]]()
    implicit val ordering = new Ordering[TopicPartition] {
      override def compare(x: TopicPartition, y: TopicPartition): Int = {
        Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
    }
    val partitions = partitionOffsets.keySet.toSeq.sorted // sort for more determinism
    partitions.foreach { tp =>
      val off = partitionOffsets(tp)
      val parts = result.getOrElse(tp.topic, new HashMap[Int, Long])
      parts += tp.partition -> off
      result += tp.topic -> parts
    }
    Serialization.write(result)
  }
}
