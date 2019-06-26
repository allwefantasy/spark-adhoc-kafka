package org.apache.spark.sql.kafka010

import java.util.Locale
import java.{util => ju}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation

/**
  * 2019-06-25 WilliamZhu(allwefantasy@gmail.com)
  */
class AdHocKafkaSourceProvider extends KafkaSourceProvider {

  import AdHocKafkaSourceProvider._

  override def shortName(): String = "AdHocKafka"

  /**
    * Returns a new base relation with the given parameters.
    *
    * @note The parameters' keywords are case insensitive and this insensitivity is enforced
    *       by the Map that is passed to the function.
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    validateBatchOptions(parameters)
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase(Locale.ROOT).startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val startingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(
      caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit)
    assert(startingRelationOffsets != LatestOffsetRangeLimit)

    val endingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(caseInsensitiveParams,
      ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
    assert(endingRelationOffsets != EarliestOffsetRangeLimit)

    new AdHocKafkaRelation(
      sqlContext,
      strategy(caseInsensitiveParams),
      sourceOptions = parameters,
      specifiedKafkaParams = specifiedKafkaParams,
      failOnDataLoss = failOnDataLoss(caseInsensitiveParams),
      startingOffsets = startingRelationOffsets,
      endingOffsets = endingRelationOffsets)
  }

  private def strategy(caseInsensitiveParams: Map[String, String]) =
    caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
      case ("assign", value) =>
        AssignStrategy(JsonUtils.partitions(value))
      case ("subscribe", value) =>
        SubscribeStrategy(value.split(",").map(_.trim()).filter(_.nonEmpty))
      case ("subscribepattern", value) =>
        SubscribePatternStrategy(value.trim())
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

  private def failOnDataLoss(caseInsensitiveParams: Map[String, String]) =
    caseInsensitiveParams.getOrElse(FAIL_ON_DATA_LOSS_OPTION_KEY, "true").toBoolean

  private def validateGeneralOptions(parameters: Map[String, String]): Unit = {
    // Validate source options
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val specifiedStrategies =
      caseInsensitiveParams.filter { case (k, _) => STRATEGY_OPTION_KEYS.contains(k) }.toSeq

    if (specifiedStrategies.isEmpty) {
      throw new IllegalArgumentException(
        "One of the following options must be specified for Kafka source: "
          + STRATEGY_OPTION_KEYS.mkString(", ") + ". See the docs for more details.")
    } else if (specifiedStrategies.size > 1) {
      throw new IllegalArgumentException(
        "Only one of the following options can be specified for Kafka source: "
          + STRATEGY_OPTION_KEYS.mkString(", ") + ". See the docs for more details.")
    }

    val strategy = caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match {
      case ("assign", value) =>
        if (!value.trim.startsWith("{")) {
          throw new IllegalArgumentException(
            "No topicpartitions to assign as specified value for option " +
              s"'assign' is '$value'")
        }

      case ("subscribe", value) =>
        val topics = value.split(",").map(_.trim).filter(_.nonEmpty)
        if (topics.isEmpty) {
          throw new IllegalArgumentException(
            "No topics to subscribe to as specified value for option " +
              s"'subscribe' is '$value'")
        }
      case ("subscribepattern", value) =>
        val pattern = caseInsensitiveParams("subscribepattern").trim()
        if (pattern.isEmpty) {
          throw new IllegalArgumentException(
            "Pattern to subscribe is empty as specified value for option " +
              s"'subscribePattern' is '$value'")
        }
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

    // Validate minPartitions value if present
    if (caseInsensitiveParams.contains(MIN_PARTITIONS_OPTION_KEY)) {
      val p = caseInsensitiveParams(MIN_PARTITIONS_OPTION_KEY).toInt
      if (p <= 0) throw new IllegalArgumentException("minPartitions must be positive")
    }

    // Validate user-specified Kafka options

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.GROUP_ID_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.GROUP_ID_CONFIG}' is not supported as " +
          s"user-specified consumer groups are not used to track offsets.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}")) {
      throw new IllegalArgumentException(
        s"""
           |Kafka option '${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}' is not supported.
           |Instead set the source option '$STARTING_OFFSETS_OPTION_KEY' to 'earliest' or 'latest'
           |to specify where to start. Structured Streaming manages which offsets are consumed
           |internally, rather than relying on the kafkaConsumer to do it. This will ensure that no
           |data is missed when new topics/partitions are dynamically subscribed. Note that
           |'$STARTING_OFFSETS_OPTION_KEY' only applies when a new Streaming query is started, and
           |that resuming will always pick up from where the query left off. See the docs for more
           |details.
         """.stripMargin)
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}' is not supported as keys "
          + "are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame operations "
          + "to explicitly deserialize the keys.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}' is not supported as "
          + "values are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame "
          + "operations to explicitly deserialize the values.")
    }

    val otherUnsupportedConfigs = Seq(
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, // committing correctly requires new APIs in Source
      ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG) // interceptors can modify payload, so not safe

    otherUnsupportedConfigs.foreach { c =>
      if (caseInsensitiveParams.contains(s"kafka.$c")) {
        throw new IllegalArgumentException(s"Kafka option '$c' is not supported")
      }
    }

    if (!caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Option 'kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}' must be specified for " +
          s"configuring Kafka consumer")
    }
  }

  private def validateStreamOptions(caseInsensitiveParams: Map[String, String]) = {
    // Stream specific options
    caseInsensitiveParams.get(ENDING_OFFSETS_OPTION_KEY).map(_ =>
      throw new IllegalArgumentException("ending offset not valid in streaming queries"))
    validateGeneralOptions(caseInsensitiveParams)
  }

  private def validateBatchOptions(caseInsensitiveParams: Map[String, String]) = {
    // Batch specific options
    KafkaSourceProvider.getKafkaOffsetRangeLimit(
      caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit) match {
      case EarliestOffsetRangeLimit => // good to go
      case LatestOffsetRangeLimit =>
        throw new IllegalArgumentException("starting offset can't be latest " +
          "for batch queries on Kafka")
      case SpecificOffsetRangeLimit(partitionOffsets) =>
        partitionOffsets.foreach {
          case (tp, off) if off == KafkaOffsetRangeLimit.LATEST =>
            throw new IllegalArgumentException(s"startingOffsets for $tp can't " +
              "be latest for batch queries on Kafka")
          case _ => // ignore
        }
    }

    KafkaSourceProvider.getKafkaOffsetRangeLimit(
      caseInsensitiveParams, ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit) match {
      case EarliestOffsetRangeLimit =>
        throw new IllegalArgumentException("ending offset can't be earliest " +
          "for batch queries on Kafka")
      case LatestOffsetRangeLimit => // good to go
      case SpecificOffsetRangeLimit(partitionOffsets) =>
        partitionOffsets.foreach {
          case (tp, off) if off == KafkaOffsetRangeLimit.EARLIEST =>
            throw new IllegalArgumentException(s"ending offset for $tp can't be " +
              "earliest for batch queries on Kafka")
          case _ => // ignore
        }
    }

    validateGeneralOptions(caseInsensitiveParams)

    // Don't want to throw an error, but at least log a warning.
    if (caseInsensitiveParams.get("maxoffsetspertrigger").isDefined) {
      logWarning("maxOffsetsPerTrigger option ignored in batch queries")
    }
  }
}

object AdHocKafkaSourceProvider extends Logging {
  private val STRATEGY_OPTION_KEYS = Set("subscribe", "subscribepattern", "assign")
  private[kafka010] val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
  private[kafka010] val ENDING_OFFSETS_OPTION_KEY = "endingoffsets"
  private[kafka010] val MULTIPLY_FACTOR = "multiplyFactor"
  private[kafka010] val MAX_SIZE_PER_PARTITION = "maxSizePerPartition"
  private[kafka010] val STARTING_TIME_OPTION_KEY = "startingTime"
  private[kafka010] val ENDING_TIME_OPTION_KEY = "endingTime"
  private[kafka010] val TIME_FORMAT_OPTION_KEY = "timeFormat"
  private[kafka010] val RECENT_OPTION_KEY = "recentNum"
  private val FAIL_ON_DATA_LOSS_OPTION_KEY = "failondataloss"
  private val MIN_PARTITIONS_OPTION_KEY = "minpartitions"

  val TOPIC_OPTION_KEY = "topic"

  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE =
    """
      |Some data may have been lost because they are not available in Kafka any more; either the
      | data was aged out by Kafka or the topic may have been deleted before all the data in the
      | topic was processed. If you want your streaming query to fail on such cases, set the source
      | option "failOnDataLoss" to "true".
    """.stripMargin

  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE =
    """
      |Some data may have been lost because they are not available in Kafka any more; either the
      | data was aged out by Kafka or the topic may have been deleted before all the data in the
      | topic was processed. If you don't want your streaming query to fail on such cases, set the
      | source option "failOnDataLoss" to "false".
    """.stripMargin


  private val deserClassName = classOf[ByteArrayDeserializer].getName

  def getKafkaOffsetRangeLimit(
                                params: Map[String, String],
                                offsetOptionKey: String,
                                defaultOffsets: KafkaOffsetRangeLimit): KafkaOffsetRangeLimit = {
    params.get(offsetOptionKey).map(_.trim) match {
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "latest" =>
        LatestOffsetRangeLimit
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "earliest" =>
        EarliestOffsetRangeLimit
      case Some(json) => SpecificOffsetRangeLimit(JsonUtils.partitionOffsets(json))
      case None => defaultOffsets
    }
  }

  def kafkaParamsForDriver(specifiedKafkaParams: Map[String, String]): ju.Map[String, Object] =
    ConfigUpdater("source", specifiedKafkaParams)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

      // Set to "earliest" to avoid exceptions. However, KafkaSource will fetch the initial
      // offsets by itself instead of counting on KafkaConsumer.
      .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      // So that consumers in the driver does not commit offsets unnecessarily
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // So that the driver does not pull too much data
      .set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, new java.lang.Integer(1))

      // If buffer config is not set, set it to reasonable value to work around
      // buffer issues (see KAFKA-3135)
      .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
      .build()

  def kafkaParamsForExecutors(
                               specifiedKafkaParams: Map[String, String],
                               uniqueGroupId: String): ju.Map[String, Object] =
    ConfigUpdater("executor", specifiedKafkaParams)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

      // Make sure executors do only what the driver tells them.
      .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

      // So that consumers in executors do not mess with any existing group id
      .set(ConsumerConfig.GROUP_ID_CONFIG, s"$uniqueGroupId-executor")

      // So that consumers in executors does not commit offsets unnecessarily
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // If buffer config is not set, set it to reasonable value to work around
      // buffer issues (see KAFKA-3135)
      .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
      .build()

  /** Class to conveniently update Kafka config params, while logging the changes */
  private case class ConfigUpdater(module: String, kafkaParams: Map[String, String]) {
    private val map = new ju.HashMap[String, Object](kafkaParams.asJava)

    def set(key: String, value: Object): this.type = {
      map.put(key, value)
      logDebug(s"$module: Set $key to $value, earlier value: ${kafkaParams.getOrElse(key, "")}")
      this
    }

    def setIfUnset(key: String, value: Object): ConfigUpdater = {
      if (!map.containsKey(key)) {
        map.put(key, value)
        logDebug(s"$module: Set $key to $value")
      }
      this
    }

    def build(): ju.Map[String, Object] = map
  }

  private[kafka010] def kafkaParamsForProducer(
                                                parameters: Map[String, String]): Map[String, String] = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    if (caseInsensitiveParams.contains(s"kafka.${ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG}' is not supported as keys "
          + "are serialized with ByteArraySerializer.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG}' is not supported as "
          + "value are serialized with ByteArraySerializer.")
    }
    parameters
      .keySet
      .filter(_.toLowerCase(Locale.ROOT).startsWith("kafka."))
      .map { k => k.drop(6).toString -> parameters(k) }
      .toMap + (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName)
  }
}
