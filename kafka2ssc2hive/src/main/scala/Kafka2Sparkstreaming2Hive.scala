import java.io.{File, FileInputStream, InputStream}
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.util.parsing.json.JSON

/**
  * describe: Kerberos环境中Spark2Streaming应用实时读取Kafka数据，解析后存入HDFS
  */
object Kafka2Sparkstreaming2Hive {
  Logger.getLogger("com").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.WARN)

  val confPath: String = System.getProperty("user.dir") + File.separator + "conf/kafka.properties"

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val prop: Properties = new Properties()
    val file: File = new File(confPath)
    if (!file.exists()) {
      println(Kafka2Sparkstreaming2Hive.getClass.getClassLoader.getResource("kafka.properties"))
      val in: InputStream = Kafka2Sparkstreaming2Hive.getClass.getClassLoader.getResourceAsStream("kafka.properties")
      prop.load(in)
    } else {
      prop.load(new FileInputStream(confPath))
    }

    //从配置文件中读取参数
    val brokers: String = prop.getProperty("kafka.brokers")
    val topics: String = prop.getProperty("kafka.topics")
    println("kafka.brokers" + brokers)
    println("kafka.topics" + topics)
    if (StringUtils.isEmpty(brokers) || StringUtils.isEmpty(topics)) {
      println("未配置kafka信息")
      System.exit(0)
    }

    //topics
    val topicSet: Set[String] = topics.split(",").toSet

    //创建sparksession
    val sparkSession: SparkSession = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .config(new SparkConf())
      .getOrCreate()

    //创建ssc
    val ssc: StreamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    //配置kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers
      , "auto.offset.reset" -> "latest"
      //      ,"security.protocol" -> "SASL_PLAINTEXT"     //kerberos认证
      //      , "sasl.kerberos.service.name" -> "kafka"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> "kafka_01"
    )

    //创建实时流
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )

    //逻辑处理
    dstream.foreachRDD(rdd => {
      val newRdd: RDD[String] = rdd.map(line => {
        val jsonOb: Option[Any] = JSON.parseFull(line.value())
        val map: Map[String, Any] = jsonOb.get.asInstanceOf[Map[String, Any]]
        //将Map数据转为以","隔开的字符串
        val userInfo: String = map.get("id").get.asInstanceOf[String].concat(",")
          .concat(map.get("name").get.asInstanceOf[String]).concat(",")
        userInfo
      })

      //将解析好的数据已流的方式写入HDFS，未使用RDD的方式可以避免数据被覆盖
      newRdd.foreachPartition(record =>{
        //设置写入hdfs的路径
        val configuration = new Configuration()
        val fs = FileSystem.get(configuration)
        val path = new Path("/opt/kafka/test.txt")
        val outPutStream: FSDataOutputStream = if(!fs.exists(path)){
          fs.create(path)
        }else{
          fs.append(path)
        }

        record.foreach(line =>{
          outPutStream.write((line +"\n").getBytes("UTF-8"))
        })
        outPutStream.close()

      })
    })

    ssc.start()
    ssc.awaitTermination()
  }


}
