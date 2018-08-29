import java.io.{File, FileInputStream, InputStream}
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}

/**
  * describe: Kerberos环境中Spark2Streaming应用实时读取Kafka数据，解析后存入HDFS
  */
object Kafka2Sparkstreaming2Hive {
  Logger.getLogger("com").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.WARN)

  private val confPath: String = System.getProperty("user.dir")+File.separator+"conf/kafka.properties"

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val prop: Properties = new Properties()
    val file: File = new File(confPath)
    if(!file.exists()){
      println(Kafka2Sparkstreaming2Hive.getClass.getClassLoader.getResource("kafka.properties"))
      val in: InputStream = Kafka2Sparkstreaming2Hive.getClass.getClassLoader.getResourceAsStream("kafka.properties")
      prop.load(in)
    }else{
      prop.load(new FileInputStream(confPath))
    }

    //从配置文件中读取参数
    val brokers: String = prop.getProperty("kafka.brokers")
    val topics: String = prop.getProperty("kafka.topics")
    println("kafka.brokers"+brokers)
    println("kafka.topics"+topics)
    if(StringUtils.isEmpty(brokers) || StringUtils.isEmpty(topics)){
      println("未配置kafka信息")
      System.exit(0)
    }

    //topics
    val topicSet: Set[String] = topics.split(",").toSet
  }

}
