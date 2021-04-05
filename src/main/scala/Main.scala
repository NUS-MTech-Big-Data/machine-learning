import org.apache.spark.sql.SparkSession

object Main extends App{
  val spark = SparkSession.builder.
    appName(getClass.getSimpleName).master("local[*]").config("dfs.client.read.shortcircuit.skip.checksum", "true").getOrCreate()
  val hostAddress = "localhost:9092"
  //val hostAddress = " 172.17.82.6:9092"
  KafkaConnection.labelTweetsWithEmotion(hostAddress)
  spark.stop()
}
