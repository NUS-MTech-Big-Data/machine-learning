import org.apache.spark.sql.SparkSession

object Main extends App{
  val spark = SparkSession.builder.
    appName(getClass.getSimpleName).master("local[2]").getOrCreate()
  val hostAddress = "192.168.1.88:9092"
  //val hostAddress = " 172.17.81.208:9092"
 val result = KafkaConnection.readTweetsFromKafkaTopic(hostAddress)
  //EmojiAnalysis.emojiAnalysis("yyyyyyyyy")

}
