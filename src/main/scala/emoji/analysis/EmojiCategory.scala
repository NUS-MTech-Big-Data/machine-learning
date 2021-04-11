package emoji.analysis

import org.apache.spark.sql.SparkSession

object EmojiCategory {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder.appName("machine-learning").master("local[*]").config("dfs.client.read.shortcircuit.skip.checksum", "true").getOrCreate()
    val spark = SparkSession.builder.appName("machine-learning").master("local[*]").getOrCreate()
    val hostAddress = "localhost:9092"
    KafkaConnection.labelTweetsWithEmotion(hostAddress)
    spark.stop()
  }

}
re