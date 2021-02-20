import org.apache.spark.sql.SparkSession

object Main extends App{
  val spark = SparkSession.builder.
    appName(getClass.getSimpleName).master("local[2]").getOrCreate()
  EmojiAnalysis.emojiAnalysis("yyyyyyyyy")

}
