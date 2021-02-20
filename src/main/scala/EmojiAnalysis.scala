import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType}
object EmojiAnalysis {

  def emojiAnalysis(tweet : String) {
    val existingSparkSession = SparkSession.builder().getOrCreate()

    val schema = new StructType()
      .add("unicode", dataType = StringType, nullable = false)
      .add("emotionCategory", dataType = StringType, nullable = false)

    val emojiDictionary = existingSparkSession.read.option("delimiter", ",")
      .schema(schema)
      .csv("./src/main/resources/emojiDictionary.csv")

    //val emojis="😃😜😍"
    emojiDictionary.show(false)
    emojiDictionary.select("emotionCategory").filter("unicode == '😃'").show()
  }


}
