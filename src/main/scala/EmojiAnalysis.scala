import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType}

object EmojiAnalysis {

  def emojiAnalysis(tweet : String) {

    //val emojis="😃😜😍"
    val inputEmoji = "ee ddd eee sss 😃😜😍";
    val ArrayEmoji = extractEmojiFromSentence(inputEmoji)
     println(ArrayEmoji.mkString(" "))
    val emojiUnicodes = getUnicodeOfEmoji(ArrayEmoji)
    emojiUnicodes.show(false)
    val value = findEmotionLabelForEmoji(emojiUnicodes)
    value.show(false)

  }

  /**
   * extract emojis from extracted text and assign into an array
   */
   def extractEmojiFromSentence (twitterSentence : String) : Seq[String] = {
    return raw"\p{block=Emoticons}".r.findAllIn(twitterSentence).toSeq
   }

  /**
   * Get unicode for the extracted emoji
   */
  def getUnicodeOfEmoji (emojiArray : Seq[String]) : DataFrame = {
    val existingSparkSession = SparkSession.builder().getOrCreate()
    import existingSparkSession.implicits._
    val df = emojiArray.toDF("emoji")
    val result = df.selectExpr(
      "emoji",
      "'U+' || trim('0' , string(hex(encode(emoji, 'utf-32')))) as unicode"
    )
    result.show(false)
    return result
  }

  /**
   * Get emoji label from dictionary
   */
  def findEmotionLabelForEmoji (emojiDataFrame : DataFrame) : DataFrame = {

    val existingSparkSession = SparkSession.builder().getOrCreate()
    val schema = new StructType()
      .add("unicode", dataType = StringType, nullable = false)
      .add("emotionCategory", dataType = StringType, nullable = false)

    val emojiDictionary = existingSparkSession.read.option("delimiter", ",")
      .schema(schema)
      .csv("./src/main/resources/emojiDictionary.csv")
   val emojiCategory = emojiDictionary.join(emojiDataFrame, "unicode")
    (emojiCategory.select("emoji","unicode","emotionCategory")).show(false)
    return  emojiCategory
  }

}
