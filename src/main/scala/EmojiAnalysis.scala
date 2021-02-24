import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions._

object EmojiAnalysis {

  def emojiAnalysis(tweet : String) {

    val inputEmoji = "I am going to school 😃 😜 😍 😡 😕 😟 😇"

    val ArrayEmoji = extractEmojiFromSentence(inputEmoji)
     println(ArrayEmoji.mkString(" "))
    val emojiUnicodes = getUnicodeOfEmoji(ArrayEmoji)
    emojiUnicodes.show(false)
    val value = findEmotionLabelForEmoji(emojiUnicodes)
    value.show(false)
    val emojiDataFrame = calculateMeanForEmojiCategories(value)
    val categoryLabel = emojiDataFrame.select("emotionCategory")
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

  /**
   * Calculate the mean for each emoji Category
   */
  def calculateMeanForEmojiCategories ( emojiData: DataFrame) : DataFrame = {
    var finalEmojiLabel : DataFrame = null
    val existingSparkSession = SparkSession.builder().getOrCreate()
    import existingSparkSession.sqlContext.implicits._
    val distinctEmojiCategories = emojiData.select("emotionCategory").distinct().count()
    print("--------"+distinctEmojiCategories)
    if ( distinctEmojiCategories == 1) {
      finalEmojiLabel = emojiData.select("emotionCategory")
      return finalEmojiLabel

    } else {
      val groupByCategoryCountDataFrame = emojiData.groupBy("emotionCategory").agg(count(lit(1)).alias("numberOfRecords")).
       groupBy("emotionCategory").agg(mean("numberOfRecords"))
      groupByCategoryCountDataFrame.show(false)
      val averageValDataFrame =  groupByCategoryCountDataFrame.groupBy("emotionCategory").agg(max("avg(numberOfRecords)"))
      averageValDataFrame.show(false)
      val aliasDataFrame = averageValDataFrame.withColumnRenamed("max(avg(numberOfRecords))", "average")
      aliasDataFrame.show(false)
      aliasDataFrame.orderBy(desc("average")).limit(1).show()
      return aliasDataFrame
    }
  }

  def createFinalDataFrame (inputSentence : String, analysedCategoryLabel : String) : DataFrame = {
    val existingSparkSession = SparkSession.builder().getOrCreate()
    import existingSparkSession.implicits._
    val columnHeadings = Seq("sentence", "label")
    val data = Seq((inputSentence, analysedCategoryLabel))
    val emojiRdd = existingSparkSession.sparkContext.parallelize(data)
    val finalEmojiAnalyzedDataFrame = emojiRdd.toDF()
    return  finalEmojiAnalyzedDataFrame
  }

 /* def extractedTextWithoutEmoji (tweetSentence : String) : String = {

  }*/

}
