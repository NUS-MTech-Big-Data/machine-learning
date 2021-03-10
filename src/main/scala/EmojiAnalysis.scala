import java.io.File
import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import java.time.LocalDateTime
import java.util.Date


object EmojiAnalysis {

  def emojiAnalysis(tweet : String) {

    val inputEmoji = "Why are you angry today😡 😕 😟 😇"

    val ArrayEmoji = extractEmojiFromSentence(inputEmoji)
     println(ArrayEmoji.mkString(" "))
    val emojiUnicodes = getUnicodeOfEmoji(ArrayEmoji)
    emojiUnicodes.show(false)
    val value = findEmotionLabelForEmoji(emojiUnicodes)
    value.show(false)
    val emojiDataFrame = calculateMeanForEmojiCategories(value, inputEmoji)
    val categoryLabel = emojiDataFrame.select("emotionCategory").collect().map(_.getString(0)).mkString("")
    val sentenceWithoutEmoji = extractedTextWithoutEmoji(inputEmoji)
   val formatedDataFrameWithLabel = createFinalDataFrame(sentenceWithoutEmoji, categoryLabel)
    formatedDataFrameWithLabel.show(false)
    combineCsvFiles()

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
    (emojiCategory.select("emoji", "unicode", "emotionCategory")).show(false)
    return  emojiCategory
  }

  /**
   * Calculate the mean for each emoji Category
   */
  def calculateMeanForEmojiCategories ( emojiData: DataFrame, tweet : String) : DataFrame = {
    var finalEmojiLabel : DataFrame = null
    val existingSparkSession = SparkSession.builder().getOrCreate()
    import existingSparkSession.sqlContext.implicits._
    val distinctEmojiCategories = emojiData.select("emotionCategory").distinct().count()
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
     val categoryLabelDataframe =  aliasDataFrame.orderBy(desc("average")).limit(1)
      return categoryLabelDataframe
    }
  }

  /**
   * Create a new data frame with the input sentence without emoji and add the respective emoji
   * write the data into a new csv file
    * @param inputSentence
   * @param analysedCategoryLabel
   * @return
   */
  def createFinalDataFrame (inputSentence : String, analysedCategoryLabel : String) : DataFrame = {
    val existingSparkSession = SparkSession.builder().getOrCreate()
    import existingSparkSession.implicits._

    val data = Seq((inputSentence, analysedCategoryLabel))
    val emojiRdd = existingSparkSession.sparkContext.parallelize(data)
    val finalEmojiAnalyzedDataFrame = emojiRdd.toDF("sentence", "label")
    finalEmojiAnalyzedDataFrame.coalesce(1).write.mode ("append")
      .format("com.databricks.spark.csv").option("delimiter", ";").save("./src/main/resources/emojiAnalysis")
    return  finalEmojiAnalyzedDataFrame
  }

  /**
   * Remove emojis from tweet
   * @param tweetSentence tweet provided by stream
   * @return sentence without emojis
   */
  def extractedTextWithoutEmoji (tweetSentence : String) : String = {
    return raw"""\P{block=Emoticons}""".r.findAllIn(tweetSentence).mkString.trim
  }

  def combineCsvFiles (): DataFrame = {
    val existingSparkSession = SparkSession.builder().getOrCreate()
    import existingSparkSession.implicits._
  val combinedCsvDataFrame =  existingSparkSession.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .load("./src/main/resources/emojiAnalysis/*.csv")
    combinedCsvDataFrame.show(false)
    createCSVFromDataFrame(combinedCsvDataFrame)
    return combinedCsvDataFrame
  }

  def createCSVFromDataFrame (generatedDataFrame : DataFrame): Unit = {
    val existingSparkSession = SparkSession.builder().getOrCreate()
    existingSparkSession.sparkContext.setLogLevel("ERROR")
    val fs = FileSystem.get(existingSparkSession.sparkContext.hadoopConfiguration)
    generatedDataFrame.coalesce(1).write.mode ("append")
    .option("delimiter", ";").csv("./src/main/resources/hourlyLabeledData/")
    val outputFileName = fs.globStatus(new Path("./src/main/resources/hourlyLabeledData/part*"))(0).getPath.getName
    val timestamp = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
    fs.rename(new Path(s"./src/main/resources/hourlyLabeledData/$outputFileName"), new Path(s"./src/main/resources/hourlyLabeledData/${timestamp}.csv"))
    fs.delete(new Path(s"./src/main/resources/hourlyLabeledData/*.csv.crc"), true)

  }

}
