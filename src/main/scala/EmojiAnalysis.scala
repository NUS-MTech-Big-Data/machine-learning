import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions._


object EmojiAnalysis {

  def emojiAnalysis(tweet : String) {

    val inputEmoji = "I am sad and crying 😡 😕 😟 😇"

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
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val tmpParquetDir = "Posts.tmp.parquet"
    val data = Seq((inputSentence, analysedCategoryLabel))
    val emojiRdd = existingSparkSession.sparkContext.parallelize(data)
    val finalEmojiAnalyzedDataFrame = emojiRdd.toDF("sentence", "label")
    //finalEmojiAnalyzedDataFrame.write.option("delimiter", ";").mode(SaveMode.Append).csv("./src/main/resources")
    finalEmojiAnalyzedDataFrame.coalesce(1).write.mode ("append")
      .format("com.databricks.spark.csv").option("delimiter", ";").save("./src/main/resources/emojiAnalysis")

   /* val srcPath=new Path("./src/main/resources/emojiAnalysis")
    val destPath= new Path("./src/main/resources/address_merged.csv")
    val srcFile=FileUtil.listFiles(new File("./src/main/resources/emojiAnalysis"))
      .filterNot(f=>f.getPath.endsWith(".csv"))(0)
    //Copy the CSV file outside of Directory and rename
    FileUtil.copy(srcFile,hdfs,destPath,true,hadoopConfig)
    //Remove Directory created by df.write()
    hdfs.delete(srcPath,true)
    //Removes CRC File
    hdfs.delete(new Path("./src/main/resources/emojiAnalysis/.address_merged.csv.crc"),true)

    // Merge Using Haddop API
    df.repartition(1).write.mode(SaveMode.Overwrite)
      .csv("./src/main/resources/emojiAnalysis")
    val srcFilePath=new Path("/tmp/address-tmp")
    val destFilePath= new Path("/tmp/address_merged2.csv")
    FileUtil.copyMerge(hdfs, srcFilePath, hdfs, destFilePath, true, hadoopConfig, null)
    //Remove hidden CRC file if not needed.
    hdfs.delete(new Path("/tmp/.address_merged2.csv.crc"),true)*/
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

}
