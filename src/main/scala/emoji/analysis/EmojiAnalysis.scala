package emoji.analysis

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object EmojiAnalysis {
  /**
   * Extract distinct emojis per tweet and store as arrays of column. Special characters are ignored
   * @param kafkaTopicDataFrame
   * @return
   */
  def extractEmojiFromDataFrame(kafkaTopicDataFrame : DataFrame) : DataFrame = {

    val emoticonResult = kafkaTopicDataFrame.filter(
      regexp_extract(col("value"), raw"(\p{block=Emoticons})", 1) =!= ""
    ).withColumn(
      "emoji",
      regexp_replace(
        col("value"),
        raw"([^\p{block=Emoticons}|\p{block=Miscellaneous Symbols and Pictographs}|\uD83E\uDD00-\uD83E\uDDFF])",
        ""
      )
    ).withColumn(
      "emoji",
      regexp_replace(
        col("emoji"),
        raw"([\p{block=Emoticons}|\p{block=Miscellaneous Symbols and Pictographs}|\uD83E\uDD00-\uD83E\uDDFF])",
        "$1 "
      )
    ).withColumn(
      "emoji",
      array_distinct(split(trim(col("emoji")), " "))
    )
    emoticonResult
  }

  /**
   * convert extracted arrays of emoji into their unicodes
   * @param extractedEmojiDataFrame
   * @return
   */
  def convertEmojiToUnicode(extractedEmojiDataFrame : DataFrame) : DataFrame = {
    val result = extractedEmojiDataFrame.withColumn(
      "unicodeValue",
      expr("transform(emoji, x -> 'U+' || ltrim('0' , string(hex(encode(x, 'utf-32')))))")
    )
    result
  }

  /**
   * Join emojiDictionary dataframe and emoji Dataframe to get unicodes for identified emojis
   * @param emojiDataframe
   * @return
   */
  def joinTwoDataframes(emojiDataframe : DataFrame) : DataFrame = {
    emojiDataframe.printSchema()
    val existingSparkSession = SparkSession.builder().getOrCreate()
    import existingSparkSession.sqlContext.implicits._

    val schema = new StructType()
      .add("unicode", dataType = StringType, nullable = false)
      .add("emotionCategory", dataType = StringType, nullable = false)

    val emojiDictionary = existingSparkSession.read.option("delimiter", ",")
      .schema(schema)
      .csv("emojiDictionary.csv")

    val x = emojiDictionary.withColumn("label", struct('emotionCategory))
  emojiDictionary.join(emojiDataframe, array_contains(emojiDataframe("unicodeValue"), emojiDictionary("unicode"))).withWatermark("eventTime", "2 seconds").groupBy(col("value"),col("key"),col("CreatedAt"),col("Location"), window(col("eventTime"), " 5 seconds")).agg(collect_list('emotionCategory).as("category"))
  }

  /**
   * Find array of emojiCategories according to lexicon based algo and average based algo
   * @param emotionDataframe
   * @return
   */
  def selectAppropriateEmotionLabel(emotionDataframe : DataFrame) : DataFrame = {
    val categorizeUDF = udf(
      (label: Seq[String]) =>
        if ((label.distinct.size != label.size) || (label.size == 1)) {
          label.groupBy(identity).maxBy(_._2.size)._1
        } else {
          "invalid"
        }
    )

      val labeledDataframe = emotionDataframe.select(
        col("CreatedAt"),
        col("key"),
        col("value") as("sentence"),
        categorizeUDF(col("category")).as("emotionCategory"),
        col("Location")
      ).filter("emotionCategory != 'invalid'")
      labeledDataframe

  }

  /**
   * Remove emojis from the finalDataframe
   */
  def removeEmojiFromTweet(labeledDataFrame : DataFrame) : DataFrame = {
    val dataFrameWithoutEmoji = labeledDataFrame.withColumn("sentence", regexp_replace(labeledDataFrame("sentence"), """[^ 'a-zA-Z0-9,.?!]""", " "))
    dataFrameWithoutEmoji.printSchema()
    dataFrameWithoutEmoji
  }
}
