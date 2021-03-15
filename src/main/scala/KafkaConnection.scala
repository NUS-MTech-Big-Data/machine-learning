
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.functions._

object KafkaConnection {

  /**
   * Consume Kafka raw topic and format the data into a list after cleaning
   * @param hostAddress kafka IP address
   */
  def readTweetsFromKafkaTopic (hostAddress: String )  = {

    /*
    Read from kafka raw topic
     */
    val existingSparkSession = SparkSession.builder().getOrCreate()

    val readStream = existingSparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", hostAddress)
      .option("subscribe", "twitter.raw") // Always read from offset 0, for dev/testing purpose
      .load()

    readStream.printSchema()

    /*
    Format the data into a schema with necessary fields
     */
    val df = readStream.selectExpr("CAST(value AS STRING)" ) // cast value from bytes to string

    val df_json = df.select(from_json(col("value"), TweetSchema.defineSchema()).alias("parsed_value"))

    df_json.printSchema()
    val df_text = df_json.withColumn("text", col("parsed_value.payload.Text"))

    val df_english = DataPreprocessing.filterNonEnglish(df_text, inputColumn = "text")
    val df_tokenized = DataPreprocessing.tokenize(df_english, inputColumn = "text", outputColumn = "words")
    val df_filtered = DataPreprocessing.removeStopWords(df_tokenized, inputColumn = "words", outputColumn = "filtered")
    val df_clean = df_filtered.select(
      col("parsed_value.payload.Id").cast("string").alias("key"), // key must be string or bytes
      to_json(struct(
        col("parsed_value.payload.*"),
        col("filtered") as "FilteredText"
      )).alias("value")
    )

    df_clean.printSchema()

    /*
     Extract tweets and store as a new column in the dataframe
    */
    val df2 = df_clean.withColumn("value", get_json_object(col("value"), "$.Text"))
    df2.printSchema()

    val tweetDataframe =  df2.select("value")

    /*
    Rewrite the pre-processed data into kafka topic
     */
    val formattedTweetList = reformatTweets(tweetDataframe)
   val writeStream = formattedTweetList
     .writeStream
     .outputMode("append")
     .format("kafka")
     .option("kafka.bootstrap.servers", hostAddress)
     .option("topic", "emoji.analysis")
     .option("checkpointLocation", "./src/main/resources/kafka")
     .start()
    existingSparkSession.streams.awaitAnyTermination()
    existingSparkSession.stop()

  }

  /**
   * Format extracted tweets by removing retweets, usernames, urls, unnecessary characters
   */
  def reformatTweets (extractedTweets : DataFrame) : DataFrame = {

    val singleLineDataframe =  extractedTweets.withColumn("value", regexp_replace(col("value"), "[\\r\\n\\n]", "."))

    val nonUrlTweetDataframe  = singleLineDataframe.withColumn("value", regexp_replace(col("value"), "http\\S+", ""))

    val nonHashTagsTweetDataframe = nonUrlTweetDataframe.withColumn("value", regexp_replace(col("value"), "#", ""))

    val nonUserNameTweets = nonHashTagsTweetDataframe.withColumn("value", regexp_replace(col("value"), "@\\w+", ""))

    val noRTDataFrame = nonUserNameTweets.withColumn("value", regexp_replace(col("value"), "RT", ""))

    val noUrlTweetDataframe  = noRTDataFrame.withColumn("value", regexp_replace(col("value"), "www\\S+", ""))

    val removeUnnecessaryCharacter  = noUrlTweetDataframe.withColumn("value", regexp_replace(col("value"), ":", ""))

    return removeUnnecessaryCharacter
  }


}
