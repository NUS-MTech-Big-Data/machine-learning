
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.functions._

object KafkaConnection {

  /**
   * Consume Kafka raw topic and format the data into a list after cleaning
   * @param hostAddress kafka IP address
   */
  def readTweetsFromKafkaTopic (hostAddress: String) = {

    /*
    Read from kafka raw topic
     */
    val existingSparkSession = SparkSession.builder().getOrCreate()
    import existingSparkSession.implicits._
    val readStream = existingSparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", hostAddress)
      .option("subscribe", "twitter.raw") // Always read from offset 0, for dev/testing purpose
      .load()

    readStream.printSchema()

    /*
    Format the data into a schema with necessary fields
     */
    val df = readStream.selectExpr("CAST(value AS STRING)" ) // cast value from bytes to string

    df.show(false)
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
    df_clean.show(false)
    df_clean.printSchema()

    /*
     Extract tweets and store as a new column in the dataframe
    */
    val df2 = df_clean.withColumn("Texts", get_json_object(col("value"), "$.Text"))
    df2.printSchema()
    df2.select("Texts").show(false)
  val tweetDataframe =  df2.select("Texts")
    tweetDataframe.show(false)

    /*
    Reformat the tweets to remove new lines
     */
  val singleLineDataframe =  tweetDataframe.withColumn("Texts", regexp_replace(col("Texts"), "[\\r\\n\\n]", "."))
    singleLineDataframe.show(false)
 val tweetList =   singleLineDataframe.select("Texts").rdd.map(r => r(0)).collect.toList
    for ( p <- tweetList)
      println(p)
    existingSparkSession.stop()
  }
}
