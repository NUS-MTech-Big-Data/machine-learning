
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}

object KafkaConnection {

  def readTweetsFromKafkaTopic (hostAddress: String) = {
    val existingSparkSession = SparkSession.builder().getOrCreate()
    import existingSparkSession.implicits._
    val readStream = existingSparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", hostAddress)
      .option("subscribe", "twitter.raw") // Always read from offset 0, for dev/testing purpose
      .load()

    readStream.printSchema()

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

    existingSparkSession.stop()
  }
}
