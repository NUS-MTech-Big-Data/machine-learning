package emoji.analysis

import org.apache.spark.sql.types._

object TweetSchema {

  def defineSchema(): StructType = {
    val userSchema = new StructType()
      .add("Id", LongType)
      .add("timestamp", TimestampType)
      .add("Name", StringType)
      .add("ScreenName", StringType) // Twitter User Handle
      .add("Location", StringType)
    val hashTagSchema = new StructType()
      .add("Text", StringType)

    new StructType()
      .add("payload", new StructType()
        .add("CreatedAt", LongType)
        .add("Id", LongType)
        .add("Text", StringType)
        .add("User", userSchema)
        .add("HashtagEntities", ArrayType(hashTagSchema))

      )
  }

  def defineS3SinkSchema() : StructType = {
    new StructType()
      .add("payload", new StructType()
        .add("sentence", StringType)
        .add("Id", LongType)
        .add("emotion", StringType)
        )
  }

}
