package datamodel

import org.apache.spark.sql.types.{StringType, StructType}

object TrainingDataSchema {
  val schema: StructType = new StructType()
    .add("sentence", dataType = StringType, nullable = false)
    .add("emotion", dataType = StringType, nullable = false)

  val emotionsMap: Map[String, Int] = Map("sadness" -> 0, "anger" -> 1, "love" -> 2, "surprise" -> 3,
    "fear" -> 4, "joy" -> 5)

}
