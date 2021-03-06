import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

object SampleUserOfModel {
  def main(args: Array[String]): Unit = {

    val spark = SparkNLP.start()

    val schema = new StructType()
      .add("sentence", dataType = StringType, nullable = false)
      .add("emotion", dataType = StringType, nullable = false)

    val testDataSet = spark.read.option("delimiter", ";")
      .schema(schema)
      .csv("./src/main/resources/test.csv")

    val mlModel = PipelineModel.read.load("../../project/v1_supervised")

    val results = mlModel.transform(testDataSet)
      .select(col("sentence"),
        col("emotion") as "label",
        concat_ws("", col("class.result")) as "prediction")

    val emotionsMap: Map[String, Double] = Map("sadness" -> 0.0, "anger" -> 1.0, "love" -> 2.0, "surprise" -> 3.0,
      "fear" -> 4.0, "joy" -> 5.0)
    val toEmotionNumeric = udf((input: String) => emotionsMap.get(input))
    spark.udf.register("toEmotionNumeric", toEmotionNumeric)

    val predictionLabel = results
      .withColumn("labelNumeric", toEmotionNumeric(col("label")))
      .withColumn("predictionNumeric", toEmotionNumeric(col("prediction")))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("labelNumeric")
      .setPredictionCol("predictionNumeric")
      .setMetricName("accuracy")

    println(evaluator.evaluate(predictionLabel))
  }
}
