import com.johnsnowlabs.nlp.SparkNLP
import datamodel.{SentenceWithPrediction, SentenceWithPredictionStrings, TrainingDataSchema}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.functions._

object SampleUserOfModel {
  def main(args: Array[String]): Unit = {

    val spark = SparkNLP.start()
    import spark.implicits._
    val testDataSet = spark.read.option("delimiter", ";")
      .schema(TrainingDataSchema.schema)
      .csv("./src/main/resources/test.csv")

    val mlModel = PipelineModel.read.load("v1_supervised_bert_sentence1615380402363")

    val results = mlModel.transform(testDataSet)
      .select(col("sentence"),
        col("emotion") as "label",
        concat_ws("", col("class.result")) as "prediction")
      .as[SentenceWithPredictionStrings]
      .map(s => SentenceWithPrediction(s.sentence, TrainingDataSchema.emotionsMap(s.label),
        TrainingDataSchema.emotionsMap(s.prediction)))

        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")

        println(evaluator.evaluate(results))
  }
}
