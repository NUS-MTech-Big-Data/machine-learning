import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.ml.PipelineModel
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

    val mlModel = PipelineModel.read.load("v1_supervised")

    val results = mlModel.transform(testDataSet)

    results.select("sentence", "emotion", "class.result").show(10)
  }
}
