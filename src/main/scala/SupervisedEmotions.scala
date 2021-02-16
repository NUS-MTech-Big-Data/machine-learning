import com.johnsnowlabs.nlp.annotators.classifier.dl.ClassifierDLApproach
import com.johnsnowlabs.nlp.embeddings.BertSentenceEmbeddings
import com.johnsnowlabs.nlp.{DocumentAssembler, SparkNLP}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.ml.Pipeline

object SupervisedEmotions {
  def main(args: Array[String]): Unit = {

    val spark = SparkNLP.start()

    val schema = new StructType()
      .add("sentence", dataType = StringType, nullable = false)
      .add("emotion", dataType = StringType, nullable = false)

    val trainingDataSet = spark.read.option("delimiter", ";")
      .schema(schema)
      .csv("./src/main/resources/train.csv")

    trainingDataSet.show()

    val document = new DocumentAssembler()
      .setInputCol("sentence")
      .setOutputCol("document")

    val bert = BertSentenceEmbeddings.pretrained("sent_small_bert_L8_512")
      .setInputCols("document")
      .setOutputCol("sentence_embeddings")

    val classifier = new ClassifierDLApproach()
      .setInputCols("sentence_embeddings")
      .setOutputCol("class")
      .setLabelColumn("emotion")
      .setMaxEpochs(20)
      .setEnableOutputLogs(true)

    val pipeline = new Pipeline()
      .setStages(Array(document, bert, classifier))

    val supervisedModel = pipeline.fit(trainingDataSet)

    val testDataSet = spark.read.option("delimiter", ";")
      .schema(schema)
      .csv("./src/main/resources/test.csv")

    testDataSet.show()

    val results = supervisedModel.transform(testDataSet)

    results.select("sentence", "emotion", "class.result").show(10)
  }
}
