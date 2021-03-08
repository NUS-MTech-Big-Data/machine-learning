package mlmodel

import com.johnsnowlabs.nlp.annotators.classifier.dl.ClassifierDLApproach
import datamodel.{Sentence, TrainingDataSchema}
import mlmodel.PipelineStages._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

object BertSentenceEmbedding {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("BertSentenceModel").master("local[3]")
      .getOrCreate()

    import spark.implicits._
    val trainingDataSet = spark.read.option("delimiter", ";")
      .schema(TrainingDataSchema.schema)
      .csv("./src/main/resources/train.csv")
      .as[Sentence]

    val valDataSet = spark.read.option("delimiter", ";")
      .schema(TrainingDataSchema.schema)
      .csv("./src/main/resources/val.csv")
      .as[Sentence]

    val combineDataSets = trainingDataSet.union(valDataSet)

    val classifierDeepLearning: ClassifierDLApproach = new ClassifierDLApproach()
      .setInputCols("sentenceEmbeddings")
      .setLabelColumn("emotion")
      .setOutputCol("class")
      .setBatchSize(160)
      .setMaxEpochs(10)
      .setEnableOutputLogs(true)

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(documentAssembler("sentence", "document")
        , bertSentenceEmbeddings("sent_small_bert_L8_512", List("document"), "sentenceEmbeddings")
        , classifierDeepLearning))

    val supervisedModel: PipelineModel = pipeline.fit(combineDataSets)

    supervisedModel.write.save(s"v1_supervised_bert_sentence${System.currentTimeMillis()}")
  }
}
