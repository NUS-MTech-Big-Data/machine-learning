package mlmodel

import com.johnsnowlabs.nlp.annotators.classifier.dl.ClassifierDLApproach
import datamodel.TrainingDataSchema
import mlmodel.PipelineStages._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

object BertSentenceEmbedding {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("BertSentenceModel").master("local[3]")
      .getOrCreate()

    val trainingDataSet = spark.read.option("delimiter", ";")
      .schema(TrainingDataSchema.schema)
      .csv("./src/main/resources/train.csv")

    val valDataSet = spark.read.option("delimiter", ";")
      .schema(TrainingDataSchema.schema)
      .csv("./src/main/resources/val.csv")

    val combineDataSets = trainingDataSet.union(valDataSet)

    val classifierDeepLearning: ClassifierDLApproach = new ClassifierDLApproach()
      .setInputCols("sentenceEmbeddings")
      .setLr(0.0001f)
      .setLabelColumn("emotion")
      .setOutputCol("class")
      .setBatchSize(8)
      .setValidationSplit(0.125f)
      .setEnableOutputLogs(true)

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(documentAssembler("sentence", "document")
        , bertSentenceEmbeddings("sent_small_bert_L8_768", List("document"), "sentenceEmbeddings")
        , classifierDeepLearning))

    val supervisedModel: PipelineModel = pipeline.fit(combineDataSets)

    supervisedModel.write.save(s"v1_supervised_bert_sentence${System.currentTimeMillis()}")
  }
}
