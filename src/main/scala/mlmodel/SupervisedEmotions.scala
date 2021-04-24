package mlmodel

import com.johnsnowlabs.nlp.SparkNLP
import com.johnsnowlabs.nlp.annotators.classifier.dl.ClassifierDLApproach
import org.apache.spark.ml.Pipeline
import PipelineStages._
import datamodel.TrainingDataSchema

object SupervisedEmotions {
  def main(args: Array[String]): Unit = {

    val spark = SparkNLP.start()

    val trainingDataSet = spark.read.option("delimiter", ";")
      .schema(TrainingDataSchema.schema)
      .csv("./src/main/resources/train.csv")

    val valDataSet = spark.read.option("delimiter", ";")
      .schema(TrainingDataSchema.schema)
      .csv("./src/main/resources/val.csv")

    val combineDataSets = trainingDataSet.union(valDataSet)

    val classifierDeepLearning = new ClassifierDLApproach()
      .setInputCols("sentenceEmbeddings")
      .setLr(0.0001f)
      .setOutputCol("class")
      .setLabelColumn("emotion")
      .setBatchSize(3)
      .setValidationSplit(0.1f)
      .setEnableOutputLogs(true)

    val pipeline = new Pipeline()
      .setStages(Array(documentAssembler("sentence", "document")
        , tokenizer("document", "token")
        , normalizer("token", "normalized")
        , stopWordsCleaner("normalized", "cleanTokens")
        , lemmatizer("cleanTokens", "lemma")
        , wordEmbeddings(List("document", "lemma"), "wordEmbeddings")
        , sentenceEmbeddings(List("document", "wordEmbeddings"), "sentenceEmbeddings")
        , classifierDeepLearning))

    val supervisedModel = pipeline.fit(combineDataSets)

    supervisedModel.write.save(s"v1_supervised_bert_word_universal_pooling${System.currentTimeMillis()}")
  }
}
