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

    val classifierDeepLearning = new ClassifierDLApproach()
      .setInputCols("sentenceEmbeddings")
      .setOutputCol("class")
      .setLabelColumn("emotion")
      .setMaxEpochs(5)
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

    val supervisedModel = pipeline.fit(trainingDataSet)

    supervisedModel.write.save("v1_supervised")
  }
}
