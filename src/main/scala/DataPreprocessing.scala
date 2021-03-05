import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.{LemmatizerModel, StopWordsCleaner, Tokenizer, Normalizer}
import com.johnsnowlabs.nlp.embeddings.{BertEmbeddings, SentenceEmbeddings}
import org.apache.spark.ml.PipelineStage

object DataPreprocessing {

  def documentAssembler(inputColumn: String, outputColumn: String): PipelineStage = {
    new DocumentAssembler()
      .setInputCol(inputColumn)
      .setOutputCol(outputColumn)
  }

  def tokenizer(inputColumn: String, outputColumn: String): PipelineStage = {
    new Tokenizer()
      .setInputCols(inputColumn)
      .setOutputCol(outputColumn)
  }

  def normalizer(inputColumn: String, outputColumn: String): PipelineStage = {
    new Normalizer().setInputCols(inputColumn)
      .setOutputCol(outputColumn)
  }

  def stopWordsCleaner(inputColumn: String, outputColumn: String): PipelineStage = {
    new StopWordsCleaner()
      .setInputCols(inputColumn)
      .setOutputCol(outputColumn)
      .setCaseSensitive(false)
  }

  def lemmatizer(inputColumn: String, outputColumn: String): PipelineStage = {
    LemmatizerModel.pretrained("lemma_antbnc")
      .setInputCols(inputColumn)
      .setOutputCol(outputColumn)
  }

  def wordEmbeddings(inputColumns: List[String], outputColumn: String): PipelineStage = {
    BertEmbeddings.pretrained("bert_base_uncased", "en")
      .setInputCols(inputColumns: _*)
      .setOutputCol(outputColumn)
      .setCaseSensitive(false)
  }

  def sentenceEmbeddings(inputColumns: List[String], outputColumn: String): PipelineStage = {
    new SentenceEmbeddings()
      .setInputCols(inputColumns: _*)
      .setOutputCol(outputColumn)
      .setPoolingStrategy("AVERAGE")
  }
}