import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.functions._

object DataPreprocessing {
  /**
   * Separate a sentence into array of words
   * any character other than words will be ignored
   */
  def tokenize(df_sentence: DataFrame, inputColumn: String, outputColumn: String): DataFrame = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol(inputColumn)
      .setOutputCol(outputColumn)
      .setPattern("\\W")
    regexTokenizer.transform(df_sentence)
  }

  /**
   *
   * @param df_tokenized Tokenized dataframe
   * @return Dataframe after removing all the stop words such as i , am , a the
   */
  def removeStopWords (df_tokenized : DataFrame, inputColumn: String, outputColumn: String): DataFrame = {
    val stopWordRemover = new StopWordsRemover()
      .setInputCol(inputColumn)
      .setOutputCol(outputColumn)
    stopWordRemover.transform(df_tokenized)
  }

  val detectLanguagePipeline = new PretrainedPipeline("detect_language_43", lang = "xx")
  /**
   * Filter out non English texts
   * @param df DataFrame contain texts
   * @return A DataFrame with only English texts
   */
  def filterNonEnglish(df: DataFrame, inputColumn: String): DataFrame = {
    val df_annotate_result = detectLanguagePipeline.annotate(df, inputColumn)
    val df_language = df_annotate_result.withColumn("language", explode(col("language.result")))
    val df_english = df_language.filter(col("language") === "en")
    df_english
  }

  def removeUrls (tweetDataframe : DataFrame, columnName : String) : DataFrame = {
   tweetDataframe.withColumn("Texts", regexp_replace(col(""+columnName), "http\\S+", ""))
    return tweetDataframe
  }

}
