package datamodel

case class SentenceWithPrediction(sentence: String, label: Double, prediction: Double)

case class SentenceWithPredictionStrings(sentence:String, label: String, prediction: String)