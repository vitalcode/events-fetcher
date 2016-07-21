package uk.vitalcode.events.fetcher.utils

import java.io.StringReader

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.{Analyzer, TokenStream}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ListBuffer

object MLUtil {

    def analyzeTokens(text: String): Seq[String] = {
        val result = ListBuffer.empty[String]
        val analyzer: Analyzer = new EnglishAnalyzer()
        val stream: TokenStream = analyzer.tokenStream(null, new StringReader(text))

        try {
            stream.reset()
            while (stream.incrementToken()) {
                result += stream.getAttribute(classOf[CharTermAttribute]).toString
            }
            stream.end()
            result.toVector
        } finally {
            stream.close()
        }
    }

    def classifier(sc: SparkContext): Double = {

        val sqlContext = SQLContextSingleton.getInstance(sc)
        import sqlContext.implicits._

        // train
        val home = "/Users/vitaliy/data/20news-bydate-short"
        val trainPath = s"$home/20news-bydate-train/*"
        val trainRDD: RDD[(String, String)] = sc.wholeTextFiles(trainPath)
        val data = trainRDD.map { case (file, text) => (file.split("/").takeRight(2).head match {
            case "alt.atheism" => 0D
            case "comp.graphics" => 1D
            case "comp.os.ms-windows.misc" => 2D
            case _ => 3D
        }, text)
        }.toDF

        val tokenizer = new Tokenizer().setInputCol("_2").setOutputCol("words")
        val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(math.pow(2, 18).toInt)
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val rf = new NaiveBayes().setLabelCol("_1").setFeaturesCol("features")
        val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, rf))

        val idfModel = pipeline.fit(data)

        // test
        val testPath = s"$home/20news-bydate-test/*"
        val testRDD = sc.wholeTextFiles(testPath)

        val testdData = trainRDD.map { case (file, text) => (file.split("/").takeRight(2).head match {
            case "alt.atheism" => 0D
            case "comp.graphics" => 1D
            case "comp.os.ms-windows.misc" => 2D
            case _ => 3D
        }, text)
        }.toDF

        val result = idfModel.transform(testdData)

        result.select("_1", "prediction")
            .collect()
            .count { case Row(ac: Double, ex: Double) => ac == ex }
            .toDouble / result.count
    }
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

    @transient
    private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
        if (instance == null) {
            instance = new SQLContext(sparkContext)
        }
        instance
    }
}


