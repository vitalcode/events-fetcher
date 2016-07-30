package uk.vitalcode.events.fetcher.utils

import java.io.StringReader

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.{Analyzer, TokenStream}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import uk.vitalcode.events.model.Category
import uk.vitalcode.events.model.Category.Category

import scala.collection.mutable.ListBuffer
import scala.reflect.io.Path

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

    def predictEventCategory(sqlContext: SQLContext, text: String): Category = {

        import sqlContext.implicits._

        val dfTest = sqlContext.sparkContext.parallelize(Seq((0, text))).toDF
        val label = getEventCategoryModel(sqlContext).transform(dfTest)
            .select("prediction")
            .map { case Row(p: Double) => p }
            .collect()
            .head.toInt

        Category(label)
    }

    def getEventCategoryModel(sqlContext: SQLContext): PipelineModel = {
        if (eventCategoryModel == null) {
            eventCategoryModel = buildEventCategoryModel(sqlContext)
        }
        eventCategoryModel
    }

    private def buildEventCategoryModel(sqlContext: SQLContext): PipelineModel = {

        import sqlContext.implicits._

        val trainPath = Path(this.getClass.getResource("/").getPath)./("EventCategoryTrain/*").toString()
        val trainRDD: RDD[(String, String)] = sqlContext.sparkContext.wholeTextFiles(trainPath)
        val data = trainRDD.map {
            case (file, text) => (Category.withName(file.split("/").takeRight(2).head.toUpperCase).id.toDouble, text)
        }.toDF

        val tokenizer = new Tokenizer().setInputCol("_2").setOutputCol("words")
        val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(math.pow(2, 18).toInt)
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val rf = new NaiveBayes().setLabelCol("_1").setFeaturesCol("features")
        val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, rf))

        pipeline.fit(data)
    }

    private var eventCategoryModel: PipelineModel = _
}



