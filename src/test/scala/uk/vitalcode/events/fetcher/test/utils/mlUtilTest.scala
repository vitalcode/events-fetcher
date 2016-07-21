package uk.vitalcode.events.fetcher.test.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import uk.vitalcode.events.fetcher.common.AppConfig

import uk.vitalcode.events.fetcher.utils.MLUtil

class MLUtilTest extends WordSpec with ShouldMatchers {
    "classifier" when {
        "given training and test data" should {
            "more then 80% accurate" in {
                val sparkConf: SparkConf = new SparkConf()
                    .setAppName(AppConfig.sparkApp)
                    .setMaster(AppConfig.sparkMaster)
                    .set("es.nodes", AppConfig.elasticNodes)
                val sc: SparkContext = new SparkContext(sparkConf)

                val x = MLUtil.classifier(sc)

                sc.stop()

                x should be > 100D
            }
        }

        "given training and test data2" should {
            "more then 80% accurate3" in {

                val text: String = "Here object is a variable, and can be of any type. If the object is anInt with the value 3, execution will continue "
                val result = MLUtil.analyzeTokens(text)

                2D should be > 0.8
            }
        }
    }
}


