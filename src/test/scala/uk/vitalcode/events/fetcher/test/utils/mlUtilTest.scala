package uk.vitalcode.events.fetcher.test.utils

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import uk.vitalcode.events.fetcher.common.AppConfig
import uk.vitalcode.events.fetcher.test.common.TestConfig
import uk.vitalcode.events.fetcher.utils.MLUtil
import uk.vitalcode.events.model.Category

import scala.io.Source
import scala.reflect.io.Path

class MLUtilTest extends WordSpec with ShouldMatchers with BeforeAndAfterEach {

    var sc: SparkContext = _

    def getEventText(path: String): String = {
        val trainPath = Path(this.getClass.getResource("/").getPath)./(s"EventCategoryTest/$path").toString()
        Source.fromFile(trainPath).getLines.mkString
    }

    override protected def beforeEach(): Unit = {
        val sparkConf: SparkConf = new SparkConf()
            .setAppName(TestConfig.sparkApp)
            .setMaster(TestConfig.sparkMaster)
            .set("es.nodes", TestConfig.elasticNodes)
        sc = new SparkContext(sparkConf)
    }

    override protected def afterEach(): Unit = {
        sc.stop()
    }

    "classifier" when {
        "given training and test data" should {
            "more then 80% accurate" in {
                val category = MLUtil.predictEventCategory(sc, getEventText("music"))
                category shouldBe Category.MUSIC

                val category2 = MLUtil.predictEventCategory(sc, getEventText("junction-juliette_burton"))
                category2 shouldBe Category.MUSIC

//                val category3 = MLUtil.predictEventCategory(sc, getEventText("fwm-guided-tour"))
//                category3 shouldBe Category.MUSEUM

                val category4 = MLUtil.predictEventCategory(sc, getEventText("fwm-family-art-week"))
                category4 shouldBe Category.FAMILY

                val category6 = MLUtil.predictEventCategory(sc, getEventText("cce-comedy-jimmy_carr"))
                category6 shouldBe Category.FAMILY

//                val category5 = MLUtil.predictEventCategory(sc, getEventText("cce-music-big_girls_dont_cry"))
//                category5 shouldBe Category.MUSIC
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


