package uk.vitalcode.events.fetcher.test.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import uk.vitalcode.events.fetcher.test.common.TestConfig
import uk.vitalcode.events.fetcher.utils.MLUtil
import uk.vitalcode.events.model.Category
import uk.vitalcode.events.model.Category.Category

import scala.io.Source
import scala.reflect.io.Path

class MLUtilTest extends WordSpec with ShouldMatchers with BeforeAndAfterEach {
    "Event prediction model" when {
        "train using predefined train data" should {
            "correctly predict [music] category" in {
                shouldPredict("music", Category.MUSIC)
                shouldPredict("junction-juliette_burton", Category.MUSIC)
                //shouldPredict("cce-music-big_girls_dont_cry", Category.MUSIC)
            }
            "correctly predict [family] category" in {
                shouldPredict("fwm-family-art-week", Category.FAMILY)
                shouldPredict("cce-comedy-jimmy_carr", Category.FAMILY)
            }
            "correctly predict [museum] category" in {
                //shouldPredict("fwm-guided-tour", Category.MUSEUM)
            }
        }
    }

    private var sqlContext: SQLContext = _
    private var sc: SparkContext = _

    private def shouldPredict(path: String, expectation: Category) = {
        MLUtil.predictEventCategory(sqlContext, getEventText(path)) shouldBe expectation
    }


    private def getEventText(path: String): String = {
        val trainPath = Path(this.getClass.getResource("/").getPath) / s"EventCategoryTest/$path";
        Source.fromFile(trainPath.toString).getLines.mkString
    }

    override protected def beforeEach(): Unit = {
        val sparkConf: SparkConf = new SparkConf()
            .setAppName(TestConfig.sparkApp)
            .setMaster(TestConfig.sparkMaster)
            .set("es.nodes", TestConfig.elasticNodes)
        sc = new SparkContext(sparkConf)
        sqlContext = new SQLContext(sc)
    }

    override protected def afterEach(): Unit = {
        sc.stop()
    }
}


