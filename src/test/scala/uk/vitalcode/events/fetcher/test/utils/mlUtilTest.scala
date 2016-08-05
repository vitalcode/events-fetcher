package uk.vitalcode.events.fetcher.test.utils

import java.util.Locale

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.scalatest.matchers.{MatchResult, Matcher}
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
                "music" / "music" should be(Category.MUSIC)
                "music" / "junction-juliette_burton" should be(Category.MUSIC)
                //"music" / "cce-music-big_girls_dont_cry" should be(Category.MUSIC)
            }
            "correctly predict [family] category" in {
                "family" / "fwm-family-art-week" should be(Category.FAMILY)
                "family" / "cce-comedy-jimmy_carr" should be(Category.FAMILY)
            }
            "correctly predict [museum] category" in {
                //"museum" / "fwm-guided-tour" should be(Category.MUSEUM)
            }
        }
    }

    private var sqlContext: SQLContext = _
    private var sc: SparkContext = _

    implicit def stringToPath(str: String): Path = Path(str)

    private def be(right: Category): Matcher[Path] = new Matcher[Path] {
        def apply(left: Path): MatchResult = {
            val prediction = MLUtil.predictEventCategory(sqlContext, getEventText(left))
            MatchResult(
                prediction == right,
                s"category [$prediction] for event path [${left.toString()}] was not predicted correctly as [$right]",
                s"category for event path [${left.toString()}] predicted as [$right], but it shouldn't have"
            )
        }
    }

    private def getEventText(path: Path): String = {
        val trainPath = Path(this.getClass.getResource("/").getPath) / s"EventCategoryTest" / path
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


