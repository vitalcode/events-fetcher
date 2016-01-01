package uk.vitalcode.events.fetcher.test

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}
import uk.vitalcode.events.fetcher.Client

class ClientTest extends FunSuite with ShouldMatchers with BeforeAndAfterAll {

    var sc: SparkContext = _

    test("HBase test rows count") {
        val result = Client.rowsCount(sc)
        result should equal(3)
    }

    override protected def beforeAll(): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("events-fetcher")
            .setMaster("local[1]")

        sc = new SparkContext(sparkConf)
    }

    override protected def afterAll(): Unit = {
        sc.stop()
    }
}
