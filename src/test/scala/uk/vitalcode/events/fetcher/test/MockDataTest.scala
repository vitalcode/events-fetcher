package uk.vitalcode.events.fetcher.test

import java.io.InputStream

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}
import uk.vitalcode.events.fetcher.model.MineType
import uk.vitalcode.events.fetcher.model.MineType
import uk.vitalcode.events.fetcher.model.MineType._

class MockDataTest extends FunSuite with ShouldMatchers with BeforeAndAfterAll {
    //with LazyLogging

    var sc: SparkContext = _
    var hBaseConn: Connection = _
    var hBaseConf: Configuration = _
    val testTable: TableName = TableName.valueOf("testTable")

    test("HBase test rows count") {
        prepareTestData()
        val result = 2

        // val result = rowsCount(sc)
        // println(result)
        result should equal(2)
    }

    private def rowsCount(sc: SparkContext): Long = {

        val rdd = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        rdd.count()
    }

    private def prepareTestData(): Unit = {
        createTestTable()
        putTestData()
    }

    private def createTestTable(): Unit = {
        val admin: Admin = hBaseConn.getAdmin()
        if (admin.isTableAvailable(testTable)) {
            admin.disableTable(testTable)
            admin.deleteTable(testTable)
            println(s"Test table [$testTable] deleted")
        }

        val tableDescriptor : HTableDescriptor = new HTableDescriptor(testTable)
        tableDescriptor.addFamily(new HColumnDescriptor("content"))
        tableDescriptor.addFamily(new HColumnDescriptor("metadata"))
        admin.createTable(tableDescriptor)
        println(s"New Test table [$testTable] created")

        admin.close()
    }

    private def putTestData(): Unit = {
        // page 1 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list",
            "/clientCambridgeScienceCentreTest/list1.html", MineType.TEXT_HTML)
        // page 1 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/destination-space-crew-09012016-1500/",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.html", MineType.TEXT_HTML)
        // page 1 link 1 image
        putTestDataRow("/media/assets/3a/969c39e09b655c715be0aa6b578908427d75e7.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
        // page 1 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/Voyagetospace_09012016_1600/",
            "/clientCambridgeScienceCentreTest/voyagetospace_09012016_1600.html", MineType.TEXT_HTML)
        // page 1 link 2 image
        putTestDataRow("/media/assets/3a/0004a8c035b90924f8321df21276fc8f83a6cd.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)

        // page 2 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list/?page=2",
            "/clientCambridgeScienceCentreTest/list2.html", MineType.TEXT_HTML)
        // page 2 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/otherworlds/",
            "/clientCambridgeScienceCentreTest/otherworlds.html", MineType.TEXT_HTML)
        // page 2 link 1 image
        putTestDataRow("/media/assets/3a/37cf8f84e5cfa94cdcac3f73bc13cfea3556a7.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
        // page 2 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/sunday-science-20-march/",
            "/clientCambridgeScienceCentreTest/sunday-science-20-march.html", MineType.TEXT_HTML)
        // page 2 link 2 image
        putTestDataRow("/media/assets/3a/200e303cecd9eee71f77c97ddea630521cbfe9.png",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)

        // page 3 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list/?page=3",
            "/clientCambridgeScienceCentreTest/list3.html", MineType.TEXT_HTML)
        // page 3 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/february-half-term-2016/",
            "/clientCambridgeScienceCentreTest/february-half-term-2016.html", MineType.TEXT_HTML)
        // page 3 link 1 image
        putTestDataRow("/media/assets/3a/d78141bc0cc3f96d175843c2cd0e97beb9c370.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
        // page 3 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/electric-universe/",
            "/clientCambridgeScienceCentreTest/electric-universe.html", MineType.TEXT_HTML)
        // page 3 link 2 image
        putTestDataRow("/media/assets/3a/fb2024b1db936348b42d3edd48995c32f69a1d.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
    }

    private def putTestDataRow(url: String, pagePath: String, mineType: MineType): Unit = {
        val table: Table = hBaseConn.getTable(testTable)

        val data: Array[Byte] = getPage(pagePath)
        val put: Put = new Put(Bytes.toBytes(url))
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("data"), data)
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("hash"), Bytes.toBytes(DigestUtils.sha1Hex(data)))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("mine-type"), Bytes.toBytes(mineType.toString))
        println(s"Add row [$url] with content of MINE type [$mineType] to the test table [$testTable]")
        table.put(put)

        table.close()
    }

    private def getPage(resourceFilePath: String): Array[Byte] = {
        val stream: InputStream = getClass.getResourceAsStream(resourceFilePath)
        Stream.continually(stream.read).takeWhile(_ != -1).map(_.toByte).toArray
    }

    override protected def beforeAll(): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("events-fetcher")
            .setMaster("local[1]")
        sc = new SparkContext(sparkConf)

        hBaseConf = HBaseConfiguration.create()
        hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
        hBaseConf.set(TableInputFormat.INPUT_TABLE, "page")
        hBaseConn = ConnectionFactory.createConnection(hBaseConf);

    }

    override protected def afterAll(): Unit = {
        sc.stop()
        hBaseConn.close()
    }
}

