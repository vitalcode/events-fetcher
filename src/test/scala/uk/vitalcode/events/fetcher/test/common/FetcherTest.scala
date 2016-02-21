package uk.vitalcode.events.fetcher.test.common

import java.io.InputStream

import org.elasticsearch.spark._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import scalaj.http.Http
import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.model.{DataTableBuilder, DataTable}
import uk.vitalcode.events.fetcher.model.MineType._

trait FetcherTest extends WordSpec with ShouldMatchers with BeforeAndAfterEach with Log {

    protected var sc: SparkContext = _
    protected var hBaseConn: Connection = _
    protected var hBaseConf: Configuration = _
    protected val testTable: TableName = TableName.valueOf(TestConfig.hbaseTable)

    protected val esIndex = TestConfig.elasticIndex
    protected val esType = TestConfig.elasticType
    protected val esResource = s"$esIndex/$esType"


    protected def esData(): DataTable = {
        Thread.sleep(1000)

        val actual = DataTableBuilder()
            .addArray(sc.esRDD(esResource).collect())
            .build()

        log.info(actual)
        actual
    }

    protected def putTestData()

    protected def prepareTestData(): Unit = {
        deleteEsIndex()
        createTestTable()
        putTestData()
    }

    private def createTestTable(): Unit = {
        val admin: Admin = hBaseConn.getAdmin()
        if (admin.isTableAvailable(testTable)) {
            admin.disableTable(testTable)
            admin.deleteTable(testTable)
            log.info(s"Test table [$testTable] deleted")
        }

        val tableDescriptor: HTableDescriptor = new HTableDescriptor(testTable)
        tableDescriptor.addFamily(new HColumnDescriptor("content"))
        tableDescriptor.addFamily(new HColumnDescriptor("metadata"))
        admin.createTable(tableDescriptor)
        log.info(s"New Test table [$testTable] created")

        admin.close()
    }

    protected def putTestDataRow(url: String, pagePath: String, mineType: MineType, indexId: String, pageId: String): Unit = {
        val table: Table = hBaseConn.getTable(testTable)

        val data: Array[Byte] = getPage(pagePath)
        val put: Put = new Put(Bytes.toBytes(url))
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("data"), data)
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("hash"), Bytes.toBytes(DigestUtils.sha1Hex(data)))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("url"), Bytes.toBytes(url))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("mineType"), Bytes.toBytes(mineType.toString))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("indexId"), Bytes.toBytes(indexId))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("pageId"), Bytes.toBytes(pageId))
        log.info(s"Add row [$url] with content of MINE type [$mineType] to the test table [$testTable]")
        table.put(put)

        table.close()
    }

    protected def getPage(resourceFilePath: String): Array[Byte] = {
        val stream: InputStream = getClass.getResourceAsStream(resourceFilePath)
        Stream.continually(stream.read).takeWhile(_ != -1).map(_.toByte).toArray
    }

    protected def deleteEsIndex(): Unit = {
        Http(s"http://${TestConfig.elasticNodes}/${TestConfig.elasticIndex}").method("DELETE").asString
    }

    override protected def beforeEach(): Unit = {
        val sparkConf: SparkConf = new SparkConf()
            .setAppName(TestConfig.sparkApp)
            .setMaster(TestConfig.sparkMaster)
            .set("es.nodes", TestConfig.elasticNodes)
        sc = new SparkContext(sparkConf)

        hBaseConf = HBaseConfiguration.create()
        hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, TestConfig.hbaseZookeeperQuorum)
        hBaseConf.set(TableInputFormat.INPUT_TABLE, Bytes.toString(testTable.getName))
        hBaseConn = ConnectionFactory.createConnection(hBaseConf)

        prepareTestData()
    }

    override protected def afterEach(): Unit = {
        sc.stop()
        hBaseConn.close()
    }
}
