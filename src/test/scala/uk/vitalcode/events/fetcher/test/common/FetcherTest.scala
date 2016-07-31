package uk.vitalcode.events.fetcher.test.common

import java.io.InputStream

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.scalatest._
import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.model.{DataTable, DataTableBuilder}
import uk.vitalcode.events.model.MineType.MineType

import scalaj.http.Http

trait FetcherTest extends WordSpec with ShouldMatchers with BeforeAndAfterEach with Log {

    protected var sc: SparkContext = _
    protected var hBaseConn: Connection = _
    protected var hBaseConf: Configuration = _

    protected val pageTable: String = TestConfig.pageTable
    protected val eventTable: String = TestConfig.eventTable

    protected val pageTableName: TableName = TableName.valueOf(pageTable)
    protected val eventTableName: TableName = TableName.valueOf(eventTable)

    protected val esIndex = TestConfig.elasticIndex
    protected val esType = TestConfig.elasticType
    protected val esResource = s"$esIndex/$esType"

    protected def esData(): DataTable = {
        Thread.sleep(1000)

        val actual = DataTableBuilder()
            .addArray(sc.esRDD(esResource).collect())
            .build()
        log.info(s"From ES [$actual]")
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
        if (admin.isTableAvailable(pageTableName)) {
            admin.disableTable(pageTableName)
            admin.deleteTable(pageTableName)
            log.info(s"Test table [$pageTableName] deleted")
        }

        if (admin.isTableAvailable(eventTableName)) {
            admin.disableTable(eventTableName)
            admin.deleteTable(eventTableName)
            log.info(s"Test table [$eventTableName] deleted")
        }

        val tableDescriptor: HTableDescriptor = new HTableDescriptor(pageTableName)
        tableDescriptor.addFamily(new HColumnDescriptor("content"))
        tableDescriptor.addFamily(new HColumnDescriptor("metadata"))
        admin.createTable(tableDescriptor)
        log.info(s"New Test table [$pageTableName] created")

        val dataTableDescriptor: HTableDescriptor = new HTableDescriptor(eventTableName)
        dataTableDescriptor.addFamily(new HColumnDescriptor("prop"))
        admin.createTable(dataTableDescriptor)
        log.info(s"New data table [$eventTableName] created")

        admin.close()
    }

    protected def putTestDataRow(url: String, pagePath: String, mineType: MineType, indexId: String, pageId: String): Unit = {
        val table: Table = hBaseConn.getTable(pageTableName)

        val data: Array[Byte] = getPage(pagePath)
        val put: Put = new Put(Bytes.toBytes(url))
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("data"), data)
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("hash"), Bytes.toBytes(DigestUtils.sha1Hex(data)))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("url"), Bytes.toBytes(url))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("mineType"), Bytes.toBytes(mineType.toString))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("indexId"), Bytes.toBytes(indexId))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("pageId"), Bytes.toBytes(pageId))
        log.info(s"Add row [$url] with content of MINE type [$mineType] to the test table [$pageTableName]")
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
        hBaseConn = ConnectionFactory.createConnection(hBaseConf)

        prepareTestData()
    }

    override protected def afterEach(): Unit = {
        sc.stop()
        hBaseConn.close()
    }
}
