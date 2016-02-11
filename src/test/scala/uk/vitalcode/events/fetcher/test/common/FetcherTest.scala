package uk.vitalcode.events.fetcher.test.common

import java.io.InputStream

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.model.MineType._

trait FetcherTest extends WordSpec with ShouldMatchers with BeforeAndAfterEach with Log {

    var sc: SparkContext = _
    var hBaseConn: Connection = _
    var hBaseConf: Configuration = _
    val testTable: TableName = TableName.valueOf(TestConfig.hbaseTable)

    protected def putTestData()

    protected def prepareTestData(): Unit = {
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

    protected def putTestDataRow(url: String, pagePath: String, mineType: MineType): Unit = {
        val table: Table = hBaseConn.getTable(testTable)

        val data: Array[Byte] = getPage(pagePath)
        val put: Put = new Put(Bytes.toBytes(url))
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("data"), data)
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("hash"), Bytes.toBytes(DigestUtils.sha1Hex(data)))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("mine-type"), Bytes.toBytes(mineType.toString))
        log.info(s"Add row [$url] with content of MINE type [$mineType] to the test table [$testTable]")
        table.put(put)

        table.close()
    }

    protected def getPage(resourceFilePath: String): Array[Byte] = {
        val stream: InputStream = getClass.getResourceAsStream(resourceFilePath)
        Stream.continually(stream.read).takeWhile(_ != -1).map(_.toByte).toArray
    }

    override protected def beforeEach(): Unit = {
        val sparkConf: SparkConf = new SparkConf()
            .setAppName(TestConfig.sparkApp)
            .setMaster(TestConfig.sparkMaster)
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
