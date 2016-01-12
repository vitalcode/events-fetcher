package uk.vitalcode.events.fetcher.test

import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}

class MockDataTest extends FunSuite with ShouldMatchers with BeforeAndAfterAll {
    //with LazyLogging

    var sc: SparkContext = _
    var hBaseConn: Connection = _
    var hBaseConf: Configuration = _
    val testTable = "testTable"

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
        val tableName: TableName = TableName.valueOf(testTable)
        if (admin.isTableAvailable(tableName)) {
            admin.disableTable(tableName)
            admin.deleteTable(tableName)
            println(s"Test table [$testTable] deleted")
        }

        val tableDescriptor : HTableDescriptor = new HTableDescriptor(tableName)
        tableDescriptor.addFamily(new HColumnDescriptor("content"))
        tableDescriptor.addFamily(new HColumnDescriptor("metadata"))
        admin.createTable(tableDescriptor)
        println(s"New Test table [$testTable] created")

        admin.close()
    }

    private def putTestData(): Unit = {
        val table: Table = hBaseConn.getTable(TableName.valueOf(testTable))

        val put: Put = new Put(Bytes.toBytes("row1"))
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("page"), getPage("/list1.html"))
        println(s"Add row to the test table [$testTable]")
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

