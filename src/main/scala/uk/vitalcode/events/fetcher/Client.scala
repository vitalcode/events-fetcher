package uk.vitalcode.events.fetcher

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase._
import org.apache.spark.{SparkConf, SparkContext}
import uk.vitalcode.events.Pages
import uk.vitalcode.events.fetcher.common.AppConfig
import uk.vitalcode.events.fetcher.service.FetcherService
import uk.vitalcode.events.model.{Page, PageBuilder, PropBuilder, PropType}

object Client {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf()
            .setAppName(AppConfig.sparkApp)
            .setMaster(AppConfig.sparkMaster)
            .set("es.nodes", AppConfig.elasticNodes)
        val sc = new SparkContext(sparkConf)

        val hBaseConf: Configuration = HBaseConfiguration.create()
        hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, AppConfig.hbaseZookeeperQuorum)
        hBaseConf.set(TableInputFormat.INPUT_TABLE, AppConfig.pageTable)

        createEventTable(hBaseConf)
        fetchPages(sc, hBaseConf)

        sc.stop()
    }

    private def createEventTable(hBaseConf: Configuration): Unit = {
        val hBaseConn = ConnectionFactory.createConnection(hBaseConf)
        val admin: Admin = hBaseConn.getAdmin
        val eventTable = TableName.valueOf(AppConfig.eventTable)

        if (admin.isTableAvailable(eventTable)) {
            admin.disableTable(eventTable)
            admin.deleteTable(eventTable)
        }

        val dataTableDescriptor: HTableDescriptor = new HTableDescriptor(eventTable)
        dataTableDescriptor.addFamily(new HColumnDescriptor("prop"))
        admin.createTable(dataTableDescriptor)
        admin.close()
    }

    private def fetchPages(sc: SparkContext, hBaseConf: Configuration): Unit = {

        val testIndex = AppConfig.elasticIndex
        val testType = AppConfig.elasticType

        val pageTable = AppConfig.pageTable
        val eventTable = AppConfig.eventTable

        FetcherService.fetchPages(Pages.all, sc, hBaseConf, pageTable, eventTable, testIndex, testType)
    }
}