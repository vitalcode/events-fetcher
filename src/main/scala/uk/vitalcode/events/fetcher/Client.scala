package uk.vitalcode.events.fetcher

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}
import uk.vitalcode.events.cambridge
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
        hBaseConf.set(TableInputFormat.INPUT_TABLE, AppConfig.hbaseTable)

        fetchPages(sc, hBaseConf)

        sc.stop()
    }

    def fetchPages(sc: SparkContext, hBaseConf: Configuration): Unit = {

        val testIndex = AppConfig.elasticIndex
        val testType = AppConfig.elasticType
        val pages = Set[Page](cambridge.Pages.cambridgeScienceCentre)

        FetcherService.fetchPages(pages, sc, hBaseConf, testIndex, testType)
    }
}
