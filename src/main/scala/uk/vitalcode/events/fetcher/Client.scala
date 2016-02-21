package uk.vitalcode.events.fetcher

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}
import uk.vitalcode.events.fetcher.common.AppConfig
import uk.vitalcode.events.fetcher.model.{Page, PageBuilder, PropBuilder, PropType}
import uk.vitalcode.events.fetcher.service.FetcherService

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
        val pages = Set[Page](buildPageDescription())

        FetcherService.fetchPages(pages, sc, hBaseConf, testIndex, testType)
    }

    private def buildPageDescription(): Page = {
        PageBuilder()
            .setId("list")
            .setUrl("http://www.cambridgesciencecentre.org/whats-on/list/")
            .addPage(PageBuilder()
                .isRow(true)
                .setId("description")
                .setLink("div.main_wrapper > section > article > ul > li > h2 > a")
                .addPage(PageBuilder()
                    .setId("image")
                    .setLink("section.event_detail > div.page_content > article > img")
                    .addProp(PropBuilder()
                        .setName("image")
                        .setKind(PropType.Image)
                    )
                )
                .addProp(PropBuilder()
                    .setName("description")
                    .setCss("div.main_wrapper > section.event_detail > div.page_content p:nth-child(4)")
                    .setKind(PropType.Text)
                )
                .addProp(PropBuilder()
                    .setName("cost")
                    .setCss("div.main_wrapper > section.event_detail > div.page_content p:nth-child(5)")
                    .setKind(PropType.Text)
                )
            )
            .addPage(PageBuilder()
                .setRef("list")
                .setId("pagination")
                .setLink("div.pagination > div.omega > a")
            )
            .build()
    }
}
