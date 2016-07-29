package uk.vitalcode.events.fetcher.test.common

import com.typesafe.config.{Config, ConfigFactory}

object TestConfig {

    val conf: Config = ConfigFactory.load()

    def pageTable: String = {
        conf.getString("fetcher.hbase.pageTable")
    }

    def eventTable: String = {
        conf.getString("fetcher.hbase.eventTable")
    }

    def hbaseZookeeperQuorum: String = {
        conf.getString("fetcher.hbase.zookeeperQuorum")
    }

    def sparkApp: String = {
        conf.getString("fetcher.spark.app")
    }

    def sparkMaster: String = {
        conf.getString("fetcher.spark.master")
    }

    def elasticIndex: String = {
        conf.getString("fetcher.elastic.index")
    }

    def elasticType: String = {
        conf.getString("fetcher.elastic.type")
    }

    def elasticNodes: String = {
        conf.getString("fetcher.elastic.nodes")
    }
}
