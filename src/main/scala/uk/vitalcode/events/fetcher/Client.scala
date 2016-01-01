package uk.vitalcode.events.fetcher {

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}

object Client {

    def main(args: Array[String]) {
        val sparkConf: SparkConf = new SparkConf().setAppName("events-fetcher")
            .setMaster("local[1]")

        val sc = new SparkContext(sparkConf)

        println("HBase rows count:" + rowsCount(sc))
        sc.stop()
    }

    def rowsCount(sc: SparkContext): Long = {

        val conf: Configuration = HBaseConfiguration.create()
        conf.clear()
        conf.set(HConstants.ZOOKEEPER_QUORUM, "robot1.vk,robot2.vk,robot3.vk")
        conf.set(TableInputFormat.INPUT_TABLE, "page")

        val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        rdd.count()
    }
}

}
