package uk.vitalcode.events.fetcher.service

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.model.HBaseRow
import uk.vitalcode.events.fetcher.utils.HBaseUtil.getColumnValue
import uk.vitalcode.events.model.Page


object FetcherService extends Serializable with Log {

    def fetchPages(pages: Set[Page], sc: SparkContext, hBaseConf: Configuration, esIndex: String, esType: String) = {
        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        val esResource = s"$esIndex/$esType"

        // Add link to rowData
        pages.foreach(page => {
            rdd.map(row => (getIndexId(row), HBaseRow(row)))
                .mapValues(_.fetchPropertyValues(page))
                .filter(_._2.nonEmpty)
                .reduceByKey((x, y) => x ++: y)
                .mapValues(_.groupBy(_._1)
                    .map {
                        case (prop, value) => (prop, value.map(_._2).toList)
                    }
                )
                .flatMapValues(p => p
                    .filter(_._1 == "when")
                    .flatMap(_._2)
                    .map {
                        case w: Vector[_] => p +("from" -> List(w(0)), "to" -> List(w(1))) - "when"
                    }
                )
                .map(d => d._2 + ("url" -> List(d._1)))
                .saveToEs(esResource)
        })
    }

    private def getIndexId(row: (ImmutableBytesWritable, Result)): String = {
        getColumnValue(row._2, "metadata", "indexId")
    }
}
