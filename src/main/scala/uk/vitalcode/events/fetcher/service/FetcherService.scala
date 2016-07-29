package uk.vitalcode.events.fetcher.service

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.model.HBaseRow
import uk.vitalcode.events.fetcher.utils.HBaseUtil.getValueString
import uk.vitalcode.events.fetcher.utils.{HBaseUtil, Model}
import uk.vitalcode.events.model.{Category, Page}

import scala.collection.JavaConversions.asScalaBuffer

object FetcherService extends Serializable with Log {

    private def createRDDFromTable(sc: SparkContext, hBaseConf: Configuration, table: String) = {
        hBaseConf.set(TableInputFormat.INPUT_TABLE, table)
        sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])
    }

    def fetchPages(pages: Set[Page], sc: SparkContext, hBaseConf: Configuration, pageTable: String, eventTable: String,
                   esIndex: String, esType: String): Unit = {

        fetchEvents(pages, sc, hBaseConf, pageTable, eventTable)
        categorizeEvents(sc, hBaseConf, eventTable)
        buildIndex(sc, hBaseConf, eventTable, esIndex, esType)
    }

    def fetchEvents(pages: Set[Page], sc: SparkContext, hBaseConf: Configuration, inputTable: String, outputTable: String) = {
        val rdd = createRDDFromTable(sc, hBaseConf, inputTable)
        val jobConfig: JobConf = new JobConf(hBaseConf, this.getClass)
        jobConfig.setOutputFormat(classOf[TableOutputFormat])
        jobConfig.set(TableOutputFormat.OUTPUT_TABLE, outputTable)

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
                .map(r => {
                    val rowKey = Bytes.toBytes(r._1)
                    val put = new Put(rowKey)
                    r._2.foreach {
                        case (prop, value) => {
                            put.addColumn(Bytes.toBytes("prop"), Bytes.toBytes(prop), HBaseUtil.objectToBytes(value))
                        }
                    }
                    (new ImmutableBytesWritable(rowKey), put)
                })
                .saveAsHadoopDataset(jobConfig)
        })
    }

    def categorizeEvents(sc: SparkContext, hBaseConf: Configuration, eventTable: String) = {


        val rdd = createRDDFromTable(sc, hBaseConf, eventTable)
        val jobConfig: JobConf = new JobConf(hBaseConf, this.getClass)
        jobConfig.setOutputFormat(classOf[TableOutputFormat])
        jobConfig.set(TableOutputFormat.OUTPUT_TABLE, eventTable)

        val gg: RDD[(String, String)] = rdd.map(row => {
            (Bytes.toString(row._1.get()), HBaseUtil.getValueObject[List[String]](row._2, "prop", "description").mkString(" "))
        })


        val sqlContext = new SQLContext(sc)
        val dfTest = sqlContext.createDataFrame(gg)

        val label = Model.getInstance(sc).transform(dfTest)

        label.select("_1", "prediction")
            .map {
                case Row(_1: String, p: Double) => {
                    val rowKey = Bytes.toBytes(_1)
                    val put = new Put(rowKey)
                    put.addColumn(Bytes.toBytes("prop"), Bytes.toBytes("category"), HBaseUtil.objectToBytes(List(Category(p.toInt).toString.toLowerCase())))
                    (new ImmutableBytesWritable(rowKey), put)
                }
            }
            .saveAsHadoopDataset(jobConfig)
    }

    def buildIndex(sc: SparkContext, hBaseConf: Configuration, eventTable: String, esIndex: String, esType: String): Unit = {

        import org.elasticsearch.spark._

        val rdd = createRDDFromTable(sc, hBaseConf, eventTable)
        val esResource = s"$esIndex/$esType"

        rdd.mapValues(value =>
            asScalaBuffer(value.listCells()).map(cell => {
                val column: String = new String(CellUtil.cloneQualifier(cell))
                val value2: Seq[Any] = HBaseUtil.bytesToObject[Seq[Any]](CellUtil.cloneValue(cell))
                (column, value2)
            }).toMap
        )
            .flatMapValues(p => p
                .filter(_._1 == "when")
                .flatMap(_._2)
                .map {
                    case w: Vector[_] => {
                        if (w(1) != None) {
                            p +("from" -> List(w(0)), "to" -> List(w(1))) - "when"
                        } else {
                            p + ("from" -> List(w(0))) - "when"
                        }
                    }
                }
            )
            .map(d => d._2 + ("url" -> List(Bytes.toString(d._1.get()))))
            .saveToEs(esResource)
    }

    private def getIndexId(row: (ImmutableBytesWritable, Result)): String = {
        getValueString(row._2, "metadata", "indexId")
    }
}
