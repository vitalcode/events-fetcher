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
import uk.vitalcode.events.fetcher.model.PageTableRow
import uk.vitalcode.events.fetcher.utils.{HBaseUtil, MLUtil}
import uk.vitalcode.events.fetcher.utils.HBaseUtil.getValueString
import uk.vitalcode.events.model.{Category, Page}

import scala.collection.JavaConversions.asScalaBuffer

object FetcherService extends Serializable with Log {

    def fetchPages(pages: Set[Page], sc: SparkContext, hBaseConf: Configuration, pageTable: String, eventTable: String,
                   esIndex: String, esType: String): Unit = {

        fetchEvents(pages, sc, hBaseConf, pageTable, eventTable)
        categorizeEvents(sc, hBaseConf, eventTable)
        buildIndex(sc, hBaseConf, eventTable, esIndex, esType)
    }

    def fetchEvents(pages: Set[Page], sc: SparkContext, hBaseConf: Configuration, pageTable: String, eventTable: String) = {
        val rdd = createRDDFromTable(sc, hBaseConf, pageTable)
        val jobConfig: JobConf = createJobConfig(hBaseConf, eventTable)

        pages.foreach(page => {
            rdd.map {
                case (key: ImmutableBytesWritable, row: Result) =>
                    (getValueString(row, "metadata", "indexId"), PageTableRow(row))
            }
                .mapValues(_.fetchPropertyValues(page))
                .filter(_._2.nonEmpty)
                .reduceByKey((x, y) => x ++: y)
                .mapValues(_.groupBy(_._1).mapValues(_.map(_._2)))
                .map(row => {
                    val rowKey = Bytes.toBytes(row._1)
                    val put = new Put(rowKey)
                    row._2.foreach {
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
        val sqlContext = new SQLContext(sc)
        val rdd = createRDDFromTable(sc, hBaseConf, eventTable)
        val jobConfig: JobConf = createJobConfig(hBaseConf, eventTable)

        val rddTest: RDD[(String, String)] = rdd.map {
            case (key, value) =>
                (Bytes.toString(key.get()),
                    (HBaseUtil.getValueObject[Seq[String]](value, "prop", "title").getOrElse(Seq()) ++
                        HBaseUtil.getValueObject[Seq[String]](value, "prop", "description").getOrElse(Seq())).mkString(" "))
        }
        val dfTest = sqlContext.createDataFrame(rddTest)

        MLUtil.getEventCategoryModel(sqlContext).transform(dfTest)
            .select("_1", "prediction")
            .map {
                case Row(_1: String, p: Double) => {
                    val rowKey = Bytes.toBytes(_1)
                    val put = new Put(rowKey)
                    put.addColumn(Bytes.toBytes("prop"), Bytes.toBytes("category"),
                        HBaseUtil.objectToBytes(List(Category(p.toInt).toString.toLowerCase())))
                    (new ImmutableBytesWritable(rowKey), put)
                }
            }
            .saveAsHadoopDataset(jobConfig)
    }

    def buildIndex(sc: SparkContext, hBaseConf: Configuration, eventTable: String, esIndex: String, esType: String): Unit = {
        import org.elasticsearch.spark._
        val rdd = createRDDFromTable(sc, hBaseConf, eventTable)
        val esResource = s"$esIndex/$esType"

        rdd.mapValues(data =>
            asScalaBuffer(data.listCells()).map(cell => {
                val column: String = new String(CellUtil.cloneQualifier(cell))
                val value: Seq[Any] = HBaseUtil.bytesToObject[Seq[Any]](CellUtil.cloneValue(cell))
                (column, value)
            }).toMap
        ).flatMapValues(p => p
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
        ).map(d => d._2 + ("url" -> List(Bytes.toString(d._1.get()))))
            .saveToEs(esResource)
    }

    private def createRDDFromTable(sc: SparkContext, hBaseConf: Configuration, table: String): RDD[(ImmutableBytesWritable, Result)] = {
        hBaseConf.set(TableInputFormat.INPUT_TABLE, table)
        sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])
    }

    private def createJobConfig(hBaseConf: Configuration, outputTable: String): JobConf = {
        val jobConfig: JobConf = new JobConf(hBaseConf, this.getClass)
        jobConfig.setOutputFormat(classOf[TableOutputFormat])
        jobConfig.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
        jobConfig
    }
}
