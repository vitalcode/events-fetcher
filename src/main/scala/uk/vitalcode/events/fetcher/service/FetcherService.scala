package uk.vitalcode.events.fetcher.service

import java.io.{IOException, _}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.model.HBaseRow
import uk.vitalcode.events.fetcher.utils.HBaseUtil.getColumnValue
import uk.vitalcode.events.fetcher.utils.Model
import uk.vitalcode.events.model.{Category, Page}

import scala.collection.JavaConversions.asScalaBuffer

case class RowA(id: String, value: String)


object FetcherService extends Serializable with Log {

    def buildIndex(sc: SparkContext, hBaseConf: Configuration, esIndex: String, esType: String): Unit = {

        import org.elasticsearch.spark._

        val hBaseConfM = HBaseConfiguration.create()
        hBaseConfM.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
        hBaseConfM.set(TableInputFormat.INPUT_TABLE, "dataTable")

        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConfM, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        val esResource = s"$esIndex/$esType"

        rdd.mapValues(value =>
            asScalaBuffer(value.listCells()).map(cell => {
                val column: String = new String(CellUtil.cloneQualifier(cell))
                val value2: Seq[Any] = getColumnObject[Seq[Any]](CellUtil.cloneValue(cell))
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
//
//                    .foreach(d => {
//
//                        d
//                    })
            .saveToEs(esResource)


            //        }.map(p => p
            //            .filter(_._1 == "when")
            //            .flatMap(_._2)
            //            .map {
            //                case w: Vector[_] => {
            //                    if (w(1) != None) {
            //                        p +("from" -> List(w(0)), "to" -> List(w(1))) - "when"
            //                    } else {
            //                        p + ("from" -> List(w(0))) - "when"
            //                    }
            //                }
            //            }
            //        )
            //

            //.count()

        //            (row => (getIndexId(row), HBaseRow(row)))
        //                    .mapValues(_.fetchPropertyValues(page))
        //                    .filter(_._2.nonEmpty)
        //                    .reduceByKey((x, y) => x ++: y)
        //                    .mapValues(_.groupBy(_._1)
        //                        .map {
        //                            case (prop, value) => (prop, value.map(_._2).toList)
        //                        }
        //                    )
        //                    .flatMapValues(p => p
        //                        .filter(_._1 == "when")
        //                        .flatMap(_._2)
        //                        .map {
        //                            case w: Vector[_] => {
        //                                if (w(1) != None) {
        //                                    p +("from" -> List(w(0)), "to" -> List(w(1))) - "when"
        //                                } else {
        //                                    p + ("from" -> List(w(0))) - "when"
        //                                }
        //                            }
        //                        }
        //                    )
        //                    .map(d => d._2 + ("url" -> List(d._1)))
        //                    .saveToEs(esResource)
        //            })
        //  }
    }

    def categorizePages(sc: SparkContext, hBaseConf: Configuration) = {

        import scala.collection.JavaConverters._

        //        val sparkConf: SparkConf = new SparkConf()
        //            .setAppName("app")
        //            .setMaster("local[1]")
        //            .set("spark.driver.allowMultipleContexts", "true")
        //
        //        val sc = new SparkContext(sparkConf)

        val hBaseConfM = HBaseConfiguration.create()
        hBaseConfM.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
        hBaseConfM.set(TableInputFormat.INPUT_TABLE, "dataTable")



        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConfM, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        val gg: RDD[(String, String)] = rdd.map(row => {
            (Bytes.toString(row._1.get()), getColumnObject[List[String]](row._2, "prop", "description").mkString(" "))
        })


        val sqlContext = new SQLContext(sc)
        val dfTest = sqlContext.createDataFrame(gg)

        val jobConfig: JobConf = new JobConf(hBaseConfM, this.getClass)
        jobConfig.setOutputFormat(classOf[TableOutputFormat])
        jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "dataTable")

        val label = Model.getInstance(sc).transform(dfTest)

        label.select("_1", "prediction")
            .map {
                case Row(_1: String, p: Double) => {
                    val rowKey = Bytes.toBytes(_1)
                    val put = new Put(rowKey)
                    put.addColumn(Bytes.toBytes("prop"), Bytes.toBytes("category"), objectToBytes(List(Category(p.toInt).toString.toLowerCase())))
                    (new ImmutableBytesWritable(rowKey), put)
                }
            }
            .saveAsHadoopDataset(jobConfig)

        Category.FUNDRAISING
    }

    def fetchPages(pages: Set[Page], sc: SparkContext, hBaseConf: Configuration, esIndex: String, esType: String) = {


        // val f = categorizePages(sc, hBaseConfM)


        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        val esResource = s"$esIndex/$esType"


        //        val hBaseConfM = HBaseConfiguration.create()
        //        hBaseConfM.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
        //        hBaseConfM.set(TableInputFormat.INPUT_TABLE, "mTable")
        //        hBaseConfM.set(TableOutputFormat.OUTPUT_TABLE, "mTable")

        val jobConfig: JobConf = new JobConf(hBaseConf, this.getClass)
        jobConfig.setOutputFormat(classOf[TableOutputFormat])
        jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "dataTable")


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
                            //                            put.addColumn(Bytes.toBytes("prop"), Bytes.toBytes(prop), Bytes.toBytes(value.mkString("~")))

                            put.addColumn(Bytes.toBytes("prop"), Bytes.toBytes(prop), objectToBytes(value))

                        }
                    }
                    (new ImmutableBytesWritable(rowKey), put)
                })
                .saveAsHadoopDataset(jobConfig)

        })

        val f = categorizePages(sc, hBaseConf)
        buildIndex(sc, hBaseConf, esIndex, esType)
//        val g = 0
    }

    //    def fetchPages(pages: Set[Page], sc: SparkContext, hBaseConf: Configuration, esIndex: String, esType: String) = {
    //        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
    //            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    //            classOf[org.apache.hadoop.hbase.client.Result])
    //
    //        val esResource = s"$esIndex/$esType"
    //
    //        // Add link to rowData
    //        pages.foreach(page => {
    //            rdd.map(row => (getIndexId(row), HBaseRow(row)))
    //                .mapValues(_.fetchPropertyValues(page))
    //                .filter(_._2.nonEmpty)
    //                .reduceByKey((x, y) => x ++: y)
    //                .mapValues(_.groupBy(_._1)
    //                    .map {
    //                        case (prop, value) => (prop, value.map(_._2).toList)
    //                    }
    //                )
    //                .flatMapValues(p => p
    //                    .filter(_._1 == "when")
    //                    .flatMap(_._2)
    //                    .map {
    //                        case w: Vector[_] => {
    //                            if (w(1) != None) {
    //                                p +("from" -> List(w(0)), "to" -> List(w(1))) - "when"
    //                            } else {
    //                                p + ("from" -> List(w(0))) - "when"
    //                            }
    //                        }
    //                    }
    //                )
    //                .map(d => d._2 + ("url" -> List(d._1)))
    //                .saveToEs(esResource)
    //        })
    //    }

    private def getIndexId(row: (ImmutableBytesWritable, Result)): String = {
        getColumnValue(row._2, "metadata", "indexId")
    }

    private def objectToBytes(obj: Any): Array[Byte] = {
        val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
        var out: ObjectOutput = null
        try {
            out = new ObjectOutputStream(bos)
            out.writeObject(obj)
            bos.toByteArray
        } finally {
            try {
                if (out != null) {
                    out.close()
                }
            } catch {
                case ex: IOException =>
            }
            try {
                bos.close()
            } catch {
                case ex: IOException =>
            }
        }
    }

    private def bytesToObject(bytes: Array[Byte]): Any = {
        val bis: ByteArrayInputStream = new ByteArrayInputStream(bytes)
        var in: ObjectInput = null
        try {
            in = new ObjectInputStream(bis)
            in.readObject()
        } finally {
            try {
                bis.close()
            } catch {
                case ex: IOException =>
            }
            try {
                if (in != null) {
                    in.close()
                }
            } catch {
                case ex: IOException =>
            }
        }
    }

    private def getColumnObject[T](result: Result, family: String, column: String): T = {
        bytesToObject(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column))).asInstanceOf[T]
    }

    private def getColumnObject[T](bytes: Array[Byte]): T = {
        bytesToObject(bytes).asInstanceOf[T]
    }
}
