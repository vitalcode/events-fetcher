package uk.vitalcode.events.fetcher.service

import java.util.UUID

import jodd.jerry.Jerry._
import jodd.jerry.{Jerry, JerryNodeFunction}
import jodd.lagarto.dom.Node
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.model._

import scala.collection.mutable

object FetcherService extends Serializable with Log {

    var table: mutable.Map[String, mutable.Map[String, String]] = new mutable.HashMap[String, mutable.Map[String, String]]
    var rowColumns: mutable.Map[String, String] = new mutable.HashMap[String, String]

    def logOutcame(): Unit = {
        log.info(s"inside logOutcame table rows [${table.size}], columns [${rowColumns.size}]")
        table += (rowID.concat(UUID.randomUUID().toString) -> rowColumns)

        table.foreach(t => {
            log.info(s"--- row: ${t._1}")
            t._2.foreach(col => {
                log.info(s"--- --- column: ${col._1} --- ${col._2}")
            })
        })
    }

    var rowID: String = ""
    var dataRowBuilder: DataRowBuilder = DataRowBuilder()

    def fetchPage(page: Page, sc: SparkContext, hBaseConf: Configuration): DataTable = {

        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        val builder: DataTableBuilder = DataTableBuilder()
        fetchPage(page, rdd, builder)

        dataRowBuilder.setRowId(rowID)
        builder.addRow(dataRowBuilder)

        builder.build()
    }

    def fetchPage(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)], builder: DataTableBuilder): Unit = {

        log.info(s"Fetching data for $page")
        fetchPageData(page, rdd) match {
            case Some(result) =>

                if (result.page.isRow && rowID.nonEmpty) {
                    table += (s"%s___%s".format(rowID, UUID.randomUUID().toString) -> rowColumns)
                    dataRowBuilder.setRowId(rowID)
                    builder.addRow(dataRowBuilder)

                    rowColumns = new mutable.HashMap[String, String]
                    dataRowBuilder = DataRowBuilder()
                    rowID = ""
                }

                if (result.page.isRow && rowID.isEmpty) {
                    rowID = page.url
                    //dataRowBuilder.setRowId(page.url)
                }

                result.page.props.values.foreach(prop => {
                    prop.value.foreach(value => {
                        log.info(s"Fetched page prop: ${prop.name} -- $value")
                        rowColumns += (s"%s___%s".format(prop.name, UUID.randomUUID().toString) -> value)
                    })
                    dataRowBuilder.addColumn(prop.name, prop.value)

                })
                log.info(s"Fetched child pages ${result.childPages}")


                result.childPages.foreach(p => {
                    println(s"fetching child page: -- $p")
                    fetchPage(p, rdd, builder)
                })
            case None =>
        }
    }

    private def fetchPageData(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)]): Option[FetchResult] = {
        rdd.filter(row => Bytes.toString(row._1.get()) == page.url)
            .map(row => jerry(Bytes.toString(row._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("data")))))
            .map(dom => {
                // find child pages
                val childPages = collection.mutable.ArrayBuffer[Page]()
                // 1. treat page with self child pages
                page.pages.foreach(p => {
                    val cp: Page = if (p.ref == null) p else getParentPageByRef(p, p.ref)
                    if (p.link != null) {
                        dom.$(p.link).each(new JerryNodeFunction {
                            override def onNode(node: Node, index: Int): Boolean = {
                                val childPageUrl = node.getAttribute("href")
                                val childPage: Page = Page(cp.id, cp.ref, childPageUrl, cp.link, cp.props, cp.pages, cp.parent, cp.isRow)
                                childPages += childPage
                                true
                            }
                        })
                    }
                })

                fetchPageProperties(page, dom)
                FetchResult(page, childPages.toSeq)
            })
            .collect()
            .headOption
    }

    private def getParentPageByRef(page: Page, ref: String): Page = {
        if (page.id.equals(ref)) page else getParentPageByRef(page.parent, ref)
    }

    private def fetchPageProperties(currentPage: Page, dom: Jerry): Unit = {
        currentPage.props.values.foreach(p => fetchProperty(p, dom))
    }

    private def fetchProperty(prop: Prop, dom: Jerry): Unit = {
        prop.reset()
        dom.$(prop.css).each(new JerryNodeFunction {
            override def onNode(node: Node, index: Int): Boolean = {
                val value = prop.kind match {
                    case PropType.Link => node.getAttribute("href")
                    case _ => node.getTextContent
                }
                val propValue: String = value.replaceAll( """\s{2,}""", " ").replaceAll( """^\s|\s$""", "")
                println(s"${prop.name} -- $propValue")
                prop.value += propValue
                true
            }
        })
    }


    //    def getPageDom(currentPage: Page): Unit = {
    //        rdd.filter(f => Bytes.toString(f._1.get()) == currentPage.url)
    //            .foreach(f => {
    //                val pageData: String = Bytes.toString(f._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("data")))
    //                val pageDom: Jerry = jerry(pageData)
    //                currentPage.pages.foreach(p => {
    //                    pageDom.$(p.link).each(new JerryNodeFunction {
    //                        override def onNode(node: Node, index: Int): Boolean = {
    //                            val childPageUrl = node.getAttribute("href")
    //                            println(s"childPageUrl [$childPageUrl]")
    //                            val childPage: Page = Page(currentPage.id, currentPage.ref, childPageUrl, currentPage.link, currentPage.props, currentPage.pages, currentPage.parent, currentPage.isRow)
    //                            println(s"childPage [$childPage]")
    //                            //getPageDom(childPage)
    //                            true
    //                        }
    //                    })
    //                })
    //                //println(pageData)
    //            })
    //    }
}
