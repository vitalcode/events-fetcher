package uk.vitalcode.events.fetcher.service

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
import uk.vitalcode.events.fetcher.parser.ParserLike
import scala.reflect.runtime.universe._

object FetcherService extends Serializable with Log {

    def fetchPage(page: Page, sc: SparkContext, hBaseConf: Configuration): DataTable = {

        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        val tableBuilder: DataTableBuilder = DataTableBuilder()
        val rowBuilder: DataRowBuilder = DataRowBuilder()

        fetchPage(page, rdd, tableBuilder, rowBuilder)
        tableBuilder.addRow(rowBuilder)

        val table = tableBuilder.build()
        log.info(table.toString())
        table
    }

    def fetchPage(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)], builder: DataTableBuilder, dataRowBuilder: DataRowBuilder): Unit = {
        log.info(s"Fetching data for $page")
        fetchPageData(page, rdd) match {
            case Some(result) =>
                if (result.page.isRow) {
                    if (!dataRowBuilder.isEmpty()) {
                        builder.addRow(dataRowBuilder)
                        dataRowBuilder.reset()
                        dataRowBuilder.setRowId(page.url)
                    } else {
                        dataRowBuilder.setRowId(page.url)
                    }
                }
                result.page.props.values.foreach(prop => {
                    prop.value.foreach(value => {
                        log.info(s"Fetched page property [${prop.name}] -- [$value]")
                    })

                    dataRowBuilder.addColumn(prop.name, prop.value.map(_.toString))
                })
                log.info(s"Fetched child pages [${result.childPages}]")
                result.childPages.foreach(p => {
                    fetchPage(p, rdd, builder, dataRowBuilder)
                })
            case None =>
        }
    }

    private def fetchPageData(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)]): Option[FetchResult] = {
        rdd.filter(row => Bytes.toString(row._1.get()) == page.url)
            .map(row => jerry(Bytes.toString(row._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("data")))))
            .map(dom => {
                val derivedPages = collection.mutable.ArrayBuffer[Page]()
                page.pages.foreach(childPages => {
                    val nextPages: Page = if (childPages.ref == null) childPages else getParentPageByRef(childPages, childPages.ref)
                    if (childPages.link != null) {
                        dom.$(childPages.link).each(new JerryNodeFunction {
                            override def onNode(node: Node, index: Int): Boolean = {
                                val childPageUrl = node.getAttribute("href")
                                val childPage: Page = Page(nextPages.id, nextPages.ref, childPageUrl, nextPages.link, nextPages.props, nextPages.pages, nextPages.parent, nextPages.isRow)
                                derivedPages += childPage
                                true
                            }
                        })
                    }
                })

                fetchPageProperties(page, dom)
                FetchResult(page, derivedPages.toSeq)
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

    private def fetchProperty[T](prop: Prop[T], dom: Jerry)(implicit tag: WeakTypeTag[T]): Unit = {
        val targs = tag.tpe match { case TypeRef(_, _, args) => args }
        println(targs)

        prop.reset()
        var raw: Set[String] = Set.empty[String]
        dom.$(prop.css).each(new JerryNodeFunction {
            override def onNode(node: Node, index: Int): Boolean = {
                val value = prop.kind match {
                    case PropType.Link => node.getAttribute("href")
                    case _ => node.getTextContent
                }

//                val propValue: String = value.replaceAll( """\s{2,}""", " ").replaceAll( """^\s|\s$""", "")
//                prop.value += propValue
                    raw += value
                true
            }
        })

        ParserLike.parse2(prop, raw)
    }
}

// TODO run from client main app
// TODO run on yarn mode spark
