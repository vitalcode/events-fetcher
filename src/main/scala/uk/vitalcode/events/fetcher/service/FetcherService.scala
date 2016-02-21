package uk.vitalcode.events.fetcher.service

import java.net.URI

import jodd.jerry.Jerry._
import jodd.jerry.{Jerry, JerryNodeFunction}
import jodd.lagarto.dom.Node
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import uk.vitalcode.events.fetcher.common.Log
import uk.vitalcode.events.fetcher.model._


object FetcherService extends Serializable with Log {

    def fetchPages(pages: Set[Page], sc: SparkContext, hBaseConf: Configuration, esIndex: String, esType: String) = {
        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        val esResource = s"$esIndex/$esType"

        pages.foreach(page => {
            rdd.map(row => (getIndexId(row._2), getRowData(row)))
                .groupByKey()
                .map(document => (
                    document._1,
                    document._2.flatMap(tt => tt.fetchPropertyValues(page))
                        .groupBy(_._1)
                        .map { case (prop, value) => (prop, value.map(_._2).toList) }
                    )
                )
                .saveToEsWithMeta(esResource)
        })
    }

    private def getIndexId(row: Result): String = {
        getColumnValue(row, "metadata", "indexId")
    }

    private def getRowData(row: (ImmutableBytesWritable, Result)): RowData = {
        val url = getColumnValue(row._2, "metadata", "url")
        val data = getColumnValue(row._2, "content", "data")
        val mineType = getColumnValue(row._2, "metadata", "mineType")
        val pageId = getColumnValue(row._2, "metadata", "pageId")

        RowData(url, data, mineType, pageId)
    }

    private def getColumnValue(result: Result, family: String, column: String): String = {
        Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)))
    }

    private case class RowData(url: String, data: String, mineType: String, pageId: String) extends Serializable {

        def fetchPropertyValues(page: Page): Seq[(String, Any)] = {
            val props: Set[Prop] = getPageProperties(Set(page), pageId)
            var propertyValues: Vector[(String, Any)] = Vector()

            props.foreach(prop => {
                prop.kind match {
                    case PropType.Text =>
                        if (prop.css != null) {
                            jerry(data).$(prop.css).each(new JerryNodeFunction {
                                override def onNode(node: Node, index: Int): Boolean = {
                                    val propertyValue = (prop.name, node.getTextContent.replaceAll( """\s{2,}""", " ").replaceAll( """^\s|\s$""", ""))
                                    propertyValues = propertyValues :+ propertyValue
                                    true
                                }
                            })
                        }
                    case _ => props.foreach(prop => propertyValues = propertyValues :+(prop.name, url))
                }
            })
            propertyValues
        }

        private def getPageProperties(pages: Set[Page], pageId: String): Set[Prop] = {
            val page = pages.find(page => page.id == pageId)
            if (page.isDefined) {
                page.get.props
            } else {
                pages.flatMap(page => getPageProperties(page.pages, pageId))
            }
        }
    }


    //    def fetchPages(pages: Set[Page], sc: SparkContext, hBaseConf: Configuration, myEsIndex: String, myEsType: String) = {
    //        pages.foreach(page => {
    //            val table: DataTable = fetchPage(page, sc, hBaseConf)
    //            sc.makeRDD(table.dataRows.map(t => (t.rowId, t.columns)).toSeq).saveToEsWithMeta(s"$myEsIndex/$myEsType")
    //        })
    //    }

//    def fetchPage(page: Page, sc: SparkContext, hBaseConf: Configuration): DataTable = {
//
//        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
//            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//            classOf[org.apache.hadoop.hbase.client.Result])
//
//        val tableBuilder: DataTableBuilder = DataTableBuilder()
//        val rowBuilder: DataRowBuilder = DataRowBuilder()
//
//        fetchPage(page, rdd, tableBuilder, rowBuilder)
//        tableBuilder.addRow(rowBuilder)
//
//        val table = tableBuilder.build()
//        log.info(table.toString())
//        table
//    }
//
//    def fetchPage(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)], builder: DataTableBuilder, dataRowBuilder: DataRowBuilder): Unit = {
//        log.info(s"Fetching data for $page")
//        fetchPageData(page, rdd) match {
//            case Some(result) =>
//                if (result.page.isRow) {
//                    if (!dataRowBuilder.isEmpty()) {
//                        builder.addRow(dataRowBuilder)
//                        dataRowBuilder.reset()
//                        dataRowBuilder.setRowId(page.url)
//                    } else {
//                        dataRowBuilder.setRowId(page.url)
//                    }
//                }
//                result.page.props.foreach(prop => {
//                    prop.values.foreach(value => {
//                        log.info(s"Fetched page property [${prop.name}] -- [$value]")
//                    })
//
//                    dataRowBuilder.addColumn(prop.name, PropertyService.getFormattedValues(prop))
//                })
//                log.info(s"Fetched child pages [${result.childPages}]")
//                result.childPages.foreach(p => {
//                    //                    fetchPage(p, rdd, builder, dataRowBuilder)
//                })
//            case None =>
//        }
//    }
//
//    private def fetchPageData(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)]): Option[FetchResult] = {
//        rdd.filter(row => Bytes.toString(row._1.get()) == page.url)
//            .map(row => jerry(Bytes.toString(row._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("data")))))
//            .map(dom => {
//                val derivedPages = collection.mutable.ArrayBuffer[Page]()
//                page.pages.foreach(childPages => {
//                    val nextPages: Page = if (childPages.ref == null) childPages else getParentPageByRef(childPages, childPages.ref)
//                    if (childPages.link != null) {
//                        dom.$(childPages.link).each(new JerryNodeFunction {
//                            override def onNode(node: Node, index: Int): Boolean = {
//                                val baseUri = new URI(page.url)
//                                val childLinkUrl = node.getAttribute("href")
//                                val childImageUrl = node.getAttribute("src")
//                                val childUri = if (childLinkUrl != null) new URI(childLinkUrl) else new URI(childImageUrl)
//                                val resolvedUri = baseUri.resolve(childUri).toString
//                                val childPage: Page = Page(nextPages.id, nextPages.ref, resolvedUri, nextPages.link, nextPages.props, nextPages.pages, nextPages.parent, nextPages.isRow)
//                                derivedPages += childPage
//                                true
//                            }
//                        })
//                    }
//                })
//
//                fetchPageProperties(page, dom)
//                FetchResult(page, derivedPages.toSeq)
//            })
//            .collect()
//            .headOption
//    }
//
//    private def getParentPageByRef(page: Page, ref: String): Page = {
//        if (page.id.equals(ref)) page else getParentPageByRef(page.parent, ref)
//    }
//
//    private def fetchPageProperties(currentPage: Page, dom: Jerry): Unit = {
//        currentPage.props = currentPage.props.map(prop => {
//            var propReset = prop.copy(values = Set.empty[String])
//            if (propReset.css != null) {
//                dom.$(propReset.css).each(new JerryNodeFunction {
//                    override def onNode(node: Node, index: Int): Boolean = {
//                        val value = node.getTextContent.replaceAll( """\s{2,}""", " ").replaceAll( """^\s|\s$""", "")
//                        propReset = propReset.copy(values = propReset.values + value)
//                        true
//                    }
//                })
//            } else {
//                propReset = propReset.copy(values = propReset.values + currentPage.url)
//            }
//            propReset
//        })
//    }
}
