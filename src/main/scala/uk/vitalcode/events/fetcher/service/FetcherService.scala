package uk.vitalcode.events.fetcher.service

import jodd.jerry.Jerry._
import jodd.jerry.JerryNodeFunction
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
            var propertyValues: Vector[(String, Any)] = Vector[(String, Any)]()

            props.foreach(prop => {
                prop.kind match {
                    case PropType.Text | PropType.Date =>
                        var fullProp = prop
                        if (prop.css != null) {
                            jerry(data).$(prop.css).each(new JerryNodeFunction {
                                override def onNode(node: Node, index: Int): Boolean = {
                                    fullProp = fullProp.copy(values = fullProp.values + node.getTextContent.replaceAll( """\s{2,}""", " ").replaceAll( """^\s|\s$""", ""))
                                    true
                                }
                            })
                            val propertyValue: Vector[(String, String)] = PropertyService.getFormattedValues(fullProp)

                            propertyValues = propertyValues ++ propertyValue
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

}
