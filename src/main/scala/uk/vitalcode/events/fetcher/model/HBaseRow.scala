package uk.vitalcode.events.fetcher.model

import jodd.jerry.Jerry._
import jodd.jerry.JerryNodeFunction
import jodd.lagarto.dom.Node
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import uk.vitalcode.events.fetcher.service.PropertyService
import uk.vitalcode.events.model.{Page, Prop, PropType}
import uk.vitalcode.events.fetcher.utils.HBaseUtil.getColumnValue

class HBaseRow(url: String, data: String, mineType: String, pageId: String) extends Serializable {

    def this(row: (ImmutableBytesWritable, Result)) = this(
        getColumnValue(row._2, "metadata", "url"),
        getColumnValue(row._2, "content", "data"),
        getColumnValue(row._2, "metadata", "mineType"),
        getColumnValue(row._2, "metadata", "pageId")
    )

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
                                fullProp = fullProp.copy(values = fullProp.values + node.getTextContent
                                    .replaceAll( """\s{2,}""", " ")
                                    .replaceAll( """^\s|\s$""", ""))
                                true
                            }
                        })
                        val propertyValue: Vector[(String, Any)] = PropertyService.getFormattedValues(fullProp)

                        propertyValues = propertyValues ++ propertyValue
                    }
                case _ => props.foreach(prop => propertyValues = propertyValues :+(prop.name, url))
            }
        })
        //if (propertyValues.nonEmpty) Some(propertyValues) else None
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

object HBaseRow {
    def apply(row: (ImmutableBytesWritable, Result)) = new HBaseRow(row)
}