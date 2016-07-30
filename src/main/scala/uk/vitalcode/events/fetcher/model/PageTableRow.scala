package uk.vitalcode.events.fetcher.model

import jodd.jerry.Jerry._
import jodd.jerry.JerryNodeFunction
import jodd.lagarto.dom.Node
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import uk.vitalcode.events.fetcher.service.PropertyService
import uk.vitalcode.events.model.{Page, Prop, PropType}
import uk.vitalcode.events.fetcher.utils.HBaseUtil.getValueString

class PageTableRow(url: String, data: String, mineType: String, pageId: String) extends Serializable {

    def this(row: Result) = this(
        getValueString(row, "metadata", "url"),
        getValueString(row, "content", "data"),
        getValueString(row, "metadata", "mineType"),
        getValueString(row, "metadata", "pageId")
    )

    def fetchPropertyValues(page: Page): Seq[(String, Any)] = {
        val props: Set[Prop] = getPageProperties(Set(page), pageId)
        var propertyValues: Vector[(String, Any)] = Vector[(String, Any)]()

        props.foreach(prop => {
            prop.kind match {
                case PropType.Text | PropType.Date =>
                    var fullProp = prop
                    if (prop.values.isEmpty && prop.css != null) {
                        jerry(data).$(prop.css).each(new JerryNodeFunction {
                            override def onNode(node: Node, index: Int): Boolean = {
                                fullProp = fullProp.copy(values = fullProp.values :+ node.getTextContent
                                    .replaceAll( """\s{2,}""", " ")
                                    .replaceAll( """^\s|\s$""", ""))
                                true
                            }
                        })}
                    if (fullProp.values.nonEmpty) {
                        val propertyValue: Vector[(String, Any)] = PropertyService.getFormattedValues(fullProp)
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

object PageTableRow {
    def apply(row: Result) = new PageTableRow(row)
}