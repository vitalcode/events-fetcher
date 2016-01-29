package uk.vitalcode.events.fetcher.test.parser

import java.time.LocalDateTime

import org.scalatest._
import shapeless.ops.hlist.IsHCons
import shapeless.{HList, HNil}
import uk.vitalcode.events.fetcher.model._
import uk.vitalcode.events.fetcher.parser.PropertyParser

class ParserTest extends FunSuite with ShouldMatchers {

    test("Property parser test") {

        val page = Page()
            .setId("id")
            .setRef("ref")
            .setUrl("url")
            .setLink("link")
            .addProp(
                Prop[String]()
                    .setName("name string")
                    .setCss("css string")
                    .setKind(PropType.Text)
                    .setRaw(Set[String]("    raw!     ")) ::
                    Prop[(LocalDateTime, LocalDateTime)]()
                        .setName("name date")
                        .setCss("css date")
                        .setKind(PropType.Text)
                        .setRaw(Set[String]("date")) :: HNil
            )


        val propStringExp: Prop[String] = Prop[String]("name string", "css string", PropType.Text, Set[String]("    raw!     "), Set[String]("raw!"))
        val propDateExp: Prop[(LocalDateTime, LocalDateTime)] =
            Prop[(LocalDateTime, LocalDateTime)]("name date", "css date", PropType.Text, Set[String]("date"),
                Set[(LocalDateTime, LocalDateTime)]((LocalDateTime.of(2016, 1, 27, 0, 0, 0), LocalDateTime.of(2016, 6, 27, 0, 0, 0))))
        val pageExp = Page("id", "ref", "url", "link", null, null, isRow = false, propStringExp :: propDateExp :: HNil)

        val pageAct = page.copy(props = page.props.map(PropertyParser))
        println(s"actual page [$pageAct]")

        def printItem[I <: HList, P <:HList](props: I)(implicit ev: IsHCons[I], ta: IsHCons[P]) : HList = {
            println(props.head)
            printItem(props.tail)
        }

        printItem(pageAct.pages)

        val head = pageAct.props.head
        val tail = pageAct.props.tail
        println(head)
        println(tail)

        pageAct shouldBe pageExp
    }
}