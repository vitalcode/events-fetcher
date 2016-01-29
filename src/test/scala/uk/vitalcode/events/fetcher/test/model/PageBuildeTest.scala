//package uk.vitalcode.events.fetcher.test.model
//
//import java.time.LocalDateTime
//
//import org.scalatest._
//import shapeless.{HList, HNil}
//import uk.vitalcode.events.fetcher.model.{Page, PageBuilder, Prop, PropBuilder, PropType}
//
//class PageBuildeTest extends FunSuite with ShouldMatchers {
//
//    test("Page builder test") {
//
//        val pageAct = PageBuilder("id", "ref", "url", "link", null, null, isRow = false, HNil)
//            .setId("id")
//            .setRef("ref")
//            .setUrl("url")
//            .setLink("link")
//            .addProp(PropBuilder[(LocalDateTime, LocalDateTime)]()
//                .setName("name date")
//                .setCss("css date")
//                .setKind(PropType.Text)
//                .setRaw(Set[String]("date"))
//            )
//            .addProp(PropBuilder[String]()
//                .setName("name string")
//                .setCss("css string")
//                .setKind(PropType.Text)
//                .setRaw(Set[String]("    raw!     "))
//            )
//            .build()
//
//        val propString: Prop[String] = Prop[String]("name string", "css string", PropType.Text, Set[String]("    raw!     "))
//        val propDate: Prop[(LocalDateTime, LocalDateTime)] = Prop[(LocalDateTime, LocalDateTime)]("name date", "css date", PropType.Text, Set[String]("date"))
//        val pageExp = Page("id", "ref", "url", "link", null, null, isRow = false, propString :: propDate :: HNil)
//
//        pageAct shouldBe pageExp
//    }
//}
