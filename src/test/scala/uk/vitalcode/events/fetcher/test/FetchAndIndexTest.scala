package uk.vitalcode.events.fetcher.test

import org.elasticsearch.spark._
import uk.vitalcode.events.fetcher.model._
import uk.vitalcode.events.fetcher.service.{EsQueryService, FetcherService}
import uk.vitalcode.events.fetcher.test.common.FetcherTest

class FetchAndIndexTest extends FetcherTest {

    "A Fetcher" when {
        "fetching data from Cambridge science centre web site" when {
            "building event record from description event pages only" should {
                "should fetch all expected property values" in {
                    val testIndex = "test_index"
                    val testType = "test_type"
                    val resource = s"$testIndex/$testType"
                    val pages = Set[Page](buildTestPageDescription())

                    FetcherService.fetchPages(pages, sc, hBaseConf, testIndex, testType)
                    Thread.sleep(1000)

                    sc.esRDD(resource, EsQueryService.matchAll("Cambridge")).count() shouldBe 2
                    sc.esRDD(resource, EsQueryService.matchAll("cambridge festival")).count() shouldBe 1
                }
            }
        }
    }

    override protected def putTestData(): Unit = {
        // page 1 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list",
            "/clientCambridgeScienceCentreTest/list1.html", MineType.TEXT_HTML)
        // page 1 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/destination-space-crew-09012016-1500/",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.html", MineType.TEXT_HTML)
        // page 1 link 1 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/969c39e09b655c715be0aa6b578908427d75e7.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
        // page 1 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/Voyagetospace_09012016_1600/",
            "/clientCambridgeScienceCentreTest/voyagetospace_09012016_1600.html", MineType.TEXT_HTML)
        // page 1 link 2 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/0004a8c035b90924f8321df21276fc8f83a6cd.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)

        // page 2 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list/?page=2",
            "/clientCambridgeScienceCentreTest/list2.html", MineType.TEXT_HTML)
        // page 2 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/otherworlds/",
            "/clientCambridgeScienceCentreTest/otherworlds.html", MineType.TEXT_HTML)
        // page 2 link 1 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/37cf8f84e5cfa94cdcac3f73bc13cfea3556a7.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
        // page 2 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/sunday-science-20-march/",
            "/clientCambridgeScienceCentreTest/sunday-science-20-march.html", MineType.TEXT_HTML)
        // page 2 link 2 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/200e303cecd9eee71f77c97ddea630521cbfe9.png",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)

        // page 3 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list/?page=3",
            "/clientCambridgeScienceCentreTest/list3.html", MineType.TEXT_HTML)
        // page 3 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/february-half-term-2016/",
            "/clientCambridgeScienceCentreTest/february-half-term-2016.html", MineType.TEXT_HTML)
        // page 3 link 1 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/d78141bc0cc3f96d175843c2cd0e97beb9c370.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
        // page 3 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/electric-universe/",
            "/clientCambridgeScienceCentreTest/electric-universe.html", MineType.TEXT_HTML)
        // page 3 link 2 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/fb2024b1db936348b42d3edd48995c32f69a1d.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
    }

    private def buildTestPageDescription(): Page = {
        PageBuilder()
            .setId("list")
            .setUrl("http://www.cambridgesciencecentre.org/whats-on/list")
            .addPage(PageBuilder()
                .isRow(true)
                .setId("description")
                .setLink("div.main_wrapper > section > article > ul > li > h2 > a")
                .addPage(PageBuilder()
                    .setId("image")
                    .setLink("section.event_detail > div.page_content > article > img")
                )
                .addProp(PropBuilder()
                    .setName("description")
                    .setCss("div.main_wrapper > section.event_detail > div.page_content p:nth-child(4)")
                    .setKind(PropType.Text)
                )
                .addProp(PropBuilder()
                    .setName("cost")
                    .setCss("div.main_wrapper > section.event_detail > div.page_content p:nth-child(5)")
                    .setKind(PropType.Text)
                )
            )
            .addPage(PageBuilder()
                .setRef("list")
                .setId("pagination")
                .setLink("div.pagination > div.omega > a")
            )
            .build()
    }
}
