package uk.vitalcode.events.fetcher.test.service

import uk.vitalcode.events.cambridge
import uk.vitalcode.events.fetcher.model.{DataRowBuilder, DataTable, DataTableBuilder}
import uk.vitalcode.events.fetcher.service.FetcherService
import uk.vitalcode.events.fetcher.test.common.FetcherTest
import uk.vitalcode.events.model._


class CambridgeScienceCentreTest extends FetcherTest {

    "A Fetcher" when {
        "fetching data from Cambridge science centre web site" when {
            "building event record from description event pages only" should {
                "fetch all expected property values" in {
                    FetcherService.fetchPages(Set[Page](cambridge.CambridgeScienceCentre.page), sc, hBaseConf, pageTable, eventTable, esIndex, esType)
                    val actual = esData()
                    val expected = expectedEsDataDescription()
                    actual shouldBe expected
                }
            }
        }
    }

    override protected def putTestData(): Unit = {
        // page 1 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events-calendar/",
            "/clientCambridgeScienceCentreTest/list1.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events-calendar/gums-bumsshow-10/", "cambridgeScienceCentre:list")
        // page 1 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events-calendar/gums-bumsshow-10/",
            "/clientCambridgeScienceCentreTest/list1-details-1.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events-calendar/gums-bumsshow-10/", "cambridgeScienceCentre:description")
        // page 1 link 1 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/cache/25/44/25446f694110fcaa739951a7a5151025.jpg",
            "/clientCambridgeScienceCentreTest/image.jpeg", MineType.IMAGE_JPEG,
            "http://www.cambridgesciencecentre.org/whats-on/events-calendar/gums-bumsshow-10/", "cambridgeScienceCentre:image")

        // page 2 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events-calendar/week/2016-09-12/#pagination-fragment",
            "/clientCambridgeScienceCentreTest/list2.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events-calendar/recycle-life-0910/", "cambridgeScienceCentre:list")
        // page 2 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events-calendar/recycle-life-0910/",
            "/clientCambridgeScienceCentreTest/list2-details-1.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events-calendar/recycle-life-0910/", "cambridgeScienceCentre:description")
        // page 2 link 1 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/cache/3e/9a/3e9a8c833f775230fc4f2bf9c740bb70.jpg",
            "/clientCambridgeScienceCentreTest/image.jpeg", MineType.IMAGE_JPEG,
            "http://www.cambridgesciencecentre.org/whats-on/events-calendar/recycle-life-0910/", "cambridgeScienceCentre:image")
    }

    private def expectedEsDataDescription(): DataTable = {
        DataTableBuilder()
            .addRow(buildEventList1Details1("2016-09-10T11:00:00"))
            .addRow(buildEventList1Details1("2016-09-10T15:00:00"))

            .addRow(buildEventList1Details2("2016-09-10T12:00:00"))
            .build()
    }

    private def buildEventList1Details1(from: String): DataRowBuilder = {
        DataRowBuilder()
            .addColumn("title", "Gums to Bums")
            .addColumn("description",
                "What goes in must come out! Explore what happens from the moment food enters the mouth, all the way until it reaches the toilet. See a series of revolting demos and even volunteer to help along the way.",
                "This show is delivered as part of Summer at the Museums.")
            .addColumn("cost", "Free with admission")
            .addColumn("image", "http://www.cambridgesciencecentre.org/media/cache/25/44/25446f694110fcaa739951a7a5151025.jpg")
            .addColumn("url", "http://www.cambridgesciencecentre.org/whats-on/events-calendar/gums-bumsshow-10/")
            .addColumn("venue", "Cambridge Science Centre, 18 Jesus Lane, Cambridge, CB5 8BQ")
            .addColumn("from", from)
            .addColumn("venue-category", "family")
            .addColumn("category", "family")
    }

    private def buildEventList1Details2(from: String): DataRowBuilder = {
        DataRowBuilder()
            .addColumn("title", "Recycle for Life")
            .addColumn("description",
                "Find out, through a series of fun and interactive demonstrations, how nature recycles its resources to keep the Earth in balance and the impact our modern lifestyle is having. What lessons can we learn from natural recycling to maintain the delicate balance?",
                "This show is suitable for all ages and delivered as part of Summer at the Museums.")
            .addColumn("cost", "Free with admission")
            .addColumn("image", "http://www.cambridgesciencecentre.org/media/cache/3e/9a/3e9a8c833f775230fc4f2bf9c740bb70.jpg")
            .addColumn("url", "http://www.cambridgesciencecentre.org/whats-on/events-calendar/recycle-life-0910/")
            .addColumn("venue", "Cambridge Science Centre, 18 Jesus Lane, Cambridge, CB5 8BQ")
            .addColumn("from", from)
            .addColumn("venue-category", "family")
            .addColumn("category", "family")
    }
}
