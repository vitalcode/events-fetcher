package uk.vitalcode.events.fetcher.test.service

import uk.vitalcode.events.cambridge
import uk.vitalcode.events.fetcher.model.{DataRowBuilder, DataTable, DataTableBuilder}
import uk.vitalcode.events.fetcher.service.FetcherService
import uk.vitalcode.events.fetcher.test.common.FetcherTest
import uk.vitalcode.events.model._


class ClientVisitCambridgeTest extends FetcherTest {

    "A Fetcher" when {
        "fetching data from visit cambridge web site" when {
            "building event record from description event pages only" should {
                "fetch all expected property values" in {
                    FetcherService.fetchPages(Set[Page](cambridge.VisitCambridge.page), sc, hBaseConf, esIndex, esType)
                    val actual = esData()
                    val expected = expectedEsDataDescription()
                    actual shouldBe expected
                }
            }
        }
    }

    override protected def putTestData(): Unit = {
        // page 1 link 1
        putTestDataRow("http://www.visitcambridge.org/whats-on/official-guided-tours-cambridge-college-tour-including-kings-college-p568001",
            "/clientVisitCambridgeTest/list1-details-1.html", MineType.TEXT_HTML,
            "http://www.visitcambridge.org/whats-on/official-guided-tours-cambridge-college-tour-including-kings-college-p568001", "visitCambridge:description")
        // page 1 link 1 image
        putTestDataRow("http://www.visitcambridge.org/imageresizer/?image=%2Fdmsimgs%2FGuided%2DTour%2D6%5F68928799%2Ejpg&action=ProductMain",
            "/clientVisitCambridgeTest/image.jpeg", MineType.IMAGE_JPEG,
            "http://www.visitcambridge.org/whats-on/official-guided-tours-cambridge-college-tour-including-kings-college-p568001", "visitCambridge:image")
        // page 1 link 2
        putTestDataRow("http://www.visitcambridge.org/whats-on/cambridge-thai-festival-p686901",
            "/clientVisitCambridgeTest/list1-details-2.html", MineType.TEXT_HTML,
            "http://www.visitcambridge.org/whats-on/cambridge-thai-festival-p686901", "visitCambridge:description")
        // page 1 link 2 image
        putTestDataRow("http://www.visitcambridge.org/imageresizer/?image=%2Fdmsimgs%2FThai%5FFestival%5F2%2Ejpg%5FResized%5F269021640%2Ejpg&action=ProductMain",
            "/clientVisitCambridgeTest/image.jpeg", MineType.IMAGE_JPEG,
            "http://www.visitcambridge.org/whats-on/cambridge-thai-festival-p686901", "visitCambridge:image")
    }

    private def expectedEsDataDescription(): DataTable = {
        DataTableBuilder()
            .addRow(buildEventList1Details1("2016-01-03T11:00:00", "2016-01-03T13:00:00"))
            .addRow(buildEventList1Details1("2016-01-04T11:00:00", "2016-01-04T13:00:00"))
            .addRow(buildEventList1Details1("2016-01-06T11:00:00", "2016-01-06T13:00:00"))
            .addRow(buildEventList1Details1("2016-01-10T11:00:00", "2016-01-10T13:00:00"))

            .addRow(buildEventList1Details2("2016-08-06T10:00:00", "2016-08-06T19:00:00"))
            .addRow(buildEventList1Details2("2016-08-07T10:00:00", "2016-08-07T19:00:00"))
            .build()
    }

    private def buildEventList1Details1(from: String, to: String): DataRowBuilder = {
        DataRowBuilder().setRowId("http://www.visitcambridge.org/whats-on/official-guided-tours-cambridge-college-tour-including-kings-college-p568001") // TODO ?
            .addColumn("title", "Official Guided Tours: Cambridge College Tour - including King’s College")
            .addColumn("description",
                "Only our Official Cambridge Guides are permitted to take groups inside the Cambridge Colleges so visit with an expert and don’t just settle for looking at these wonderful buildings from the outside! Our Blue and Green Badge Guides bring the history of Cambridge to life with fun facts and great stories.",
                "Hear about the famous people connected with Cambridge whilst looking at some of the best-known and impressive sights the city has to offer. Entrance to the magnificent King's College and Chapel is included in the ticket price.",
                "What people have said about our tours... \"We were delighted with our guide who I can personally say was the best guide I have ever had on the numerous tours I have undertaken both here and abroad. She judged her audience well and her sense of humour struck many chords!\""
            )
            .addColumn("image", "http://www.visitcambridge.org/imageresizer/?image=%2Fdmsimgs%2FGuided%2DTour%2D6%5F68928799%2Ejpg&action=ProductMain")
            .addColumn("venue", "Visitor Information Centre Peas Hill Cambridge Cambs CB2 3AD")
            .addColumn("telephone", "Tel: 01223 791501")
            .addColumn("url", "http://www.visitcambridge.org/whats-on/official-guided-tours-cambridge-college-tour-including-kings-college-p568001")
            .addColumn("from", from)
            .addColumn("to", to)
            .addColumn("category", "family")
    }

    private def buildEventList1Details2(from: String, to: String): DataRowBuilder = {
        DataRowBuilder().setRowId("http://www.visitcambridge.org/whats-on/cambridge-thai-festival-p686901")
            .addColumn("title", "Cambridge Thai Festival")
            .addColumn("description",
                "A two day event to experience the taste of Thailand this summer at Parker's Piece, Cambridge is brought to you by Magic of Thailand. Treat yourself to a weekend wonder and bring along your families and friends to feel a touch of Thai culture, where you can challenge your taste buds and feast your eyes on world class food and performances. Unwind with a traditional Thai massage and don't miss the unique eating competition and Muay Thai boxing. The festival starts with a traditional food offering to Buddhist monks and entertainment throughout the day including Muay Thai boxing and blindfold boxing competition, traditional music and dancing, Ladyboy show, Thai beer garden, Thai food and produce stalls, Thai massage and children's play area.",
                "Day Ticket: Adult: £3; Child: £1; Under 5’s: Free"
            )
            .addColumn("cost", "Day Ticket: Adult: £3; Child: £1; Under 5’s: Free")
            .addColumn("image", "http://www.visitcambridge.org/imageresizer/?image=%2Fdmsimgs%2FThai%5FFestival%5F2%2Ejpg%5FResized%5F269021640%2Ejpg&action=ProductMain")
            .addColumn("venue", "Parker's Piece Cambridge CB2 1AA")
            .addColumn("url", "http://www.visitcambridge.org/whats-on/cambridge-thai-festival-p686901")
            .addColumn("from", from)
            .addColumn("to", to)
            .addColumn("category", "family")
    }
}

// TODO Create commom property name enum