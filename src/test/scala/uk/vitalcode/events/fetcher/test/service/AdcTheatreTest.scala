package uk.vitalcode.events.fetcher.test.service

import uk.vitalcode.events.cambridge
import uk.vitalcode.events.fetcher.model.{DataRowBuilder, DataTable, DataTableBuilder}
import uk.vitalcode.events.fetcher.service.FetcherService
import uk.vitalcode.events.fetcher.test.common.FetcherTest
import uk.vitalcode.events.model._


class AdcTheatreTest extends FetcherTest {

    "A Fetcher" when {
        "fetching data from Adc Theatre web site" when {
            "building event record from description event pages only" should {
                "fetch all expected property values" in {
                    FetcherService.fetchPages(Set[Page](cambridge.AdcTheater.page), sc, hBaseConf, pageTable, eventTable, esIndex, esType)
                    val actual = esData()
                    val expected = expectedEsDataDescription()
                    actual shouldBe expected
                }
            }
        }
    }

    override protected def putTestData(): Unit = {
        // page 1 link 1
        putTestDataRow("http://www.adctheatre.com/whats-on/drama/cast-2016-as-you-like-it-preview.aspx",
            "/adcTheatre/list1-details-1.html", MineType.TEXT_HTML,
            "http://www.adctheatre.com/whats-on/drama/cast-2016-as-you-like-it-preview.aspx", "adcTheater:description")
        // page 1 link 1 image
        putTestDataRow("http://www.adctheatre.com/media/107947393/As-You-Like-It_Landscape.jpg",
            "/adcTheatre/image.jpeg", MineType.IMAGE_JPEG,
            "http://www.adctheatre.com/whats-on/drama/cast-2016-as-you-like-it-preview.aspx", "adcTheater:image")

        // page 1 link 2
        putTestDataRow("http://www.adctheatre.com/whats-on/workshop/backstage-at-the-adc-theatre.aspx",
            "/adcTheatre/list1-details-2.html", MineType.TEXT_HTML,
            "http://www.adctheatre.com/whats-on/workshop/backstage-at-the-adc-theatre.aspx", "adcTheater:description")
        // page 1 link 2 image
        putTestDataRow("http://www.adctheatre.com/media/997805/curtain_Landscape.jpg",
            "/adcTheatre/image.jpeg", MineType.IMAGE_JPEG,
            "http://www.adctheatre.com/whats-on/workshop/backstage-at-the-adc-theatre.aspx", "adcTheater:image")

        // page 1 link 3
        putTestDataRow("http://www.adctheatre.com/whats-on/musical/made-in-dagenham.aspx",
            "/adcTheatre/list1-details-3.html", MineType.TEXT_HTML,
            "http://www.adctheatre.com/whats-on/musical/made-in-dagenham.aspx", "adcTheater:description")
        // page 1 link 3 image
        putTestDataRow("http://www.adctheatre.com/media/112832935/Made-in-Dagenham_Landscape.jpg",
            "/adcTheatre/image.jpeg", MineType.IMAGE_JPEG,
            "http://www.adctheatre.com/whats-on/musical/made-in-dagenham.aspx", "adcTheater:image")
    }

    private def expectedEsDataDescription(): DataTable = {
        DataTableBuilder()
            .addRow(buildEventList1Details1("2016-08-25T19:45:00"))

            .addRow(buildEventList1Details2("2016-09-09T12:00:00"))
            .addRow(buildEventList1Details2("2016-09-09T14:00:00"))
            .addRow(buildEventList1Details2("2016-09-09T16:00:00"))

            .addRow(buildEventList1Details3("2016-09-15T19:45:00"))
            .addRow(buildEventList1Details3("2016-09-16T19:45:00"))
            .addRow(buildEventList1Details3("2016-09-17T19:45:00"))
            .build()
    }

    private def buildEventList1Details1(from: String): DataRowBuilder = {
        DataRowBuilder()
            .addColumn("title", "As You Like It (Preview)")
            .addColumn("description",
                "William Shakespeare",
                "‘All the world's a stage, and all the men and women merely players...’",
                "Duke Frederick has usurped and exiled his brother Duke Ferdinand. Oliver de Boys is plotting against his brother Orlando’s life. Rosalind has disguised herself as a man and run off into the Forest of Arden alongside her cousin Celia. Shakespeare’s early comedy takes us into a world of mistaken identities, murderous plots, cross-dressing, pastoral love, and wrestling.",
                "With a modern, bohemian aesthetic, live music, and a lot of ribbons, CAST brings one of Shakespeare’s best-loved comedies to the stage.",
                "Join us under the Greenwood tree."
            )
            .addColumn("image", "http://www.adctheatre.com/media/107947393/As-You-Like-It_Landscape.jpg")
            .addColumn("cost", "£12/£9")
            .addColumn("from", from)
            .addColumn("venue", "ADC Theatre, Park Street, Cambridge, CB5 8AS")
            .addColumn("venue-category", "theatre")
            .addColumn("telephone", "01223 300085")
            .addColumn("url", "http://www.adctheatre.com/whats-on/drama/cast-2016-as-you-like-it-preview.aspx")
            .addColumn("category", "family")
    }

    private def buildEventList1Details2(from: String): DataRowBuilder = {
        DataRowBuilder()
            .addColumn("title", "Backstage at the ADC Theatre")
            .addColumn("description", "The ADC is a centre of student drama in Cambridge and a space that has helped launch the careers of theatre luminaries such as Sir Ian McKellen, Emma Thompson and Rachel Weiss. This tour explores what happens behind the curtains, taking you to areas that are usually closed to the public.")
            .addColumn("image", "http://www.adctheatre.com/media/997805/curtain_Landscape.jpg")
            .addColumn("cost", "Free")
            .addColumn("from", from)
            .addColumn("venue", "ADC Theatre, Park Street, Cambridge, CB5 8AS")
            .addColumn("venue-category", "theatre")
            .addColumn("telephone", "01223 300085")
            .addColumn("url", "http://www.adctheatre.com/whats-on/workshop/backstage-at-the-adc-theatre.aspx")
            .addColumn("category", "family")
    }

    private def buildEventList1Details3(from: String): DataRowBuilder = {
        DataRowBuilder()
            .addColumn("title", "Made in Dagenham")
            .addColumn("description",
                "Book by Richard Bean, Music by David Arnold and Lyrics by Richard Thomas",
                "Based on the 2010 BAFTA nominated film, this rowdy, stirring, thoroughly British comedy musical centres around a group of female workers at Ford's Dagenham plant who go on strike to fight inequality of pay for women. The events portrayed in the musical ultimately led to the Equal Pay Act of 1970.",
                "Expect energy and humour by the bucketload from our talented and vibrant cast."
            )
            .addColumn("image", "http://www.adctheatre.com/media/112832935/Made-in-Dagenham_Landscape.jpg")
            .addColumn("cost", "£14/£11 (Thu £12/£9)")
            .addColumn("from", from)
            .addColumn("venue", "ADC Theatre, Park Street, Cambridge, CB5 8AS")
            .addColumn("venue-category", "theatre")
            .addColumn("telephone", "01223 300085")
            .addColumn("url", "http://www.adctheatre.com/whats-on/musical/made-in-dagenham.aspx")
            .addColumn("category", "fundraising")
    }
}
