package uk.vitalcode.events.fetcher.test.service

import uk.vitalcode.events.fetcher.model._
import uk.vitalcode.events.fetcher.service.FetcherService
import uk.vitalcode.events.fetcher.test.common.FetcherTest


class FetcherServiceTest extends FetcherTest {

    "A Fetcher" when {
        "fetching data from Cambridge science centre web site" when {
            "building event record from description event pages only" should {
                "fetch all expected property values" in {
                    FetcherService.fetchPages(Set[Page](buildPageDescription()), sc, hBaseConf, esIndex, esType)
                    val actual = esData()
                    val expected = expectedEsDataDescription()
                    actual shouldBe expected
                }
            }
        }
    }

    override protected def putTestData(): Unit = {
        // page 1 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list/",
            "/clientCambridgeScienceCentreTest/list1.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events/destination-space-crew-09012016-1500/", "list")
        // page 1 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/destination-space-crew-09012016-1500/",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events/destination-space-crew-09012016-1500/", "description")
        // page 1 link 1 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/969c39e09b655c715be0aa6b578908427d75e7.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG,
            "http://www.cambridgesciencecentre.org/whats-on/events/destination-space-crew-09012016-1500/", "image")
        // page 1 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/Voyagetospace_09012016_1600/",
            "/clientCambridgeScienceCentreTest/voyagetospace_09012016_1600.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events/Voyagetospace_09012016_1600/", "description")
        // page 1 link 2 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/0004a8c035b90924f8321df21276fc8f83a6cd.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG,
            "http://www.cambridgesciencecentre.org/whats-on/events/Voyagetospace_09012016_1600/", "image")

        // page 2 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list/?page=2",
            "/clientCambridgeScienceCentreTest/list2.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events/otherworlds/", "list")
        // page 2 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/otherworlds/",
            "/clientCambridgeScienceCentreTest/otherworlds.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events/otherworlds/", "description")
        // page 2 link 1 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/37cf8f84e5cfa94cdcac3f73bc13cfea3556a7.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG,
            "http://www.cambridgesciencecentre.org/whats-on/events/otherworlds/", "image")
        // page 2 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/sunday-science-20-march/",
            "/clientCambridgeScienceCentreTest/sunday-science-20-march.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events/sunday-science-20-march/", "description")
        // page 2 link 2 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/200e303cecd9eee71f77c97ddea630521cbfe9.png",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG,
            "http://www.cambridgesciencecentre.org/whats-on/events/sunday-science-20-march/", "image")

        // page 3 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list/?page=3",
            "/clientCambridgeScienceCentreTest/list3.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events/february-half-term-2016/", "list")
        // page 3 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/february-half-term-2016/",
            "/clientCambridgeScienceCentreTest/february-half-term-2016.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events/february-half-term-2016/", "description")
        // page 3 link 1 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/d78141bc0cc3f96d175843c2cd0e97beb9c370.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG,
            "http://www.cambridgesciencecentre.org/whats-on/events/february-half-term-2016/", "image")
        // page 3 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/electric-universe/",
            "/clientCambridgeScienceCentreTest/electric-universe.html", MineType.TEXT_HTML,
            "http://www.cambridgesciencecentre.org/whats-on/events/electric-universe/", "description")
        // page 3 link 2 image
        putTestDataRow("http://www.cambridgesciencecentre.org/media/assets/3a/fb2024b1db936348b42d3edd48995c32f69a1d.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG,
            "http://www.cambridgesciencecentre.org/whats-on/events/electric-universe/", "image")
    }

    private def buildPageDescription(): Page = {
        PageBuilder()
            .setId("list")
            .setUrl("http://www.cambridgesciencecentre.org/whats-on/list/")
            .addPage(PageBuilder()
                .isRow(true)
                .setId("description")
                .setLink("div.main_wrapper > section > article > ul > li > h2 > a")
                .addPage(PageBuilder()
                    .setId("image")
                    .setLink("section.event_detail > div.page_content > article > img")
                    .addProp(PropBuilder()
                        .setName("image")
                        .setKind(PropType.Image)
                    )
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
                .addProp(PropBuilder()
                    .setName("when")
                    .setCss("div.main_wrapper > section.event_detail > div > span:nth-child(2)")
                    .setKind(PropType.Date)

                )
            )
            .addPage(PageBuilder()
                .setRef("list")
                .setId("pagination")
                .setLink("div.pagination > div.omega > a")
            )
            .build()
    }

    private def expectedEsDataDescription(): DataTable = {
        DataTableBuilder()
            .addRow(DataRowBuilder().setRowId("http://www.cambridgesciencecentre.org/whats-on/events/destination-space-crew-09012016-1500/")
                .addColumn("description", "Destination Space! Tim Peake, the first British European Space Agency astronaut, is heading into space in December. Join us to explore his mission to the ISS. From launching a rocket to experiencing life in microgravity, this show is full of amazing demonstrations. An out of this world show not to be missed!")
                .addColumn("cost", "Please be aware that normal admission charges to the centre apply.")
                .addColumn("image", "http://www.cambridgesciencecentre.org/media/assets/3a/969c39e09b655c715be0aa6b578908427d75e7.jpg")
                .addColumn("from", "2016-01-09T15:00:00"))
            .addRow(DataRowBuilder().setRowId("http://www.cambridgesciencecentre.org/whats-on/events/Voyagetospace_09012016_1600/")
                .addColumn("description", "Explore Your Universe in our Cosmic show as we take a look at the stars! Discover how we can use hidden light and special cameras to discover more about our world, our Sun and other solar systems.")
                .addColumn("cost", "Credit: NASA, H.E. Bond and E. Nelan (Space Telescope Science Institute, Baltimore, Md.); M. Barstow and M. Burleigh (University of Leicester, U.K.); and J.B. Holberg (University of Arizona)")
                .addColumn("image", "http://www.cambridgesciencecentre.org/media/assets/3a/0004a8c035b90924f8321df21276fc8f83a6cd.jpg")
                .addColumn("from", "2016-01-09T16:00:00"))
            .addRow(DataRowBuilder().setRowId("http://www.cambridgesciencecentre.org/whats-on/events/otherworlds/")
                .addColumn("description", "The first planet orbiting another star was discovered by Didier Queloz in 1995; now thousands more have been found. Discuss with Professor Queloz what these exotic worlds may be like and how we are continuing our search.")
                .addColumn("cost", "Might such exotic environments support life and how would we recognise it? Dr William Bains will speculate on possibilities for simple, complex and intelligent life on other worlds.")
                .addColumn("image", "http://www.cambridgesciencecentre.org/media/assets/3a/37cf8f84e5cfa94cdcac3f73bc13cfea3556a7.jpg")
                .addColumn("from", "2016-03-17T19:00:00")
                .addColumn("to", "2016-03-17T20:30:00"))
            .addRow(DataRowBuilder().setRowId("http://www.cambridgesciencecentre.org/whats-on/events/sunday-science-20-march/")
                .addColumn("description", "Join us for a fun-filled day of non-stop shows and hands-on workshops for all the family.")
                .addColumn("cost", "Part of the Cambridge Science Festival.")
                .addColumn("image", "http://www.cambridgesciencecentre.org/media/assets/3a/200e303cecd9eee71f77c97ddea630521cbfe9.png")
                .addColumn("from", "2016-03-20T10:00:00")
                .addColumn("to", "2016-03-20T17:00:00"))
            .addRow(DataRowBuilder().setRowId("http://www.cambridgesciencecentre.org/whats-on/events/february-half-term-2016/")
                .addColumn("description", "Engineers from Airbus Defence and Space will be with over weekend, 13/14th February, to talk to you about engineering spacecraft and rovers to explore other planets. Get hands on with the technology, become a clean room engineer and join in some fun workshop activities.")
                .addColumn("cost", "Then throughout the week scientists from the Cambridge Exoplanet Research Group will be visiting to talk out their search for other worlds and what these exotic planets might be like. Zoom in to take a look at some of them or visit the Exoplanet Travel Bureau to plan a truly exotic holiday.")
                .addColumn("image", "http://www.cambridgesciencecentre.org/media/assets/3a/d78141bc0cc3f96d175843c2cd0e97beb9c370.jpg")
                .addColumn("from", "2016-02-13T10:00:00"))
            .addRow(DataRowBuilder().setRowId("http://www.cambridgesciencecentre.org/whats-on/events/electric-universe/")
                .addColumn("description", "Join us for a unique and controversial tour of the Universe. This talk follows contemporary speculation into the role that electro-dynamics plays in forming the objects we see in modern astronomy.")
                .addColumn("cost", "Doors open at 6.30pm, with a 7pm start.")
                .addColumn("image", "http://www.cambridgesciencecentre.org/media/assets/3a/fb2024b1db936348b42d3edd48995c32f69a1d.jpg")
                .addColumn("from", "2016-02-16T19:00:00")
                .addColumn("to", "2016-02-16T21:00:00"))
            .build()
    }
}

// TODO Create commom property name enum