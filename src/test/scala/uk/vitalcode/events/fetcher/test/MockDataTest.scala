package uk.vitalcode.events.fetcher.test

import java.io.InputStream
import java.util.UUID

import jodd.jerry.Jerry._
import jodd.jerry.{Jerry, JerryNodeFunction}
import jodd.lagarto.dom.Node
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}
import uk.vitalcode.events.fetcher.model.MineType._
import uk.vitalcode.events.fetcher.model.{MineType, _}

import scala.collection.mutable


case class DataTable(dataRows: Set[DataRow]) extends Serializable

case class DataTableBuilder() extends Builder {
    private var dataRows: Set[DataRow] = Set.empty[DataRow]

    def addRow(dataRowBuilder: DataRowBuilder): DataTableBuilder = {
        this.dataRows += dataRowBuilder.build()
        this
    }

    override type t = DataTable

    override def build(): DataTable = new DataTable(dataRows)
}


case class DataRow(row: String, columns: Map[String, Set[String]]) extends Serializable

case class DataRowBuilder() extends Builder {
    private var columns: Map[String, Set[String]] = Map.empty[String, Set[String]]
    private var row: String = _

    def setRowId(row: String): DataRowBuilder = {
        this.row = row
        this
    }

    def addColumn(column: String, value: String*): DataRowBuilder = {
        addColumn(column, value.toSet)
        this
    }

    def addColumn(column: String, value: Set[String]): DataRowBuilder = {
        if (this.columns.contains(column)){
            val prev: Option[Set[String]] = this.columns.get(column)
            val newVal = prev.get ++ value
            this.columns += (column -> newVal)
        } else {
            this.columns += (column -> value)
        }
        this
    }

    override type t = DataRow

    override def build(): DataRow = new DataRow(row, columns)
}


case class FetchResult(page: Page, childPages: Seq[Page]) extends Serializable

object FetcherService extends Serializable with log {

    var table: mutable.Map[String, mutable.Map[String, String]] = new mutable.HashMap[String, mutable.Map[String, String]]
    var rowColumns: mutable.Map[String, String] = new mutable.HashMap[String, String]

    def logOutcame(): Unit = {
        LOG.info(s"inside logOutcame table rows [${table.size}], columns [${rowColumns.size}]")
        table += (rowID.concat(UUID.randomUUID().toString) -> rowColumns)

        table.foreach(t => {
            LOG.info(s"--- row: ${t._1}")
            t._2.foreach(col => {
                LOG.info(s"--- --- column: ${col._1} --- ${col._2}")
            })
        })
    }

    var rowID: String = ""
    var dataRowBuilder: DataRowBuilder = DataRowBuilder()

    def fetchPage(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)]): DataTable = {
        val builder: DataTableBuilder = DataTableBuilder()
        fetchPage(page, rdd, builder)

        dataRowBuilder.setRowId(rowID)
        builder.addRow(dataRowBuilder)

        builder.build()
    }

    def fetchPage(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)], builder: DataTableBuilder): Unit = {

        LOG.info(s"Fetching data for $page")
        fetchPageData(page, rdd) match {
            case Some(result) =>

                if (result.page.isRow && rowID.nonEmpty) {
                    table += (s"%s___%s".format(rowID, UUID.randomUUID().toString) -> rowColumns)
                    dataRowBuilder.setRowId(rowID)
                    builder.addRow(dataRowBuilder)

                    rowColumns = new mutable.HashMap[String, String]
                    dataRowBuilder = DataRowBuilder()
                    rowID = ""
                }

                if (result.page.isRow && rowID.isEmpty) {
                    rowID = page.url
                    //dataRowBuilder.setRowId(page.url)
                }

                result.page.props.values.foreach(prop => {
                    prop.value.foreach(value => {
                        LOG.info(s"Fetched page prop: ${prop.name} -- $value")
                        rowColumns += (s"%s___%s".format(prop.name, UUID.randomUUID().toString) -> value)
                    })
                    dataRowBuilder.addColumn(prop.name, prop.value)

                })
                LOG.info(s"Fetched child pages ${result.childPages}")


                result.childPages.foreach(p => {
                    println(s"fetching child page: -- $p")
                    fetchPage(p, rdd, builder)
                })
            case None =>
        }
    }

    private def fetchPageData(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)]): Option[FetchResult] = {
        rdd.filter(row => Bytes.toString(row._1.get()) == page.url)
            .map(row => jerry(Bytes.toString(row._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("data")))))
            .map(dom => {
                // find child pages
                val childPages = collection.mutable.ArrayBuffer[Page]()
                    // 1. treat page with self child pages
                    page.pages.foreach(p => {
                        val cp: Page = if (p.ref == null) p else getParentPageByRef(p, p.ref)
                        if (p.link != null) {
                            dom.$(p.link).each(new JerryNodeFunction {
                                override def onNode(node: Node, index: Int): Boolean = {
                                    val childPageUrl = node.getAttribute("href")
                                    val childPage: Page = Page(cp.id, cp.ref, childPageUrl, cp.link, cp.props, cp.pages, cp.parent, cp.isRow)
                                    childPages += childPage
                                    true
                                }
                            })
                        }
                    })

                fetchPageProperties(page, dom)
                FetchResult(page, childPages.toSeq)
            })
            .collect()
            .headOption
    }

//    private def getChildPages(page: Page): Set[Page] = {
//        if (page.ref == null) {
//            page.pages
//        } else {
//            val parentPage = FetcherService.getParentPageByRef(page, page.ref)
//            parentPage.url = page.url
//            Set(parentPage)
//        }
//    }

    private def getParentPageByRef(page: Page, ref: String): Page = {
        if (page.id.equals(ref)) page else getParentPageByRef(page.parent, ref)
    }

    private def fetchPageProperties(currentPage: Page, dom: Jerry): Unit = {
        currentPage.props.values.foreach(p => fetchProperty(p, dom))
    }

    private def fetchProperty(prop: Prop, dom: Jerry): Unit = {
        prop.reset()
        dom.$(prop.css).each(new JerryNodeFunction {
            override def onNode(node: Node, index: Int): Boolean = {
                val value = prop.kind match {
                    case PropType.Link => node.getAttribute("href")
                    case _ => node.getTextContent
                }
                val propValue: String = value.replaceAll( """\s{2,}""", " ").replaceAll( """^\s|\s$""", "")
                println(s"${prop.name} -- $propValue")
                prop.value += propValue
                true
            }
        })
    }


    //    def getPageDom(currentPage: Page): Unit = {
    //        rdd.filter(f => Bytes.toString(f._1.get()) == currentPage.url)
    //            .foreach(f => {
    //                val pageData: String = Bytes.toString(f._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("data")))
    //                val pageDom: Jerry = jerry(pageData)
    //                currentPage.pages.foreach(p => {
    //                    pageDom.$(p.link).each(new JerryNodeFunction {
    //                        override def onNode(node: Node, index: Int): Boolean = {
    //                            val childPageUrl = node.getAttribute("href")
    //                            println(s"childPageUrl [$childPageUrl]")
    //                            val childPage: Page = Page(currentPage.id, currentPage.ref, childPageUrl, currentPage.link, currentPage.props, currentPage.pages, currentPage.parent, currentPage.isRow)
    //                            println(s"childPage [$childPage]")
    //                            //getPageDom(childPage)
    //                            true
    //                        }
    //                    })
    //                })
    //                //println(pageData)
    //            })
    //    }
}

trait log {
    val LOG = Logger.getLogger(this.getClass) // classOf[MockDataTest])
}

class MockDataTest extends FunSuite with ShouldMatchers with BeforeAndAfterAll with Serializable with log {
    //with LazyLogging

    //    Logger.getLogger("org").setLevel(Level.ERROR)
    //    Logger.getLogger("akka").setLevel(Level.ERROR)

    //    private val LOG = Logger.getLogger(this.getClass)// classOf[MockDataTest])
    LOG.error("Error")
    LOG.info("INFO")


    var sc: SparkContext = _
    var hBaseConn: Connection = _
    var hBaseConf: Configuration = _
    val testTable: TableName = TableName.valueOf("testTable")

    test("HBase test rows count") {
        prepareTestData()

        val page: Page = PageBuilder()
            .setId("list")
            .setUrl("http://www.cambridgesciencecentre.org/whats-on/list")
            .isRow(true)
            .addProp(PropBuilder()
                .setName("title")
                .setCss("div.whats-on ul.omega > li > h2")
                .setKind(PropType.Text)
            )
            .addPage(PageBuilder()
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

        val rdd: RDD[(ImmutableBytesWritable, client.Result)] = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result])

        //        val linksProp: Prop = PropBuilder()
        //            .setName("link")
        //            .setCss("div.main_wrapper > section > article > ul > li > h2 > a")
        //            .setKind(PropType.Link)
        //            .build()
        //
        //        val titleProp: Prop = PropBuilder()
        //            .setName("title")
        //            .setCss("div.whats-on ul.omega > li > h2")
        //            .setKind(PropType.Text)
        //            .build()

        //        rdd.foreach(e => println("%s | %s".format(
        //            Bytes.toString(e._1.get()),
        //            Bytes.toString(e._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("hash"))))))
        //
        //        rdd.foreach(e => {
        //
        //            val mineType = MineType.withName(Bytes.toString(e._2.getValue(Bytes.toBytes("metadata"), Bytes.toBytes("mine-type"))))
        //            println("%s | %s".format(Bytes.toString(e._1.get()), mineType))
        //
        //            if (mineType == MineType.TEXT_HTML) {
        //                val dom: Jerry = jerry(Bytes.toString(e._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("data"))))
        //                FetcherService.fe fetchProperty(linksProp, dom)
        //                FetcherService.getProperty(titleProp, dom)
        //            }
        //
        //        })

        println(s"child page: -- $page")
        val actual = FetcherService.fetchPage(page, rdd)
        FetcherService.logOutcame()

        val expected: DataTable = DataTableBuilder()
            .addRow(DataRowBuilder().setRowId("http://www.cambridgesciencecentre.org/whats-on/list")
                .addColumn("title", "Destination Space: Join the crew!", "Explore Your Universe: Star Light, Star Bright")
                .addColumn("description",
                    "Destination Space! Tim Peake, the first British European Space Agency astronaut, is heading into space in December. Join us to explore his mission to the ISS. From launching a rocket to experiencing life in microgravity, this show is full of amazing demonstrations. An out of this world show not to be missed!",
                    "Explore Your Universe in our Cosmic show as we take a look at the stars! Discover how we can use hidden light and special cameras to discover more about our world, our Sun and other solar systems.")
                .addColumn("cost",
                    "Please be aware that normal admission charges to the centre apply.",
                    "Credit: NASA, H.E. Bond and E. Nelan (Space Telescope Science Institute, Baltimore, Md.); M. Barstow and M. Burleigh (University of Leicester, U.K.); and J.B. Holberg (University of Arizona)"))
            .addRow(DataRowBuilder().setRowId("http://www.cambridgesciencecentre.org/whats-on/list/?page=2")
                .addColumn("title", "Other Worlds", "Sunday Science 2016")
                .addColumn("description",
                    "Join us for a fun-filled day of non-stop shows and hands-on workshops for all the family.",
                    "The first planet orbiting another star was discovered by Didier Queloz in 1995; now thousands more have been found. Discuss with Professor Queloz what these exotic worlds may be like and how we are continuing our search.")
                .addColumn("cost",
                    "Part of the Cambridge Science Festival.",
                    "Might such exotic environments support life and how would we recognise it? Dr William Bains will speculate on possibilities for simple, complex and intelligent life on other worlds."))
            .addRow(DataRowBuilder().setRowId("http://www.cambridgesciencecentre.org/whats-on/list/?page=3")
                .addColumn("title", "Half Term 13th-21st February 10-5", "Electric Universe")
                .addColumn("description",
                    "Engineers from Airbus Defence and Space will be with over weekend, 13/14th February, to talk to you about engineering spacecraft and rovers to explore other planets. Get hands on with the technology, become a clean room engineer and join in some fun workshop activities.",
                    "Join us for a unique and controversial tour of the Universe. This talk follows contemporary speculation into the role that electro-dynamics plays in forming the objects we see in modern astronomy.")
                .addColumn("cost",
                    "Then throughout the week scientists from the Cambridge Exoplanet Research Group will be visiting to talk out their search for other worlds and what these exotic planets might be like. Zoom in to take a look at some of them or visit the Exoplanet Travel Bureau to plan a truly exotic holiday.",
                    "Doors open at 6.30pm, with a 7pm start."))
            .build()

        actual should equal(expected)
    }

    private def prepareTestData(): Unit = {
        createTestTable()
        putTestData()
    }

    private def createTestTable(): Unit = {
        val admin: Admin = hBaseConn.getAdmin()
        if (admin.isTableAvailable(testTable)) {
            admin.disableTable(testTable)
            admin.deleteTable(testTable)
            println(s"Test table [$testTable] deleted")
        }

        val tableDescriptor: HTableDescriptor = new HTableDescriptor(testTable)
        tableDescriptor.addFamily(new HColumnDescriptor("content"))
        tableDescriptor.addFamily(new HColumnDescriptor("metadata"))
        admin.createTable(tableDescriptor)
        println(s"New Test table [$testTable] created")

        admin.close()
    }

    private def putTestData(): Unit = {
        // page 1 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list",
            "/clientCambridgeScienceCentreTest/list1.html", MineType.TEXT_HTML)
        // page 1 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/destination-space-crew-09012016-1500/",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.html", MineType.TEXT_HTML)
        // page 1 link 1 image
        putTestDataRow("/media/assets/3a/969c39e09b655c715be0aa6b578908427d75e7.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
        // page 1 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/Voyagetospace_09012016_1600/",
            "/clientCambridgeScienceCentreTest/voyagetospace_09012016_1600.html", MineType.TEXT_HTML)
        // page 1 link 2 image
        putTestDataRow("/media/assets/3a/0004a8c035b90924f8321df21276fc8f83a6cd.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)

        // page 2 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list/?page=2",
            "/clientCambridgeScienceCentreTest/list2.html", MineType.TEXT_HTML)
        // page 2 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/otherworlds/",
            "/clientCambridgeScienceCentreTest/otherworlds.html", MineType.TEXT_HTML)
        // page 2 link 1 image
        putTestDataRow("/media/assets/3a/37cf8f84e5cfa94cdcac3f73bc13cfea3556a7.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
        // page 2 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/sunday-science-20-march/",
            "/clientCambridgeScienceCentreTest/sunday-science-20-march.html", MineType.TEXT_HTML)
        // page 2 link 2 image
        putTestDataRow("/media/assets/3a/200e303cecd9eee71f77c97ddea630521cbfe9.png",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)

        // page 3 list
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/list/?page=3",
            "/clientCambridgeScienceCentreTest/list3.html", MineType.TEXT_HTML)
        // page 3 link 1
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/february-half-term-2016/",
            "/clientCambridgeScienceCentreTest/february-half-term-2016.html", MineType.TEXT_HTML)
        // page 3 link 1 image
        putTestDataRow("/media/assets/3a/d78141bc0cc3f96d175843c2cd0e97beb9c370.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
        // page 3 link 2
        putTestDataRow("http://www.cambridgesciencecentre.org/whats-on/events/electric-universe/",
            "/clientCambridgeScienceCentreTest/electric-universe.html", MineType.TEXT_HTML)
        // page 3 link 2 image
        putTestDataRow("/media/assets/3a/fb2024b1db936348b42d3edd48995c32f69a1d.jpg",
            "/clientCambridgeScienceCentreTest/destination-space-crew-09012016-1500.jpg", MineType.IMAGE_JPEG)
    }

    private def putTestDataRow(url: String, pagePath: String, mineType: MineType): Unit = {
        val table: Table = hBaseConn.getTable(testTable)

        val data: Array[Byte] = getPage(pagePath)
        val put: Put = new Put(Bytes.toBytes(url))
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("data"), data)
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("hash"), Bytes.toBytes(DigestUtils.sha1Hex(data)))
        put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("mine-type"), Bytes.toBytes(mineType.toString))
        println(s"Add row [$url] with content of MINE type [$mineType] to the test table [$testTable]")
        table.put(put)

        table.close()
    }

    private def getPage(resourceFilePath: String): Array[Byte] = {
        val stream: InputStream = getClass.getResourceAsStream(resourceFilePath)
        Stream.continually(stream.read).takeWhile(_ != -1).map(_.toByte).toArray
    }

    override protected def beforeAll(): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("events-fetcher-test")
            .setMaster("local[1]")
        sc = new SparkContext(sparkConf)

        hBaseConf = HBaseConfiguration.create()
        hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
        hBaseConf.set(TableInputFormat.INPUT_TABLE, Bytes.toString(testTable.getName))
        hBaseConn = ConnectionFactory.createConnection(hBaseConf)

    }

    override protected def afterAll(): Unit = {
        sc.stop()
        hBaseConn.close()
    }

    // TODO refactor
}

