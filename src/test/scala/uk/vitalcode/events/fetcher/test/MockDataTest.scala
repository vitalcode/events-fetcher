package uk.vitalcode.events.fetcher.test

import java.io.InputStream

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
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}
import uk.vitalcode.events.fetcher.model.MineType._
import uk.vitalcode.events.fetcher.model.{MineType, _}

case class FetchResult(page: Page, childPages: Seq[Page]) extends Serializable

object FetcherService extends Serializable {

    // TODO should return rows of fetched data
    def fetchPage(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)]): Unit = {
        fetchPageData(page, rdd) match {
            case Some(result) =>
                result.page.props.values.foreach(p => {
                    println(s"found page prop: ${p.name} -- ${p.value}")
                })
                result.childPages.foreach(p => {
                    println(s"fetching child page: -- $p")
                    fetchPage(p, rdd)
                })
            case None =>
        }
    }

    private def fetchPageData(page: Page, rdd: RDD[(ImmutableBytesWritable, client.Result)]): Option[FetchResult] = {
        rdd.filter(row => Bytes.toString(row._1.get()) == page.url)
            .map(row => jerry(Bytes.toString(row._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("data")))))
            .map(dom => {
                val childPages = collection.mutable.ArrayBuffer[Page]()
                getChildPages(page).foreach(p => {
                    if (p.link != null) {
                        dom.$(p.link).each(new JerryNodeFunction {
                            override def onNode(node: Node, index: Int): Boolean = {
                                val childPageUrl = node.getAttribute("href")
                                val childPage: Page = Page(p.id, p.ref, childPageUrl, p.link, p.props, p.pages, p.parent, p.isRow)
                                childPages += childPage
                                true
                            }
                        })
                    } else {
                        childPages += p
                    }
                })
                fetchPageProperties(page, dom)
                FetchResult(page, childPages)
            })
            .collect()
            .headOption
    }

    // Self child or parent child
    private def getChildPages(page: Page): Set[Page] = {
        if (page.ref == null) {
            println(s"Got [${page.pages.size}] child pages of the current page")
            page.pages
        } else {
            val parentPage = FetcherService.getParentPageByRef(page, page.ref)
            parentPage.url = page.url
            println(s"Got reference to the parent page [$parentPage]")
            Set(parentPage)
        }
    }

    private def getParentPageByRef(page: Page, ref: String): Page = {
        if (page.id.equals(ref)) page else getParentPageByRef(page.parent, ref)
    }

    private def fetchPageProperties(currentPage: Page, dom: Jerry): Unit = {
        currentPage.props.values.foreach(p => fetchProperty(p, dom))
    }

    private def fetchProperty(prop: Prop, dom: Jerry): Unit = {
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
    val LOG = Logger.getLogger(this.getClass)// classOf[MockDataTest])
}

class MockDataTest extends FunSuite with ShouldMatchers with BeforeAndAfterAll with Serializable with log{
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
        FetcherService.fetchPage(page, rdd)

        15 should equal(15)
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
    // TODO collect all properties in separate collection ??? => Map[row(event item), Map[column(prop), value]]
    // TODO use isRow Page property
    // TODO Logging in scala 2.10
    // TODO use isRow page model property
}

