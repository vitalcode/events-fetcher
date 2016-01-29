package uk.vitalcode.events.fetcher.model

case class Page(var id: String, var ref: String,
                var url: String, var link: String,
                var props: Set[Prop], var pages: Set[Page],
                var parent: Page, var isRow: Boolean) extends Serializable {

    def this() = this(
        null, null,
        null, null,
        collection.immutable.Set[Prop](),
        collection.immutable.Set(),
        null, false
    )

    override def toString: String = s"Page(id: $id, ref: $ref, url: $url, link: $link, " +
        s"props: $props, pages: $pages, isRow: $isRow)"

    override def hashCode: Int = {
        var result: Int = 17
        result = 31 * result + (if (id != null) id.hashCode else 0)
        result = 31 * result + (if (ref != null) ref.hashCode else 0)
        result = 31 * result + (if (url != null) url.hashCode else 0)
        result = 31 * result + (if (link != null) link.hashCode else 0)
        result = 31 * result + (if (props != null) props.hashCode else 0)
        result = 31 * result + (if (pages != null) pages.hashCode else 0)
        result = 31 * result + (if (isRow) 1 else 0)
        result
    }

    override def equals(obj: scala.Any): Boolean = {
        val pageObj: Page = obj match {
            case c: Page => c
            case _ => {
                return false
            }
        }
        if (!(if (id == null) pageObj.id == null else id.equals(pageObj.id))) return false
        if (!(if (ref == null) pageObj.ref == null else ref.equals(pageObj.ref))) return false
        if (!(if (url == null) pageObj.url == null else url.equals(pageObj.url))) return false
        if (!(if (link == null) pageObj.link == null else link.equals(pageObj.link))) return false
        if (!(if (props == null) pageObj.props == null else props.equals(pageObj.props))) return false
        if (!(if (pages == null) pageObj.pages == null else pages.equals(pageObj.pages))) return false
        if (!isRow.equals(pageObj.isRow)) return false
        true
    }
}

case class PageBuilder() extends Builder {

    private val page = new Page()

    def setId(id: String): PageBuilder = {
        page.id = id
        this
    }

    def setRef(ref: String): PageBuilder = {
        page.ref = ref
        this
    }

    def setUrl(url: String): PageBuilder = {
        page.url = url
        this
    }

    def setLink(link: String): PageBuilder = {
        page.link = link
        this
    }

    def addProp(propBuilder: PropBuilder): PageBuilder = {
        page.props += propBuilder.build()
        this
    }

    def addPage(pageBuilder: PageBuilder): PageBuilder = {
        pageBuilder.setParent(page)
        val childPage: Page = pageBuilder.build()
        addPage(childPage)
        this
    }

    def addPage(childPage: Page): PageBuilder = {
        page.pages += childPage
        this
    }

    def isRow(isRow: Boolean): PageBuilder = {
        page.isRow = isRow
        this
    }

    private def setParent(parent: Page): PageBuilder = {
        page.parent = parent
        this
    }

    override type t = Page

    override def build(): Page = page
}


