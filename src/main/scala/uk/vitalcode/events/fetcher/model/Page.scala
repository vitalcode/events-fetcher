package uk.vitalcode.events.fetcher.model

import shapeless.{HList, HNil}

case class Page[L <: HList](id: String, ref: String,
                            url: String, link: String,
                            parent: Page[L], pages: Set[Page[L]] = Set.empty[Page[L]],
                            isRow: Boolean = false, props: L = HNil) extends Serializable {

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
        val pageObj: Page[L] = obj match {
            case c: Page[L] => c
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

    def setId(id: String): Page[L] = copy(id = id)

    def setRef(ref: String): Page[L] = copy(ref = ref)

    def setUrl(url: String): Page[L] = copy(url = url)

    def setLink(link: String): Page[L] = copy(link = link)

    def addProp[M <: HList](newProps: M): Page[M] = Page[M](id, ref, url, link, null, null, isRow, newProps)

    def isRow(isRow: Boolean): Page[L] = copy(isRow = isRow)
}

object Page {
    def apply() = new Page(null, null, null, null, null, null, false, HNil)
}

//case class Page[L <: HList](id: String, ref: String,
//                            url: String, link: String,
//                            parent: Page[L], pages: Set[Page[L]] = Set.empty[Page[L]],
//                            isRow: Boolean = false, props: L = HNil) extends Serializable {
//
//    override def toString: String = s"Page(id: $id, ref: $ref, url: $url, link: $link, " +
//        s"props: $props, pages: $pages, isRow: $isRow)"
//
//    override def hashCode: Int = {
//        var result: Int = 17
//        result = 31 * result + (if (id != null) id.hashCode else 0)
//        result = 31 * result + (if (ref != null) ref.hashCode else 0)
//        result = 31 * result + (if (url != null) url.hashCode else 0)
//        result = 31 * result + (if (link != null) link.hashCode else 0)
//        result = 31 * result + (if (props != null) props.hashCode else 0)
//        result = 31 * result + (if (pages != null) pages.hashCode else 0)
//        result = 31 * result + (if (isRow) 1 else 0)
//        result
//    }
//
//    override def equals(obj: scala.Any): Boolean = {
//        val pageObj: Page[L] = obj match {
//            case c: Page[L] => c
//            case _ => {
//                return false
//            }
//        }
//        if (!(if (id == null) pageObj.id == null else id.equals(pageObj.id))) return false
//        if (!(if (ref == null) pageObj.ref == null else ref.equals(pageObj.ref))) return false
//        if (!(if (url == null) pageObj.url == null else url.equals(pageObj.url))) return false
//        if (!(if (link == null) pageObj.link == null else link.equals(pageObj.link))) return false
//        if (!(if (props == null) pageObj.props == null else props.equals(pageObj.props))) return false
//        if (!(if (pages == null) pageObj.pages == null else pages.equals(pageObj.pages))) return false
//        if (!isRow.equals(pageObj.isRow)) return false
//        true
//    }
//}

//
//case class PageBuilder[L <: HList](page: Page[L]) {
//
////    def this() = this(
////        null, null,
////        null, null
////    )
//
//
//    def setId(id: String): PageBuilder[L] = {
//        PageBuilder(page.copy(id = id))
//    }
//
//    def setRef(ref: String): PageBuilder[L] = {
//        PageBuilder(page.copy(ref = ref))
//    }
//
//    def setUrl(url: String): PageBuilder[L] = {
//        PageBuilder(page.copy(url = url))
//    }
//
//    def setLink(link: String): PageBuilder[L] = {
//        PageBuilder(page.copy(link = link))
//    }
//
//    def addProp[M <: HList](propBuilder: PropBuilder[String]) = {
//        val prop = propBuilder.build()
//        val props: M = page.props
//        val newPage = page.clonePage(prop :: page.props)
//
//        PageBuilder(page.clonePage(prop :: page.props))
//    }
//
//    def isRow(isRow: Boolean): PageBuilder[L]  = {
//        PageBuilder(page.copy(isRow = isRow))
//    }
//
//    def build() = {
//        page
//    }
//}


//case class PageBuilder[L <: HList]() { //extends Builder {
//
//    private val page = new Page(null, null,
//        null, null,
//        null, null)
//
//    private var props: HList = HNil
//
//    def setId(id: String): PageBuilder[L] = {
//        page.id = id
//        this
//    }
//
//    def setRef(ref: String): PageBuilder[L]  = {
//        page.ref = ref
//        this
//    }
//
//    def setUrl(url: String): PageBuilder[L]  = {
//        page.url = url
//        this
//    }
//
//    def setLink(link: String): PageBuilder[L]  = {
//        page.link = link
//        this
//    }
//
//    def addProp(propBuilder: PropBuilder[_]): PageBuilder[L]  = {
//        val prop = propBuilder.build()
//        props = prop :: props
//        //page.props =  prop :: page.props
//        this
//    }
//
//    def addPage(pageBuilder: PageBuilder[L]): PageBuilder[L]  = {
//        pageBuilder.setParent(page)
//        val childPage: Page[L] = pageBuilder.build()
//        addPage(childPage)
//        this
//    }
//
//    def addPage(childPage: Page[L]): PageBuilder[L]  = {
//        page.pages += childPage
//        this
//    }
//
//    def isRow(isRow: Boolean): PageBuilder[L]  = {
//        page.isRow = isRow
//        this
//    }
//
//    private def setParent(parent: Page[L]): PageBuilder[L]  = {
//        page.parent = parent
//        this
//    }
//
//    //override type t = Page[L]
//
//    def build() = {
//        Page("id", "ref", "url", "link", null, null, isRow = false, props)
//    }
//}


