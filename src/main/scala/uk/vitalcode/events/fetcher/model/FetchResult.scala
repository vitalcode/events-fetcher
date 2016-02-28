package uk.vitalcode.events.fetcher.model

import uk.vitalcode.events.model.Page

case class FetchResult(page: Page, childPages: Seq[Page]) extends Serializable
