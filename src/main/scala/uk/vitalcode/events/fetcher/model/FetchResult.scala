package uk.vitalcode.events.fetcher.model

case class FetchResult(page: Page, childPages: Seq[Page]) extends Serializable
