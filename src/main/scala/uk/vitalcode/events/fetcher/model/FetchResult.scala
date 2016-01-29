package uk.vitalcode.events.fetcher.model

import shapeless.HList

case class FetchResult(page: Page[HList], childPages: Seq[Page[HList]]) extends Serializable
