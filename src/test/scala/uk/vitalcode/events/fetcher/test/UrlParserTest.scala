package uk.vitalcode.events.fetcher.test

import java.net.{URI, URL}

import org.scalatest._

class UrlParserTest extends WordSpec with ShouldMatchers {

    "Uri" when {
        "parsing uri string" should {
            "be able to get all requested url parts correctly" in {

                val baseUri: URI = new URI("http://www.cambridgesciencecentre.org/whats-on/list/")
                val relativeUri: URI = new URI("/whats-on/events/13022016_02/")
                val absoluteUri: URI = new URI("http://www.cambridgesciencecentre.org/whats-on/events/13022016_02/")

                baseUri.isAbsolute shouldBe true
                baseUri.getHost shouldBe "www.cambridgesciencecentre.org"
                baseUri.getPath shouldBe "/whats-on/list/"

                relativeUri.isAbsolute shouldBe false
                relativeUri.getHost shouldBe null
                relativeUri.getPath shouldBe "/whats-on/events/13022016_02/"

                baseUri.resolve(relativeUri).toString shouldBe "http://www.cambridgesciencecentre.org/whats-on/events/13022016_02/"
                baseUri.resolve(absoluteUri).toString shouldBe "http://www.cambridgesciencecentre.org/whats-on/events/13022016_02/"
            }
        }
    }
}
