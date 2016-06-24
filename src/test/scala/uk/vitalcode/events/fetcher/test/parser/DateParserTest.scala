package uk.vitalcode.events.fetcher.test.parser

import java.time.{LocalDateTime, Month}

import org.scalatest._
import uk.vitalcode.events.fetcher.parser.DateParser
import uk.vitalcode.events.model.{Prop, PropType}

class DateParserTest extends WordSpec with ShouldMatchers {

    "DateParser" when {
        "parsing standard set of possible date formats" should {
            "parse dd MMM yyyy date correctly" in {
                assertDateParser("6 May 2016",
                    LocalDateTime.of(2016, Month.MAY, 6, 0, 0))
            }
        }
        "parsing dates from CambridgeScienceCentre" should {
            "parse date + from time" in {
                assertDateParser("Saturday, January 9th 2016 from 3:00 PM",
                    LocalDateTime.of(2016, Month.JANUARY, 9, 15, 0))
            }
            "parse date + from time + to time" in {
                assertDateParser("Thursday, March 17th 2016 from 7:00 PM to 8:30 PM",
                    LocalDateTime.of(2016, Month.MARCH, 17, 19, 0),
                    LocalDateTime.of(2016, Month.MARCH, 17, 20, 30))
            }

            // TODO + to time nex day "Sunday, March 20th 2016 from 10:00 PM to 5:00 AM" ???
        }
        "parsing dates from Cambridge Junction" should {
            "parse week month date no year + from | to time" in {
                val year = LocalDateTime.now().getYear
                assertDateParser("Fri 24 Jun          6:45pm (doors) | 11pm (curfew) ",
                    LocalDateTime.of(year, Month.JUNE, 24, 18, 45),
                    LocalDateTime.of(year, Month.JUNE, 24, 23, 0))
            }
            "parse week month date no year + from" in {
                val year = LocalDateTime.now().getYear
                assertDateParser("Date:Fri 08 Jul, Time:8pm",
                    LocalDateTime.of(year, Month.JULY, 8, 20, 0))
            }
        }
    }

    private def assertDateParser(dateText: String, expectedFrom: LocalDateTime, expectedTo: LocalDateTime): Unit = {
        val prop = Prop(null, null, PropType.Date, Set[String](dateText))
        DateParser.parseAsDateTime(prop) shouldBe(expectedFrom, expectedTo)
    }

    private def assertDateParser(dateText: String, expectedFrom: LocalDateTime): Unit = {
        assertDateParser(dateText, expectedFrom, expectedFrom)
    }
}
