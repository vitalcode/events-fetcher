package uk.vitalcode.events.fetcher.test.parser

import java.time.{LocalDateTime, Month}

import org.scalatest._
import uk.vitalcode.events.fetcher.parser.dateParser.DateParser
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
                    (LocalDateTime.of(2016, Month.MARCH, 17, 19, 0), LocalDateTime.of(2016, Month.MARCH, 17, 20, 30)))
            }

            // TODO + to time nex day "Sunday, March 20th 2016 from 10:00 PM to 5:00 AM" ???
        }
        "parsing dates from Cambridge Junction" should {
            "parse week month date no year + from | to time" in {
                val year = LocalDateTime.now().getYear
                assertDateParser("Fri 24 Jun          6:45pm (doors) | 11pm (curfew) ",
                    (LocalDateTime.of(year, Month.JUNE, 24, 18, 45), LocalDateTime.of(year, Month.JUNE, 24, 23, 0)))
            }
            "parse week month date no year + from" in {
                val year = LocalDateTime.now().getYear
                assertDateParser("Date:Fri 08 Jul, Time:8pm",
                    LocalDateTime.of(year, Month.JULY, 8, 20, 0))
            }
            "parse week day month no year + from with dot" in {
                val year = LocalDateTime.now().getYear
                assertDateParser("Date:Sun 10 Jul Time:5.45pm",
                    LocalDateTime.of(year, Month.JULY, 10, 17, 45))
            }
            "parse week day month no year + from with dot 44" in {
                val year = LocalDateTime.now().getYear
                assertDateParser("Time:Sun 27 Nov",
                    LocalDateTime.of(year, Month.NOVEMBER, 27, 0, 0))
            }
            //            "parse multiple week & day combination followed by month - no year + from" in {
            //                val year = LocalDateTime.now().getYear
            //                assertDateParser("Date:Sun 25, Tue 27 &amp; Fri 30 Sep Time:8pm",
            //                    (LocalDateTime.of(year, Month.SEPTEMBER, 25, 20, 0), LocalDateTime.of(year, Month.SEPTEMBER, 25, 20, 0)),
            //                    (LocalDateTime.of(year, Month.SEPTEMBER, 27, 20, 0), LocalDateTime.of(year, Month.SEPTEMBER, 27, 20, 0)),
            //                    (LocalDateTime.of(year, Month.SEPTEMBER, 30, 20, 0), LocalDateTime.of(year, Month.SEPTEMBER, 30, 20, 0))
            //                )
            //            }

            "parse multiple week & day combination followed by month - no year + from4" in {
                assertDateParser("(1 Jan 2016 - 3 Jan 2016) 11:00 13:00",
                    (LocalDateTime.of(2016, Month.JANUARY, 1, 11, 0), LocalDateTime.of(2016, Month.JANUARY, 1, 13, 0)),
                    (LocalDateTime.of(2016, Month.JANUARY, 2, 11, 0), LocalDateTime.of(2016, Month.JANUARY, 2, 13, 0)),
                    (LocalDateTime.of(2016, Month.JANUARY, 3, 11, 0), LocalDateTime.of(2016, Month.JANUARY, 3, 13, 0))
                )
            }

            "parse multiple week & day combination followed by month - no year + from2" in {
                assertDateParser("(1 Jan 2016 - 3 Jan 2016) Sunday 11:00 13:00",
                    (LocalDateTime.of(2016, Month.JANUARY, 3, 11, 0), LocalDateTime.of(2016, Month.JANUARY, 3, 13, 0))
                )
            }

            "parse multiple week & day combination followed by month - no year + from3" in {
                assertDateParser("(1 Jan 2016 - 4 Jan 2016) Monday 11:00 13:00 Tuesday 14:00 15:00 Friday 16:05 17:20 Sunday 19:30 20:45",
                    (LocalDateTime.of(2016, Month.JANUARY, 1, 16, 5), LocalDateTime.of(2016, Month.JANUARY, 1, 17, 20)),
                    (LocalDateTime.of(2016, Month.JANUARY, 3, 19, 30), LocalDateTime.of(2016, Month.JANUARY, 3, 20, 45)),
                    (LocalDateTime.of(2016, Month.JANUARY, 4, 11, 0), LocalDateTime.of(2016, Month.JANUARY, 4, 13, 0))
                )
            }

            "parse multiple week & day combination followed by month - no year + from10" in {
                assertDateParser("(3 Feb 2017) Friday 19:30 21:30",
                    (LocalDateTime.of(2017, Month.FEBRUARY, 3, 19, 30), LocalDateTime.of(2017, Month.FEBRUARY, 3, 21, 30))
                )
            }

            "parse multiple week & day combination followed by month - no year + from11" in {
                assertDateParser("(1 Feb 2016 - 31 Oct 2016)")
            }
        }
    }

    private def assertDateParser(dateText: String, timeRanges: (LocalDateTime, LocalDateTime)*): Unit = {
        val prop = Prop(null, null, PropType.Date, Vector[String](dateText))
        DateParser.parseAsDateTime(prop) shouldBe timeRanges.map(d => (d._1, Some(d._2))).toSet
    }

    private def assertDateParser(dateText: String, expectedFrom: LocalDateTime): Unit = {
        val prop = Prop(null, null, PropType.Date, Vector[String](dateText))
        DateParser.parseAsDateTime(prop) shouldBe Set((expectedFrom, None))
    }

    private def assertDateParser(dateText: String): Unit = {
        val prop = Prop(null, null, PropType.Date, Vector[String](dateText))
        DateParser.parseAsDateTime(prop) shouldBe Set()
    }
}



