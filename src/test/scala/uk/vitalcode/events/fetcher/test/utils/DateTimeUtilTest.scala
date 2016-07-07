package uk.vitalcode.events.fetcher.test.utils

import java.time.{DayOfWeek, LocalDate, Month}

import org.scalatest._
import uk.vitalcode.events.fetcher.utils.DateTimeUtil.datesInRange

class DateTimeUtilTest extends WordSpec with ShouldMatchers {
    "datesInRange" when {
        "date range: [2016-01-01 to 2016-01-10]" should {
            "return 10 dates" in {
                datesInRange(
                    LocalDate.of(2016, 1, 1),
                    LocalDate.of(2016, 1, 10)
                ) shouldBe Set(
                    LocalDate.of(2016, 1, 1),
                    LocalDate.of(2016, 1, 2),
                    LocalDate.of(2016, 1, 3),
                    LocalDate.of(2016, 1, 4),
                    LocalDate.of(2016, 1, 5),
                    LocalDate.of(2016, 1, 6),
                    LocalDate.of(2016, 1, 7),
                    LocalDate.of(2016, 1, 8),
                    LocalDate.of(2016, 1, 9),
                    LocalDate.of(2016, 1, 10)
                )
            }
        }
        "date range: [2016-02-26 to 2016-03-02]" should {
            "return 5 dates" in {
                datesInRange(
                    LocalDate.of(2016, 2, 26),
                    LocalDate.of(2016, 3, 2)
                ) shouldBe Set(
                    LocalDate.of(2016, 2, 26),
                    LocalDate.of(2016, 2, 27),
                    LocalDate.of(2016, 2, 28),
                    LocalDate.of(2016, 2, 29),
                    LocalDate.of(2016, 3, 1),
                    LocalDate.of(2016, 3, 2)
                )
            }
        }
        "date range: [2016-07-27 to 2016-08-08] and days of the week: [Tuesday, Saturday, Sunday] " should {
            "return 5 dates" in {
                datesInRange(
                    LocalDate.of(2016, Month.JULY, 27),
                    LocalDate.of(2016, Month.AUGUST, 8),
                    Set(DayOfWeek.TUESDAY, DayOfWeek.SATURDAY, DayOfWeek.SUNDAY)
                ) shouldBe Set(
                    LocalDate.of(2016, Month.JULY, 30),
                    LocalDate.of(2016, Month.JULY, 31),
                    LocalDate.of(2016, Month.AUGUST, 2),
                    LocalDate.of(2016, Month.AUGUST, 6),
                    LocalDate.of(2016, Month.AUGUST, 7)
                )
            }
        }
        "date range: [2016-01-01 to 2016-01-04] and days of the week: [Monday, Tuesday, Friday, Sunday]" should {
            "return 3 dates" in {
                datesInRange(
                    LocalDate.of(2016, Month.JANUARY, 1),
                    LocalDate.of(2016, Month.JANUARY, 4),
                    Set(DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.FRIDAY, DayOfWeek.SUNDAY)
                ) shouldBe Set(
                    LocalDate.of(2016, Month.JANUARY, 1),
                    LocalDate.of(2016, Month.JANUARY, 3),
                    LocalDate.of(2016, Month.JANUARY, 4)
                )
            }
        }
    }
}
