package uk.vitalcode.events.fetcher.test.utils

import java.time.LocalDate
import org.scalatest._
import uk.vitalcode.events.fetcher.utils.DateTimeUtil

class DateTimeUtilTest extends WordSpec with ShouldMatchers {
    "DateTimeUtil" when {
        "calling datesInRange method for date range 2016-01-01 - 2016-01-10" should {
            "return 10 dates" in {
                DateTimeUtil.datesInRange(
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
        "calling datesInRange method for date range 2016-02-26 - 2016-03-02" should {
            "return 5 dates" in {
                DateTimeUtil.datesInRange(
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
    }
}
