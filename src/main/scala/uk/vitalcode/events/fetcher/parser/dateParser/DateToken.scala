package uk.vitalcode.events.fetcher.parser.dateParser

import java.time.format.{DateTimeFormatterBuilder, SignStyle}
import java.time.temporal.ChronoField._
import java.time.{DayOfWeek, LocalDate, LocalTime}

import scala.util.Try


sealed trait DateTokenLike[T] {
    def token: String
    def apply: Boolean
    def getValue: T
}

case class YearToken(token: String) extends DateTokenLike

case class MonthToken(token: String) extends DateTokenLike {
    def getMonth: Int = {
        token.toInt
    }
}

case class DayOfMonthToken(token: String) extends DateTokenLike

case class DateToken(dateTime: LocalDate) extends DateTokenLike {
    def token: String = {
        "ddd"
    }
}

case class DayOfWeekToken(token: DayOfWeek) extends DateTokenLike

case class TimeToken(token: String) extends DateTokenLike {


    private val formatter24H = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NEVER)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .toFormatter

    private val formatter12H = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_AMPM, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .appendText(AMPM_OF_DAY)
        .toFormatter()

    private val formatter12HwithDot = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_AMPM, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral('.')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral('.')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .appendText(AMPM_OF_DAY)
        .toFormatter()


    private def format24H: Option[LocalTime] = Try {
        LocalTime.parse(token, formatter24H)
    }.toOption

    private def format12H: Option[LocalTime] = Try {
        if (token.contains(':')) LocalTime.parse(token, formatter12H)
        else LocalTime.parse(token, formatter12HwithDot)
    }.toOption

    def time: LocalTime = {
        if (format12H.isEmpty) {
            if (format24H.isEmpty) LocalTime.parse("11:11PM", formatter12H) ///  // refactor may return null
            else format24H.get
        }
        else format12H.get
    }
}

