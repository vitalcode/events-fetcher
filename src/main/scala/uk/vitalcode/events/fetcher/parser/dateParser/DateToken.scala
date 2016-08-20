package uk.vitalcode.events.fetcher.parser.dateParser

import java.text.DateFormatSymbols
import java.time.format.{DateTimeFormatterBuilder, SignStyle}
import java.time.temporal.ChronoField._
import java.time.{DayOfWeek, LocalDate, LocalTime}
import java.util.Locale

import scala.util.Try


trait DateTokenLike[T] {
    def value: T
}


case class YearToken(value: Int) extends DateTokenLike[Int]

object YearToken {

    // Matches year in interval 1900-2099
    // from: http://stackoverflow.com/questions/4374185/regular-expression-match-to-test-for-a-valid-year
    private val yearRegEx =
        """(19|20)\d{2}""".r

    def of(token: String): Option[YearToken] = {
        val year = yearRegEx.findFirstIn(token)
        if (year.nonEmpty) Some(YearToken(year.get.toInt)) else None
    }
}


case class MonthToken(value: Int) extends DateTokenLike[Int]

object MonthToken {

    private def getMonths: Seq[String] = new DateFormatSymbols(Locale.UK).getMonths.map(m => m.toLowerCase())

    private def getShortMonths: Seq[String] = new DateFormatSymbols(Locale.UK).getShortMonths.map(m => m.toLowerCase())

    def of(token: String): Option[MonthToken] = {
        val tokenLowCase = token.toLowerCase()
        val monthsIndex = getMonths.indexOf(tokenLowCase)
        val shortMonthsIndex = getShortMonths.indexOf(tokenLowCase)
        if (monthsIndex != -1) Some(MonthToken(monthsIndex + 1))
        else if (shortMonthsIndex != -1) Some(MonthToken(shortMonthsIndex + 1))
        else None
    }
}


case class DayOfMonthToken(value: Int) extends DateTokenLike[Int]

object DayOfMonthToken {

    private val dayOfMonthRegEx = """[0123]?[0-9]""".r //TODO fix

    def of(token: String): Option[DayOfMonthToken] = {
        val dayOfMonth = dayOfMonthRegEx.findFirstIn(token)
        if (dayOfMonth.nonEmpty) Some(DayOfMonthToken(dayOfMonth.get.toInt)) else None
    }
}


case class DayOfWeekToken(value: DayOfWeek) extends DateTokenLike[DayOfWeek]

object DayOfWeekToken {

    private def getWeekdays: Seq[String] = new DateFormatSymbols(Locale.UK).getWeekdays.map(w => w.toLowerCase())

    private def getShortWeekdays: Seq[String] = new DateFormatSymbols(Locale.UK).getShortWeekdays.map(w => w.toLowerCase())

    private def getDayOfWeek(index: Int): DayOfWeek = DayOfWeek.of(if (index > 1) index - 1 else 7)

    def of(token: String): Option[DayOfWeekToken] = {
        val tokenLowCase = token.toLowerCase()
        val weekdaysIndex = getWeekdays.indexOf(tokenLowCase)
        val shortWeekdaysIndex = getShortWeekdays.indexOf(tokenLowCase)
        if (weekdaysIndex != -1) Some(DayOfWeekToken(getDayOfWeek(weekdaysIndex)))
        else if (shortWeekdaysIndex != -1) Some(DayOfWeekToken(getDayOfWeek(shortWeekdaysIndex)))
        else None
    }
}


//case class TimeToken(value: LocalTime) extends DateTokenLike[LocalTime]
//
//object TimeToken { // TODO refactor
//
//    // Matches times seperated by either : or . will match a 24 hour time, or a 12 hour time with AM or PM specified. Allows 0-59 minutes, and 0-59 seconds. Seconds are not required.
//    // Replace (AM|am|aM|Am|PM|pm|pM|Pm) with (?i:(a|p)(\.)?( )?m(\.)?) and you will match any combination of am/pm (case insensitive) including with periods and spaces (i.e., AM, A.M., or A. M.)
//    // Matches: 1:01 AM | 23:52:01 | 03.24.36 AM
//    // Non-Matches: 19:31 AM | 9:9 PM | 25:60:61
//    // From: http://regexlib.com/REDetails.aspx?regexp_id=144
//    private val timeRegEx =
//        """((([0]?[1-9]|1[0-2])((:|\.)[0-5][0-9])?((:|\.)[0-5][0-9])?( )?(AM|am|aM|Am|PM|pm|pM|Pm))|(([0]?[0-9]|1[0-9]|2[0-3])(:|\.)[0-5][0-9]((:|\.)[0-5][0-9])?))""".r
//    //"""^((([0]?[1-9]|1[0-2])(:|\.)[0-5][0-9]((:|\.)[0-5][0-9])?( )?(AM|am|aM|Am|PM|pm|pM|Pm))|(([0]?[0-9]|1[0-9]|2[0-3])(:|\.)[0-5][0-9]((:|\.)[0-5][0-9])?))$""".r
//
//    private val formatter24H = new DateTimeFormatterBuilder()
//        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NEVER)
//        .appendLiteral(':')
//        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
//        .optionalStart()
//        .appendLiteral(':')
//        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
//        .optionalStart()
//        .appendFraction(NANO_OF_SECOND, 0, 9, true)
//        .toFormatter
//
//    private val formatter12H = new DateTimeFormatterBuilder()
//        .appendValue(HOUR_OF_AMPM, 1, 2, SignStyle.NEVER)
//        .optionalStart()
//        .appendLiteral(':')
//        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
//        .optionalStart()
//        .appendLiteral(':')
//        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
//        .optionalStart()
//        .appendFraction(NANO_OF_SECOND, 0, 9, true)
//        .optionalEnd()
//        .optionalEnd()
//        .optionalEnd()
//        .appendText(AMPM_OF_DAY)
//        .toFormatter()
//
//    private val formatter12HwithDot = new DateTimeFormatterBuilder()
//        .appendValue(HOUR_OF_AMPM, 1, 2, SignStyle.NEVER)
//        .optionalStart()
//        .appendLiteral('.')
//        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
//        .optionalStart()
//        .appendLiteral('.')
//        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
//        .optionalStart()
//        .appendFraction(NANO_OF_SECOND, 0, 9, true)
//        .optionalEnd()
//        .optionalEnd()
//        .optionalEnd()
//        .appendText(AMPM_OF_DAY)
//        .toFormatter()
//
//
//    private def format24H(text: String): Option[LocalTime] = Try {
//        LocalTime.parse(text, formatter24H)
//    }.toOption
//
//    private def format12H(text: String): Option[LocalTime] = Try {
//        if (text.contains(':')) LocalTime.parse(text, formatter12H)
//        else LocalTime.parse(text, formatter12HwithDot)
//    }.toOption
//
//    def parseTimeString(text: String): LocalTime = { // todo do not repeat call to parser
//        if (format12H(text).isEmpty) {
//            if (format24H(text).isEmpty) LocalTime.parse("11:11PM", formatter12H) ///  // refactor may return null
//            else format24H(text).get
//        }
//        else format12H(text).get
//    }
//
//    def of(token: String): Option[TimeToken] = {
//
//        val time = timeRegEx.findFirstIn(token)
//
//        val timeToken = if (time.nonEmpty) Some(TimeToken(parseTimeString(time.get.replaceAll("\\s", "").toUpperCase())))
//        else None
//        return timeToken
//    }
//}


case class RangeToken(value: String) extends DateTokenLike[String]

object RangeToken {

    private val rangeRegEx = """(-|to|until)""".r

    def of(token: String): Option[RangeToken] = {
        val range = rangeRegEx.findFirstIn(token)
        if (range.nonEmpty) Some(RangeToken(range.get)) else None
    }
}


case class DateToken(value: LocalDate) extends DateTokenLike[LocalDate]

