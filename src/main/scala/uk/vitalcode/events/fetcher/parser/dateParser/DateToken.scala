package uk.vitalcode.events.fetcher.parser.dateParser

import java.text.DateFormatSymbols
import java.time.{DayOfWeek, LocalDate}
import java.util.Locale


trait DateTokenLike[T] {
    def value: T

    def index: Int
}


case class YearToken(value: Int, index: Int) extends DateTokenLike[Int]

object YearToken {

    // Matches year in interval 1900-2099
    // from: http://stackoverflow.com/questions/4374185/regular-expression-match-to-test-for-a-valid-year
    private val yearRegEx =
        """(19|20)\d{2}""".r

    def of(token: String, index: Int): Option[YearToken] = {
        val year = yearRegEx.findFirstIn(token)
        if (year.nonEmpty) Some(YearToken(year.get.toInt, index)) else None
    }
}


case class MonthToken(value: Int, index: Int) extends DateTokenLike[Int]

object MonthToken {

    private def getMonths: Seq[String] = new DateFormatSymbols(Locale.UK).getMonths.map(m => m.toLowerCase())

    private def getShortMonths: Seq[String] = new DateFormatSymbols(Locale.UK).getShortMonths.map(m => m.toLowerCase())

    def of(token: String, index: Int): Option[MonthToken] = {
        val tokenLowCase = token.toLowerCase()
        val monthsIndex = getMonths.indexOf(tokenLowCase)
        val shortMonthsIndex = getShortMonths.indexOf(tokenLowCase)
        if (monthsIndex != -1) Some(MonthToken(monthsIndex + 1, index))
        else if (shortMonthsIndex != -1) Some(MonthToken(shortMonthsIndex + 1, index))
        else None
    }
}


case class DayOfMonthToken(value: Int, index: Int) extends DateTokenLike[Int]

object DayOfMonthToken {

    private val dayOfMonthRegEx = """[0123]?[0-9]""".r //TODO fix

    def of(token: String, index: Int): Option[DayOfMonthToken] = {
        val dayOfMonth = dayOfMonthRegEx.findFirstIn(token)
        if (dayOfMonth.nonEmpty) Some(DayOfMonthToken(dayOfMonth.get.toInt, index)) else None
    }
}


case class DayOfWeekToken(value: DayOfWeek, index: Int) extends DateTokenLike[DayOfWeek]

object DayOfWeekToken {

    private def getWeekdays: Seq[String] = new DateFormatSymbols(Locale.UK).getWeekdays.map(w => w.toLowerCase())

    private def getShortWeekdays: Seq[String] = new DateFormatSymbols(Locale.UK).getShortWeekdays.map(w => w.toLowerCase())

    private def getDayOfWeek(index: Int): DayOfWeek = DayOfWeek.of(if (index > 1) index - 1 else 7)

    def of(token: String, index: Int): Option[DayOfWeekToken] = {
        val tokenLowCase = token.toLowerCase()
        val weekdaysIndex = getWeekdays.indexOf(tokenLowCase)
        val shortWeekdaysIndex = getShortWeekdays.indexOf(tokenLowCase)
        if (weekdaysIndex != -1) Some(DayOfWeekToken(getDayOfWeek(weekdaysIndex), index))
        else if (shortWeekdaysIndex != -1) Some(DayOfWeekToken(getDayOfWeek(shortWeekdaysIndex), index))
        else None
    }
}


case class RangeToken(value: String, index: Int) extends DateTokenLike[String]

object RangeToken {

    private val rangeRegEx = """(-|to|until)""".r

    def of(token: String, index: Int): Option[RangeToken] = {
        val range = rangeRegEx.findFirstIn(token)
        if (range.nonEmpty) Some(RangeToken(range.get, index)) else None
    }
}


case class DateToken(value: LocalDate, index: Int) extends DateTokenLike[LocalDate]

