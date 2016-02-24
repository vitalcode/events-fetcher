package uk.vitalcode.events.fetcher.parser

import java.text.DateFormatSymbols
import java.time.temporal.ChronoField._
import java.time.{LocalTime, LocalDateTime}
import java.time.format.{SignStyle, ResolverStyle, DateTimeFormatterBuilder, DateTimeFormatter}
import java.util.Locale

import uk.vitalcode.events.fetcher.model.Prop

object DateParser extends ParserLike[String] {

    // Split on white space or "-" (range character, including "-" as return token, but not before AM/PM)
    val splitRegEx =
        """(?<![-])[\s]+(?![-]|PM|pm|AM|am)|(?=[-])|(?<=[-])""".r

    val dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

    override def parse(prop: Prop): Set[String] = {
        val when = parseAsDateTime(prop)
        Set(when._1.format(dateFormatter), when._2.format(dateFormatter))
    }

    def parseAsDateTime(prop: Prop): (LocalDateTime, LocalDateTime) = {
        val tokens = splitRegEx.split(prop.values.reduce((a, b) => a.concat(b.concat(" "))))
            .flatMap(t => DateTokenFactory.create(t))

        val dates: Vector[LocalDateTime] = tokens.grouped(3)
            .flatMap(g => {
                val year = g.find(ty => ty.isInstanceOf[YearToken])
                val month = g.filter(tm => tm.isInstanceOf[MonthToken]).map(ty => ty.asInstanceOf[MonthToken]).headOption
                val dayOfMonth = g.find(ty => ty.isInstanceOf[DayOfMonthToken])
                if (year.nonEmpty && month.nonEmpty && dayOfMonth.nonEmpty) {
                    Vector(DateToken(LocalDateTime.of(year.get.token.toInt, month.get.getMonth, dayOfMonth.get.token.toInt, 0, 0)))
                } else g
            })
            .filter(d => d.isInstanceOf[DateToken])
            .map(d => d.asInstanceOf[DateToken].dateTime)
            .toVector

        val times: Vector[LocalTime] = tokens.flatMap(t => t match {
            case t: TimeToken => Vector(t.time)
            case _ => None
        }).toVector

        dates.size match {
            case 1 => analyseOneDatePattern(dates, times)
            case _ => ???
        }
    }

    // TODO make as strategy
    private def analyseOneDatePattern(dates: Vector[LocalDateTime], times: Vector[LocalTime]): (LocalDateTime, LocalDateTime) = {
        val fromDate = dateWithTime(dates.head, times.headOption)
        val toDate = dateWithTime(dates.head, times.lastOption)
        (fromDate, toDate)
    }

    private def dateWithTime(date: LocalDateTime, time: Option[LocalTime]): LocalDateTime = {
        if (time.nonEmpty) {
            date.withHour(time.get.getHour)
                .withMinute(time.get.getMinute)
                .withSecond(time.get.getSecond)
                .withNano(time.get.getNano)
        } else date
    }
}


object DateTokenFactory {

    // Matches times seperated by either : or . will match a 24 hour time, or a 12 hour time with AM or PM specified. Allows 0-59 minutes, and 0-59 seconds. Seconds are not required.
    // Replace (AM|am|aM|Am|PM|pm|pM|Pm) with (?i:(a|p)(\.)?( )?m(\.)?) and you will match any combination of am/pm (case insensitive) including with periods and spaces (i.e., AM, A.M., or A. M.)
    // Matches: 1:01 AM | 23:52:01 | 03.24.36 AM
    // Non-Matches: 19:31 AM | 9:9 PM | 25:60:61
    // From: http://regexlib.com/REDetails.aspx?regexp_id=144
    private val timeRegEx =
        """^((([0]?[1-9]|1[0-2])(:|\.)[0-5][0-9]((:|\.)[0-5][0-9])?( )?(AM|am|aM|Am|PM|pm|pM|Pm))|(([0]?[0-9]|1[0-9]|2[0-3])(:|\.)[0-5][0-9]((:|\.)[0-5][0-9])?))$""".r

    // Matches year in interval 1900-2099
    // From: http://stackoverflow.com/questions/4374185/regular-expression-match-to-test-for-a-valid-year
    private val yearRegEx =
        """^(19|20)\d{2}$""".r

    private val monthRegEx = """(?i)january|february|march|april|may|june|july|august|september|october|november|december""".r

    private val dayOfMonthRegEx = """[0123]?[0-9]""".r //TODO fix

    def create(token: String): Option[DateTokenLike] = {
        val time = timeRegEx.findFirstIn(token)
        if (time.nonEmpty) Some(TimeToken(time.get))
        else {
            val year = yearRegEx.findFirstIn(token)
            if (year.nonEmpty) Some(YearToken(year.get))
            else {
                val month = monthRegEx.findFirstIn(token)
                if (month.nonEmpty) Some(MonthToken(month.get))
                else {
                    val dayOfMonth = dayOfMonthRegEx.findFirstIn(token)
                    if (dayOfMonth.nonEmpty) Some(DayOfMonthToken(dayOfMonth.get))
                    else None
                }
            }
        }
    }
}


trait DateTokenLike {
    def token: String
}

case class YearToken(token: String) extends DateTokenLike

case class MonthToken(token: String) extends DateTokenLike {
    def getMonth: Int = {
        new DateFormatSymbols(Locale.UK).getMonths.indexOf(token) + 1
    }
}

case class DayOfMonthToken(token: String) extends DateTokenLike

case class DateToken(dateTime: LocalDateTime) extends DateTokenLike {
    def token: String = {
        "ddd"
    }
}

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
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .optionalEnd()
        .appendLiteral(' ')
        .appendText(AMPM_OF_DAY)
        .toFormatter()

    private def format24H: Option[LocalTime] = {
        try {
            Some(LocalTime.parse(token, formatter24H))
        } catch {
            case _: Throwable => None
        }
    }

    private def format12H: Option[LocalTime] = {
        try {
            Some(LocalTime.parse(token, formatter12H))
        } catch {
            case _: Throwable => None
        }
    }

    def time: LocalTime = {
        if (format12H.nonEmpty) format12H.get else format24H.get
    }
}