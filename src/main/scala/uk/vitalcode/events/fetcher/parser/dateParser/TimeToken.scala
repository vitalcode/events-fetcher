package uk.vitalcode.events.fetcher.parser.dateParser

import java.time.LocalTime
import java.time.format.DateTimeFormatter

import scala.util.Try

case class TimeToken(value: LocalTime) extends DateTokenLike[LocalTime]

object TimeToken {

    private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(""
        + "[h.m[.s]a]"
        + "[h[:m][:s]a]"
        + "[H.m[.s]]"
        + "[H:m[:s]]"
    )

    // Matches times seperated by either : or . will match a 24 hour time, or a 12 hour time with AM or PM specified. Allows 0-59 minutes, and 0-59 seconds. Seconds are not required.
    // From: http://regexlib.com/REDetails.aspx?regexp_id=144
    private val timeRegEx =
        """((([0]?[1-9]|1[0-2])((:|\.)[0-5]?[0-9])?((:|\.)[0-5]?[0-9])?( )?(AM|am|aM|Am|PM|pm|pM|Pm))|(([0]?[0-9]|1[0-9]|2[0-3])(:|\.)[0-5]?[0-9]((:|\.)[0-5]?[0-9])?))""".r

    private def preProcessTimeString(str: String): String = {
        str.replaceAll("""\s*""", "").toUpperCase
    }

    private def parseTime(str: String): Try[LocalTime] = Try {
        LocalTime.parse(preProcessTimeString(str), formatter)
    }

    def of(token: String): Option[TimeToken] = {
        val timeStr = timeRegEx.findFirstIn(token)
        if (timeStr.isDefined){
            val time = parseTime(timeStr.get)
            if (time.isSuccess) Some(TimeToken(time.get)) else None
        }
        else None
    }
}