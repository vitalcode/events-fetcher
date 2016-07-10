package uk.vitalcode.events.fetcher.parser.dateParser

object DateTokenFactory {

    def create(token: String): Option[DateTokenLike[_]] = {
        val week = DayOfWeekToken of token
        if (week.nonEmpty) week
        else {
            val time = TimeToken of token
            if (time.nonEmpty) time
            else {
                val year = YearToken of token
                if (year.nonEmpty) year
                else {
                    val month = MonthToken of token
                    if (month.nonEmpty) month
                    else {
                        val dayOfMonth = DayOfMonthToken of token
                        if (dayOfMonth.nonEmpty) dayOfMonth
                        else {
                            val range = RangeToken of token
                            if (range.nonEmpty) range else None
                        }
                    }
                }
            }
        }
    }
}
