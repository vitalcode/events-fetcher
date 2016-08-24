package uk.vitalcode.events.fetcher.parser.dateParser

object DateTokenFactory {

    def create(token: String, index: Int): Option[DateTokenLike[_]] = {
        val week = DayOfWeekToken.of(token, index)
        if (week.nonEmpty) week
        else {
            val time = TimeToken.of(token, index)
            if (time.nonEmpty) time
            else {
                val year = YearToken.of(token, index)
                if (year.nonEmpty) year
                else {
                    val month = MonthToken.of(token, index)
                    if (month.nonEmpty) month
                    else {
                        val dayOfMonth = DayOfMonthToken.of(token, index)
                        if (dayOfMonth.nonEmpty) dayOfMonth
                        else {
                            val range = RangeToken.of(token, index)
                            if (range.nonEmpty) range else None
                        }
                    }
                }
            }
        }
    }
}
