package uk.vitalcode.events.fetcher.model

import scala.collection.{mutable, Map}

case class DataRow(rowId: String, columns: Map[String, Set[String]]) extends Serializable

case class DataRowBuilder() extends Builder {
    private var columns: Map[String, Set[String]] = mutable.LinkedHashMap[String, Set[String]]()
    private var row: String = _

    def setRowId(row: String): DataRowBuilder = {
        this.row = row
        this
    }

    def addColumn(column: String, value: String*): DataRowBuilder = {
        addColumn(column, value.toSet)
        this
    }

    def addColumn(column: String, value: Set[String]): DataRowBuilder = {
        if (this.columns.contains(column)){
            val prev: Option[Set[String]] = this.columns.get(column)
            val newVal = prev.get ++ value
            this.columns += (column -> newVal)
        } else {
            this.columns += (column -> value)
        }
        this
    }

    def isEmpty(): Boolean = columns.isEmpty
    def reset(): Unit = columns = Map.empty[String, Set[String]]

    override type t = DataRow

    override def build(): DataRow = new DataRow(row, columns)
}