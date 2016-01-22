package uk.vitalcode.events.fetcher.model

import scala.collection.mutable

case class DataTable(dataRows: Set[DataRow]) extends Serializable {

    override def toString: String = {
        val builder = mutable.StringBuilder.newBuilder
        builder.append(s"Table rows count [${dataRows.size}]\n")
        dataRows.foreach(row => {
            builder.append(s"--- Row id [${row.rowId}]\n")
            row.columns.foreach(column => {
                column._2.foreach(value => {
                    builder.append(s"------ Column [${column._1}] --- [$value]\n")
                })
            })
        })
        builder.toString()
    }
}

case class DataTableBuilder() extends Serializable with Builder {
    private var dataRows: Set[DataRow] = Set.empty[DataRow]

    def addRow(dataRowBuilder: DataRowBuilder): DataTableBuilder = {
        this.dataRows += dataRowBuilder.build()
        this
    }

    override type t = DataTable

    override def build(): DataTable = new DataTable(dataRows)
}