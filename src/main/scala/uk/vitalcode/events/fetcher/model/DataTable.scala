package uk.vitalcode.events.fetcher.model

import java.text.SimpleDateFormat
import java.util.Date

import uk.vitalcode.events.model.Builder

import scala.collection.JavaConversions.JListWrapper
import scala.collection.Map

case class DataTable(dataRows: Set[DataRow]) extends Serializable {

    override def toString: String = {
        val builder = scala.collection.mutable.StringBuilder.newBuilder
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
    private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    private var dataRows: Set[DataRow] = Set.empty[DataRow]

//    def addArray(array: Array[(String, Map[String, AnyRef])]): DataTableBuilder = {
//        this.dataRows = array.map(r => DataRow(r._1, r._2.map(c => c._1 match {
//            case "from" | "to" => (c._1, c._2.asInstanceOf[JListWrapper[Date]].map(m => dateFormat.format(m)).toSet)
//            case _ => (c._1, c._2.asInstanceOf[JListWrapper[String]].toSet)
//        }))).toSet
//        this
//    }

    def addArray(array: Array[(String, Map[String, AnyRef])]): DataTableBuilder = {
        this.dataRows = array.map(r => DataRow(null, r._2.map(c => c._1 match {
            case "from" | "to" => (c._1, c._2.asInstanceOf[JListWrapper[Date]].map(m => dateFormat.format(m)).toSet)
            case _ => (c._1, c._2.asInstanceOf[JListWrapper[String]].toSet)
        }))).toSet
        this
    }

    def addRow(dataRowBuilder: DataRowBuilder): DataTableBuilder = {
        this.dataRows += dataRowBuilder.build()
        this
    }

    override type t = DataTable

    override def build(): DataTable = new DataTable(dataRows)
}