package uk.vitalcode.events.fetcher.model

case class DataTable(dataRows: Set[DataRow]) extends Serializable

case class DataTableBuilder() extends Builder {
    private var dataRows: Set[DataRow] = Set.empty[DataRow]

    def addRow(dataRowBuilder: DataRowBuilder): DataTableBuilder = {
        this.dataRows += dataRowBuilder.build()
        this
    }

    override type t = DataTable

    override def build(): DataTable = new DataTable(dataRows)
}