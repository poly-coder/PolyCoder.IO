module PolyCoder.IO.DataSets

open System

type ResizeDataColumn<'a> = ResizeDataColumn of name: string * content: ResizeArray<'a>

module TextReader =
  open PolyCoder.IO.TextReader

  type OnDataParsedDelegate = delegate of inref<ReadOnlySpan<char>> -> unit

  type ColumnParserEvents = {
    setColumnName: OnDataParsedDelegate
    addColumnValue: OnDataParsedDelegate
  }

  module Csv =
    open PolyCoder.IO.TextReader.Csv

    type ParseOptions = {
      format: CsvFormat
      columns: ColumnParserEvents[]
    }
    
    let parse options reader = async {
      let mutable currentRow = 0
      let mutable currentColumn = -1

      let checkNextRow() =
        if currentColumn + 1 <> options.columns.Length then
          invalidOp (sprintf "Row #%d has the wrong number of columns. Expected %d but found %d" currentRow options.columns.Length (currentColumn + 1))

      let checkNextColumn index =
        if currentColumn + 1 <> index then
          invalidOp (sprintf "Row #%d has is trying to skip columns. Expected %d but found %d" currentRow (currentColumn + 1) index)

      let newRow () =
        checkNextRow()
        currentColumn <- -1
        currentRow <- currentRow + 1

      let finish () =
        checkNextRow()

      let setColumnName = OnColumnParsedDelegate(fun index (nameSpan: inref<ReadOnlySpan<char>>) ->
        checkNextColumn index
        currentColumn <- index
        options.columns.[index].setColumnName.Invoke(&nameSpan))

      let setColumnValue = OnColumnParsedDelegate(fun index (valueSpan: inref<ReadOnlySpan<char>>) ->
        checkNextColumn index
        currentColumn <- index
        options.columns.[index].addColumnValue.Invoke(&valueSpan))

      let events: TableParserEvents = {
        finish = finish
        newRow = newRow
        setColumnName = setColumnName
        setColumnValue = setColumnValue
      }

      do! parseCsv { format = options.format; events = events } reader
    }
      