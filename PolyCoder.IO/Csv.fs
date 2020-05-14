namespace PolyCoder.IO
open System
open System.IO
open System.Text

module TextReader =
  type OnRowItemParsed = delegate of int * inref<ReadOnlySpan<char>> -> unit
  type OnValueParsed = delegate of inref<ReadOnlySpan<char>> -> unit

  type TableParserEvents = {
    setColumnName: OnRowItemParsed
    setColumnValue: OnRowItemParsed
    newRow: unit -> unit
    finish: unit -> unit
  }

  type ColumnParserEvents = {
    setColumnName: OnValueParsed
    addColumnValue: OnValueParsed
  }

  module Csv =

    type CsvFormat = {
      hasHeaders: bool
      skipEmptyLines: bool
      trimLines: bool
      trimValues: bool
      separator: string
      newLine: string
    }

    module CsvFormat =
      let typical = {
        hasHeaders = true
        skipEmptyLines = true
        trimLines = true
        trimValues = true
        separator = ","
        newLine = "\n"
      }

      let withHasHeaders hasHeaders format = { format with hasHeaders = hasHeaders }
      let withSkipEmptyLines skipEmptyLines format = { format with skipEmptyLines = skipEmptyLines }
      let withTrimLines trimLines format = { format with trimLines = trimLines }
      let withTrimValues trimValues format = { format with trimValues = trimValues }
      let withSeparator separator format = { format with separator = separator }
      let withNewLine newLine format = { format with newLine = newLine }

    module Internals =
      type CsvInternalOptions = {
        separatorChars: char[]
        format: CsvFormat
      }

      let toInternalOptions format =
        {
          format = format
          separatorChars = format.separator.ToCharArray()
        }

      let splitCsvRow options onValidRow (onColumn: OnRowItemParsed) (lineSpan: ReadOnlySpan<char>) =
        let mutable remaining = if options.format.trimLines then lineSpan.Trim() else lineSpan
        if not options.format.skipEmptyLines || remaining.Length > 0 then
          let mutable columnIndex = 0
          let separatorSpan = ReadOnlySpan(options.separatorChars)
          onValidRow()

          while remaining.IsEmpty |> not do
            let index = remaining.IndexOf separatorSpan

            let columnSpan =
              let columnSpan =
                if index = -1 then
                  let result = remaining
                  remaining <- ReadOnlySpan.Empty
                  result
                else
                  let result = remaining.Slice(0, index)
                  remaining <- remaining.Slice(index + separatorSpan.Length)
                  result
              if options.format.trimValues then columnSpan.Trim() else columnSpan
          
            onColumn.Invoke(columnIndex, &columnSpan)

            columnIndex <- columnIndex + 1
          ()


    open Internals

    type ParseCsvOptions = {
      format: CsvFormat
      events: TableParserEvents
    }

    let parseCsv options (reader: TextReader) = async {
      let internalOptions = toInternalOptions options.format

      let mutable line = ""
      let mutable headersWereRead = not options.format.hasHeaders
      while isNull line |> not do
        match! reader.ReadLineAsync() |> Async.AwaitTask with
        | text when isNull text |> not ->
          line <- text
          let lineSpan = line.AsSpan()

          if not headersWereRead then
            splitCsvRow internalOptions ignore options.events.setColumnName lineSpan
            headersWereRead <- true
          else
            splitCsvRow internalOptions options.events.newRow options.events.setColumnValue lineSpan
        
        | _ -> line <- null

      options.events.finish()
    }

    type ParseCsvColumnsOptions = {
      format: CsvFormat
      columns: ColumnParserEvents[]
    }
    
    let parseCsvColumns options reader = async {
      let mutable currentRow = 0
      let mutable currentColumn = -1

      let checkNextRow() =
        if currentColumn + 1 <> options.columns.Length then
          invalidOp (sprintf "Row #%d has the wrong number of columns. Expected %d but found %d" currentRow options.columns.Length (currentColumn + 1))

      let checkNextColumn index =
        if currentColumn + 1 <> index then
          invalidOp (sprintf "Row #%d is trying to skip columns. Expected %d but found %d" currentRow (currentColumn + 1) index)
        if index >= options.columns.Length then
          invalidOp (sprintf "Row #%d has the wrong number of columns. Expected %d but found %d" currentRow options.columns.Length (index + 1))

      let newRow () =
        checkNextRow()
        currentColumn <- -1
        currentRow <- currentRow + 1

      let finish () =
        checkNextRow()

      let setColumnName = OnRowItemParsed(fun index (nameSpan: inref<ReadOnlySpan<char>>) ->
        checkNextColumn index
        currentColumn <- index
        options.columns.[index].setColumnName.Invoke(&nameSpan))

      let setColumnValue = OnRowItemParsed(fun index (valueSpan: inref<ReadOnlySpan<char>>) ->
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

    type ReadIndexedUniformCsvOptions<'index, 'value, 'row> = {
      indexName: string
      columnNames: string[]
      parseIndex: CharSpanParser<'index>
      parseValue: CharSpanParser<'value>
      createRow: 'index -> 'value[] -> 'row
      csvFormat: CsvFormat
    }

    let readIndexedUniformCsv options (reader: TextReader) = async {
      let mutable currentIndex = Unchecked.defaultof<'index>
      let currentValues = Array.zeroCreate<'value> options.columnNames.Length

      let hasColumnName (name: string) =
        OnValueParsed(fun span ->
          let nameSpan = name.AsSpan()
          if not (span.Equals(nameSpan, StringComparison.InvariantCulture)) then
            invalidOp (sprintf "Expected column '%s' but found '%s'" name (span.ToString())))

      let result = ResizeArray()

      let addIndex = OnValueParsed(fun span ->
        currentIndex <- options.parseIndex.Invoke(span))

      let addValue index = OnValueParsed(fun span ->
        currentValues.[index] <- options.parseValue.Invoke(span)
        if index = options.columnNames.Length - 1 then
          let row = options.createRow currentIndex currentValues
          result.Add(row)
      )

      let columns =
        [|
            yield {
              setColumnName = hasColumnName options.indexName
              addColumnValue = addIndex
            }

            for i = 0 to options.columnNames.Length - 1 do
              yield {
                setColumnName = hasColumnName options.columnNames.[i]
                addColumnValue = addValue i
              }
        |]

      do! reader
        |> parseCsvColumns {
          format = options.csvFormat
          columns = columns
        }

      return result
    }
