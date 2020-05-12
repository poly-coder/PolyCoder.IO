namespace PolyCoder.IO
open System
open System.IO
open System.Text

module TextReader =
  type OnColumnParsedDelegate = delegate of int * inref<ReadOnlySpan<char>> -> unit

  type TableParserEvents = {
    setColumnName: OnColumnParsedDelegate
    setColumnValue: OnColumnParsedDelegate
    newRow: unit -> unit
    finish: unit -> unit
  }

  module Csv =

    type CsvFormat = {
      hasHeaders: bool
      skipEmptyLines: bool
      trimLines: bool
      trimValues: bool
      separator: string
      newLine: string
      encoding: Encoding
    }

    module CsvFormat =
      let typical = {
        hasHeaders = true
        skipEmptyLines = true
        trimLines = true
        trimValues = true
        separator = ","
        newLine = "\n"
        encoding = Encoding.UTF8
      }

      let withHasHeaders hasHeaders format = { format with hasHeaders = hasHeaders }
      let withSkipEmptyLines skipEmptyLines format = { format with skipEmptyLines = skipEmptyLines }
      let withTrimLines trimLines format = { format with trimLines = trimLines }
      let withTrimValues trimValues format = { format with trimValues = trimValues }
      let withSeparator separator format = { format with separator = separator }
      let withNewLine newLine format = { format with newLine = newLine }
      let withEncoding encoding format = { format with encoding = encoding }

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

      let splitCsvRow options (onColumn: OnColumnParsedDelegate) (lineSpan: ReadOnlySpan<char>) =
        let mutable remaining = if options.format.trimLines then lineSpan else lineSpan.Trim()
        if not options.format.skipEmptyLines || remaining.Length > 0 then
          let mutable columnIndex = 0
          let separatorSpan = ReadOnlySpan(options.separatorChars)

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
            splitCsvRow internalOptions options.events.setColumnName lineSpan
            headersWereRead <- true
          else
            options.events.newRow()
            splitCsvRow internalOptions options.events.setColumnName lineSpan
        
        | _ -> line <- null

      options.events.finish()
    }
