[<NUnit.Framework.TestFixture>]
module PolyCoder.IO.TextReader.Csv

open System
open NUnit.Framework
open Swensen.Unquote
open System.Globalization
open PolyCoder.IO.TextReader.Csv
open System.IO
open System.Threading.Tasks

let invariantCulture = CultureInfo.InvariantCulture
  
[<Test>]
let ``TextReader.Csv.parseCsv should parse a csv file`` () =
  async {
    let csvContent = """ DateTime , Ask,  Bid  
  2020-05-13 14:36:38.1234 , 1.3456, 1.3453 
  2020-05-14 14:36:38.1234 , 1.1234, 1.1231    
    
  2020-05-15 14:36:38.1234 , 1.2345, 1.2342 
  2020-05-16 14:36:38.1234 , 1.5678, 1.5676  
 
           """

    let setColumnNameCalls = ResizeArray()
    let setColumnName = OnRowItemParsed(fun index span ->
      setColumnNameCalls.Add((index, span.ToString())))

    let setColumnValueCalls = ResizeArray()
    let setColumnValue = OnRowItemParsed(fun index span ->
      setColumnValueCalls.Add((index, span.ToString())))

    let newRowCalls = ref 0
    let newRow() =
      newRowCalls := !newRowCalls + 1

    let finishCalls = ref 0
    let finish() =
      finishCalls := !finishCalls + 1

    let options: ParseCsvOptions = {
      format = CsvFormat.typical
                |> CsvFormat.withNewLine Environment.NewLine
      events = {
        setColumnName = setColumnName
        setColumnValue = setColumnValue
        newRow = newRow
        finish = finish
      }
    }

    use reader = new StringReader(csvContent)
    do! parseCsv options reader

    test <@ !finishCalls = 1 @>
    test <@ !newRowCalls = 4 @>
    
    let setColumnNameCalls = List.ofSeq setColumnNameCalls
    let setColumnValueCalls = List.ofSeq setColumnValueCalls

    test <@ setColumnNameCalls = [(0, "DateTime"); (1, "Ask"); (2, "Bid")] @>
    test <@ setColumnValueCalls = [
      (0, "2020-05-13 14:36:38.1234"); (1, "1.3456"); (2, "1.3453");
      (0, "2020-05-14 14:36:38.1234"); (1, "1.1234"); (2, "1.1231");
      (0, "2020-05-15 14:36:38.1234"); (1, "1.2345"); (2, "1.2342");
      (0, "2020-05-16 14:36:38.1234"); (1, "1.5678"); (2, "1.5676")] @>
    
  }
  |> Async.StartAsTask
  :> Task
  
[<Test>]
let ``TextReader.Csv.parseCsvColumns should parse a csv file`` () =
  async {
    let csvContent = """ DateTime , Ask,  Bid  
  2020-05-13 14:36:38.1234 , 1.3456, 1.3453 
  2020-05-14 14:36:38.1234 , 1.1234, 1.1231    
    
  2020-05-15 14:36:38.1234 , 1.2345, 1.2342 
  2020-05-16 14:36:38.1234 , 1.5678, 1.5676  
 
           """

    let setColumnNameCalls = [| ResizeArray(); ResizeArray(); ResizeArray() |]
    let setColumnName column = OnValueParsed(fun span -> setColumnNameCalls.[column].Add(span.ToString()))

    let addColumnValueCalls = [| ResizeArray(); ResizeArray(); ResizeArray() |]
    let addColumnValue column = OnValueParsed(fun span -> addColumnValueCalls.[column].Add(span.ToString()))

    let options: ParseCsvColumnsOptions = {
      format = CsvFormat.typical
                |> CsvFormat.withNewLine Environment.NewLine
      columns = [|
        { setColumnName = setColumnName 0; addColumnValue = addColumnValue 0 }
        { setColumnName = setColumnName 1; addColumnValue = addColumnValue 1 }
        { setColumnName = setColumnName 2; addColumnValue = addColumnValue 2 }
      |]
    }

    use reader = new StringReader(csvContent)
    do! parseCsvColumns options reader

    let testList (array: ResizeArray<_>) (expected: _ list) =
      test <@ List.ofSeq array = expected @>

    testList setColumnNameCalls.[0] ["DateTime"]
    testList setColumnNameCalls.[1] ["Ask"]
    testList setColumnNameCalls.[2] ["Bid"]
    
    testList addColumnValueCalls.[0] [
      "2020-05-13 14:36:38.1234"; "2020-05-14 14:36:38.1234";
      "2020-05-15 14:36:38.1234"; "2020-05-16 14:36:38.1234"]
    
    testList addColumnValueCalls.[1] ["1.3456"; "1.1234"; "1.2345"; "1.5678"]
    
    testList addColumnValueCalls.[2] ["1.3453"; "1.1231"; "1.2342"; "1.5676"]
  }
  |> Async.StartAsTask
  :> Task
