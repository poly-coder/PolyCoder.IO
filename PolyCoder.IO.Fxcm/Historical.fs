module PolyCoder.IO.Fxcm.Historical

open System
open System.IO
open System.IO.Compression
open System.Text
open PolyCoder.IO
open PolyCoder.IO.DataSets
open PolyCoder.IO.TextReader
open PolyCoder.IO.TextReader.Csv
open System.Globalization
open System.Collections.Generic

let DateTimeFormat = "MM/dd/yyyy HH:mm:ss.fff"

type FxcmTickData = {
  dateTime: DateTime
  bid: float32
  ask: float32
}

let DefaultBufferSize = 0x10000

[<RequireQualifiedAccess>]
module TickData =
  let MagicNumber = 2103249824
  let Instruments = [
    "AUDCAD"
    "AUDCHF"
    "AUDJPY"
    "AUDNZD"
    "AUDUSD"
    "CADCHF"
    "CADJPY"
    "EURAUD"
    "EURCHF"
    "EURGBP"
    "EURJPY"
    "EURUSD"
    "EURNZD"
    "GBPCHF"
    "GBPJPY"
    "GBPNZD"
    "GBPUSD"
    "GBPCAD"
    "NZDCAD"
    "NZDCHF"
    "NZDJPY"
    "NZDUSD"
    "USDCAD"
    "USDCHF"
    "USDJPY"
    "USDTRY"
  ]

  let ofCsvReader reader =
    reader
      |> readIndexedUniformCsv {
        indexName = "DateTime"
        columnNames = [| "Bid"; "Ask" |]
        parseIndex = Text.makeFixedPositionDateParser DateTimeKind.Utc DateTimeFormat
        parseValue = Text.parseFloat32
        createRow = fun index [| bid; ask |] -> { dateTime = index; bid = bid; ask = ask }
        csvFormat = CsvFormat.typical |> CsvFormat.withNewLine "\r\n"
      }

  let ofLocalCsvFile filePath = async {
    use reader = filePath |> TextReader.fromFile DefaultBufferSize Encoding.Unicode
    return! ofCsvReader reader
  }

  let ofWebUrl url = async {
    let! stream = Stream.ofUrlAsync url
    use reader = stream |> TextReader.fromStream true DefaultBufferSize Encoding.Unicode
    return! ofCsvReader reader
  }

  let toBinaryWriter (data: IList<FxcmTickData>) (writer: BinaryWriter) =
    let dates = data |> mapAsArray (fun t -> t.dateTime)
    writer |> BinaryWriter.writeMonotonicDateTimes dates

    let bids = data |> mapAsArray (fun t -> t.bid)
    writer |> BinaryWriter.writeBrownianFloat32 bids

    let asks = data |> mapAsArray (fun t -> t.ask)
    writer |> BinaryWriter.writeBrownianFloat32 asks
  
  let toBinaryLocalFile data filePath =
    use writer = filePath |> BinaryWriter.fromFile CompressionLevel.Fastest DefaultBufferSize Encoding.UTF8
    toBinaryWriter data writer

  let ofBinaryReader reader =
    let dates = reader |> BinaryReader.readMonotonicDateTimes DateTimeKind.Utc |> Seq.toArray
    let bids = reader |> BinaryReader.readBrownianFloat32 |> Seq.toArray
    let asks = reader |> BinaryReader.readBrownianFloat32 |> Seq.toArray
    assert (bids.Length = dates.Length && asks.Length = dates.Length)
    let data = Array.zeroCreate dates.Length
    for i = 0 to dates.Length - 1 do
      data.[i] <- { dateTime = dates.[i]; bid = bids.[i]; ask = asks.[i] }
    data

  let ofLocalBinFile filePath =
    use reader = filePath |> BinaryReader.fromFile DefaultBufferSize Encoding.UTF8
    ofBinaryReader reader
    

module CandleData =
  let Instruments = [
    "AUDCAD"
    "AUDCHF"
    "AUDJPY"
    "AUDNZD"
    "CADCHF"
    "EURAUD"
    "EURCHF"
    "EURGBP"
    "EURJPY"
    "EURUSD"
    "GBPCHF"
    "GBPJPY"
    "GBPNZD"
    "GBPUSD"
    "NZDCAD"
    "NZDCHF"
    "NZDJPY"
    "NZDUSD"
    "USDCAD"
    "USDCHF"
    "USDJPY"
  ]
