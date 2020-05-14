[<AutoOpen>]
module PolyCoder.IO.Preamble

open System
open System.IO
open System.IO.Compression
open System.Net
open PolyCoder
open System.Globalization
open System.Text.RegularExpressions
open System.Text

type CharSpanParser<'a> = delegate of span: ReadOnlySpan<char> -> 'a

module Text =

  let parseInt8 = CharSpanParser(fun span -> SByte.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture))
  let parseInt16 = CharSpanParser(fun span -> Int16.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture))
  let parseInt32 = CharSpanParser(fun span -> Int32.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture))
  let parseInt64 = CharSpanParser(fun span -> Int64.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture))

  let parseUInt8 = CharSpanParser(fun span -> Byte.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture))
  let parseUInt16 = CharSpanParser(fun span -> UInt16.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture))
  let parseUInt32 = CharSpanParser(fun span -> UInt32.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture))
  let parseUInt64 = CharSpanParser(fun span -> UInt64.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture))

  let parseFloat32 = CharSpanParser(fun span -> Single.Parse(span, NumberStyles.Float, CultureInfo.InvariantCulture))
  let parseFloat = CharSpanParser(fun span -> Double.Parse(span, NumberStyles.Float, CultureInfo.InvariantCulture))
  let parseDecimal = CharSpanParser(fun span -> Decimal.Parse(span, NumberStyles.Float, CultureInfo.InvariantCulture))

  let parseString = CharSpanParser(fun span -> span.ToString())

  let internal parseInt (span: ReadOnlySpan<char>) = Int32.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture)

  let makeFixedPositionDateParser (kind: DateTimeKind) format: CharSpanParser<DateTime> =
    let missingPattern name = invalidOp (sprintf "Missing pattern in date time format: %s" name)
    let tryFindPattern pattern =
      let m = Regex.Match(format, pattern)
      if not m.Success 
      then None
      else
        let group = m.Groups.Item("pattern")
        Some (group.Index, group.Length)
    let findPattern name =
      tryFindPattern >> Option.defaultWith (fun () -> missingPattern name)

    let yearIndex, yearLength = findPattern "yyyy" "(?:^|[^y])(?<pattern>yyyy)(?!y)"
    let monthIndex, monthLength = findPattern "MM" "(?:^|[^M])(?<pattern>MM)(?!M)"
    let dayIndex, dayLength = findPattern "dd" "(?:^|[^d])(?<pattern>dd)(?!d)"
    let hourPos = tryFindPattern "(?:^|[^H])(?<pattern>HH)(?!H)"
    let minutePos = tryFindPattern "(?:^|[^m])(?<pattern>mm)(?!m)"
    let secondPos = tryFindPattern "(?:^|[^s])(?<pattern>ss)(?!s)"
    let fractionPos = tryFindPattern "(?:^|[^f])(?<pattern>f{1,7})(?!f)"

    match fractionPos with
    | Some (fractionIndex, fractionLength) ->
      let parseOrFail name = Option.defaultWith (fun () -> missingPattern name)
      let secondIndex, secondLength = secondPos |> parseOrFail "ss"
      let minuteIndex, minuteLength = minutePos |> parseOrFail "mm"
      let hourIndex, hourLength = hourPos |> parseOrFail "HH"
      let fractionMultiplier = pown 10L (7 - fractionLength)
      CharSpanParser<DateTime>(fun span ->
        let fraction = parseInt (span.Slice(fractionIndex, fractionLength))
        let second = parseInt (span.Slice(secondIndex, secondLength))
        let minute = parseInt (span.Slice(minuteIndex, minuteLength))
        let hour = parseInt (span.Slice(hourIndex, hourLength))
        let day = parseInt (span.Slice(dayIndex, dayLength))
        let month = parseInt (span.Slice(monthIndex, monthLength))
        let year = parseInt (span.Slice(yearIndex, yearLength))
        let dt = DateTime(year, month, day, hour, minute, second, kind)
        if fraction = 0 then dt else
        let ticks = dt.Ticks + (int64 fraction) * fractionMultiplier
        DateTime(ticks, kind))

    | None ->
      CharSpanParser<DateTime>(fun span ->
        let second =
          match secondPos with
          | Some (i, l) -> parseInt (span.Slice(i, l))
          | None -> 0
        let minute = 
          match minutePos with
          | Some (i, l) -> parseInt (span.Slice(i, l))
          | None -> 0
        let hour = 
          match hourPos with
          | Some (i, l) -> parseInt (span.Slice(i, l))
          | None -> 0
        let day = parseInt (span.Slice(dayIndex, dayLength))
        let month = parseInt (span.Slice(monthIndex, monthLength))
        let year = parseInt (span.Slice(yearIndex, yearLength))
        DateTime(year, month, day, hour, minute, second, kind))

module File =
  let (|IsWebException|_|) = Exn.findInner<WebException>

  let (|IsHttpWebException|_|) =
    Exn.findInner<WebException>
    >> Option.bind (fun ex ->
      match ex.Response with
      | :? HttpWebResponse as response -> Some response
      | _ -> None)

  let fileExists (fileName: string) = async {
    let info = FileInfo(fileName)
    return info.Exists
  }

  let extension (filePath: string) = Path.GetExtension filePath

  let isGZipped filePath = extension filePath = ".gz"

  let downloadToTemp (sourceUrl: string) = async {
    use client = new WebClient()
    let tempFileName = Path.GetTempFileName()
    try
      do! client.DownloadFileTaskAsync(sourceUrl, tempFileName) |> Async.AwaitTask
      return Some tempFileName
    with
    | IsHttpWebException response when response.StatusCode = HttpStatusCode.NotFound ->
      try File.Delete(tempFileName) with _ -> ()
      return None
    | exn -> return Exn.reraise exn
  }

  let downloadToFile createDirectory (sourceUrl: string) (targetFile: string) = async {
    match! fileExists targetFile with
    | false ->
      match! downloadToTemp sourceUrl with
      | Some tempFileName ->
        let targetDirectory = Path.GetDirectoryName(targetFile)
        createDirectory targetDirectory
        File.Move(tempFileName, targetFile)
        return true
      | None ->
        return false
    | _ ->
      return true
  }

module Stream =
  let buffered (stream: Stream) = new BufferedStream(stream) :> Stream

  let bufferedWith bufferSize (stream: Stream) = new BufferedStream(stream, bufferSize) :> Stream

  let gzipCompressedWith (level: CompressionLevel) (stream: Stream) =
    new GZipStream(stream, level, false) :> Stream

  let gzipCompressed stream = gzipCompressedWith CompressionLevel.Fastest stream

  let gzipDecompressed stream = new GZipStream(stream, CompressionMode.Decompress, false) :> Stream

  let readFileWith bufferize filePath =
    new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferize) :> Stream

  let readFile filePath =
    new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read) :> Stream

  let createFileWith bufferSize filePath =
    new FileStream(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, bufferSize) :> Stream

  let createFile filePath =
    new FileStream(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read) :> Stream

  let createNewFileWith bufferSize filePath =
    new FileStream(filePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, bufferSize) :> Stream

  let createNewFile filePath =
    new FileStream(filePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read) :> Stream

  let ofUrlAsync (url: string) = async {
    let web = new WebClient()
    let! stream = web.OpenReadTaskAsync(url) |> Async.AwaitTask
    web.OpenReadCompleted.Subscribe(fun _event -> web.Dispose()) |> ignore
    return stream
  }

  let ofUrl (url: string) =
    let web = new WebClient()
    let stream = web.OpenRead(url)
    web.OpenReadCompleted.Subscribe(fun _event -> web.Dispose()) |> ignore
    stream
    

module TextReader =
  let ofStreamWith encoding bufferSize (stream: Stream) =
    new StreamReader(stream, encoding, false, bufferSize, false)
  let ofStream encoding stream = ofStreamWith encoding 4096 stream

  let ofUtf8StreamWith bufferSize = ofStreamWith Encoding.UTF8 bufferSize
  let ofUtf8Stream stream = ofUtf8StreamWith 4096 stream

  let fromStream isGZipped bufferSize encoding stream =
    stream
      |> (if isGZipped then Stream.gzipDecompressed else id)
      |> Stream.bufferedWith bufferSize
      |> ofStreamWith encoding bufferSize
      :> TextReader

  let fromFile bufferSize encoding filePath =
    filePath
      |> Stream.readFileWith bufferSize
      |> fromStream (File.isGZipped filePath) bufferSize encoding

module TextWriter =
  let ofStreamWith encoding bufferSize (stream: Stream) =
    new StreamWriter(stream, encoding, bufferSize)
  let ofStream encoding stream = ofStreamWith encoding 4096 stream

  let ofUtf8StreamWith bufferSize = ofStreamWith Encoding.UTF8 bufferSize
  let ofUtf8Stream stream = ofUtf8StreamWith 4096 stream

  let fromStream compressionLevel bufferSize encoding stream =
    stream
      |> match compressionLevel with
          | CompressionLevel.NoCompression -> id
          | level -> Stream.gzipCompressedWith level
      |> Stream.bufferedWith bufferSize
      |> ofStreamWith encoding bufferSize
      :> TextWriter

  let fromFile compressionLevel bufferSize encoding filePath =
    filePath
      |> Stream.createFileWith bufferSize
      |> fromStream compressionLevel bufferSize encoding
  
module Binary =
  let unsignedSize value =
    if value <= uint64 Byte.MaxValue then 0uy
    elif value <= uint64 UInt16.MaxValue then 1uy
    elif value <= uint64 UInt32.MaxValue then 2uy
    else 3uy

  let signedSize value =
    if abs (value) <= int64 SByte.MaxValue then 0uy
    elif abs (value) <= int64 Int16.MaxValue then 1uy
    elif abs (value) <= int64 Int32.MaxValue then 2uy
    else 3uy

module BinaryReader =
  let ofStream encoding (stream: Stream) =
    new BinaryReader(stream, encoding, false)

  let ofUtf8Stream stream = ofStream Encoding.UTF8 stream

  let fromStream isGZipped bufferSize encoding stream =
    stream
      |> (if isGZipped then Stream.gzipDecompressed else id)
      |> Stream.bufferedWith bufferSize
      |> ofStream encoding

  let fromFile bufferSize encoding filePath =
    filePath
      |> Stream.readFileWith bufferSize
      |> fromStream (File.isGZipped filePath) bufferSize encoding

  let readUnsigned (size: byte) =
    if size = 0uy then fun (reader: BinaryReader) -> reader.ReadByte() |> uint64
    elif size = 1uy then fun (reader: BinaryReader) -> reader.ReadUInt16() |> uint64
    elif size = 2uy then fun (reader: BinaryReader) -> reader.ReadUInt32() |> uint64
    else fun (reader: BinaryReader) -> reader.ReadUInt64()

  let readSigned (size: byte) =
    if size = 0uy then fun (reader: BinaryReader) -> reader.ReadSByte() |> int64
    elif size = 1uy then fun (reader: BinaryReader) -> reader.ReadInt16() |> int64
    elif size = 2uy then fun (reader: BinaryReader) -> reader.ReadInt32() |> int64
    else fun (reader: BinaryReader) -> reader.ReadInt64()

module BinaryWriter =
  let ofStream encoding (stream: Stream) =
    new BinaryWriter(stream, encoding, false)

  let ofUtf8Stream stream = ofStream Encoding.UTF8 stream

  let fromStream compressionLevel bufferSize encoding stream =
    stream
      |> match compressionLevel with
          | CompressionLevel.NoCompression -> id
          | level -> Stream.gzipCompressedWith level
      |> Stream.bufferedWith bufferSize
      |> ofStream encoding

  let fromFile compressionLevel bufferSize encoding filePath =
    filePath
      |> Stream.createFileWith bufferSize
      |> fromStream compressionLevel bufferSize encoding

  let writeUnsigned (size: byte) =
    if size = 0uy then fun (value: uint64) (writer: BinaryWriter) -> writer.Write(byte value)
    elif size = 1uy then fun (value: uint64) (writer: BinaryWriter) -> writer.Write(uint16 value)
    elif size = 2uy then fun (value: uint64) (writer: BinaryWriter) -> writer.Write(uint32 value)
    else fun (value: uint64) (writer: BinaryWriter) -> writer.Write(value)

  let writeSigned (size: byte) =
    if size = 0uy then fun (value: int64) (writer: BinaryWriter) -> writer.Write(int8 value)
    elif size = 1uy then fun (value: int64) (writer: BinaryWriter) -> writer.Write(int16 value)
    elif size = 2uy then fun (value: int64) (writer: BinaryWriter) -> writer.Write(int32 value)
    else fun (value: int64) (writer: BinaryWriter) -> writer.Write(value)
