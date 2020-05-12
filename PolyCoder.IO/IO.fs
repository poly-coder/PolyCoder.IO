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

module Text =
  type CharSpanParser<'a> = delegate of span: ReadOnlySpan<char> -> 'a

  let inline parseInt8 (span: ReadOnlySpan<char>) =
    SByte.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture)
  let inline parseInt16 (span: ReadOnlySpan<char>) =
    Int16.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture)
  let inline parseInt32 (span: ReadOnlySpan<char>) =
    Int32.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture)
  let inline parseInt64 (span: ReadOnlySpan<char>) =
    Int64.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture)

  let inline parseUInt8 (span: ReadOnlySpan<char>) =
    Byte.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture)
  let inline parseUInt16 (span: ReadOnlySpan<char>) =
    UInt16.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture)
  let inline parseUInt32 (span: ReadOnlySpan<char>) =
    UInt32.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture)
  let inline parseUInt64 (span: ReadOnlySpan<char>) =
    UInt64.Parse(span, NumberStyles.Integer, CultureInfo.InvariantCulture)

  let inline parseFloat32 (span: ReadOnlySpan<char>) =
    Single.Parse(span, NumberStyles.Float, CultureInfo.InvariantCulture)
  let inline parseFloat (span: ReadOnlySpan<char>) =
    Double.Parse(span, NumberStyles.Float, CultureInfo.InvariantCulture)
  let inline parseDecimal (span: ReadOnlySpan<char>) =
    Decimal.Parse(span, NumberStyles.Float, CultureInfo.InvariantCulture)

  let inline parseString (span: ReadOnlySpan<char>) = span.ToString()

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
        let fraction = parseInt32 (span.Slice(fractionIndex, fractionLength))
        let second = parseInt32 (span.Slice(secondIndex, secondLength))
        let minute = parseInt32 (span.Slice(minuteIndex, minuteLength))
        let hour = parseInt32 (span.Slice(hourIndex, hourLength))
        let day = parseInt32 (span.Slice(dayIndex, dayLength))
        let month = parseInt32 (span.Slice(monthIndex, monthLength))
        let year = parseInt32 (span.Slice(yearIndex, yearLength))
        let dt = DateTime(year, month, day, hour, minute, second, kind)
        if fraction = 0 then dt else
        let ticks = dt.Ticks + (int64 fraction) * fractionMultiplier
        DateTime(ticks, kind))

    | None ->
      CharSpanParser<DateTime>(fun span ->
        let second =
          match secondPos with
          | Some (i, l) -> parseInt32 (span.Slice(i, l))
          | None -> 0
        let minute = 
          match minutePos with
          | Some (i, l) -> parseInt32 (span.Slice(i, l))
          | None -> 0
        let hour = 
          match hourPos with
          | Some (i, l) -> parseInt32 (span.Slice(i, l))
          | None -> 0
        let day = parseInt32 (span.Slice(dayIndex, dayLength))
        let month = parseInt32 (span.Slice(monthIndex, monthLength))
        let year = parseInt32 (span.Slice(yearIndex, yearLength))
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
