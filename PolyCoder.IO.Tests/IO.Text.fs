
[<NUnit.Framework.TestFixture>]
module PolyCoder.IO.Preamble

open System
open NUnit.Framework
open FsCheck
open FsCheck.NUnit
open Swensen.Unquote
open System.Globalization
open System.Diagnostics

let invariantCulture = CultureInfo.InvariantCulture
  
[<Property>]
let ``Text.parseInt8 should parse any sbyte`` (original: sbyte) =
  let str = original.ToString(invariantCulture)
  let span = str.AsSpan()
  let actual = Text.parseInt8 span
  
  test <@ actual = original @>
  
[<Property>]
let ``Text.parseInt16 should parse any int16`` (original: int16) =
  let str = original.ToString(invariantCulture)
  let span = str.AsSpan()
  let actual = Text.parseInt16 span
  
  test <@ actual = original @>
  
[<Property>]
let ``Text.parseInt32 should parse any int32`` (original: int32) =
  let str = original.ToString(invariantCulture)
  let span = str.AsSpan()
  let actual = Text.parseInt32 span
  
  test <@ actual = original @>
  
[<Property>]
let ``Text.parseInt64 should parse any int64`` (original: int64) =
  let str = original.ToString(invariantCulture)
  let span = str.AsSpan()
  let actual = Text.parseInt64 span
  
  test <@ actual = original @>
  
[<Property>]
let ``Text.parseUInt8 should parse any byte`` (original: byte) =
  let str = original.ToString(invariantCulture)
  let span = str.AsSpan()
  let actual = Text.parseUInt8 span
  
  test <@ actual = original @>
  
[<Property>]
let ``Text.parseUInt16 should parse any uint16`` (original: uint16) =
  let str = original.ToString(invariantCulture)
  let span = str.AsSpan()
  let actual = Text.parseUInt16 span
  
  test <@ actual = original @>
  
[<Property>]
let ``Text.parseUInt32 should parse any uint32`` (original: uint32) =
  let str = original.ToString(invariantCulture)
  let span = str.AsSpan()
  let actual = Text.parseUInt32 span
  
  test <@ actual = original @>
  
[<Property>]
let ``Text.parseUInt64 should parse any uint64`` (original: uint64) =
  let str = original.ToString(invariantCulture)
  let span = str.AsSpan()
  let actual = Text.parseUInt64 span
  
  test <@ actual = original @>
  
[<Property>]
let ``Text.parseFloat32 should parse any float32`` (original: float32) =
  if original |> Single.IsFinite then
    let str = original.ToString(invariantCulture)
    let span = str.AsSpan()
    let actual = Text.parseFloat32 span
  
    test <@ actual = original @>
  
[<Property>]
let ``Text.parseFloat should parse any float`` (original: float) =
  if original |> Double.IsFinite then
    let str = original.ToString(invariantCulture)
    let span = str.AsSpan()
    let actual = Text.parseFloat span
  
    test <@ actual = original @>
  
[<Property>]
let ``Text.parseDecimal should parse any decimal`` (original: decimal) =
  let str = original.ToString(invariantCulture)
  let span = str.AsSpan()
  let actual = Text.parseDecimal span
  
  test <@ actual = original @>
  
[<Property>]
let ``Text.parseString should parse any string`` (original: NonNull<string>) =
  let span = original.Get.AsSpan()
  let actual = Text.parseString span
  
  test <@ actual = original.Get @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when missing year pattern, should throw an exception``() =
  let exn = Assert.Throws<InvalidOperationException>(fun () ->
    Text.makeFixedPositionDateParser DateTimeKind.Utc "MM-dd HH:mm:ss.fffffff" |> ignore)
  test <@ exn.Message = "Missing pattern in date time format: yyyy" @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a shorter year pattern, should throw an exception``() =
  let exn = Assert.Throws<InvalidOperationException>(fun () ->
    Text.makeFixedPositionDateParser DateTimeKind.Utc "yyy-MM-dd HH:mm:ss.fffffff" |> ignore)
  test <@ exn.Message = "Missing pattern in date time format: yyyy" @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given longer year pattern, should throw an exception``() =
  let exn = Assert.Throws<InvalidOperationException>(fun () ->
    Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyyy-MM-dd HH:mm:ss.fffffff" |> ignore)
  test <@ exn.Message = "Missing pattern in date time format: yyyy" @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when missing month pattern, should throw an exception``() =
  let exn = Assert.Throws<InvalidOperationException>(fun () ->
    Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-dd HH:mm:ss.fffffff" |> ignore)
  test <@ exn.Message = "Missing pattern in date time format: MM" @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a shorter month pattern, should throw an exception``() =
  let exn = Assert.Throws<InvalidOperationException>(fun () ->
    Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-M-dd HH:mm:ss.fffffff" |> ignore)
  test <@ exn.Message = "Missing pattern in date time format: MM" @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a longer month pattern, should throw an exception``() =
  let exn = Assert.Throws<InvalidOperationException>(fun () ->
    Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-MMM-dd HH:mm:ss.fffffff" |> ignore)
  test <@ exn.Message = "Missing pattern in date time format: MM" @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a pattern with only missing seconds, should throw an exception``() =
  let exn = Assert.Throws<InvalidOperationException>(fun () ->
    Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-MM-dd HH:mm.fffffff" |> ignore)
  test <@ exn.Message = "Missing pattern in date time format: ss" @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a pattern with only missing minutes, should throw an exception``() =
  let exn = Assert.Throws<InvalidOperationException>(fun () ->
    Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-MM-dd HH:ss.fffffff" |> ignore)
  test <@ exn.Message = "Missing pattern in date time format: mm" @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a pattern with only missing hours, should throw an exception``() =
  let exn = Assert.Throws<InvalidOperationException>(fun () ->
    Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-MM-dd mm:ss.fffffff" |> ignore)
  test <@ exn.Message = "Missing pattern in date time format: HH" @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a long valid year pattern, should return a valid parser``() =
  let parseDate = Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-MM-dd HH:mm:ss.fffffff"
  test <@ isNull parseDate |> not @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a valid all shuffled patterns, should return a valid parser``() =
  let parseDate = Text.makeFixedPositionDateParser DateTimeKind.Utc "ss-MM-yyyy?HH,fffffff+ddmm"
  test <@ isNull parseDate |> not @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a valid pattern without fractions, should return a valid parser``() =
  let parseDate = Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-MM-dd HH:mm:ss"
  test <@ isNull parseDate |> not @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a valid pattern without seconds and fractions, should return a valid parser``() =
  let parseDate = Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-MM-dd HH:mm"
  test <@ isNull parseDate |> not @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a valid pattern without minutes, seconds and fractions, should return a valid parser``() =
  let parseDate = Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-MM-dd HH"
  test <@ isNull parseDate |> not @>

[<Test>]
let ``Text.makeFixedPositionDateParser, when given a valid pattern without hours, minutes, seconds and fractions, should return a valid parser``() =
  let parseDate = Text.makeFixedPositionDateParser DateTimeKind.Utc "yyyy-MM-dd"
  test <@ isNull parseDate |> not @>

[<Property>]
let ``Text.makeFixedPositionDateParser, when parsing with a full pattern, should return the same DateTime``(original: DateTime) =
  let pattern = "yyyy-MM-dd HH:mm:ss.fffffff"
  let str = original.ToString(pattern, invariantCulture)
  let span = str.AsSpan()
  let parseDate = Text.makeFixedPositionDateParser original.Kind pattern
  let actual = parseDate.Invoke(span)
  
  test <@ actual = original @>

[<Property>]
let ``Text.makeFixedPositionDateParser, when parsing with a shorter fraction, should return the same DateTime``(original: DateTime) =
  let pattern = "yyyy-MM-dd HH:mm:ss.f"
  let str = original.ToString(pattern, invariantCulture)
  let span = str.AsSpan()
  let parseDate = Text.makeFixedPositionDateParser original.Kind pattern
  let actual = parseDate.Invoke(span)
  test <@ actual <= original @>
  let difference = original - actual
  test <@ difference <= TimeSpan.FromSeconds(0.1) @>

[<Property>]
let ``Text.makeFixedPositionDateParser, when parsing down to seconds, should return the same DateTime``(original: DateTime) =
  let pattern = "yyyy-MM-dd HH:mm:ss"
  let str = original.ToString(pattern, invariantCulture)
  let span = str.AsSpan()
  let parseDate = Text.makeFixedPositionDateParser original.Kind pattern
  let actual = parseDate.Invoke(span)
  test <@ actual <= original @>
  let difference = original - actual
  test <@ difference <= TimeSpan.FromSeconds(1.0) @>

[<Property>]
let ``Text.makeFixedPositionDateParser, when parsing down to minutes, should return the same DateTime``(original: DateTime) =
  let pattern = "yyyy-MM-dd HH:mm"
  let str = original.ToString(pattern, invariantCulture)
  let span = str.AsSpan()
  let parseDate = Text.makeFixedPositionDateParser original.Kind pattern
  let actual = parseDate.Invoke(span)
  test <@ actual <= original @>
  let difference = original - actual
  test <@ difference <= TimeSpan.FromMinutes(1.0) @>

[<Property>]
let ``Text.makeFixedPositionDateParser, when parsing down to hours, should return the same DateTime``(original: DateTime) =
  let pattern = "yyyy-MM-dd HH"
  let str = original.ToString(pattern, invariantCulture)
  let span = str.AsSpan()
  let parseDate = Text.makeFixedPositionDateParser original.Kind pattern
  let actual = parseDate.Invoke(span)
  test <@ actual <= original @>
  let difference = original - actual
  test <@ difference <= TimeSpan.FromHours(1.0) @>

[<Property>]
let ``Text.makeFixedPositionDateParser, when parsing down to days, should return the same DateTime``(original: DateTime) =
  let pattern = "yyyy-MM-dd"
  let str = original.ToString(pattern, invariantCulture)
  let span = str.AsSpan()
  let parseDate = Text.makeFixedPositionDateParser original.Kind pattern
  let actual = parseDate.Invoke(span)
  test <@ actual <= original @>
  let difference = original - actual
  test <@ difference <= TimeSpan.FromDays(1.0) @>

[<Property>]
let ``Text.makeFixedPositionDateParser, when given a full pattern, should be faster than ParseExact``(original: DateTime) =
  let pattern = "yyyy-MM-dd HH:mm:ss.fffffff"
  let str = original.ToString(pattern, invariantCulture)
  let parseDate = Text.makeFixedPositionDateParser original.Kind pattern
  let repetitions = 10000

  let parseExactStopwatch = Stopwatch.StartNew()
  for i = 1 to repetitions do
    DateTime.ParseExact(str, pattern, invariantCulture) |> ignore
  parseExactStopwatch.Stop()

  let parseDateStopwatch = Stopwatch.StartNew()
  for i = 1 to repetitions do
    let span = str.AsSpan()
    parseDate.Invoke(span) |> ignore
  parseDateStopwatch.Stop()

  test <@ parseExactStopwatch.Elapsed > parseDateStopwatch.Elapsed @>

[<Property>]
let ``Text.makeFixedPositionDateParser, when given a short pattern, should be faster than ParseExact``(original: DateTime) =
  let pattern = "yyyy-MM-dd HH"
  let str = original.ToString(pattern, invariantCulture)
  let parseDate = Text.makeFixedPositionDateParser original.Kind pattern
  let repetitions = 10000

  let parseExactStopwatch = Stopwatch.StartNew()
  for i = 1 to repetitions do
    DateTime.ParseExact(str, pattern, invariantCulture) |> ignore
  parseExactStopwatch.Stop()

  let parseDateStopwatch = Stopwatch.StartNew()
  for i = 1 to repetitions do
    let span = str.AsSpan()
    parseDate.Invoke(span) |> ignore
  parseDateStopwatch.Stop()

  test <@ parseExactStopwatch.Elapsed > parseDateStopwatch.Elapsed @>
