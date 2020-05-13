[<NUnit.Framework.TestFixture>]
module PolyCoder.IO.DataSets.Binary

open System
open NUnit.Framework
open FsCheck
open FsCheck.NUnit
open Swensen.Unquote
open System.Globalization
open PolyCoder.IO.DataSets
open System.IO
open System.Text

let invariantCulture = CultureInfo.InvariantCulture
  
[<Test>]
let ``DataSets.Binary.writeSeqData with empty data should be symetric with readSeqData`` () =
  let writeFloat (value: float) (writer: BinaryWriter) = writer.Write value
  let readFloat (reader: BinaryReader) = reader.ReadDouble()
  
  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqData 32u writeFloat Array.empty writer
    writer.Flush()

  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDataAsArray readFloat reader

  test <@ actualData = [||] @>
  test <@ memLength = 2L @>
  
[<Test>]
let ``DataSets.Binary.writeSeqData with single window should be symetric with readSeqData`` () =
  let writeFloat (value: float) (writer: BinaryWriter) = writer.Write value
  let readFloat (reader: BinaryReader) = reader.ReadDouble()

  let itemCount = 20
  let originalData = [| for i = 1 to itemCount do yield float i |]
  
  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqData 32u writeFloat originalData writer
    writer.Flush()

  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDataAsArray readFloat reader

  test <@ actualData = originalData @>
  test <@ memLength = 163L @>
  
[<Test>]
let ``DataSets.Binary.writeSeqData with many windows should be symetric with readSeqData`` () =
  let writeFloat (value: float) (writer: BinaryWriter) = writer.Write value
  let readFloat (reader: BinaryReader) = reader.ReadDouble()

  let itemCount = 120
  let originalData = [| for i = 1 to itemCount do yield float i |]
  
  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqData 32u writeFloat originalData writer
    writer.Flush()

  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDataAsArray readFloat reader

  test <@ actualData = originalData @>
  test <@ memLength = 966L @>



[<Test>]
let ``DataSets.Binary.writeSeqPositiveDiffUInt64Data with empty data should be symetric with readSeqPositiveDiffUInt64DataAsArray`` () =
  let originalData: uint64[] = [||]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqPositiveDiffUInt64Data 8ul originalData writer
    writer.Flush()

  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqPositiveDiffUInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 2L @>

[<Test>]
let ``DataSets.Binary.writeSeqPositiveDiffUInt64Data with single window of small diff data should be symetric with readSeqPositiveDiffUInt64DataAsArray`` () =
  let originalData = [|0UL;1UL;2UL;3UL;5UL;8UL|]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqPositiveDiffUInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqPositiveDiffUInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 17L @>

[<Test>]
let ``DataSets.Binary.writeSeqPositiveDiffUInt64Data with single window of small diff data and new large diff data should be symetric with readSeqPositiveDiffUInt64DataAsArray`` () =
  let originalData = [|0UL;1UL;2UL;610UL|]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqPositiveDiffUInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqPositiveDiffUInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 18L @>

[<Test>]
let ``DataSets.Binary.writeSeqPositiveDiffUInt64Data with many windows of small diff data should be symetric with readSeqPositiveDiffUInt64DataAsArray`` () =
  let originalData = [| for i = 1UL to 20UL do i |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqPositiveDiffUInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqPositiveDiffUInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 49L @>

[<Test>]
let ``DataSets.Binary.writeSeqPositiveDiffUInt64Data with many windows of small diff data and then large diff data should be symetric with readSeqPositiveDiffUInt64DataAsArray`` () =
  let originalData = [|
    for i = 1UL to 20UL do i
    for i = 1UL to 20UL do i * 256UL
  |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqPositiveDiffUInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqPositiveDiffUInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 108L @>

[<Test>]
let ``DataSets.Binary.writeSeqPositiveDiffUInt64Data with many windows and diff steps should be symetric with readSeqPositiveDiffUInt64DataAsArray`` () =
  let originalData = [|
    for i = 1UL to 20UL do i
    for i = 1UL to 20UL do i * 0x100UL
    for i = 1UL to 20UL do i * 0x10000UL
    for i = 1UL to 20UL do i * 0x1000000UL
    for i = 1UL to 20UL do 20UL * 0x1000000UL + i * 0x10000UL
    for i = 1UL to 20UL do 40UL * 0x1000000UL + i * 0x100UL
    for i = 1UL to 20UL do 60UL * 0x1000000UL + i
  |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqPositiveDiffUInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqPositiveDiffUInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 507L @>

[<Test>]
let ``DataSets.Binary.writeSeqPositiveDiffUInt64Data with many larger windows and diff steps should be symetric with readSeqPositiveDiffUInt64DataAsArray`` () =
  let originalData = [|
    for i = 1UL to 20UL do i
    for i = 1UL to 20UL do i * 0x100UL
    for i = 1UL to 20UL do i * 0x10000UL
    for i = 1UL to 20UL do i * 0x1000000UL
    for i = 1UL to 20UL do 20UL * 0x1000000UL + i * 0x10000UL
    for i = 1UL to 20UL do 40UL * 0x1000000UL + i * 0x100UL
    for i = 1UL to 20UL do 60UL * 0x1000000UL + i
  |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqPositiveDiffUInt64Data 64ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqPositiveDiffUInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 409L @>

[<Test>]
let ``DataSets.Binary.writeSeqPositiveDiffUInt64Data with many larger windows and short diff steps should be symetric with readSeqPositiveDiffUInt64DataAsArray`` () =
  let originalData = [|
    for i = 1UL to 10UL do i
    for i = 1UL to 10UL do i * 0x100UL
    for i = 1UL to 10UL do i * 0x10000UL
    for i = 1UL to 10UL do i * 0x1000000UL
    for i = 1UL to 10UL do 10UL * 0x1000000UL + i * 0x10000UL
    for i = 1UL to 10UL do 20UL * 0x1000000UL + i * 0x100UL
    for i = 1UL to 10UL do 30UL * 0x1000000UL + i
  |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqPositiveDiffUInt64Data 64ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqPositiveDiffUInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 254L @>



[<Test>]
let ``DataSets.Binary.writeSeqDiffInt64Data with empty data should be symetric with readSeqDiffInt64DataAsArray`` () =
  let originalData: int64[] = [||]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqDiffInt64Data 8ul originalData writer
    writer.Flush()

  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDiffInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 2L @>

[<Test>]
let ``DataSets.Binary.writeSeqDiffInt64Data with single window of small diff data should be symetric with readSeqDiffInt64DataAsArray`` () =
  let originalData = [|0L;1L;2L;3L;5L;8L|]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqDiffInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDiffInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 17L @>

[<Test>]
let ``DataSets.Binary.writeSeqDiffInt64Data with single window of small diff data and new large diff data should be symetric with readSeqDiffInt64DataAsArray`` () =
  let originalData = [|0L;1L;2L;610L|]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqDiffInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDiffInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 18L @>

[<Test>]
let ``DataSets.Binary.writeSeqDiffInt64Data with many windows of small diff data should be symetric with readSeqDiffInt64DataAsArray`` () =
  let originalData = [| for i = 1L to 20L do i |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqDiffInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDiffInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 49L @>

[<Test>]
let ``DataSets.Binary.writeSeqDiffInt64Data with many windows of small diff data and then large diff data should be symetric with readSeqDiffInt64DataAsArray`` () =
  let originalData = [|
    for i = 1L to 20L do i
    for i = 1L to 20L do i * 256L
  |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqDiffInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDiffInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 108L @>

[<Test>]
let ``DataSets.Binary.writeSeqDiffInt64Data with many windows and diff steps should be symetric with readSeqDiffInt64DataAsArray`` () =
  let originalData = [|
    for i = 1L to 20L do i
    for i = 1L to 20L do i * 0x100L
    for i = 1L to 20L do i * 0x10000L
    for i = 1L to 20L do i * 0x1000000L
    for i = 1L to 20L do 20L * 0x1000000L + i * 0x10000L
    for i = 1L to 20L do 40L * 0x1000000L + i * 0x100L
    for i = 1L to 20L do 60L * 0x1000000L + i
  |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqDiffInt64Data 8ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDiffInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 507L @>

[<Test>]
let ``DataSets.Binary.writeSeqDiffInt64Data with many larger windows and diff steps should be symetric with readSeqDiffInt64DataAsArray`` () =
  let originalData = [|
    for i = 1L to 20L do i
    for i = 1L to 20L do i * 0x100L
    for i = 1L to 20L do i * 0x10000L
    for i = 1L to 20L do i * 0x1000000L
    for i = 1L to 20L do 20L * 0x1000000L + i * 0x10000L
    for i = 1L to 20L do 40L * 0x1000000L + i * 0x100L
    for i = 1L to 20L do 60L * 0x1000000L + i
  |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqDiffInt64Data 64ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDiffInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 410L @>

[<Test>]
let ``DataSets.Binary.writeSeqDiffInt64Data with many larger windows and short diff steps should be symetric with readSeqDiffInt64DataAsArray`` () =
  let originalData = [|
    for i = 1L to 10L do i
    for i = 1L to 10L do i * 0x100L
    for i = 1L to 10L do i * 0x10000L
    for i = 1L to 10L do i * 0x1000000L
    for i = 1L to 10L do 10L * 0x1000000L + i * 0x10000L
    for i = 1L to 10L do 20L * 0x1000000L + i * 0x100L
    for i = 1L to 10L do 30L * 0x1000000L + i
  |]

  use mem = new MemoryStream()
  do
    use writer = new BinaryWriter(mem, Encoding.UTF8, true)
    Binary.Internals.writeSeqDiffInt64Data 64ul originalData writer
    writer.Flush()
  
  let memLength = mem.Length
  mem.Seek(0L, SeekOrigin.Begin) |> ignore

  let actualData =
    use reader = new BinaryReader(mem)
    Binary.Internals.readSeqDiffInt64DataAsArray reader

  test <@ actualData = originalData @>
  test <@ memLength = 256L @>
