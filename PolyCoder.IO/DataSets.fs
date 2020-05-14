module PolyCoder.IO.DataSets

open PolyCoder
open System
open System.IO
open System.Collections.Generic

let inline fraction value = value - floor value

let maximumCommonDenominatorInt64 a b =
  let mutable x = abs (max a b)
  let mutable y = abs (min a b)
  while y <> 0L do
    let rem = x % y
    x <- y
    y <- rem
  if x <> 0L then x
  else invalidOp "Cannot find MCD out of zero"

let maximunCommonDenominatorInt64All (source: IList<int64>) =
  let lastIndex = source.Count - 1
  let mutable mcd = 0L
  for i = 0 to lastIndex do
    let item = source.[i]
    mcd <-
      if item = 0L then mcd
      elif mcd = 0L then item
      else maximumCommonDenominatorInt64 item mcd
  if mcd <> 0L then mcd
  else invalidOp "Cannot find MCD out of zero"

let maximunCommonDenominatorInt64Seq (source: seq<int64>) =
  let mutable mcd = 0L

  source |> Seq.iter (fun item ->
    mcd <-
      if item = 0L then mcd
      elif mcd = 0L then item
      else maximumCommonDenominatorInt64 item mcd)

  if mcd <> 0L then mcd
  else invalidOp "Cannot find MCD out of zero"

let maximumCommonDenominatorUInt64 a b =
  let mutable x = max a b
  let mutable y = a + b - x
  while y <> 0UL do
    let rem = x % y
    x <- y
    y <- rem
  if x <> 0UL then x
  else invalidOp "Cannot find MCD out of zero"

let maximunCommonDenominatorUInt64All (source: IList<uint64>) =
  let lastIndex = source.Count - 1
  let mutable mcd = 0UL
  for i = 0 to lastIndex do
    let item = source.[i]
    mcd <-
      if item = 0UL then mcd
      elif mcd = 0UL then item
      else maximumCommonDenominatorUInt64 item mcd
  if mcd <> 0UL then mcd
  else invalidOp "Cannot find MCD out of zero"

let maximunCommonDenominatorUInt64Seq (source: seq<uint64>) =
  let mutable mcd = 0UL

  source |> Seq.iter (fun item ->
    mcd <-
      if item = 0UL then mcd
      elif mcd = 0UL then item
      else maximumCommonDenominatorUInt64 item mcd)

  if mcd <> 0UL then mcd
  else invalidOp "Cannot find MCD out of zero"
  
let mapAsArray fn (data: IList<'a>) =
  let values = Array.zeroCreate data.Count
  for i = 0 to data.Count - 1 do values.[i] <- fn data.[i]
  values

let findExponentMultiplierFloat32 (b: float32) (maxExponent: int) (data: IList<float32>) =
  let mutable exponent = 0
  let mutable multiplier = 1.0f
  let mutable index = 0
  while index < data.Count && exponent <= maxExponent do
    let itemAbs = abs(data.[index])
    while exponent <= maxExponent && fraction(itemAbs * multiplier) <> 0.0f do
      exponent <- exponent + 1
      multiplier <- pown b exponent
    index <- index + 1
  if exponent <= maxExponent then Some multiplier else None

module Binary =
  module Sequential =
    // TODO: Optimize to reduce allocations
    let write (windowSize: uint32) writeItem data (writer: BinaryWriter) =
      let windowSizeSize = Binary.unsignedSize (uint64 windowSize)
      // 1 - WRITE Number of bytes needed for windows sizes
      writer.Write windowSizeSize
      let windowSizeWriter = BinaryWriter.writeUnsigned windowSizeSize
      data
        |> Seq.windowedAll (int32 windowSize)
        |> Seq.iter (fun window ->
          // 2 - WRITE the window size to start writing items
          windowSizeWriter (uint64 window.Length) writer
          for i = 0 to window.Length - 1 do
            // 2.1 - WRITE an item
            writeItem window.[i] writer
        )
      // 3 - WRITE 0 as windows size to indicate end of data
      windowSizeWriter 0UL writer

    // TODO: Optimize to reduce allocations
    let read readItem (reader: BinaryReader) =
      // 1 - READ Number of bytes needed for windows sizes
      let windowSizeSize = reader.ReadByte()
      let windowSizeReader = BinaryReader.readUnsigned windowSizeSize
      let rec windowLoop() = seq {
        // 2 - READ the window size to start reading items
        let windowSize = windowSizeReader reader
        // 3 - READING 0 as windows size indicate end of data
        if windowSize <> 0UL then
          for i = 1UL to windowSize do
            // 2.1 - READ an item
            yield readItem reader
          yield! windowLoop ()
      }
      windowLoop()

    let readAsArray readItem = read readItem >> Seq.toArray


    // TODO: Optimize to reduce allocations
    let writeDifferences
        (getDiffSize: 'diff -> byte)
        (createDiffWriter: byte -> 'diff -> BinaryWriter -> unit)
        (maxWindowSize: uint32)
        (substract: 'item -> ' item -> 'diff)
        (writeItem: 'item -> BinaryWriter -> unit)
        (data: seq<'item>)
        (writer: BinaryWriter) =
      let MAX_SMALLER_ITEMS_TO_INCLUDE = 16
      let maxWindowSizeSize = Binary.unsignedSize(uint64 maxWindowSize)
      let windowSizeWriter = BinaryWriter.writeUnsigned maxWindowSizeSize
      // 1 - WRITE Number of bytes needed for windows sizes
      writer.Write(maxWindowSizeSize)

      let window: 'item[] = Array.zeroCreate (int maxWindowSize)

      // 2 - WRITE a segment of data
      let writeCompressedSegment startIndex itemCount diffSize =
        // 2.1 - WRITE window size
        writer |> windowSizeWriter (uint64 itemCount)
        // 2.2 - WRITE first item in the window
        writer |> writeItem window.[startIndex]
        // 2.3 - WRITE diffSize
        if itemCount > 1 then
          let diffWriter = createDiffWriter diffSize
          writer.Write(diffSize)
          // 2.4 - WRITE all differences
          for i = 1 to itemCount - 1 do
            let diff = substract window.[startIndex + i] window.[startIndex + i - 1]
            writer |> diffWriter diff

      let startIndex = ref 0
      let endIndex = ref 0
      let lastCurrentDiffPos = ref -1
      let currentDiffSize = ref 0uy

      let rearrangeIfNeeded() =
        if !endIndex >= window.Length then
          if !startIndex = 0 then
            // There is no more capacity in current segment
            writeCompressedSegment 0 window.Length !currentDiffSize
            startIndex := 0
            endIndex := 0
            lastCurrentDiffPos := -1
          else
            let count = !endIndex - !startIndex
            Array.blit window !startIndex window 0 count
            endIndex := !endIndex - !startIndex
            lastCurrentDiffPos := !lastCurrentDiffPos - !startIndex
            startIndex := 0

      let addItem item =
        assert (!endIndex < window.Length)
        window.[!endIndex] <- item // store new index in current index

        match !endIndex + 1 - !startIndex with
        | 1 -> () // This is just the first item in current segment, so no diff computing is required or possible

        | itemCount ->
          let diffSize =
            substract window.[!endIndex] window.[!endIndex - 1]
              |> getDiffSize
          
          if itemCount = 2 then
            // This is the second item in the segment, initialize values
            currentDiffSize := diffSize
            lastCurrentDiffPos := !endIndex

          else
            if diffSize > !currentDiffSize then
              // In this case we've found a larger diff than previously
              if itemCount < MAX_SMALLER_ITEMS_TO_INCLUDE then
                // If there are too few smaller items, consider them to be included in current segment, with larger differences
                currentDiffSize := diffSize
                lastCurrentDiffPos := !endIndex
              else
                // Write previous segment and start the new one from endIndex on
                writeCompressedSegment !startIndex (itemCount - 1) !currentDiffSize
                startIndex := !endIndex
                currentDiffSize := 0uy

            elif diffSize < !currentDiffSize then
              if !endIndex - !lastCurrentDiffPos > MAX_SMALLER_ITEMS_TO_INCLUDE then
                // We have found too many smaller diff items, lets cut out looses here, 
                // write the larger segment and start considering the smaller sizes
                writeCompressedSegment !startIndex (!lastCurrentDiffPos - !startIndex + 1) !currentDiffSize
                // Now recompute the new segment values
                startIndex := !lastCurrentDiffPos + 1
                currentDiffSize :=
                  seq { !startIndex + 1 .. !endIndex }
                  |> Seq.fold (fun m i -> max m (substract window.[i] window.[i-1] |> getDiffSize)) 0uy
                lastCurrentDiffPos := !endIndex

              else
                // Some smaller diff items can be included, as long as there are not too many of them
                ()

            else
              // The diffSize is kept so we just have to keep adding to current segment
              lastCurrentDiffPos := !endIndex
        
        endIndex := !endIndex + 1

        rearrangeIfNeeded()

      data |> Seq.iter addItem

      if !startIndex < !endIndex then
        writeCompressedSegment !startIndex (!endIndex - !startIndex) !currentDiffSize

      // 3 - WRITE 0 to indicate that no more segments are to be found
      writer |> windowSizeWriter 0UL

    // TODO: Optimize to reduce allocations
    let readDifferences
        (createDiffReader: byte -> BinaryReader -> 'diff)
        (add: 'item -> 'diff -> 'item)
        (readItem: BinaryReader -> 'item)
        (reader: BinaryReader) =
      // 1 - READ Number of bytes needed for windows sizes
      let maxWindowSizeSize = reader.ReadByte()
      let windowSizeReader = BinaryReader.readUnsigned maxWindowSizeSize

      // 2 - READ a segment of data
      let rec loop() = seq {
        // 2.1 - READ segment size
        let itemCount = windowSizeReader reader

        if itemCount > 0UL then
          // 2.2 - READ first item in the window
          let item = ref (readItem reader)
          yield !item
          if itemCount > 1UL then
            // 2.3 - READ diffSize
            let diffSize = reader.ReadByte()
            let diffReader = createDiffReader diffSize
            // 2.4 - READ all differences and yield the accumulated values
            for i = 1UL to itemCount - 1UL do
              let diff = diffReader reader
              item := add !item diff
              yield !item

          yield! loop()
      }
        
      loop()

    let readDifferencesAsArray createDiffReader add readItem =
      readDifferences createDiffReader add readItem >> Seq.toArray

    
    let writePositiveDifferences maxWindowSize substract writeItem data =
      writeDifferences Binary.unsignedSize BinaryWriter.writeUnsigned maxWindowSize substract writeItem data

    let readPositiveDifferences add readItem =
      readDifferences BinaryReader.readUnsigned add readItem

    let readPositiveDifferencesAsArray add readItem =
      readPositiveDifferences add readItem >> Seq.toArray


    let writePositiveDifferencesUInt64 (maxWindowSize: uint32) =
      let substract (a: uint64) (b: uint64) =
        assert (a >= b)
        a - b
      writePositiveDifferences maxWindowSize substract (fun item writer -> writer.Write(item))

    let readPositiveDifferencesUInt64 reader =
      let add a b = a + b
      readPositiveDifferences add (fun reader -> reader.ReadUInt64()) reader
    
    let readPositiveDifferencesUInt64AsArray reader =
      readPositiveDifferencesUInt64 reader |> Seq.toArray

    
    let writeSignedDifferences maxWindowSize substract writeItem data =
      writeDifferences Binary.signedSize BinaryWriter.writeSigned maxWindowSize substract writeItem data

    let readSignedDifferences add readItem =
      readDifferences BinaryReader.readSigned add readItem

    let readSignedDifferencesAsArray add readItem =
      readSignedDifferences add readItem >> Seq.toArray


    let writeSignedDifferencesInt64 (maxWindowSize: uint32) =
      let substract (a: int64) (b: int64) =
        assert (a >= b)
        a - b
      writeSignedDifferences maxWindowSize substract (fun item writer -> writer.Write(item))

    let readSignedDifferencesInt64 reader =
      let add a b = a + b
      readSignedDifferences add (fun reader -> reader.ReadInt64()) reader
    
    let readSignedDifferencesInt64AsArray reader =
      readSignedDifferencesInt64 reader |> Seq.toArray

  module Arrays =
    let write
        (writeItem: 'item -> BinaryWriter -> unit)
        (data: IList<'item>)
        (writer: BinaryWriter) =
      let dataCount = data.Count
      writer.Write(dataCount)
      for i = 0 to dataCount - 1 do
        writer |> writeItem data.[i]

    let read
        (readItem: BinaryReader -> 'item)
        (reader: BinaryReader)=
      let dataCount = reader.ReadInt32()
      let data = Array.zeroCreate dataCount
      for i = 0 to dataCount - 1 do
        data.[i] <- readItem reader
      data

    // TODO: Optimize to reduce allocations
    let writeDifferences
        (getDiffSize: 'diff -> byte)
        (createDiffWriter: byte -> 'diff -> BinaryWriter -> unit)
        (substract: 'item -> ' item -> 'diff)
        (writeItem: 'item -> BinaryWriter -> unit)
        (data: IList<'item>)
        (writer: BinaryWriter) =

      let MAX_SMALLER_ITEMS_TO_INCLUDE = 16
      // 1 - WRITE Number of items in total
      let dataSize = data.Count
      writer.Write(dataSize)
      let windowSizeWriter = uint64 dataSize |> Binary.unsignedSize |> BinaryWriter.writeUnsigned

      // 2 - WRITE a segment of data
      let writeCompressedSegment startIndex itemCount diffSize =
        // 2.1 - WRITE segment size
        writer |> windowSizeWriter (uint64 itemCount)
        // 2.2 - WRITE first item in the segment
        writer |> writeItem data.[startIndex]
        if itemCount > 1 then
          let diffWriter = createDiffWriter diffSize
          // 2.3 - WRITE diffSize
          writer.Write(diffSize)
          // 2.4 - WRITE all differences
          for i = 1 to itemCount - 1 do
            let diff = substract data.[startIndex + i] data.[startIndex + i - 1]
            writer |> diffWriter diff

      let startIndex = ref 0
      let lastCurrentDiffPos = ref -1
      let currentDiffSize = ref 0uy

      for endIndex = 0 to dataSize - 1 do
        let item = data.[endIndex]
        let itemCount = endIndex + 1 - !startIndex

        if itemCount > 1 then
          let diffSize = substract item data.[endIndex - 1] |> getDiffSize
          
          if itemCount = 2 then
            // This is the second item in the segment, initialize values
            currentDiffSize := diffSize
            lastCurrentDiffPos := endIndex

          else
            if diffSize > !currentDiffSize then
              // In this case we've found a larger diff than previously
              if itemCount < MAX_SMALLER_ITEMS_TO_INCLUDE then
                // If there are too few smaller items, consider them to be included in current segment, with larger differences
                currentDiffSize := diffSize
                lastCurrentDiffPos := endIndex
              else
                // Write previous segment and start the new one from endIndex on
                writeCompressedSegment !startIndex (itemCount - 1) !currentDiffSize
                startIndex := endIndex
                currentDiffSize := 0uy

            elif diffSize < !currentDiffSize then
              if endIndex - !lastCurrentDiffPos > MAX_SMALLER_ITEMS_TO_INCLUDE then
                // We have found too many smaller diff items, lets cut out looses here, 
                // write the larger segment and start considering the smaller sizes
                writeCompressedSegment !startIndex (!lastCurrentDiffPos - !startIndex + 1) !currentDiffSize
                // Now recompute the new segment values
                startIndex := !lastCurrentDiffPos + 1
                currentDiffSize := (
                  let mutable maxDiff = 0uy
                  for i = !startIndex + 1 to endIndex do
                    let diff = substract data.[i] data.[i-1] |> getDiffSize
                    maxDiff <- max maxDiff diff
                  maxDiff)
                lastCurrentDiffPos := endIndex

              else
                // Some smaller diff items can be included, as long as there are not too many of them
                ()

            else
              // The diffSize is kept so we just have to keep adding to current segment
              lastCurrentDiffPos := endIndex

      if !startIndex < dataSize then
        writeCompressedSegment !startIndex (dataSize - !startIndex) !currentDiffSize


    // TODO: Optimize to reduce allocations
    let readDifferences
        (createDiffReader: byte -> BinaryReader -> 'diff)
        (add: 'item -> 'diff -> 'item)
        (readItem: BinaryReader -> 'item)
        (reader: BinaryReader) =
      // 1 - READ Number of items in total
      let dataSize = reader.ReadInt32()
      let windowSizeReader = uint64 dataSize |> Binary.unsignedSize |> BinaryReader.readUnsigned

      let data = Array.zeroCreate dataSize

      let mutable accumSize = 0

      while accumSize < dataSize do
        // 2.1 - READ segment size
        let itemCount = int (windowSizeReader reader)
        // 2.2 - READ first item in the segment
        let mutable item = readItem reader
        data.[accumSize] <- item
        
        if itemCount > 1 then
          // 2.3 - READ diffSize
          let diffSize = reader.ReadByte()
          let diffReader = createDiffReader diffSize
          // 2.4 - READ all differences
          for i = 1 to itemCount - 1 do
            let diff = diffReader reader
            item <- add item diff
            data.[accumSize + i] <- item

        accumSize <- accumSize + itemCount

      data

    
    let writePositiveDifferences substract writeItem data =
      writeDifferences Binary.unsignedSize BinaryWriter.writeUnsigned substract writeItem data

    let readPositiveDifferences add readItem =
      readDifferences BinaryReader.readUnsigned add readItem


    let writePositiveDifferencesUInt64 data =
      let substract (a: uint64) (b: uint64) =
        assert (a >= b)
        a - b
      writePositiveDifferences substract (fun item writer -> writer.Write(item)) data

    let readPositiveDifferencesUInt64 reader =
      let add a b = a + b
      readPositiveDifferences add (fun reader -> reader.ReadUInt64()) reader

    
    let writeSignedDifferences substract writeItem data =
      writeDifferences Binary.signedSize BinaryWriter.writeSigned substract writeItem data

    let readSignedDifferences add readItem =
      readDifferences BinaryReader.readSigned add readItem


    let writeSignedDifferencesInt64 data =
      let substract (a: int64) (b: int64) = a - b
      writeSignedDifferences substract (fun item writer -> writer.Write(item)) data

    let readSignedDifferencesInt64 reader =
      let add a b = a + b
      readSignedDifferences add (fun reader -> reader.ReadInt64()) reader
     

module BinaryWriter =
  let writeMonotonicUInt64 data (writer: BinaryWriter) =
    match Option.catch maximunCommonDenominatorUInt64All data with
    | None ->
      // 1 - WRITE 0 to indicate that all values are 0
      writer.Write(0uy)
      // 2 - WRITE The total amount of zeros
      writer.Write(data.Count)

    | Some multiplier ->
      let values =
        if multiplier = 1UL then data
        else data |> mapAsArray (fun v -> v / multiplier) :> IList<_>
      // 1 - WRITE 1 to indicate that data follows
      writer.Write(1uy)
      // 2 - WRITE multiplier
      writer.Write(multiplier)
      // 3 - WRITE data using differences
      writer |> Binary.Arrays.writePositiveDifferencesUInt64 values
      
  let writeMonotonicDateTimes (data: IList<DateTime>) =
    data
      |> mapAsArray (fun d -> uint64 d.Ticks)
      |> writeMonotonicUInt64

  let writeBrownianInt64 data (writer: BinaryWriter) =
    match Option.catch maximunCommonDenominatorInt64All data with
    | None ->
      // 1 - WRITE 0 to indicate that all values are 0
      writer.Write(0uy)
      // 2 - WRITE The total amount of zeros
      writer.Write(data.Count)

    | Some multiplier ->
      let values =
        if multiplier = 1L then data
        else data |> mapAsArray (fun v -> v / multiplier) :> IList<_>
      // 1 - WRITE 1 to indicate that data follows
      writer.Write(1uy)
      // 2 - WRITE multiplier
      writer.Write(multiplier)
      // 3 - WRITE data using differences
      writer |> Binary.Arrays.writeSignedDifferencesInt64 values

  let writeBrownianFloat32 data (writer: BinaryWriter) =
    match findExponentMultiplierFloat32 10.0f 10 data with
    | None ->
      // 1 - WRITE 0 to indicate that we could not find any multiplier to turn floats into ints
      writer.Write(0uy)
      // 2 - WRITE floats normally
      Binary.Arrays.write (fun v w -> w.Write(v: float32)) data writer

    | Some multiplier ->
      // 1 - WRITE 1 to indicate that we found a multiplier to turn floats into ints
      writer.Write(1uy)
      // 2 - WRITE float32 multiplier
      writer.Write(multiplier)
      // 3 - WRITE data as brownian Int64
      let values = data |> mapAsArray (fun v -> v * multiplier |> round |> int64)
      writer |> writeBrownianInt64 values

module BinaryReader =
  let readMonotonicUInt64 (reader: BinaryReader) =
    // 1 - READ mode to know how to read the next content
    let mode = reader.ReadByte()
    match mode with
    | 0uy ->
      // 2 - READ The total amount of zeros
      let dataCount = reader.Read()
      Array.zeroCreate dataCount

    | 1uy ->
      // 2 - READ multiplier
      let multiplier = reader.ReadUInt64()
      // 3 - Read data using differences
      let data = reader |> Binary.Arrays.readPositiveDifferencesUInt64
      if multiplier = 1UL then data
      else data |> mapAsArray ((*) multiplier)

    | _ ->
      invalidOp (sprintf "Unknown file format")

  let readMonotonicDateTimes kind reader =
    readMonotonicUInt64 reader
      |> mapAsArray (fun ticks -> DateTime(int64 ticks, kind))
    
  let readBrownianInt64 (reader: BinaryReader) =
    // 1 - READ mode to know how to read the next content
    let mode = reader.ReadByte()
    match mode with
    | 0uy ->
      // 2 - READ The total amount of zeros
      let dataCount = reader.Read()
      Array.zeroCreate dataCount

    | 1uy ->
      // 2 - READ multiplier
      let multiplier = reader.ReadInt64()
      // 3 - Read data using differences
      let data = reader |> Binary.Arrays.readSignedDifferencesInt64
      if multiplier = 1L then data
      else data |> mapAsArray ((*) multiplier)

    | _ ->
      invalidOp (sprintf "Unknown file format")
    
  let readBrownianFloat32 (reader: BinaryReader) =
    // 1 - READ mode to know how to read the next content
    let mode = reader.ReadByte()
    match mode with
    | 0uy ->
      // 2 - READ floats normally
      reader |> Binary.Arrays.read (fun r -> r.ReadSingle())

    | 1uy ->
      // 2 - READ float32 multiplier
      let multiplier = reader.ReadSingle()
      // 3 - READ data brownian Int64
      let data = reader |> readBrownianInt64
      data |> mapAsArray (fun v -> float32 v / multiplier)

    | _ ->
      invalidOp (sprintf "Unknown file format")
