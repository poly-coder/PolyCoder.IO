module PolyCoder.IO.DataSets

open System
open System.Collections.Generic
open PolyCoder

// TODO: Move to Preamble
module Seq =
  let windowedAll windowSize (source: _ seq) =
    let e = source.GetEnumerator()
    let array = ResizeArray(windowSize: int)
    let rec loop() = seq {
      if e.MoveNext() then
        array.Add(e.Current)
        if array.Count >= windowSize then
          yield array.ToArray()
          array.Clear()
        yield! loop()
      else
        if array.Count > 0 then yield array.ToArray()
        e.Dispose()
    }
    loop()

  let dfsKeyedWith (comparer: IEqualityComparer<'key>) getKey getChildren (source: seq<'item>) =
    let foundSet = HashSet<_>(comparer)
    let rec loop item = seq {
      let key = getKey item
      if foundSet.Add key then
        yield item
        yield! getChildren item |> Seq.collect loop
    }
    source |> Seq.collect loop

  let dfsKeyed getKey = dfsKeyedWith EqualityComparer<_>.Default getKey
  let dfsWith comparer getChildren = dfsKeyedWith comparer id getChildren
  let dfs getChildren = dfsWith EqualityComparer<_>.Default getChildren

  let dlsKeyedWith (comparer: IEqualityComparer<'key>) getKey getChildren (source: seq<'item>) =
    let foundSet = HashSet<_>(comparer)
    let rec loop item = seq {
      let key = getKey item
      if foundSet.Add key then
        yield! getChildren item |> Seq.collect loop
        yield item
    }
    source |> Seq.collect loop

  let dlsKeyed getKey = dlsKeyedWith EqualityComparer<_>.Default getKey
  let dlsWith comparer getChildren = dlsKeyedWith comparer id getChildren
  let dls getChildren = dlsWith EqualityComparer<_>.Default getChildren

let mcdWith isUnit remainder a b =
  let rec loop a b =
    if a < b then loop b a
    elif isUnit b then a
    else loop b (remainder a b)
  loop a b

let mcd = mcdWith ((=) 1) (%)
let mcdInt64 = mcdWith ((=) 1L) (%)
let mcdInt32 = mcdWith ((=) 1) (%)
let mcdInt16 = mcdWith ((=) 1s) (%)
let mcdInt8 = mcdWith ((=) 1y) (%)
let mcdUInt64 = mcdWith ((=) 1UL) (%)
let mcdUInt32 = mcdWith ((=) 1u) (%)
let mcdUInt16 = mcdWith ((=) 1us) (%)
let mcdUInt8 = mcdWith ((=) 1uy) (%)

let mcdSeqWith isUnit remainder source =
  let mcdLocal = mcdWith isUnit remainder
  source
    |> Seq.foldWhile
        (fun acc item ->
          match acc with
          | Some acc ->
            let next = mcdLocal acc item
            if isUnit next
            then BreakWith (Some next)
            else ContinueWith (Some next)
          | None ->
            if isUnit item
            then BreakWith (Some item)
            else ContinueWith (Some item)
        )
        None
    |> Option.get

let mcdSeq source = source |> mcdSeqWith ((=) 1) (%)
let mcdInt64Seq source = source |> mcdSeqWith ((=) 1L) (%)
let mcdInt32Seq source = source |> mcdSeqWith ((=) 1) (%)
let mcdInt16Seq source = source |> mcdSeqWith ((=) 1s) (%)
let mcdInt8Seq source = source |> mcdSeqWith ((=) 1y) (%)
let mcdUInt64Seq source = source |> mcdSeqWith ((=) 1UL) (%)
let mcdUInt32Seq source = source |> mcdSeqWith ((=) 1u) (%)
let mcdUInt16Seq source = source |> mcdSeqWith ((=) 1us) (%)
let mcdUInt8Seq source = source |> mcdSeqWith ((=) 1uy) (%)


module Binary =
  open System.IO

  module Internals =
    // TODO: Optimize to reduce allocations
    let writeSeqData (windowSize: uint32) writeItem data (writer: BinaryWriter) =
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
    let readSeqData readItem (reader: BinaryReader) =
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

    let readSeqDataAsArray readItem = readSeqData readItem >> Seq.toArray


    // TODO: Optimize to reduce allocations
    let writeSeqAnyDiffData
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
    let readSeqAnyDiffData
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

    let readSeqAnyDiffDataAsArray createDiffReader add readItem =
      readSeqAnyDiffData createDiffReader add readItem >> Seq.toArray

    
    let writeSeqPositiveDiffData maxWindowSize substract writeItem data =
      writeSeqAnyDiffData Binary.unsignedSize BinaryWriter.writeUnsigned maxWindowSize substract writeItem data

    let readSeqPositiveDiffData add readItem =
      readSeqAnyDiffData BinaryReader.readUnsigned add readItem

    let readSeqPositiveDiffDataAsArray add readItem =
      readSeqPositiveDiffData add readItem >> Seq.toArray


    let writeSeqPositiveDiffUInt64Data (maxWindowSize: uint32) =
      let substract (a: uint64) (b: uint64) =
        assert (a >= b)
        a - b
      writeSeqPositiveDiffData maxWindowSize substract (fun item writer -> writer.Write(item))

    let readSeqPositiveDiffUInt64Data reader =
      let add a b = a + b
      readSeqPositiveDiffData add (fun reader -> reader.ReadUInt64()) reader
    
    let readSeqPositiveDiffUInt64DataAsArray reader =
      readSeqPositiveDiffUInt64Data reader |> Seq.toArray


    let writeSeqPositiveDiffUInt32Data (maxWindowSize: uint32) =
      let substract (a: uint32) (b: uint32) =
        assert (a >= b)
        a - b |> uint64
      writeSeqPositiveDiffData maxWindowSize substract (fun item writer -> writer.Write(item))

    let readSeqPositiveDiffUInt32Data reader =
      let add (a: uint32) (b: uint64) = a + uint32 b
      readSeqPositiveDiffData add (fun reader -> reader.ReadUInt32()) reader
    
    let readSeqPositiveDiffUInt32DataAsArray reader =
      readSeqPositiveDiffUInt32Data reader |> Seq.toArray


    let writeSeqPositiveDiffUInt16Data (maxWindowSize: uint32) =
      let substract (a: uint16) (b: uint16) =
        assert (a >= b)
        a - b |> uint64
      writeSeqPositiveDiffData maxWindowSize substract (fun item writer -> writer.Write(item))

    let readSeqPositiveDiffUInt16Data reader =
      let add (a: uint16) (b: uint64) = a + uint16 b
      readSeqPositiveDiffData add (fun reader -> reader.ReadUInt16()) reader
    
    let readSeqPositiveDiffUInt16DataAsArray reader =
      readSeqPositiveDiffUInt16Data reader |> Seq.toArray

    
    let writeSeqDiffData maxWindowSize substract writeItem data =
      writeSeqAnyDiffData Binary.signedSize BinaryWriter.writeSigned maxWindowSize substract writeItem data

    let readSeqDiffData add readItem =
      readSeqAnyDiffData BinaryReader.readSigned add readItem

    let readSeqDiffDataAsArray add readItem =
      readSeqDiffData add readItem >> Seq.toArray


    let writeSeqDiffInt64Data (maxWindowSize: uint32) =
      let substract (a: int64) (b: int64) =
        assert (a >= b)
        a - b
      writeSeqDiffData maxWindowSize substract (fun item writer -> writer.Write(item))

    let readSeqDiffInt64Data reader =
      let add a b = a + b
      readSeqDiffData add (fun reader -> reader.ReadInt64()) reader
    
    let readSeqDiffInt64DataAsArray reader =
      readSeqDiffInt64Data reader |> Seq.toArray


    let writeSeqDiffInt32Data (maxWindowSize: uint32) =
      let substract (a: int32) (b: int32) =
        assert (a >= b)
        a - b |> int64
      writeSeqDiffData maxWindowSize substract (fun item writer -> writer.Write(item))

    let readSeqDiffInt32Data reader =
      let add (a: int32) (b: int64) = a + int32 b
      readSeqDiffData add (fun reader -> reader.ReadInt32()) reader
    
    let readSeqDiffInt32DataAsArray reader =
      readSeqDiffInt32Data reader |> Seq.toArray


    let writeSeqDiffInt16Data (maxWindowSize: uint32) =
      let substract (a: int16) (b: int16) =
        assert (a >= b)
        a - b |> int64
      writeSeqDiffData maxWindowSize substract (fun item writer -> writer.Write(item))

    let readSeqDiffInt16Data reader =
      let add (a: int16) (b: int64) = a + int16 b
      readSeqDiffData add (fun reader -> reader.ReadInt16()) reader
    
    let readSeqDiffInt16DataAsArray reader =
      readSeqDiffInt16Data reader |> Seq.toArray
