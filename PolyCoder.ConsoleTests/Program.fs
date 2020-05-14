// Learn more about F# at http://fsharp.org

open System
open System.IO
open System.Text
open PolyCoder.IO
open PolyCoder.IO.TextReader
open PolyCoder.IO.TextReader.Csv
open System.Diagnostics
open PolyCoder.IO.Fxcm.Historical
open System.Collections.Generic

let measureTime name fn =
  printf "%s ... " name
  let watch = Stopwatch.StartNew()
  let data = fn()
  watch.Stop()
  printfn "elapsed: %O" watch.Elapsed
  data

let measureTimeAsync name fn = async {
  printf "%s ... " name
  let watch = Stopwatch.StartNew()
  let! data = fn()
  watch.Stop()
  printfn "elapsed: %O" watch.Elapsed
  return data
}

let printFxcmTicks (data: IList<FxcmTickData>) =
  printfn "Total rows: %d" data.Count
  for tick in data |> Seq.take 10 do
    printfn "%s  %10f %10f" (tick.dateTime.ToString(DateTimeFormat)) tick.bid tick.ask

let readSampleFromLocalFile() = async {
  let! data = measureTimeAsync "From local CSV file" (fun () ->
    TickData.ofLocalCsvFile @"D:\ForexData\FXCM\Original\TickData\AUDCAD\2018\6.csv.gz")
  printFxcmTicks data
  return data.ToArray()
}

let readSampleFromUrl() = async {
  let! data = measureTimeAsync "From remote CSV URL" (fun () ->
    TickData.ofWebUrl @"https://tickdata.fxcorporate.com/AUDCAD/2018/6.csv.gz")
  printFxcmTicks data
}

let writeSampleToBinLocalFile (data: IList<_>) = async {
  measureTime "To local BIN file" (fun () ->
    TickData.toBinaryLocalFile data @"D:\ForexData\FXCM\Original\TickData\AUDCAD-2018-6.frame.gz")
}

let readSampleFromBinLocalFile() = async {
  let data = measureTime "From local BIN file" (fun () ->
    TickData.ofLocalBinFile @"D:\ForexData\FXCM\Original\TickData\AUDCAD-2018-6.frame.gz")
  printFxcmTicks data
  return data
}

let compareBothData data data2 =
  let areEqual = measureTime "Comparing data" (fun () -> data = data2)
  printfn "Both data are equal: %A" areEqual

[<EntryPoint>]
let main argv =
  let data = readSampleFromLocalFile() |> Async.RunSynchronously
  printfn ""
  //readSampleFromUrl() |> Async.RunSynchronously
  //printfn ""
  writeSampleToBinLocalFile(data) |> Async.RunSynchronously
  printfn ""
  let data2 = readSampleFromBinLocalFile() |> Async.RunSynchronously
  printfn ""
  compareBothData data data2

  0 // return an integer exit code
