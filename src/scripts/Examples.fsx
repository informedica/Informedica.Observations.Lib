

#r "nuget: ClosedXML"
#r "nuget: Microsoft.Data.SqlClient"
#r "nuget: Informedica.Utils.Lib"

#load "../Database.fs"
#load "../ClosedXML.fs"
#load "../Types.fs"
#load "../Signal.fs"
#load "../Observation.fs"
#load "../DataSet.fs"
#load "../Convert.fs"
#load "../Filter.fs"
#load "../Collapse.fs"
#load "../Definitions.fs"


open System
open System.IO
open Informedica.Observations.Lib

fsi.AddPrinter<DateTime> (fun dt -> dt.ToString("dd-MM-yyyy HH:mm"))

let path = Path.Combine(__SOURCE_DIRECTORY__, "../../data/Definitions.xlsx")
File.Exists(path)

let returnNone _ = None


module Convert =

    open Types


    let setSignalText signal text =
        { signal with 
            Value = 
                if text = "" then NoValue
                else text |> Text
        }
    
    let servoI : Convert = 
        fun signal -> 
            match signal |> Signal.valueToString with
            | s when s = "3"  -> "VC-CMV"
            | s when s = "16" -> "VC-CMV"
            | s when s = "2"  -> "PC-CMVs"
            | s when s = "18" -> "PC-CMVs"
            | s when s = "4"  -> "PC-CMVa"
            | s when s = "20" -> "PC-CMVa"
            | s when s = "8"  -> "PC-CSV"
            | s when s = "21" -> "PC-CSV"
            | s when s = "12" -> "PC-CMVniv"
            | s when s = "13" -> "PC-CSVniv"
            | s when s = "14" -> "PC-CSVncap"
            | _ -> ""
            |> setSignalText signal


    let tempMode : Convert =
        fun signal -> 
            match signal.Id with
            | Some id when id = 5490  -> "rectal"
            | Some id when id = 8025  -> "ear"
            | Some id when id = 14759 -> "axillary"
            | Some id when id = 8601  -> "skin"
            | _ -> ""
            |> setSignalText signal
    

    let ventMachine : Convert =
        fun signal -> 
            match signal.Id with
            | Some id when id = 19660 -> "Carescape"
            | Some id when id = 16254 -> "Engstrom"
            | Some id when id = 7464  -> "ServoI"
            | Some id when id = 20059 -> "ServoU"
            | _ -> ""
            |> setSignalText signal


    let servoU : Convert = 
        fun signal -> 
            match signal |> Signal.valueToString with
            | s when s = "4"    -> "VC-CMV"
            | s when s = "1"    -> "PC-CMVs"
            | s when s = "2"    -> "PC-CMVs"
            | s when s = "2003" -> "PC-CMVs"
            | s when s = "7"    -> "PC-CMVa"
            | s when s = "8"    -> "PC-CMVa"
            | s when s = "2009" -> "PC-CMVa"
            | s when s = "11"  -> "PC-CSV"
            | s when s = "16" -> "PC-CMVniv"
            | s when s = "17" -> "PC-CSVniv"
            | s when s = "18" -> "PC-CSVncap"
            | _ -> ""
            |> setSignalText signal


    let engstrom : Convert = 
        fun signal -> 
            match signal |> Signal.valueToString with
            | s when s = "v"  -> "VC-CMV"
            | s when s = "b" -> "VC-CMV"
            | s when s = "p"  -> "PC-CMVs"
            | s when s = "g"  -> "PC-CMVa"
            | s when s = "c"  -> "PC-CSV"
            | s when s = "a" -> "PC-CSV"
            | s when s = "n" -> "PC-CMVniv"
            | s when s = "N" -> "PC-CSVncap"
            | _ -> ""
            |> setSignalText signal


    let map s =
        match s with
        | _ when s = "engstrom"    -> engstrom    |> Some
        | _ when s = "servoi"      -> servoI      |> Some
        | _ when s = "servou"      -> servoU      |> Some
        | _ when s = "carescape"   -> id          |> Some
        | _ when s = "ventmachine" -> ventMachine |> Some
        | _ when s = "tempmode"    -> tempMode    |> Some
        | _ -> None


module Filter =

    let map s =
        match s with
        | _ when s = "validated" -> Filter.onlyValid |> Some
        | _ -> None


module Collapse =

    open Types


    let sum : Collapse =
        fun signals ->
            if signals |> List.isEmpty then NoValue
            else
                signals
                |> List.sumBy (fun signal ->
                    match signal.Value with
                    | Numeric x -> x
                    | _ -> 0.
                )
                |> Numeric 


    let calcOxygenationIndex : Collapse = 
        fun signals ->
            let getNumeric = Option.bind Signal.getNumericValue
            let fiO2 = signals |> List.tryFind (fun signal -> signal.Name = "fio2") |> getNumeric
            let map = signals  |> List.tryFind (fun signal -> signal.Name = "mean airway p")  |> getNumeric
            let pO2 = signals  |> List.tryFind (fun signal -> signal.Name = "po2")  |> getNumeric
            match fiO2, map, pO2 with
            | Some fiO2, Some map, Some pO2 -> 
                ((fiO2 * 100. * map) / pO2)
                |> int 
                |> float
                |> Numeric 
            | _ -> NoValue

    let map s : Collapse option =
        match s with
        | _ when s = "concat(;)" -> 
            fun sgns -> 
                sgns 
                |> List.map Signal.valueToString
                |> String.concat ";"
                |> Text
            |> Some
        | _ -> None


open Types


let none = NoDateTime
let now = DateTime.Now |> SomeDateTime
let add n dt =
    match dt with
    | SomeDateTime dt -> dt.AddMinutes (n |> float) |> SomeDateTime
    | _ -> dt
let period start stop =
    match start, stop with 
    | SomeDateTime dt1, SomeDateTime dt2 -> Period (dt1, dt2)
    | _ -> 
        $"start and stop should be SomeDateTime not: {start}, {stop}"
        |> failwith


let createWithId = Signal.createWithIdValidated
let createNoId = Signal.createNoIdValidated


// signals 'raw data'
[
    // patient 1
    createWithId 5373 "gender" ("male" |> Text) "1" none
    createWithId 5473 "heart rate" (120. |> Numeric) "1" now
    createWithId 5461 "mean ibp" (60. |> Numeric) "1" now
    createWithId 5473 "heart rate" (124. |> Numeric) "1" (now |> add 1)
    // the temperature and the temp mode will be in the output
    createWithId 5490 "temp rect" (37.8 |> Numeric) "1" (now |> add 1)
    // this valueWithId will be filtered out
    createWithId 5473 "heart rate" (600. |> Numeric) "1" (now |> add 2)
    createWithId 5473 "heart rate" (125. |> Numeric) "1" (now |> add 3)
    createWithId 7348 "mean airway p" (12. |> Numeric) "1" (now |> add 1)
    createWithId 7464 "servoI mode" ("21" |> Text) "1" (now |> add 1)
    // medication that runs over a period signal
    createNoId "midazolam" ("midazolam 0.1 mg/kg/h" |> Text) "1" (period now (now |> add 5))
    // patient 2
    createWithId 5373 "gender" ("female" |> Text) "2" none
    createWithId 5473 "heart rate" (111. |> Numeric) "2" now
    // this value will be filtered out
    createWithId 5461 "mean ibp" (-100. |> Numeric) "2" now

    createWithId 5473 "heart rate" (103. |> Numeric) "2" (now |> add 1)
    createWithId 5461 "mean ibp" (100. |> Numeric) "2" (now |> add 1)
    // diuresis
    createWithId 6862 "spont diuresis" (12. |> Numeric) "2" now
    createWithId 6863 "cath diuresis" (15. |> Numeric) "2" now
    createWithId 16254 "engstrom mode" ("c" |> Text) "2" (now |> add 1)
    // the below 3 signals with the same date time will be collapsed to an OI
    createWithId 7348 "mean airway p" (20. |> Numeric) "2" (now |> add 1)
    createWithId 7345 "fio2" (0.6 |> Numeric) "2" (now |> add 1)
    createWithId 4100 "po2" (56. |> Numeric) "2" (now |> add 1)
    // medication signal over a period
    createNoId "morfine" ("morfine 10 mcg/kg/ur" |> Text) "2" (period now (now |> add 3))
]
// process the signals
|> DataSet.get (Definitions.readXML path Convert.map Filter.map Collapse.map)
|> DataSet.anonymize
|> fun (ds, _) ->
    ds.Columns
    |> List.map (fun c ->
        $"{c.Name}"
    )
    |> String.concat "\t"
    |> fun s ->
        ds.Data
        |> List.map (fun (id, dt, row) ->
            let row =
                row
                |> List.map (fun d ->
                    let d = 
                        match d with
                        | NoValue   -> "null"
                        | Text s    -> s
                        | Numeric x -> x |> sprintf "%A"
                        | DateTime dt -> dt.ToString("dd-MM-yyyy HH:mm")

                    $"{d}"
                )
                |> String.concat "\t"
            $"{id}\t{dt |> DataSet.timeStampToString}\t{row}"
        )
        |> String.concat "\n"
        |> sprintf "%s\n%s" s
    |> fun s -> File.WriteAllLines("observations.csv", [s]) 