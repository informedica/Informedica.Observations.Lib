

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
open Informedica.Utils.Lib.BCL

fsi.AddPrinter<DateTime> (fun dt -> dt.ToString("dd-MM-yyyy HH:mm"))

let path = Path.Combine(__SOURCE_DIRECTORY__, "../../data/Definitions.xlsx")
File.Exists(path)

let returnNone _ = None


module Convert =

    open Types
    open Convert

    
    let private setText s t = setSignalText t s 

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
            |> setText signal


    let tempMode : Convert =
        fun signal -> 
            match signal.Id with
            | Some id when id = "5490"  -> "rectal"
            | Some id when id = "8025"  -> "ear"
            | Some id when id = "14759" -> "axillary"
            | Some id when id = "8601"  -> "skin"
            | _ -> ""
            |> setText signal
    

    let ventMachine : Convert =
        fun signal -> 
            match signal.Id with
            | Some id when id = "19660" -> "Carescape"
            | Some id when id = "16254" -> "Engstrom"
            | Some id when id = "7464"  -> "ServoI"
            | Some id when id = "20059" -> "ServoU"
            | _ -> ""
            |> setText signal


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
            |> setText signal


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
            |> setText signal


    let map s =
        match s with
        | _ when s = "engstrom"     -> engstrom    |> Some
        | _ when s = "servo_i"      -> servoI      |> Some
        | _ when s = "servo_u"      -> servoU      |> Some
        | _ when s = "carescape"    -> id          |> Some
        | _ when s = "vent_machine" -> ventMachine |> Some
        | _ when s = "temp_mode"    -> tempMode    |> Some
        | _ -> None



module Filter =

    open Informedica.Utils.Lib.BCL

    let getParams s =
        "\(([^)]+)\)"
        |> String.regex
        |> fun regex -> 
            regex.Match(s).Value
            |> String.split ","
            |> List.filter (String.isNullOrWhiteSpace >> not)
            |> List.map (String.replace "(" "")
            |> List.map (String.replace ")" "")
            |> List.collect (String.split ",")

    let map s =
        match s with
        | _ when s = "validated" -> Filter.onlyValid |> Some
        | _ when s |> String.startsWith "min_(" ->
            match s |> getParams with
            | [x] -> 
                x 
                |> Double.tryParse
                |> Option.bind (fun x -> Filter.filterGTE x |> Some)
            | _ -> None
        | _ when s |> String.startsWith "max_(" ->
            match s |> getParams with
            | [x] -> 
                x 
                |> Double.tryParse
                |> Option.bind (fun x -> Filter.filterSTE x |> Some)
            | _ -> None
        | _ -> None



module Collapse =

    open Types


    let sum : Collapse =
        fun signals ->
            if signals |> List.isEmpty || 
               signals |> List.forall Signal.isNonNumeric then NoValue
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
        | _ when s = "sum" ->
            sum
            |> Some
        | _ when s = "concat_;" -> 
            fun sgns -> 
                sgns 
                |> List.map Signal.valueToString
                |> String.concat ";"
                |> Text
            |> Some
        | _ when s = "text_portion" -> 
            fun sgns ->
                sgns 
                |> List.exists (Signal.hasValue)
                |> fun b -> 
                    if not b then NoValue 
                    else
                        "portion" |> Text
            |> Some
        | _ when s = "text_daily"   -> 
            fun sgns ->
                sgns 
                |> List.exists (Signal.hasValue)
                |> fun b -> 
                    if not b then NoValue 
                    else
                        "daily" |> Text
            |> Some

        | _ -> None


open Types


// https://docs.google.com/spreadsheets/d/1ZAk5enAvdkFNv5DD7n5o1tTkAL9MedKNC1YFFdmjL-8/edit?usp=sharing
let docId = "1ZAk5enAvdkFNv5DD7n5o1tTkAL9MedKNC1YFFdmjL-8"


// signals 'raw data'
// createWithId 5373 "patient gender" ("male" |> Text) "1" none // patient gender
// createWithId 5473 "heart rate" (120. |> Numeric) "1" now
// createWithId 5461 "mean ibp" (60. |> Numeric) "1" now
// createWithId 5473 "heart rate" (124. |> Numeric) "1" (now |> add 1)
// // the temperature and the temp mode will be in the output
// createWithId 5490 "temp rect" (37.8 |> Numeric) "1" (now |> add 1)
// // this valueWithId will be filtered out
// createWithId 5473 "heart rate" (600. |> Numeric) "1" (now |> add 2)
// createWithId 5473 "heart rate" (125. |> Numeric) "1" (now |> add 3)
// createWithId 7348 "mean airway p" (12. |> Numeric) "1" (now |> add 1)
// createWithId 7464 "servoI mode" ("21" |> Text) "1" (now |> add 1)
// // medication that runs over a period signal
// createNoId "midazolam" ("midazolam 0.1 mg/kg/h" |> Text) "1" (period now (now |> add 5))
// // patient 2
// createWithId 5373 "gender" ("female" |> Text) "2" none
// createWithId 5473 "heart rate" (111. |> Numeric) "2" now
// // this value will be filtered out
// createWithId 5461 "mean ibp" (-100. |> Numeric) "2" now

// createWithId 5473 "heart rate" (103. |> Numeric) "2" (now |> add 1)
// createWithId 5461 "mean ibp" (100. |> Numeric) "2" (now |> add 1)
// // diuresis
// createWithId 6862 "spont diuresis" (12. |> Numeric) "2" now
// createWithId 6863 "cath diuresis" (15. |> Numeric) "2" now
// createWithId 16254 "engstrom mode" ("c" |> Text) "2" (now |> add 1)
// // the below 3 signals with the same date time will be collapsed to an OI
// createWithId 7348 "mean airway p" (20. |> Numeric) "2" (now |> add 1)
// createWithId 7345 "fio2" (0.6 |> Numeric) "2" (now |> add 1)
// createWithId 4100 "po2" (56. |> Numeric) "2" (now |> add 1)
// // medication signal over a period
// createNoId "morfine" ("morfine 10 mcg/kg/ur" |> Text) "2" (period now (now |> add 3))


let signalsList =

    Definitions.getSheet None (docId |> Some) "Signals"
    |> fun data ->
        let columns = data |> List.head
        data
        |> List.tail
        |> List.map (fun r ->
            {
                Id = r |> Definitions.getColumn columns "id"
                Name = r |> Definitions.getColumn columns "name"
                PatientId = r |> Definitions.getColumn columns "patientid"
                TimeStamp = 
                    let start = r |> Definitions.getColumn<DateTime option> columns "start"
                    let stop  = r |> Definitions.getColumn<DateTime option> columns "stop"
                    match start, stop with
                    | None, _ -> NoDateTime
                    | Some dt, None -> dt |> SomeDateTime
                    | Some dt1, Some dt2 -> (dt1, dt2) |> Period
                Value = 
                    let v = r |> Definitions.getColumn columns "value"
                    match r |> Definitions.getColumn columns "type" with
                    | s when s = "numeric" -> 
                        match v |> Double.tryParse with
                        | Some x -> x |> Numeric
                        | None   -> NoValue
                    | s when s = "datetime" -> 
                        match DateTime.TryParse(v) with
                        | true, x -> x |> DateTime
                        | _ -> NoValue
                    | _ -> 
                        printfn "setting text value: %s" v
                        v |> Text
                Validated = 
                    r |> Definitions.getColumn columns "validated" = "TRUE"
            }
        )



let dsToCsv ds =
    let path = Path.Combine(__SOURCE_DIRECTORY__, "../../data/dataset.csv")

    ds
    |> DataSet.anonymize
    |> fun (ds, _) -> ds |> DataSet.toCSV path


// let xmlDs = 
//     // process the signals
//     signalsList
//     |> DataSet.get None (Definitions.readXML path Convert.map Filter.map Collapse.map)


let onlineDs = 
    // signals 'raw data'
    signalsList
    // process the signals
    |> DataSet.get None (Definitions.readGoogle docId Convert.map Filter.map Collapse.map)


onlineDs
|> DataSet.removeEmpty
|> dsToCsv


onlineDs
|> fun ds ->
    let getColumnIndex c =
        ds.Columns 
        |> List.findIndex ((=) c)
        |> fun i -> i - 2

    let columns =
        ds.Columns
        |> List.skip 2
        |> List.filter (fun c ->
            ds.Data
            |> List.map (fun (_, _, row) ->
                row.[c |> getColumnIndex ]
            )
            |> List.forall ((=) NoValue)
            |> not
        )

    {
        Columns = columns
        Data =
            ds.Data
            |> List.fold (fun acc (id, dt, row) ->
                row
                |> List.mapi (fun i v ->
                    (i, v)  
                )
                |> List.filter (fun (i, _) ->
                    columns 
                    |> List.exists (fun c -> c = ds.Columns.[i + 2])
                )
                |> List.map snd
                |> fun r -> [ (id, dt, r)]
                |> List.append acc
            ) []
    }
