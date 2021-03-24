

open System
open System.IO

fsi.AddPrinter<DateTime> (fun dt -> dt.ToString("dd-MM-yyyy HH:mm"))


module Types =

    type SignalId = int option
    type PatientId = string


    type Value =
        | NoValue
        | Text of string
        | Numeric of float
        | DateTime of DateTime


    type DateTimeOption = 
        | SomeDateTime of DateTime
        | Period of start : DateTime * stop : DateTime
        | NoDateTime


    type Signal = 
        {
            Id: SignalId
            Name : string
            Value : Value
            PatientId : string
            DateTime : DateTimeOption
        }


    /// A function to summarize a list of values
    /// as one value
    type Collapse = Signal list -> Value

    /// A conversion function for a value
    type Convert = Signal -> Signal

    /// An observation describes wich signals to
    /// that describe that observation (Sources)
    /// along with a collaps function that summarizes
    /// the obtained values to one value.
    /// Each source value also can be converted to
    /// a different or adjusted value.
    type Observation =
        { 
            Name : string 
            Type : string
            Sources : Source list
            Collapse : Collapse 
        }
    and Source =
        { 
            Id : int
            Name : string
            Convert : Convert 
        }



    /// The resulting dataset with colums
    /// and rows of data. Each row has a
    /// unique hospital number, date time.
    type DataSet =
        { Columns : Column list
          Data : (PatientId * TimeStamp * DataRow) list }
    and Column = 
        { 
            Name: string
            Type: string
        }
    and DataRow = Value list
    and TimeStamp = | Exact of DateTime | Relative of int 


    type Transform = Observation list ->  Signal list -> DataSet


    type Conversions =
        | Engstrom
        | ServoU
        | VentMachine
        | TempMode



module Signal =

    open Types


    let create id name value patId datetime =
        {
            Id = id 
            Name = name
            Value = value
            PatientId = patId
            DateTime = datetime
        }

    let createWithId id = create (id |> Some)


    let createNoId = create None


    let createText id name value patId datetime =
        let value = value |> Text
        create id name value patId datetime


    let createNumeric id name value patId datetime =
        let value = value |> Numeric
        create id name value patId datetime
        

    let createDateTime id name value patId datetime =
        let value = value |> DateTime
        create id name value patId datetime


    let hasId i (signal : Signal) =
        match signal.Id with
        | Some id -> id = i
        | None    -> false


    let getNumericValue (signal : Signal) =
        match signal.Value with
        | Numeric x -> x |> Some
        | _         -> None


    let valueToString (signal : Signal) =
        match signal.Value with
        | NoValue   -> ""
        | Text s    -> s
        | Numeric x -> x |> sprintf "%A"
        | DateTime dt -> dt.ToString("dd-MM-yyyy HH:mm")


    let dateTimeIsSome (signal : Signal) =
        match signal.DateTime with
        | SomeDateTime _ -> true
        | _              -> false

    let dateTimeIsPeriod (signal : Signal) =
        match signal.DateTime with
        | Period _ -> true
        | _              -> false


    let periodToDateTime (signal : Signal) =
        match signal.DateTime with
        | Period (start, stop) ->
            [0. .. (stop - start).TotalMinutes ]
            |> List.map (fun min ->
                {
                    signal with
                        DateTime = start.AddMinutes(min) |> SomeDateTime
                }
            )
        | _ -> [ signal ]

    let getDateTimeValue (signal : Signal) =
        match signal.DateTime with
        | SomeDateTime dt -> dt
        | _ -> 
            $"Cannot get datetime from: {signal}"
            |> failwith



module Observation =

    open Types


    let create name type' sources collapseFn =
        {
            Name = name
            Type = type'
            Sources = sources
            Collapse = collapseFn
        }


    let createSource id name convertFn =
        {
            Id = id
            Name = name
            Convert = convertFn
        }


    let mapToObservations definitions =
        definitions 
        |> List.map (fun (name, type', collapseFn, sources) ->
            let sources =
                sources 
                |> List.map (fun (id, name, convertFn) -> createSource id name convertFn)
            create name type' sources collapseFn
        )

    let signalBelongsToSource (signal : Signal) (source : Source) =
        if signal.Id.IsSome then signal.Id.Value = source.Id
        else
            signal.Name.Trim().ToLower() = source.Name.Trim().ToLower()



module DataSet =

    open Types

    let timeStampToString = function
        | Exact dt   -> dt.ToString("dd-MM-yyyy HH:mm")
        | Relative i -> i |> string

    let empty = 
        { 
            Columns = []
            Data = []
        }

    let get : Transform =
        fun observations signals ->
            let columns =
                observations
                |> List.map (fun obs ->
                    {
                        Name = obs.Name
                        Type = obs.Type
                    }
                )
                |> List.append [
                    { Name = "pat_id"; Type = "varchar(50)" }
                    { Name = "pat_timestamp"; Type = "datetime" }
                ]

            signals 
            |> List.sortBy (fun signal -> signal.PatientId)
            // get all signals per patient
            |> List.groupBy (fun signal -> signal.PatientId)
            // split date time dependent and independent
            |> List.collect (fun (patId, patSignals) ->
                patSignals
                |> List.collect Signal.periodToDateTime
                |> List.partition Signal.dateTimeIsSome
                |> fun (dependent, independent) ->
                    dependent
                    |> List.sortBy Signal.getDateTimeValue
                    |> List.groupBy Signal.getDateTimeValue
                    |> List.map (fun (dt, dateSignals) ->
                        {|
                            patId = patId
                            dateTime = dt
                            signals = independent @ dateSignals
                        |}
                    )
            )
            |> List.fold (fun acc x ->
                 { 
                    acc with
                        Data = [
                            x.patId, x.dateTime |> Exact, [
                                for obs in observations do
                                    x.signals 
                                    // get all signals that belong to 
                                    // an observation, i.e. is in source list
                                    |> List.filter (fun signal -> 
                                        obs.Sources 
                                        |> List.exists (Observation.signalBelongsToSource signal)
                                    )
                                    // convert the signal value
                                    |> List.map (fun signal ->
                                        let source = 
                                            obs.Sources 
                                            |> List.find (Observation.signalBelongsToSource signal)
                                        signal |> source.Convert
                                    )
                                    // collapse to a single value
                                    |> obs.Collapse
                            ]
                        ]
                        // add to existing data
                        |> List.append acc.Data
                 }
            ) { empty with Columns = columns }


    let anonymize (ds : DataSet) =
        ds.Data
        |> List.groupBy (fun (pat, _, _) -> pat)
        |> List.fold (fun (ds, ids) (pat, xs) ->
            let id = Guid.NewGuid().ToString()
            match xs with
            | [] -> (ds, (pat, id)::ids)
            | _  ->
                let (_, fstDate, _) = xs |> List.head 
                let data = 
                    xs 
                    |> List.map (fun (_, dt, r) -> 
                        match fstDate, dt with
                        | Relative _, _ 
                        | _, Relative _ -> (id, dt, r)
                        | Exact fdt, Exact sdt  -> (id, (sdt - fdt).TotalMinutes |> int |> Relative, r) 
                    )
                {
                    ds with
                        Data =
                            if ids |> List.isEmpty then data
                            else 
                                data
                                |> List.append ds.Data
                }, (pat, id)::ids

        ) (ds, [])



module Convert =

    open Types

    let filterLowHigh low high : Convert = 
        fun signal ->
            let value =
                signal
                |> Signal.getNumericValue
                |> Option.bind (fun value -> 
                    if value < low || value > high then NoValue
                    else value |> Numeric 
                    |> Some
                )
                |> Option.defaultValue NoValue
            { signal with Value = value }


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



module Collapse =

    open Types

    let toFirst : Collapse =
        fun signals ->
            match signals |> List.tryHead with
            | None -> NoValue
            | Some signal -> signal.Value


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



module Definitions =

    open Types
    // Convert Fn
    let tempMode = Convert.tempMode
    let filterIbpMean = Convert.filterLowHigh 0. 300. 
    let filterHr = Convert.filterLowHigh 0. 250.

    let machine = Convert.ventMachine
    let engstrom = Convert.engstrom
    let servoI = Convert.servoI
    let servoU = Convert.servoU

    // Collapse Fn
    let toFirst = Collapse.toFirst
    let sum = Collapse.sum

    let definitions =
        //   name, type, collapse Fn, [list of (id, convert Fn)]
        [ 
            ("pat_gender", "varchar(10)", toFirst, [(5373, "", id)])
            ("mon_hr", "float", toFirst, [(5473, "", filterHr); (10156, "", filterHr); (10157, "", filterHr)])
            ("mon_ibp_mean", "float", toFirst, [(5461, "", filterIbpMean)])
            ("mon_temp_mode", "varchar(50)", toFirst, [(5490, "", tempMode); (8025, "", tempMode); (14759, "", tempMode); (8601, "", tempMode)])
            ("mon_temp", "float", toFirst, [(5490, "", id); (8025, "", id); (14759, "", id); (8601, "", id)])
            ("obs_fb_urine", "float", sum, [(6863, "", id); (6862, "", id)] )
            ("lab_bg_po2", "float", toFirst, [(4100, "", id); (4108, "", id)])
            ("vent_s_fio2", "float",  toFirst, [(7345, "", id)])
            ("vent_m_mean", "float",  toFirst, [(7348, "", id)])
            ("cacl_oi", "float", Collapse.calcOxygenationIndex, [(7348, "", id); (7345, "", id); (4100, "", id)])
            ("vent_machine", "varchar(100)",  toFirst, [(16254, "", machine);(7464, "", machine);(20059, "", machine)])
            ("vent_mode", "float",  toFirst, [(16254, "", engstrom);(7464, "", servoI);(20059, "", servoU)])
            ("med_sedation", "varchar(100)", toFirst, [(0, "midazolam", id); (0, "morfine", id)])
        ]



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


// signals 'raw data'
[
    // patient 1
    Signal.createWithId 5373 "gender" ("male" |> Text) "1" none
    Signal.createWithId 5473 "heart rate" (120. |> Numeric) "1" now
    Signal.createWithId 5461 "mean ibp" (60. |> Numeric) "1" now
    Signal.createWithId 5473 "heart rate" (124. |> Numeric) "1" (now |> add 1)
    // the temperature and the temp mode will be in the output
    Signal.createWithId 5490 "temp rect" (37.8 |> Numeric) "1" (now |> add 1)
    // this valueWithId will be filtered out
    Signal.createWithId 5473 "heart rate" (600. |> Numeric) "1" (now |> add 2)
    Signal.createWithId 5473 "heart rate" (125. |> Numeric) "1" (now |> add 3)
    Signal.createWithId 7348 "mean airway p" (12. |> Numeric) "1" (now |> add 1)
    Signal.createWithId 7464 "servoI mode" ("21" |> Text) "1" (now |> add 1)
    // medication that runs over a period signal
    Signal.createNoId "midazolam" ("midazolam 0.1 mg/kg/h" |> Text) "1" (period now (now |> add 5))
    // patient 2
    Signal.createWithId 5373 "gender" ("female" |> Text) "2" none
    Signal.createWithId 5473 "heart rate" (111. |> Numeric) "2" now
    // this value will be filtered out
    Signal.createWithId 5461 "mean ibp" (-100. |> Numeric) "2" now

    Signal.createWithId 5473 "heart rate" (103. |> Numeric) "2" (now |> add 1)
    Signal.createWithId 5461 "mean ibp" (100. |> Numeric) "2" (now |> add 1)
    // diuresis
    Signal.createWithId 6862 "spont diuresis" (12. |> Numeric) "2" now
    Signal.createWithId 6863 "cath diuresis" (15. |> Numeric) "2" now
    Signal.createWithId 16254 "engstrom mode" ("c" |> Text) "2" (now |> add 1)
    // the below 3 signals with the same date time will be collapsed to an OI
    Signal.createWithId 7348 "mean airway p" (20. |> Numeric) "2" (now |> add 1)
    Signal.createWithId 7345 "fio2" (0.6 |> Numeric) "2" (now |> add 1)
    Signal.createWithId 4100 "po2" (56. |> Numeric) "2" (now |> add 1)
    // medication signal over a period
    Signal.createNoId "morfine" ("morfine 10 mcg/kg/ur" |> Text) "2" (period now (now |> add 3))
]
// process the signals
|> DataSet.get (Definitions.definitions 
                |> Observation.mapToObservations)
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