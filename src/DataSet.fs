namespace Informedica.Observations.Lib


module DataSet =

    open System
    open System.IO
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
                                        
                                        signal 
                                        |> (source.Conversions |> List.fold (>>) id)
                                    )
                                    |> (obs.Filters |> List.fold (>>) id)
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
                        | _, Relative _ -> 
                            (id, dt, r)
                        | Exact fdt, Exact sdt  -> 
                            (id, (sdt - fdt).TotalMinutes |> int |> Relative, r) 
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


    let toCSV path (ds : DataSet) =
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
                $"{id}\t{dt |> timeStampToString}\t{row}"
            )
            |> String.concat "\n"
            |> sprintf "%s\n%s" s
        |> fun s -> File.WriteAllLines(path, [s]) 
