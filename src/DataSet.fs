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
        fun tr observations signals ->
            let columns =
                observations
                |> List.map (fun obs ->
                    {
                        Name = obs.Name
                        Type = obs.Type
                        Length = obs.Length
                    }
                )
                |> List.append [
                    { Name = "pat_id"; Type = "varchar"; Length = Some 50 }
                    { Name = "pat_timestamp"; Type = "datetime"; Length = None }
                ]

            signals 
            // get all signals per patient
            |> List.groupBy (fun signal -> signal.PatientId)
            // split date time dependent and independent
            |> List.collect (fun (patId, patSignals) ->
                // patSignals
                // |> List.collect Signal.periodToDateTime
                // |> List.partition Signal.timeStampIsDateTime
                // |> fun (dependent, independent) ->
                let notDateTime = 
                    patSignals
                    |> List.filter (Signal.timeStampIsDateTime >> not)
                patSignals
                |> List.filter Signal.timeStampIsDateTime
                |> List.sortBy Signal.getDateTimeValue
                |> List.groupBy Signal.getDateTimeValue
                |> List.map (fun (dt, dateSignals) ->
                    {|
                        patId = patId
                        dateTime = dt
                        inPeriod = 
                            patSignals
                            |> List.filter (Signal.dateTimeInPeriod dt)
                        dateSignals = dateSignals
                    |}
                )
                |> fun rows ->
                    match tr with 
                    | None   -> 
                        rows
                        |> List.map (fun r -> 
                            {|
                                patId = r.patId
                                dateTime = r.dateTime
                                signals = r.inPeriod @ r.dateSignals
                            |}
                        )
                    | Some t -> 
                        match rows with
                        | []  -> []
                        | [h] -> 

                            [
                                {|
                                    patId = h.patId
                                    dateTime = h.dateTime
                                    signals = h.inPeriod @ h.dateSignals
                                |}
                            ]
                        | _ ->
                            let first = rows |> List.head
                            let last  = rows |> List.last
                            let c = 
                                let n = (last.dateTime - first.dateTime).TotalMinutes |> int
                                if n % t = 0 then n
                                else n + t
                            // create a list of offsets
                            [0 .. t .. c ]
                            |> List.collect (fun x ->
                                let x = x |> float                                
                                let start = first.dateTime.AddMinutes(x)
                                let stop = first.dateTime.AddMinutes(x + (float t))
                                rows 
                                |> List.filter (fun r -> 
                                    r.dateTime >= start &&
                                    r.dateTime < stop
                                )
                                |> fun filtered ->
                                    filtered 
                                    |> List.tryHead
                                    |> function
                                    | None -> []
                                    | Some h ->
                                        [
                                            {|
                                                patId = h.patId
                                                dateTime = start
                                                signals = 
                                                    filtered
                                                    |> List.collect (fun f -> f.dateSignals)
                                                    |> List.append h.inPeriod 
                                            |}
                                        ]
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
                        Columns =
                            ds.Columns
                            |> List.map (fun c ->
                                if c.Name = "pat_timestamp" then
                                    { c with Type = "int" }
                                else c
                            )
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


    let removeEmpty ds =
        let getColumnIndex c =
            ds.Columns 
            |> List.findIndex ((=) c)
            |> fun i -> i - 2
        // the columns to retain
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
        // new data set with only data from retained columns
        {
            Columns = (ds.Columns |> List.take 2) @ columns
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

    /// Create a database if this doesn't exist, otherwise just get the database.
    /// If dropTable and the table exists drop the table and create a new table with the dataset.
    /// Otherwise use the existing one if this doesn't exist create one.
    let toDatabase connString dbName tblName dropTable (ds : DataSet) =
        let connBuilder = 
            connString
            |> SqlConnectionStringBuilder.tryCreate
            |> Option.get

        let mapColumns ds =
            ds.Columns
            |> List.map (fun c -> 
                {
                    Table.Name = c.Name
                    Table.Type = 
                        match c.Type with
                        | s when s = "varchar" -> 
                            c.Length
                            |> function
                            | Some n -> n |> Table.VarChar 
                            | None   -> Table.VarCharMax
                        | s when s = "float"    -> Table.Float
                        | s when s = "datetime" -> Table.DateTime
                        | s when s = "int" -> Table.Int
                        | _ -> 
                            $"could not map column {c.Type}" 
                            |> failwith
                    Table.IsKey =
                        c.Name = "pat_id" || c.Name = "pat_timestamp"
                }
            )

        let boxData ds =
            ds.Data
            |> List.map (fun (id, dt, row) ->
                [ 
                    id |> box
                    
                    dt 
                    |> function
                    | Exact dt   -> dt |> box
                    | Relative n -> n  |> box
                    
                    yield! row |> List.map Value.boxValue
                ]
            )

        let db = Database.create connBuilder.DataSource dbName
        let columns = ds |> mapColumns
        
        match Table.tableExists db.ConnectionString dbName tblName, dropTable with 
        | false, _ ->
            columns
            |> Table.createTable db.ConnectionString tblName
            |> function
            | false -> 
                $"could not create tabel {tblName} in database {dbName}"
                |> failwith
            | true ->
                Table.bulkInsert columns (ds |> boxData) tblName db

        | true, true ->
            Table.dropTable db.ConnectionString tblName |> ignore

            columns
            |> Table.createTable db.ConnectionString tblName
            |> function
            | false -> 
                $"could not create tabel {tblName} in database {dbName}"
                |> failwith
            | true ->
                Table.bulkInsert columns (ds |> boxData) tblName db
                
        | true, false ->
            Table.bulkInsert columns (ds |> boxData) tblName db