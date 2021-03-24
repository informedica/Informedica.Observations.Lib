namespace Informedica.Observations.Lib


module Signal =

    open Types


    let create id name value valid patId ts =
        {
            Id = id 
            Name = name
            Value = value
            Validated = valid
            PatientId = patId
            TimeStamp = ts
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


    let isValid (signal : Signal) = signal.Validated


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
        match signal.TimeStamp with
        | SomeDateTime _ -> true
        | _              -> false


    let dateTimeIsPeriod (signal : Signal) =
        match signal.TimeStamp with
        | Period _ -> true
        | _              -> false


    let periodToDateTime (signal : Signal) =
        match signal.TimeStamp with
        | Period (start, stop) ->
            [0. .. (stop - start).TotalMinutes ]
            |> List.map (fun min ->
                {
                    signal with
                        TimeStamp = start.AddMinutes(min) |> SomeDateTime
                }
            )
        | _ -> [ signal ]


    let getDateTimeValue (signal : Signal) =
        match signal.TimeStamp with
        | SomeDateTime dt -> dt
        | _ -> 
            $"Cannot get datetime from: {signal}"
            |> failwith
