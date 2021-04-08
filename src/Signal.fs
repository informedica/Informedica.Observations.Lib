namespace Informedica.Observations.Lib


module Value =

    open Types

    let boxValue = function
        | NoValue     -> null |> box
        | Text s      -> s |> box
        | Numeric x   -> x |> box
        | DateTime dt -> dt |> box



module Signal =

    open Types


    /// <summary>
    /// Create a `Signal`
    /// </summary>
    /// <param name="id">Optional string id</param>
    /// <param name="name">Name of the signal</param>
    /// <param name="value">The value of the signal</param>
    /// <param name="valid">Whether the signal is validated</param>
    /// <param name="patId">The patient id</param>
    /// <param name="ts">A timestamp of the signal</param>
    /// <returns>`Signal`</returns>
    let create id name value valid patId ts =
        {
            Id = id 
            Name = name
            Value = value
            Validated = valid
            PatientId = patId
            TimeStamp = ts
        }


    /// <summary>
    /// Create a `Signal` with an `string` `SignalId`
    /// </summary>
    /// <param name="id">Optional string id</param>
    /// <param name="name">Name of the signal</param>
    /// <param name="value">The value of the signal</param>
    /// <param name="valid">Whether the signal is validated</param>
    /// <param name="patId">The patient id</param>
    /// <param name="ts">A timestamp of the signal</param>
    /// <returns>`Signal`</returns>
    let createWithId id name value valid patId ts = 
        create (id |> Some)  name value valid patId ts


    /// <summary>
    /// Create a `Signal` with an `string` `SignalId`
    /// </summary>
    /// <param name="id">Optional string id</param>
    /// <param name="name">Name of the signal</param>
    /// <param name="value">The value of the signal</param>
    /// <param name="patId">The patient id</param>
    /// <param name="ts">A timestamp of the signal</param>
    /// <returns>`Signal`</returns>
    let createWithIdValidated id name value patId ts = 
        create (id |> Some)  name value true patId ts


    /// <summary>
    /// Create a `Signal` without a `SignalId`
    /// </summary>
    /// <param name="name">Name of the signal</param>
    /// <param name="value">The value of the signal</param>
    /// <param name="valid">Whether the signal is validated</param>
    /// <param name="patId">The patient id</param>
    /// <param name="ts">A timestamp of the signal</param>
    /// <returns>Signal</returns>
    let createNoId name value valid patId ts = 
        create None name value valid patId ts 


    /// <summary>
    /// Create a `Signal` without a `SignalId`
    /// </summary>
    /// <param name="name">Name of the signal</param>
    /// <param name="value">The value of the signal</param>
    /// <param name="patId">The patient id</param>
    /// <param name="ts">A timestamp of the signal</param>
    /// <returns>Signal</returns>
    let createNoIdValidated name value patId ts = 
        create None name value true patId ts 


    /// <summary>
    /// Create a `Signal` with a `Text` `Value`
    /// </summary>
    /// <param name="id">Optional string id</param>
    /// <param name="name">Name of the signal</param>
    /// <param name="value">The value of the signal</param>
    /// <param name="valid">Whether the signal is validated</param>
    /// <param name="patId">The patient id</param>
    /// <param name="ts">A timestamp of the signal</param>
    /// <returns>`Signal`</returns>
    let createText id name value valid patId ts =
        let value = value |> Text
        create id name value valid patId ts


    /// <summary>
    /// Create a `Signal` with a `Numeric` `Value`
    /// </summary>
    /// <param name="id">Optional string id</param>
    /// <param name="name">Name of the signal</param>
    /// <param name="value">The value of the signal</param>
    /// <param name="valid">Whether the signal is validated</param>
    /// <param name="patId">The patient id</param>
    /// <param name="ts">A timestamp of the signal</param>
    /// <returns>`Signal`</returns>
    let createNumeric id name value valid patId ts =
        let value = value |> Numeric
        create id name value valid patId ts
        

    /// <summary>
    /// Create a `Signal` with a `DateTime` `Value`
    /// </summary>
    /// <param name="id">Optional string id</param>
    /// <param name="name">Name of the signal</param>
    /// <param name="value">The value of the signal</param>
    /// <param name="valid">Whether the signal is validated</param>
    /// <param name="patId">The patient id</param>
    /// <param name="ts">A timestamp of the signal</param>
    /// <returns>`Signal`</returns>
    let createDateTime id name value valid patId ts =
        let value = value |> DateTime
        create id name value valid patId ts


    /// <summary>
    /// Create a `Signal` without a `Value`
    /// </summary>
    /// <remarks>
    /// Assumes validate = `true`
    /// </remarks>
    /// <param name="id">Optional string id</param>
    /// <param name="name">Name of the signal</param>
    /// <param name="patId">The patient id</param>
    /// <param name="ts">A timestamp of the signal</param>
    /// <returns>`Signal`</returns>
    let createNoValue id name patId ts =
        create id name NoValue true patId ts


    /// <summary>
    /// Check whether a `Signal` has `int` id `i`
    /// </summary>
    /// <param name="id">The `string` id</param>
    /// <param name="signal">The `Signal` to check</param>
    /// <returns>`bool`</returns>
    let hasId id (signal : Signal) =
        match signal.Id with
        | Some x -> id = x
        | None    -> false


    /// <summary>
    /// Checks whether a `Signal` is valid 
    /// </summary>
    /// <param name="signal">The `Signal` to check</param>
    /// <returns></returns>
    let isValid (signal : Signal) = signal.Validated


    /// <summary>
    /// Get the numeric value as an option
    /// </summary>
    /// <param name="signal"></param>
    /// <returns>`float option`</returns>
    let getNumericValue (signal : Signal) =
        match signal.Value with
        | Numeric x -> x |> Some
        | _         -> None


    /// <summary>
    /// 
    /// </summary>
    /// <param name="signal"></param>
    /// <returns></returns>
    let valueToString (signal : Signal) =
        match signal.Value with
        | NoValue     -> ""
        | Text s      -> s
        | Numeric x   -> x |> sprintf "%A"
        | DateTime dt -> dt.ToString("dd-MM-yyyy HH:mm")


    let hasValue (signal : Signal) = signal.Value = NoValue |> not


    let isNumeric (signal : Signal) = 
        match signal.Value with
        | Numeric _ -> true
        | _ -> false 


    let isNonNumeric = isNumeric >> not


    /// <summary>
    /// Checks whether a signal has a specific datetime
    /// timestamp
    /// </summary>
    /// <param name="signal"></param>
    /// <returns>`bool`</returns>
    let dateTimeIsSome (signal : Signal) =
        match signal.TimeStamp with
        | SomeDateTime _ -> true
        | _              -> false


    /// <summary>
    /// Checks whether a signal has a period timestamp
    /// </summary>
    /// <param name="signal"></param>
    /// <returns>`bool`</returns>
    let dateTimeIsPeriod (signal : Signal) =
        match signal.TimeStamp with
        | Period _ -> true
        | _              -> false


    /// <summary>
    /// Turns a period signal to a list of 
    /// signals with specific datetime ts in that period
    /// </summary>
    /// <param name="signal"></param>
    /// <returns>`Signal list`</returns>
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


    /// <summary>
    /// Get the date time of a signal.
    /// </summary>
    /// <remarks>
    /// Throws an exception when the signal timestamp is not
    /// a datetime, i.e. is a period or has no datetime
    /// </remarks>
    /// <param name="signal"></param>
    /// <returns>`DateTime`</returns>
    let getDateTimeValue (signal : Signal) =
        match signal.TimeStamp with
        | SomeDateTime dt -> dt
        | _ -> 
            $"Cannot get datetime from: {signal}"
            |> failwith
