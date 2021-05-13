namespace Informedica.Observations.Lib


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


    let setSignalText text signal =
        { signal with 
            Value = 
                if text = "" then NoValue
                else text |> Text
        }


    let maxLength l : Convert =
        fun signal ->
            { signal with
                Value = 
                    match signal.Value with
                    | Text s -> s.Substring(0, l) |> Text
                    | _ -> signal.Value
            }
