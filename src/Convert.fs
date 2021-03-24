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


    let setSignalText signal text =
        { signal with 
            Value = 
                if text = "" then NoValue
                else text |> Text
        }
