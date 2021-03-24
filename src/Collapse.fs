namespace Informedica.Observations.Lib


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


