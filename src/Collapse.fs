namespace Informedica.Observations.Lib


module Collapse =

    open Types


    let toFirst : Collapse =
        fun signals ->
            match signals |> Array.tryHead with
            | None -> NoValue
            | Some signal -> signal.Value


    let calcValue f signals =
        signals
        |> Array.filter Signal.isNumeric
        |> function 
        | [||] -> NoValue
        | xs -> 
            xs 
            |> Array.map (Signal.getNumericValue >> Option.get)
            |> f
            |> Numeric


    let sum : Collapse = calcValue Array.sum

    let average : Collapse = calcValue Array.max

    let max : Collapse = calcValue Array.max

    let min : Collapse = calcValue Array.min

    let median : Collapse = 
        fun signals ->
            signals
            |> Array.filter Signal.isNumeric
            |> function 
            | [||] -> NoValue
            | xs -> 
                xs 
                |> Array.map (Signal.getNumericValue >> Option.get)
                |> Array.median
                |> function
                | None -> NoValue
                | Some x -> x |> Numeric
