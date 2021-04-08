namespace Informedica.Observations.Lib


module Collapse =

    open Types


    let toFirst : Collapse =
        fun signals ->
            match signals |> List.tryHead with
            | None -> NoValue
            | Some signal -> signal.Value


    let calcValue f signals =
        signals
        |> List.filter Signal.isNumeric
        |> function 
        | [] -> NoValue
        | xs -> 
            xs 
            |> List.map (Signal.getNumericValue >> Option.get)
            |> f
            |> Numeric


    let sum : Collapse = calcValue List.sum

    let average : Collapse = calcValue List.max

    let max : Collapse = calcValue List.max

    let min : Collapse = calcValue List.min

    let median : Collapse = 
        fun signals ->
            signals
            |> List.filter Signal.isNumeric
            |> function 
            | [] -> NoValue
            | xs -> 
                xs 
                |> List.map (Signal.getNumericValue >> Option.get)
                |> List.median
                |> function
                | None -> NoValue
                | Some x -> x |> Numeric
