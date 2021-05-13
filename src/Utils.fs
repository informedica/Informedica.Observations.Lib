namespace Informedica.Observations.Lib



module List =

    let median (xs : float list) =
        match xs with
        | [] ->  None
        | [x] -> Some x
        | _ ->
            let c = xs |> List.length

            if c % 2 = 0 then
                (xs.[(c / 2) - 1] + xs.[(c / 2)]) / 2.
            else 
                xs.[(c / 2)]
            |> Some



module Array =

    let median (xs : float []) =
        match xs with
        | [||] ->  None
        | [|x|] -> Some x
        | _ ->
            let c = xs |> Array.length

            if c % 2 = 0 then
                (xs.[(c / 2) - 1] + xs.[(c / 2)]) / 2.
            else 
                xs.[(c / 2)]
            |> Some


module Utils =

    open System
    open System.Collections.Generic
    open Informedica.Utils.Lib.BCL

    let getParams s =
        "\(([^)]+)\)"
        |> String.regex
        |> fun regex -> 
            regex.Match(s).Value
            |> String.split ","
            |> List.filter (String.isNullOrWhiteSpace >> not)
            |> List.map (String.replace "(" "")
            |> List.map (String.replace ")" "")
            |> List.collect (String.split ",")


    /// Returns enviroment variables as a dictionary
    let environmentVars() =
        let variables = Dictionary<string, string>()
        let userVariables = Environment.GetEnvironmentVariables(EnvironmentVariableTarget.User)
        let processVariables = Environment.GetEnvironmentVariables(EnvironmentVariableTarget.Process)

        for pair in userVariables do
            let variable = unbox<Collections.DictionaryEntry> pair
            let key = unbox<string> variable.Key
            let value = unbox<string> variable.Value
            if not (variables.ContainsKey(key)) && key <> "PATH" then variables.Add(key, value)
            
        for pair in processVariables do
            let variable = unbox<Collections.DictionaryEntry> pair
            let key = unbox<string> variable.Key
            let value = unbox<string> variable.Value
            if not (variables.ContainsKey(key)) && key <> "PATH" then variables.Add(key, value)

        variables


