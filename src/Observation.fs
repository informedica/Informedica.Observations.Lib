namespace Informedica.Observations.Lib


module Observation =

    open Types
    open Informedica.Utils.Lib.BCL

    let create name type' length filterFns collapseFn sources =
        {
            Name = name
            Type = type'
            Length = length
            Filters = filterFns
            Collapse = collapseFn
            Sources = sources
        }


    let createSource id name convertFns =
        {
            Id = id
            Name = name
            Conversions = convertFns
        }


    let mapToObservations definitions =
        definitions 
        |> List.map (fun (name, type', length, filters, collapseFn, sources) ->
            let sources =
                sources 
                |> List.map (fun (id, name, convertFn) -> createSource id name convertFn)
            create name type' length filters collapseFn sources
        )


    let signalBelongsToSource (signal : Signal) (source : Source) =
        // match on id
        if signal.Id.IsSome then signal.Id.Value = source.Id
        // match on name if source has a name
        else
            if source.Name |> String.isNullOrWhiteSpace then false
            else
                signal.Name.Trim().ToLower() = source.Name.Trim().ToLower()

