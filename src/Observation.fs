namespace Informedica.Observations.Lib


module Observation =

    open Types


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
        if signal.Id.IsSome then signal.Id.Value = source.Id
        else
            signal.Name.Trim().ToLower() = source.Name.Trim().ToLower()

