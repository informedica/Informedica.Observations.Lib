namespace Informedica.Observations.Lib


module Definitions =

    open System
    open System.IO
    open System.Net

    open ClosedXML.Excel
    open Informedica.Utils.Lib
    open Informedica.Utils.Lib.BCL

    open Types


    module Sheets =

        [<Literal>]
        let observations = "Observations"

        [<Literal>]
        let sources = "Sources"

        [<Literal>]
        let collapse = "Collapse"
        
        [<Literal>]
        let filter = "filter"
        
        [<Literal>]
        let convert = "Convert"


    let readAllLines path =
        path
        |> File.ReadAllLines


    let createUrl id sheet = 
        $"https://docs.google.com/spreadsheets/d/{id}/gviz/tq?tqx=out:csv&sheet={sheet}"


    let download url =
        printfn "downloading:\n%s" url
        async {
            let req = WebRequest.Create(Uri(url))
            use! resp = req.AsyncGetResponse()
            use stream = resp.GetResponseStream()
            use reader = new StreamReader(stream)
            return reader.ReadToEnd()
        }    



    let tryCast<'T> (x : string) : 'T =
        match x with
        | _ when typeof<'T> = typeof<string> -> x :> obj :?> 'T
        | _ when typeof<'T> = typeof<int> ->
            match Int32.TryParse(x) with
            | true, n -> n
            | _       -> 0
            :> obj :?> 'T
        | _ when typeof<'T> = typeof<int option> ->
            match Int32.TryParse(x) with
            | true, n -> n |> Some
            | _       -> None
            :> obj :?> 'T
        | _ when typeof<'T> = typeof<string option> ->
            if x |> String.isNullOrWhiteSpace then None
            else x |> Some
            :> obj :?> 'T
        | _ when typeof<'T> = typeof<DateTime option> ->
            match DateTime.TryParse(x) with
            | true, dt -> dt |> Some
            | _        -> None
            :> obj :?> 'T
        | _ -> 
            $"cannot cast this {typeof<'T>}"
            |> failwith


    let getColumn<'T> columns s sl =
        columns
        |> List.tryFindIndex ((=) s)
        |> function
        | None   -> 
            $"""cannot find column {s} in {columns |> String.concat ", "}"""
            |> failwith
        | Some i -> sl |> Seq.item i
        |> tryCast<'T>


    let parseCSV s =
        s
        |> String.split "\n"
        |> List.filter (String.isNullOrWhiteSpace >> not)
        |> List.map (String.replace "\",\"" "|")
        |> List.map (String.replace "\"" "")
        |> List.map (String.split "|")


    let getSheet (wb : XLWorkbook option) (docId : string option) sheet = 
        let getString (cell : IXLCell) = 
            cell.GetString().Trim().ToLower()

        match wb, docId with
        | None, Some docId ->
            createUrl docId sheet
            |> download
            |> Async.RunSynchronously
            |> parseCSV

        | Some wb, None ->
            wb
            |> Workbook.getCurrentRegion sheet
            |> fun rng ->
                rng.Rows()
                |> Seq.map (fun r ->
                    r.Cells()
                    |> Seq.map getString
                    |> Seq.toList
                )
                |> Seq.toList
        | Some _, Some _
        | None, None -> 
            "cannot get sheet without a workbook or a docId (or both supplied)" 
            |> failwith


    type ReadSource = | Google of string | Excel of string


    // TODO: make one read function
    let read readSource convertMap filterMap collapseMap =
        let getSheet = 
            match readSource with
            | Google docId -> getSheet None (Some docId)
            | Excel path   ->
                let wb = Workbook.open' path
                getSheet (Some wb) None

        let msgMap n mapper s =
            s
            |> mapper
            |> function
            | Some x -> Some x
            | None -> 
                if s = "" then None
                else
                    $"could not find {n} {s}" |> printfn "%s"
                    None

        let convertFns = 
            let data = getSheet Sheets.convert
            let columns = data |> List.head
            
            data
            |> List.tail
            |> List.map (fun r ->
                {|
                    observation = r |> getColumn<string> columns "observation"
                    id = r |> getColumn<string option> columns "source_id"
                    name = r |> getColumn<string> columns "source_name"
                    convertFn = 
                        r 
                        |> getColumn<string> columns "function"
                        |> msgMap "convert" convertMap
                |}
            )
            |> List.filter (fun x -> x.convertFn |> Option.isSome)

        let srcs =
            let data = getSheet Sheets.sources
            let columns = data |> List.head
            
            data 
            |> List.tail
            |> List.map (fun r ->
                let observation = r |> getColumn<string> columns "observation"
                let id = r |> getColumn<string option> columns "id"
                let name = r |> getColumn<string> columns "name"
                observation,
                {
                    Id = id
                    Name = name
                    Conversions = 
                        convertFns
                        |> List.filter (fun x -> x.observation = observation)
                        |> List.filter (fun x ->
                            (x.id.IsSome && x.id = id) || 
                            (name |> String.isNullOrWhiteSpace |> not && x.name = name)
                        )
                        |> List.map (fun x -> x.convertFn.Value)
                }
            )

        let filters =
            let data = getSheet Sheets.filter
            let columns = data |> List.head

            data
            |> List.tail
            |> List.map (fun r ->
                {|
                    observation = r |> getColumn<string> columns "observation"
                    filterFn = 
                        r 
                        |> getColumn<string> columns "function"
                        |> msgMap "filter" filterMap
                |}
            )
            |> List.filter (fun x -> x.filterFn |> Option.isSome)

        let collapse =
            let data = getSheet Sheets.collapse
            let columns = data |> List.head

            data
            |> List.tail
            |> List.map (fun r ->
                {|
                    observation = r |> getColumn<string> columns "observation"
                    collapseFn = 
                        r 
                        |> getColumn<string> columns "function"
                        |> msgMap "collapse" collapseMap
                |}
            )
            |> List.filter (fun x -> x.collapseFn |> Option.isSome)

        let data = getSheet Sheets.observations
        let columns = data |> List.head
        data
        |> List.tail        
        |> List.map (fun r ->
            let name = r |> getColumn<string> columns "name"
            {
                Name = name
                Type = r |> getColumn<string> columns "type"
                Length = r |> getColumn<int option> columns "length"

                Filters = 
                    filters
                    |> List.filter (fun x -> x.observation = name)
                    |> List.map (fun x -> x.filterFn.Value)

                Sources = 
                    srcs
                    |> List.filter (fst >> ((=) name))
                    |> List.map snd

                Collapse = 
                    collapse
                    |> List.tryFind (fun x -> x.observation = name)
                    |> function
                    | Some x -> x.collapseFn.Value
                    | None   -> Collapse.toFirst
            }
        ) 


    let readGoogle docId = read (Google docId)

    let readXML path = read (Excel path)


