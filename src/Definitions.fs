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
        |> Array.tryFindIndex ((=) s)
        |> function
        | None   -> 
            $"""cannot find column {s} in {columns |> String.concat ", "}"""
            |> failwith
        | Some i -> sl |> Array.item i
        |> tryCast<'T>


    let parseCSV (s : string) =
        s.Split("\n")
        |> Array.filter (String.isNullOrWhiteSpace >> not)
        |> Array.map (String.replace "\",\"" "|")
        |> Array.map (String.replace "\"" "")
        |> Array.map (fun s -> s.Split("|"))


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
                    |> Seq.toArray
                )
                |> Seq.toArray
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
            let columns = data |> Array.head
            
            data
            |> Array.tail
            |> Array.map (fun r ->
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
            |> Array.filter (fun x -> x.convertFn |> Option.isSome)

        let srcs =
            let data = getSheet Sheets.sources
            let columns = data |> Array.head
            
            data 
            |> Array.tail
            |> Array.map (fun r ->
                let observation = r |> getColumn<string> columns "observation"
                let id = r |> getColumn<string option> columns "id"
                let name = r |> getColumn<string> columns "name"
                observation,
                {
                    Id = id
                    Name = name
                    Conversions = 
                        convertFns
                        |> Array.filter (fun x -> x.observation = observation)
                        |> Array.filter (fun x ->
                            (x.id.IsSome && x.id = id) || 
                            (name |> String.isNullOrWhiteSpace |> not && x.name = name)
                        )
                        |> Array.map (fun x -> x.convertFn.Value)
                }
            )

        let filters =
            let data = getSheet Sheets.filter
            let columns = data |> Array.head

            data
            |> Array.tail
            |> Array.map (fun r ->
                {|
                    observation = r |> getColumn<string> columns "observation"
                    filterFn = 
                        r 
                        |> getColumn<string> columns "function"
                        |> msgMap "filter" filterMap
                |}
            )
            |> Array.filter (fun x -> x.filterFn |> Option.isSome)

        let collapse =
            let data = getSheet Sheets.collapse
            let columns = data |> Array.head

            data
            |> Array.tail
            |> Array.map (fun r ->
                {|
                    observation = r |> getColumn<string> columns "observation"
                    collapseFn = 
                        r 
                        |> getColumn<string> columns "function"
                        |> msgMap "collapse" collapseMap
                |}
            )
            |> Array.filter (fun x -> x.collapseFn |> Option.isSome)

        let data = getSheet Sheets.observations
        let columns = data |> Array.head
        data
        |> Array.tail        
        |> Array.map (fun r ->
            let name   = r |> getColumn<string> columns "name"
            let length = r |> getColumn<int option> columns "length"
            {
                Name = name
                Type = r |> getColumn<string> columns "type"
                Length = length

                Filters = 
                    filters
                    |> Array.filter (fun x -> x.observation = name)
                    |> Array.map (fun x -> x.filterFn.Value)

                Sources = 
                    srcs
                    |> Array.filter (fst >> ((=) name))
                    |> Array.map (fun (_, s) ->
                        { s with
                            Conversions = 
                                match length with
                                | None   -> s.Conversions
                                | Some l -> s.Conversions |> Array.append [| (Convert.maxLength l) |]
                        }
                    )

                Collapse = 
                    collapse
                    |> Array.tryFind (fun x -> x.observation = name)
                    |> function
                    | Some x -> x.collapseFn.Value
                    | None   -> Collapse.toFirst
            }
        ) 


    let readGoogle docId = read (Google docId)

    let readXML path = read (Excel path)


