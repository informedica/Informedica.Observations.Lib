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



    let tryCast<'T> (x : string) :  'T =
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
        | _ -> 
            $"cannot cast this {typeof<'T>}"
            |> failwith


    let getColumn<'T> columns s sl =
        columns
        |> Seq.tryFindIndex ((=) s)
        |> function
        | None   -> 
            sprintf "cannot find %s" s
            |> failwith
        | Some i -> sl |> Seq.item i
        |> tryCast<'T>


    let parseCSV s =
        s
        |> String.split "\n"
        |> List.filter (String.isNullOrWhiteSpace >> not)
        |> List.map (String.replace "\",\"" "||")
        |> List.map (String.replace "\"" "")
        |> List.map (String.split "||")


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

        let obs =
            let data = getSheet Sheets.observations
            {|
                columns = data |> Seq.head
                rows = data |> Seq.tail
            |}

        let convertFns = 
            let data = getSheet Sheets.convert
            let columns = data |> Seq.head
            
            data
            |> Seq.tail
            |> Seq.map (fun r ->
                {|
                    id = r |> getColumn<string option> columns "source_id"
                    name = r |> getColumn<string> columns "source_name"
                    convertFn = 
                        r 
                        |> getColumn<string> columns "function"
                        |> msgMap "convert" convertMap
                |}
            )
            |> Seq.filter (fun x -> x.convertFn |> Option.isSome)
            |> Seq.toList

        let srcs =
            let data = getSheet Sheets.sources
            let columns = data |> Seq.head
            
            data 
            |> Seq.tail
            |> Seq.map (fun r ->
                let id = r |> getColumn<string option> columns "id"
                let name = r |> getColumn<string> columns "name"
                r |> getColumn<string> columns "observation",
                {
                    Id = id
                    Name = name
                    Conversions = 
                        convertFns
                        |> List.filter (fun x ->
                            (x.id.IsSome && x.id = id) || x.name = name
                        )
                        |> List.map (fun x -> x.convertFn.Value)
                }
            )
            |> Seq.toList

        let filters =
            let data = getSheet Sheets.filter
            let columns = data |> Seq.head

            data
            |> Seq.tail
            |> Seq.map (fun r ->
                {|
                    observation = r |> getColumn<string> columns "observation"
                    filterFn = 
                        r 
                        |> getColumn<string> columns "function"
                        |> msgMap "filter" filterMap
                |}
            )
            |> Seq.filter (fun x -> x.filterFn |> Option.isSome)
            |> Seq.toList

        let collapse =
            let data = getSheet Sheets.collapse
            let columns = data |> Seq.head

            data
            |> Seq.tail
            |> Seq.map (fun r ->
                {|
                    observation = r |> getColumn<string> columns "observation"
                    collapseFn = 
                        r 
                        |> getColumn<string> columns "function"
                        |> msgMap "collapse" collapseMap
                |}
            )
            |> Seq.filter (fun x -> x.collapseFn |> Option.isSome)
            |> Seq.toList

        let data = getSheet Sheets.observations
        let columns = data |> Seq.head
        data
        |> Seq.tail        
        |> Seq.map (fun r ->
            let name = r |> getColumn<string> obs.columns "name"
            {
                Name = r |> getColumn<string> obs.columns "name"
                Type = r |> getColumn<string> obs.columns "type"
                Length = r |> getColumn<int option> obs.columns "length"

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
        |> Seq.toList


    // TODO: make one read function
    let readGoogle docId = read (Google docId)
    let readXML path = read (Excel path)


