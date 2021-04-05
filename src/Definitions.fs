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


    // TODO: need to apply this in both read functions
    let mapFns mapper xs =
        xs 
        |> List.map (fun s ->
            s
            |> mapper
            |> function
            | Some fn -> Some fn
            | None    -> 
                $"could not find function for {s}"
                |> printfn "%s" 
                None
        )
        |> List.filter Option.isSome
        |> List.map Option.get

    // TODO make one read function
    let readXML path convertMap filterMap collapseMap =
        let getString (cell : IXLCell) = 
            cell.GetString().Trim().ToLower()

        let getInt (cell : IXLCell) = 
            match cell.TryGetValue<float>() with
            | true, d -> d |> int
            | _ -> 0

        Workbook.open' path
        |> fun wb ->
            let convertFns =
                wb
                |> Workbook.getCurrentRegion Sheets.convert
                |> fun rng ->
                    rng.Rows()
                    |> Seq.skip 1
                    |> Seq.map (fun r ->
                        {|
                            id        = r.Cell(1) |> getInt
                            name      = r.Cell(2) |> getString
                            convertFn = r.Cell(3) |> getString
                        |}
                    )
                |> Seq.toList

            let srcs =
                wb 
                |> Workbook.getCurrentRegion Sheets.sources
                |> fun rng ->
                    rng.Rows()
                    |> Seq.skip 1
                    |> Seq.map (fun r ->
                        let id   = r.Cell(2) |> getInt
                        let name = r.Cell(3) |> getString
                        r.Cell(1) |> getString,
                        {
                            Id = id
                            Name = name
                            Conversions = 
                                convertFns
                                |> List.filter (fun f -> 
                                    // TODO: need to extract this duplicate logic
                                    f.id = id || (f.id = 0 && f.name = name)
                                ) 
                                |> List.map (fun f -> f.convertFn)
                                |> mapFns convertMap
                        }
                    )
                |> Seq.toList

            let filterFns =
                wb
                |> Workbook.getCurrentRegion Sheets.filter
                |> fun rng ->
                    rng.Rows()
                    |> Seq.skip 1
                    |> Seq.map (fun r ->
                        {|
                            name     = r.Cell(1) |> getString
                            filterFn = r.Cell(2) |> getString
                        |}
                    )
                |> Seq.toList

            let collapseFns =
                wb
                |> Workbook.getCurrentRegion Sheets.collapse
                |> fun rng ->
                    rng.Rows()
                    |> Seq.skip 1
                    |> Seq.map (fun r ->
                        {|
                            name       = r.Cell(1) |> getString
                            collapseFn = r.Cell(2) |> getString
                        |}
                    )
                |> Seq.toList

            let getFirst = 
                fun sgns ->
                    sgns
                    |> List.tryHead
                    |> function 
                    | Some x -> x.Value
                    | None   -> NoValue

            wb
            |> Workbook.getCurrentRegion Sheets.observations
            |> fun rng ->
                rng.Rows()
                |> Seq.skip 1 // skip header row
                |> Seq.map (fun r ->
                    let name = r.Cell(1) |> getString
                    {
                        Name = name
                        Type = r.Cell(2).GetString()
                        Length = 
                            match r.Cell(3).TryGetValue<float>() with
                            | true, x -> x |> int |> Some
                            | _ -> None
                        Sources = 
                            srcs
                            |> Seq.filter (fst >> ((=) name))
                            |> Seq.map snd
                            |> Seq.toList 
                        Filters = 
                            filterFns
                            |> List.filter (fun x -> x.name = name)
                            |> List.map (fun x -> x.filterFn)
                            |> mapFns filterMap
                        Collapse = 
                            collapseFns
                            |> Seq.tryFind (fun x -> x.name = name)
                            |> function
                            | Some x -> 
                                x.collapseFn 
                                |> collapseMap
                                |> function
                                | Some fn -> fn
                                | None    ->
                                    $"could not find function for {x.collapseFn}"
                                    |> printfn "%s"
                                    getFirst
                            | None -> getFirst

                        }
                    )
                    |> Seq.toList
        

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


    let parseCSV s =
        s
        |> String.split "\n"
        |> Seq.filter (String.isNullOrWhiteSpace >> not)
        |> Seq.map (String.replace "\",\"" "||")
        |> Seq.map (String.replace "\"" "")
        |> Seq.map (String.split "||")


    let getSheet docId sheet = 
        createUrl docId sheet
        |> download
        |> Async.RunSynchronously
        |> parseCSV
        //|> Seq.map (String.split "\",\"" >> (Array.map (String.replace "\"" "")))


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

    // TODO: make one read function
    let readGoogle docId convertMap filterMap collapseMap =
        let getSheet = getSheet docId

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
                    id = r |> getColumn<int> columns "source_id"
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
                let id = r |> getColumn<int> columns "id"
                let name = r |> getColumn<string> columns "name"
                r |> getColumn<string> columns "observation",
                {
                    Id = id
                    Name = name
                    Conversions = 
                        convertFns
                        |> List.filter (fun x ->
                            x.id = id || (x.id = 0 && x.name = name)
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

        obs.rows
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


