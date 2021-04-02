namespace Informedica.Observations.Lib


module Definitions =

    open ClosedXML.Excel

    open Informedica.Observations.Lib
    open Types

    module Tables =

        [<Literal>]
        let collapseTable = "CollapseTable"
        [<Literal>]
        let convertTable = "ConvertTable"
        [<Literal>]
        let filterTable = "FilterTable"
        [<Literal>]
        let observationsTable = "ObservationsTable"
        [<Literal>]
        let signalsTable = "SignalsTable"


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


    let readXML path convertMap filterMap collapseMap =
        let getString (cell : IXLCell) = 
            cell.GetString().Trim().ToLower()

        let getInt (cell : IXLCell) = 
            match cell.TryGetValue<float>() with
            | true, d -> d |> int
            | _ -> 0

        Workbook.open' path
        |> fun wb ->
            wb
            |> Workbook.getTable Tables.observationsTable
            |> fun tbl ->
                let convertFns =
                    wb
                    |> Workbook.getTable Tables.convertTable
                    |> fun tbl ->
                        tbl.Rows()
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
                    |> Workbook.getTable Tables.signalsTable
                    |> fun tbl ->
                        tbl.Rows()
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
                                        f.id = id || 
                                        f.name = name
                                    ) 
                                    |> List.map (fun f -> f.convertFn)
                                    |> mapFns convertMap
                            }
                        )
                    |> Seq.toList

                let filterFns =
                    wb
                    |> Workbook.getTable Tables.filterTable
                    |> fun tbl ->
                        tbl.Rows()
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
                    |> Workbook.getTable Tables.collapseTable
                    |> fun tbl ->
                        tbl.Rows()
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

                tbl.Rows()
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
