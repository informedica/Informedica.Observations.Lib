namespace Informedica.Observations.Lib


module Definitions =

    open Informedica.Observations.Lib
    open Types


    let read path conversionsMap filtersMap collapseMap =
        Workbook.open' path
        |> fun wb ->
            wb
            |> Workbook.getTable "Table1"
            |> fun tbl1 ->
                let srcs =
                    wb 
                    |> Workbook.getTable "Table2"
                    |> fun tbl2 ->
                        tbl2.Rows()
                        |> Seq.map (fun r ->
                            r.Cell(1).GetString().Trim().ToLower(),
                            {
                                Id = 
                                    match r.Cell(2).TryGetValue<float>() with
                                    | true, x -> x |> int 
                                    | _ -> 0
                                Name = r.Cell(3).GetString()
                                Conversions = 
                                    r.Cell(4).GetString()
                                    |> conversionsMap
                            }
                        )

                tbl1.Rows()
                |> Seq.skip 1 // skip header row
                |> Seq.map (fun r ->
                    let name = r.Cell(1).GetString().ToLower().Trim()
                    {
                        Name = r.Cell(1).GetString() 
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
                        Filters = r.Cell(5).GetString () |> filtersMap
                        Collapse = r.Cell(6).GetString () |> collapseMap 
                    }
                )
