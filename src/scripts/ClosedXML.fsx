
#r "nuget: ClosedXML, 0.95.4"

#load "../ClosedXML.fs"

open System

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

open Informedica.Observations.Lib


Workbook.open' "../../data/Definitions.xlsx"
|> Workbook.getTable "ConvertTable"
|> fun tbl -> 
    tbl.Rows()
    |> Seq.head
    |> fun r -> r.Cell(1).GetString()