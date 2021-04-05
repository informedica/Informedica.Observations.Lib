
#r "nuget: ClosedXML, 0.95.4"

#load "../ClosedXML.fs"

open System

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

open Informedica.Observations.Lib


module Workbook =

    open ClosedXML.Excel

    let getCurrentRegion (sheetName : string) (wb : XLWorkbook) =
        match wb.TryGetWorksheet(sheetName) with
        | true, sheet ->
            sheet.FirstCell().CurrentRegion
        | _ -> 
            $"could not get sheet: {sheetName}" |> failwith


Workbook.open' "../../data/Definitions.xlsx"
|> Workbook.getCurrentRegion "Observations"
