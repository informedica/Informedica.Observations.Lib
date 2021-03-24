namespace Informedica.Observations.Lib

module Workbook =
    
    open System.Data
    open ClosedXML.Excel

    let create () = 
        let wb = new XLWorkbook()
        wb

    let open' (path : string) =
        let wb = new XLWorkbook(path)
        wb

    let saveAs (path: string) (wb : XLWorkbook) = wb.SaveAs(path)
 

    let addSheet (name : string) (wb : XLWorkbook) = 
        wb.AddWorksheet(name) |> ignore
        wb

    let addTable (row : int, column : int) (data : obj[] array ) (sheetName : string) (wb : XLWorkbook) =
        match wb.Worksheets.TryGetWorksheet(sheetName) with
        | true, sheet ->
            match data |> Array.tryHead with
            | Some h -> 
                let dt = new DataTable()
                h |> Array.iter (fun c -> dt.Columns.Add(c |> string) |> ignore)
                data |> Array.tail |> Array.iter (fun r -> r |> dt.Rows.Add |> ignore)
                sheet.Cell(row, column).InsertTable(dt) |> ignore
                wb
            | None -> wb  
        | _ -> 
            $"cannot find sheet with name: {sheetName}" |> printfn "%s"
            wb

    let getTable name (wb : XLWorkbook) =
        wb.Table(name)
