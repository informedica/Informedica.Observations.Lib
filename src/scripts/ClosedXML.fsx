
#r "nuget: ClosedXML, 0.95.4"

#load "Database.fsx"
#load "../Types.fs"
#load "../Signal.fs"
#load "../Observation.fs"

open System

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__


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


let inline arrayBox xs = xs |>  Seq.map box |> Seq.toArray 


let defs =
    [ 
        ("pat_gender", "toFst", "", [(5373, "id")])
        ("pat_act_length", "toFst", "", [(9505, "id")])
        ("pat_act_weight", "toFst", "", [(8365, "id"); (21609, "id")])
        ("lab_bg_be", "toFst", "", [(4102, "id"); (4110, "id")])
        ("lab_bg_hco3", "toFst", "", [(4101, "id"); (4109, "id")])
        ("lab_bg_mode", "toFst", "", [(4103, "id"); (4111, "id")])
        ("lab_bg_origin", "toFst", "", [(4957, "id")])
        ("lab_bg_pco2", "toFst", "", [(4099, "id"); (4107, "id")])
        ("lab_bg_ph", "toFst", "", [(4098, "id"); (4106, "id")])
        ("lab_bg_po2", "toFst", "", [(4100, "id"); (4108, "id")])
        ("lab_bg_sat", "toFst", "", [(4104, "id"); (4112, "id")])
        ("lab_bl_b2m", "toFst", "", [(4244, "id")])
        ("lab_bl_bil_d", "toFst", "", [(4161, "id")])
        ("lab_bl_bil_i", "toFst", "", [(4159, "id")])
        ("lab_bl_ca2", "toFst", "", [(4142, "id")])
        ("lab_bl_catot", "toFst", "", [(4140, "id")])
        ("lab_bl_cc", "toFst", "", [(19277, "id")])
        ("lab_bl_cl", "toFst", "", [(4138, "id")])
        ("lab_bl_cr", "toFst", "", [(4156, "id")])
        ("lab_bl_CRP", "toFst", "", [(4201, "id")])
        ("lab_bl_f", "toFst", "", [(4144, "id")])
        ("lab_bl_gluc", "toFst", "", [(4148, "id"); (4149, "id")])
        ("lab_bl_hb", "toFst", "", [(4292, "id")])
        ("lab_bl_ht", "toFst", "", [(4293, "id")])
        ("lab_bl_k", "toFst", "", [(4137, "id")])
        ("lab_bl_lactate", "toFst", "", [(4164, "id")])
        ("lab_bl_leuco", "toFst", "", [(4300, "id")])
        ("lab_bl_neutro", "toFst", "", [(4304, "id")])
        ("lab_bl_mg", "toFst", "", [(4143, "id")])
        ("lab_bl_na", "toFst", "", [(4136, "id")])
        ("lab_bl_tr", "toFst", "", [(4316, "id")])
        ("lab_bl_ur", "toFst", "", [(4155, "id")])
        ("lab_bl_alat", "toFst", "", [(4181, "id")])
        ("lab_bl_asat", "toFst", "", [(4182, "id")])
        ("lab_bl_ggt", "toFst", "", [(4180, "id")])
        ("lab_bl_alp", "toFst", "", [(4170, "id")])
        ("lab_bl_ldh", "toFst", "", [(4183, "id")])
        ("lab_bl_alb", "toFst", "", [(4199, "id")])
        ("lab_bl_tg", "toFst", "", [(4217, "id")])
        ("lab_bl_fibrinogen", "toFst", "", [(4755, "id")])
        ("lab_bl_inr", "toFst", "", [(4753, "id")])
        ("lab_bl_pt", "toFst", "", [(4745, "id")])
        ("lab_bl_ferritin", "toFst", "", [(4261, "id")])
        ("lab_bl_il2", "toFst", "", [(14829, "id")])
        ("lab_bl_tropo", "toFst", "", [(4210, "id")])
        ("lab_u_alb_cr", "toFst", "", [(11420, "id")])
        ("lab_u_b2m", "toFst", "", [(4827, "id"); (4874, "id")])
        ("lab_u_ca", "toFst", "", [(4814, "id"); (4862, "id")])
        ("lab_u_cr", "toFst", "", [(4810, "id"); (4858, "id")])
        ("lab_u_f", "toFst", "", [(4815, "id"); (4863, "id")])
        ("lab_u_gluc", "toFst", "", [(4817, "id"); (4865, "id")])
        ("lab_u_k", "toFst", "", [(4812, "id"); (4860, "id")])
        //als parameter 4807 t/m 4853 is ingevuld dan P = portion, 
        // anders 4854 t/m 4902 dan C = 24 hour collection
        ("lab_u_m_port", "portion", "T", [(4807, "id"); (4853, "id")])
        ("lab_u_m_24h", "daily", "T", [(4854, "id"); (4902, "id")])
        ("lab_u_mg", "toFst", "", [(4816, "id"); (4864, "id")])
        ("lab_u_na", "toFst", "", [(4811, "id"); (4859, "id")])
        ("lab_u_ph", "toFst", "", [(4808, "id")])
        ("lab_u_pr", "toFst", "", [(4824, "id"); (4871, "id")])
        ("lab_u_pr_cr", "toFst", "", [(19459, "id")])
        ("lab_u_ur", "toFst", "", [(4809, "id"); (4857, "id")])
        ("lab_u_vol", "toFst", "", [(4855, "id")])
        ("mon_etco2", "toFst", "", [(5471, "id")])
        ("mon_hr", "toFst", "", [(5473, "id"); (10156, "id"); (10157, "id")])
        ("mon_ibp_dia", "toFst", "", [(5460, "id")])
        ("mon_ibp_mean", "toFst", "", [(5461, "id")])
        ("mon_ibp_sys", "toFst", "", [(5462, "id")])
        ("mon_nibp_dia", "toFst", "", [(5475, "id")])
        ("mon_nibp_mean", "toFst", "", [(5476, "id")])
        ("mon_nibp_sys", "toFst", "", [(5477, "id")])
        ("mon_rr", "toFst", "", [(5506, "id")])
        ("mon_sat", "toFst", "", [(5482, "id")])
        //1 = rectaal, 2 = oor, 3 = axillair, 4 = hu"id"
        ("mon_temp_mode", "toFst", "T", [(5490, "tempMode"); (8025, "tempMode"); (14759, "tempMode"); (8601, "tempMode")])
        ("mon_temp", "toFst", "", [(5490, "id"); (8025, "id"); (14759, "id"); (8601, "id")])
        ("mon_cvp", "toFst", "", [(5468, "isValid")])
        ("obs_pupil_dia_l", "toFst", "", [(6812, "id")])
        ("obs_pupil_dia_r", "toFst", "", [(6813, "id")])
        ("obs_gcs_e", "toFst", "", [(5494, "id")])
        ("obs_gcs_m", "toFst", "", [(5493, "id"); (8653, "id")])
        ("obs_gcs_v", "toFst", "", [(5496, "id"); (8654, "id"); (8656, "id"); (8657, "id")])
        ("obs_fb_urine", "total", "", [(6863, "id"); (6862, "id")] )
        ("vent_airway", "toFst", "", [(9624, "id") ]) // Airway (other than tube
        ("vent_cat", "toFst", "", [(8843, "id") (* bestaan niet?; (20062, "id"); (1258, "id")*)])
        ("vent_fio2", "toFst", "", [(9623, "id")]) //Respiratie Blender Zuurstof %
        ("vent_fio2_flow", "toFst", "", [(9622, "id")]) //Respiratie Blender Lowflow
        ("vent_fio2_mod", "toFst", "", [(7478, "id")])
        ("vent_m_fio2", "toFst", "", [(7345, "id")])
        ("vent_m_no", "toFst", "", [(7453, "id"); (7454, "id"); (17953, "id"); (17955, "id")])
        ("vent_m_peep", "toFst", "", [(7347, "id"); (19112, "id")])
        ("vent_m_ppeak", "toFst", "", [(5631, "id")])
        ("vent_m_pplat", "toFst", "", [(5632, "id")])
        ("vent_m_mean", "toFst", "", [(7348, "id")])
        ("vent_m_rr", "toFst", "", [(5630, "id"); (7349, "id"); (16264, "id")])
        ("vent_m_tv_exp", "toFst", "", [(5629, "id")])
        ("vent_m_tv_insp", "toFst", "", [(16232, "id")])
        ("vent_machine",  "toFst", "T", [(19660, "ventMachine");(16254, "ventMachine");(7464, "ventMachine");(20059, "ventMachine")])
        ("vent_mode",  "toFst", "T", [(19660, "carescape");(16254, "engstrom");(7464, "servoI");(20059, "servoU")])
        ("vent_set_fiO2",  "toFst", "", [(7495, "id"); (20038, "id"); (16229, "id")])
        ("vent_set_p_insp",  "toFst", "", [(5570, "id"); (5571, "id"); (19024, "id"); (20035, "id"); (20042, "id"); (20045, "id"); (16215, "id"); (16218, "id")])
        ("vent_set_peep",  "toFst", "", [(5569, "id"); (20044, "id"); (16214, "id")])
        ("vent_set_rr",  "toFst", "", [(5572, "id"); (20034, "id"); (16222, "id")])
        ("vent_set_tv",  "toFst", "", [(7938, "id"); (20033, "id"); (20046, "id"); (16196, "id")])
        ("vent_set_upl",  "toFst", "", [(12444, "id"); (16220, "id")])
        ("vent_tube",  "toFst", "", [(18181, "id"); (9988, "id")])
    ]


let obs =
    defs
    |> List.map (fun (n, c, t, _) ->
        
        [
            n
            if t = "T" then "varchar" else "float"
            if t = "T" then "300" else ""
            ""
            c
            ""
        ] |> arrayBox
    )
    |> List.append [
            [ "name"; "type"; "length";"unit"; "collapse"; "description"] |> arrayBox
            [ "pat_id"; ""; ""; ""; ""; "unique anonymous patient id or hospital id"] |> arrayBox
            [ "pat_timestamp"; ""; ""; ""; ""; "datetime from fst signal or exact datetime"] |> arrayBox
            [ "pat_age"; "float"; ""; "days"; "toFst"; "age in days"] |> arrayBox
            [ "pat_adm_no"; "int"; ""; ""; "toFst"; "admission number"] |> arrayBox
            [ "pat_adm_dep"; "varchar"; "30"; ""; "toFst"; "admission department"] |> arrayBox
            [ "pat_adm_spec"; "varchar"; "30"; ""; "toFst"; "admission specialty"] |> arrayBox
            [ "pat_adm_diagns"; "varchar"; "max"; ""; "concat"; "admission diagnoses list"] |> arrayBox
            [ "pat_adm_weight"; "float"; ""; "kg"; "toFst"; "admission weight in kg"] |> arrayBox
            [ "pat_adm_length"; "float"; ""; "cm"; "toFst"; "admission length in cm"] |> arrayBox
            [ "pat_adm_pim2"; "float"; ""; ""; "toFst"; "estimated PIM 2 mortality"] |> arrayBox
            [ "pat_adm_pim3"; "float"; ""; ""; "toFst"; "estimated PIM 3 mortality"] |> arrayBox
            [ "pat_adm_prismiv"; "float"; ""; ""; "toFst"; "estimated PRISM IV mortality"] |> arrayBox
            [ "pat_adm_outcome"; "varchar"; "30"; ""; "toFst"; "outcome (death or alive)"] |> arrayBox
        ]
    |> List.toArray


let signs =
    defs
    |> List.collect (fun (n, _, _, xs) ->
        xs
        |> List.map (fun (id, c) ->
            [n; id |> string; ""; c; ""] |> arrayBox
        )
    )
    |> List.append [
        for o in obs |> Array.skip 1 do
            match o |> Array.tryHead with
            | Some h ->
                let b = 
                    defs 
                    |> List.exists (fun (n, _, _, _) -> n = (h |> string))
                if b then ()
                else
                    [ (h |> string); ""; (h |> string); "id"; "" ] |> arrayBox
            | None -> ()
    ]

    |> List.append [
        [ "observation"; "id"; "name"; "convert"; "description" ] |> arrayBox
    ]
    |> List.toArray


do 
    Workbook.create ()
    |> Workbook.addSheet "Observations"
    |> Workbook.addTable (1, 1) obs "Observations"
    |> Workbook.addSheet "Signals"
    |> Workbook.addTable (1, 1) signs "Signals"
    |> Workbook.addSheet "Collapse"
    |> Workbook.addTable (1, 1) [| [| "function"; "description" |]|] "Collapse"
    |> Workbook.addSheet "Convert"
    |> Workbook.addTable (1, 1) [| [| "function"; "description" |]|] "Convert"
    |> fun wb ->
        match wb.TryGetWorksheet "Observations" with
        | true, sht -> 
            sht.Column(1).Width <- 30.
            sht.Column(2).Width <- 15.
            sht.Column(3).Width <- 10.
            sht.Column(4).Width <- 10.
            sht.Column(5).Width <- 15.
            sht.Column(6).Width <- 100.
        | _ -> ()

        match wb.TryGetWorksheet "Signals" with
        | true, sht -> 
            sht.Column(1).Width <- 30.
            sht.Column(2).Width <- 10.
            sht.Column(3).Width <- 30.
            sht.Column(4).Width <- 15.
            sht.Column(5).Width <- 100.
        | _ -> ()

        wb
    |> Workbook.saveAs "Definitions.xlsx"


module Definitions =

    open Informedica.Observations.Lib
    open Types

    let read path collapseMap convertMap =
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
                                Convert = 
                                    r.Cell(4).GetString()
                                    |> convertMap
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
                        Collapse = r.Cell(4).GetString () |> collapseMap 
                    }
                )


open Informedica.Observations.Lib
open Types

let collapseMap _ : Collapse =
    fun xs -> (xs |> List.head).Value

let convertMap _ : Convert = id

Definitions.read "Definitions.xlsx" collapseMap convertMap
|> Seq.iter (printfn "%A")