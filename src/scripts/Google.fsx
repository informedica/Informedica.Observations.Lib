
#load "../Types.fs"
#load "../Signal.fs"
#load "../Convert.fs"
#load "../Filter.fs"
#load "../Collapse.fs"


open Informedica.Observations.Lib



module String =

    let split (del : string) (s : string) = s.Split(del) 

    let replace (olds : string) (news : string) (s : string) = 
        s.Replace(olds, news)



module Definitions =

    open System
    open System.IO
    open System.Net

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


    // https://docs.google.com/spreadsheets/d/1ZAk5enAvdkFNv5DD7n5o1tTkAL9MedKNC1YFFdmjL-8/edit?usp=sharing
    let docId = "1ZAk5enAvdkFNv5DD7n5o1tTkAL9MedKNC1YFFdmjL-8"

    let createUrl id sheet = 
        $"https://docs.google.com/spreadsheets/d/{id}/gviz/tq?tqx=out:csv&sheet={sheet}"


    let download url =
        async {
            let req = WebRequest.Create(Uri(url))
            use! resp = req.AsyncGetResponse()
            use stream = resp.GetResponseStream()
            use reader = new StreamReader(stream)
            return reader.ReadToEnd()
        }    


    let getSheet sheet = 
        createUrl docId sheet
        |> download
        |> Async.RunSynchronously
        |> String.split "\n"
        |> Seq.map (String.split "," >> (Array.map (String.replace "\"" "")))


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


    let readGoogle convertMap filterMap collapseMap =
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



module Convert =

    open Types


    let setSignalText signal text =
        { signal with 
            Value = 
                if text = "" then NoValue
                else text |> Text
        }
    
    let servoI : Convert = 
        fun signal -> 
            match signal |> Signal.valueToString with
            | s when s = "3"  -> "VC-CMV"
            | s when s = "16" -> "VC-CMV"
            | s when s = "2"  -> "PC-CMVs"
            | s when s = "18" -> "PC-CMVs"
            | s when s = "4"  -> "PC-CMVa"
            | s when s = "20" -> "PC-CMVa"
            | s when s = "8"  -> "PC-CSV"
            | s when s = "21" -> "PC-CSV"
            | s when s = "12" -> "PC-CMVniv"
            | s when s = "13" -> "PC-CSVniv"
            | s when s = "14" -> "PC-CSVncap"
            | _ -> ""
            |> setSignalText signal


    let tempMode : Convert =
        fun signal -> 
            match signal.Id with
            | Some id when id = 5490  -> "rectal"
            | Some id when id = 8025  -> "ear"
            | Some id when id = 14759 -> "axillary"
            | Some id when id = 8601  -> "skin"
            | _ -> ""
            |> setSignalText signal
    

    let ventMachine : Convert =
        fun signal -> 
            match signal.Id with
            | Some id when id = 19660 -> "Carescape"
            | Some id when id = 16254 -> "Engstrom"
            | Some id when id = 7464  -> "ServoI"
            | Some id when id = 20059 -> "ServoU"
            | _ -> ""
            |> setSignalText signal


    let servoU : Convert = 
        fun signal -> 
            match signal |> Signal.valueToString with
            | s when s = "4"    -> "VC-CMV"
            | s when s = "1"    -> "PC-CMVs"
            | s when s = "2"    -> "PC-CMVs"
            | s when s = "2003" -> "PC-CMVs"
            | s when s = "7"    -> "PC-CMVa"
            | s when s = "8"    -> "PC-CMVa"
            | s when s = "2009" -> "PC-CMVa"
            | s when s = "11"  -> "PC-CSV"
            | s when s = "16" -> "PC-CMVniv"
            | s when s = "17" -> "PC-CSVniv"
            | s when s = "18" -> "PC-CSVncap"
            | _ -> ""
            |> setSignalText signal


    let engstrom : Convert = 
        fun signal -> 
            match signal |> Signal.valueToString with
            | s when s = "v"  -> "VC-CMV"
            | s when s = "b" -> "VC-CMV"
            | s when s = "p"  -> "PC-CMVs"
            | s when s = "g"  -> "PC-CMVa"
            | s when s = "c"  -> "PC-CSV"
            | s when s = "a" -> "PC-CSV"
            | s when s = "n" -> "PC-CMVniv"
            | s when s = "N" -> "PC-CSVncap"
            | _ -> ""
            |> setSignalText signal


    let map s =
        match s with
        | _ when s = "engstrom"    -> engstrom    |> Some
        | _ when s = "servo_i"      -> servoI      |> Some
        | _ when s = "servo_u"      -> servoU      |> Some
        | _ when s = "carescape"   -> id          |> Some
        | _ when s = "vent_machine" -> ventMachine |> Some
        | _ when s = "temp_mode"    -> tempMode    |> Some
        | _ -> None



module Filter =

    let map s =
        match s with
        | _ when s = "validated" -> Filter.onlyValid |> Some
        | _ -> None



module Collapse =

    open Types


    let sum : Collapse =
        fun signals ->
            if signals |> List.isEmpty then NoValue
            else
                signals
                |> List.sumBy (fun signal ->
                    match signal.Value with
                    | Numeric x -> x
                    | _ -> 0.
                )
                |> Numeric 


    let calcOxygenationIndex : Collapse = 
        fun signals ->
            let getNumeric = Option.bind Signal.getNumericValue
            let fiO2 = signals |> List.tryFind (fun signal -> signal.Name = "fio2") |> getNumeric
            let map = signals  |> List.tryFind (fun signal -> signal.Name = "mean airway p")  |> getNumeric
            let pO2 = signals  |> List.tryFind (fun signal -> signal.Name = "po2")  |> getNumeric
            match fiO2, map, pO2 with
            | Some fiO2, Some map, Some pO2 -> 
                ((fiO2 * 100. * map) / pO2)
                |> int 
                |> float
                |> Numeric 
            | _ -> NoValue

    let map s : Collapse option =
        match s with
        | _ when s = "concat_;" -> 
            fun sgns -> 
                sgns 
                |> List.map Signal.valueToString
                |> String.concat ";"
                |> Text
            |> Some
        | _ -> None




Definitions.readGoogle Convert.map Filter.map Collapse.map
|> Seq.toList |> ignore
//|> List.filter (fun x -> x.Sources |> List.exists (fun s -> s.Conversions |> List.isEmpty |> not))
|> List.iter (printfn "%A")
