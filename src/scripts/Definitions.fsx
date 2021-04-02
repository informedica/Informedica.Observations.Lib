
#r "nuget: ClosedXML"
#r "nuget: Microsoft.Data.SqlClient"
#r "nuget: Informedica.Utils.Lib"

#load "../Database.fs"
#load "../ClosedXML.fs"
#load "../Types.fs"
#load "../Signal.fs"
#load "../Observation.fs"
#load "../DataSet.fs"
#load "../Convert.fs"
#load "../Filter.fs"
#load "../Collapse.fs"
#load "../Definitions.fs"

open System
open System.IO
open Informedica.Observations.Lib

fsi.AddPrinter<DateTime> (fun dt -> dt.ToString("dd-MM-yyyy HH:mm"))

let path = Path.Combine(__SOURCE_DIRECTORY__, "../../data/Definitions.xlsx")
File.Exists(path)

let returnNone _ = None

Definitions.read path returnNone returnNone returnNone
|> Seq.toList
