
#r "nuget: Fake.Core.Target"
#r "nuget: Fake.DotNet.CLI"

open System
open Fake.Core
open Fake.DotNet

#if !FAKE
let commandLineArgs = 
    Environment.GetCommandLineArgs() 
    |> Array.toList
    |> List.skip 2
Console.WriteLine($"{commandLineArgs}")
let execContext = Context.FakeExecutionContext.Create false "build.fsx" commandLineArgs
Context.setExecutionContext (Context.RuntimeContext.Fake execContext)
#endif

// Your Script code...

let toolRestore _ =
    Console.WriteLine("running tool restore")
    DotNet.exec id "tool" "restore"
    |> ignore

let build _ =
    DotNet.exec id "build" ""   

let test _ =
    DotNet.exec id "test" ""

Target.create "tool-restore" toolRestore

