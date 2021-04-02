namespace Informedica.Observations.Lib



module Filter =

    open Types


    let onlyValid : Filter = List.filter Signal.isValid


    let filterValuePred pred : Filter =
        fun sgns -> sgns |> List.filter pred


    let filterGTE n = 
        fun signal ->
            signal 
            |> Signal.getNumericValue
            |> function
            | Some v -> v >= n
            | None   -> false
        |> filterValuePred 


    let filterSTE n = 
        fun signal ->
            signal 
            |> Signal.getNumericValue
            |> function
            | Some v -> v <= n
            | None   -> false
        |> filterValuePred 



    let filterMinMax (min, max) = filterGTE min >> filterSTE max