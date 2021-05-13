namespace Informedica.Observations.Lib



module Filter =

    open Types


    let onlyValid : Filter = Array.filter Signal.isValid


    let filterValuePred pred : Filter =
        fun sgns -> sgns |> Array.filter pred


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

