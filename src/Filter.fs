namespace Informedica.Observations.Lib



module Filter =

    open Types


    let onlyValid sgns : Filter = List.filter Signal.isValid


