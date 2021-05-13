namespace Informedica.Observations.Lib


module Types =

    open System
    
    
    type SignalId = string option
    type PatientId = string


    type Value =
        | NoValue
        | Text of string
        | Numeric of float
        | DateTime of DateTime


    type TimeStamp = 
        | SomeDateTime of DateTime
        | Period of start : DateTime * stop : DateTime
        | NoDateTime


    type Signal = 
        {
            Id: string option
            Name : string
            Value : Value
            Validated : bool
            PatientId : string
            TimeStamp : TimeStamp
        }


    type Collapse = Signal array -> Value

    type Convert = Signal -> Signal

    type Filter = Signal array -> Signal array

    type SourceId = string option

    type Observation =
        { 
            Name : string 
            Type : string
            Length : int option
            Filters : Filter array
            Collapse : Collapse 
            Sources : Source array
        }
    and Source =
        { 
            Id : SourceId
            Name : string
            Conversions : Convert array
        }


    type DataSet =
        { Columns : Column array
          Data : (PatientId * RowTime * DataRow) array }
    and Column = 
        { 
            Name : string
            Type : string
            Length : int option
        }
    and DataRow = Value array
    and RowTime = | Exact of DateTime | Relative of int 

    type TimeResolution = int option
    type Transform = TimeResolution -> Observation array ->  Signal array -> DataSet


