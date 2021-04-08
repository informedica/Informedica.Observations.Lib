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


    type Collapse = Signal list -> Value

    type Convert = Signal -> Signal

    type Filter = Signal list -> Signal list

    type SourceId = string option

    type Observation =
        { 
            Name : string 
            Type : string
            Length : int option
            Filters : Filter list
            Collapse : Collapse 
            Sources : Source list
        }
    and Source =
        { 
            Id : SourceId
            Name : string
            Conversions : Convert list
        }


    type DataSet =
        { Columns : Column list
          Data : (PatientId * RowTime * DataRow) list }
    and Column = 
        { 
            Name : string
            Type : string
            Length : int option
        }
    and DataRow = Value list
    and RowTime = | Exact of DateTime | Relative of int 

    type TimeResolution = int option
    type Transform = TimeResolution -> Observation list ->  Signal list -> DataSet


