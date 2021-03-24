namespace Informedica.Observations.Lib


module Types =

    open System
    
    
    type SignalId = int option
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
            Id: SignalId
            Name : string
            Value : Value
            Validated : bool
            PatientId : string
            TimeStamp : TimeStamp
        }


    /// A function to summarize a list of values
    /// as one value
    type Collapse = Signal list -> Value

    /// A conversion function for a value
    type Convert = Signal -> Signal


    type Filter = Signal list -> Signal list


    /// An observation describes wich signals to
    /// that describe that observation (Sources)
    /// along with a collaps function that summarizes
    /// the obtained values to one value.
    /// Each source value also can be converted to
    /// a different or adjusted value.
    type Observation =
        { 
            Name : string 
            Type : string
            Length : int option
            Sources : Source list
            Filters : Filter list
            Collapse : Collapse 
        }
    and Source =
        { 
            Id : int
            Name : string
            Conversions : Convert list
        }



    /// The resulting dataset with colums
    /// and rows of data. Each row has a
    /// unique hospital number, date time.
    type DataSet =
        { Columns : Column list
          Data : (PatientId * RowTime * DataRow) list }
    and Column = 
        { 
            Name: string
            Type: string
        }
    and DataRow = Value list
    and RowTime = | Exact of DateTime | Relative of int 


    type Transform = Observation list ->  Signal list -> DataSet


