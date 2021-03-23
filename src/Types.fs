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


    type DateTimeOption = 
        | SomeDateTime of DateTime
        | Period of start : DateTime * stop : DateTime
        | NoDateTime


    type Signal = 
        {
            Id: SignalId
            Name : string
            Value : Value
            PatientId : string
            DateTime : DateTimeOption
        }


    /// A function to summarize a list of values
    /// as one value
    type Collapse = Signal list -> Value

    /// A conversion function for a value
    type Convert = Signal -> Signal

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
            Collapse : Collapse 
        }
    and Source =
        { 
            Id : int
            Name : string
            Convert : Convert 
        }



    /// The resulting dataset with colums
    /// and rows of data. Each row has a
    /// unique hospital number, date time.
    type DataSet =
        { Columns : Column list
          Data : (PatientId * TimeStamp * DataRow) list }
    and Column = 
        { 
            Name: string
            Type: string
        }
    and DataRow = Value list
    and TimeStamp = | Exact of DateTime | Relative of int 


    type Transform = Observation list ->  Signal list -> DataSet


    type Conversions =
        | Engstrom
        | ServoU
        | VentMachine
        | TempMode
