# Informedica.Observations.Lib

This library aims to support the transition of serial patient signals (i.e. data points for a patient over time) 
to a data set with observations over time for a patient. This data set in turn can be used for further 
statistical and machine learning purposes.

The theory of this library is described in this [blog](https://informedica.nl/?p=213).

## Data input

Once data from an electronic patient recored is extracted to `Signals`, these can be processed by this software library. A `Signal` has the following attributes:

- Id
- Name
- Time Stamp
- Patient Id
- Value
- Validated

### Id 
The id of the parameter.

### Name
The name of the parameter, for example `lab_creatinie` or `mon_heartrate`. 

### Time Stamp
The date time or period in which the signal occurs. The timestamp can be empty, meaning that the signal is time independent, for example the gender of a patient.

### Patient Id
The patient id of the patient

### Value
The value of the signal. A value can be:

- Text
- Numeric
- DateTime
- Or no value

Examples for signals can be found at this [google spreadsheet](https://docs.google.com/spreadsheets/d/1ZAk5enAvdkFNv5DD7n5o1tTkAL9MedKNC1YFFdmjL-8/edit?usp=sharing) (at the `Signals sheet`).


## Definitions

The list of signals is processed according to a data definition. The definitions constist of the following structure:

```fsharp
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
```

`Observations` will be turned into columns, and an `Observation` is derived from a `Source` list. The `Id` or `Name` of a `Source` in turn is matched to the `Id` or `Name` of a `Signal`. This allows processing of different sources to a single observation. For example urine output can be calculated by adding sources for catheter urine output to spontaneous urine output. 

Also, more complex calculated observations are possible by combining different sources into one calculation.

Besides sources the following actions can be defined:

- Conversion: convert a signal value to a different value
- Filter: filters a list of sources for an observation
- Collapse: aggregates the list of signals to one observed value

## Output

The output is a data set with the following structure:

```fsharp
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
```

This equates to a flat regular table with for each patient date time a row of observations. For observations with no values at a specific date time there will be null values.

## Features

The following features are implemented or will be implemented:

- [x] Defining observations in an online google spreadsheet
- [x] Anonymizing a data set with encrypted patient id's and relative date time points
- [x] Writing a resulting `DataSet` to a `csv` file  
- [ ] Writing a resulting `DataSet` to a database