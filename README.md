# Lab 1 Report

> This is the template for your lab report. Some guidelines and rules apply.

> You can only change the *contents* of the sections, but not the section 
> headers above the second level of hierarchy! This means you can only add
> headers starting with `###`. 
 
> You must remove the text fields prefixed with `>`, such as this one, resulting
> in a README.md without this template text in between.

> Any report violating these rules will be immediately rejected for corrections 
> (i.e. as if there is no report). 

## Usage

> Describe how to use your program. You can assume the TAs who will be grading
> this know how to do everything that is in the lab manual. You do not have to
> repeat how to use e.g. Docker etc.

> Please do explain how to provide the correct inputs to your program.

## Functional overview

> Introduce how you have approached the problem, and the global steps of your
> solution. 
> 
> Example:
>
> ### Step 1: Preparing the data
>
> * We want to be as type-safe as possible so we've declared a case class X
>   with the fields y and z. 
> * We are now able to load the ORC file into the DataSet\[X\].

> Take into consideration your robustness level (see Rubric), and what you had
> to do to make your answer as accurate as possible. Explain what information
> was missing and how you have mitigated that problem.

## Result
 
> Present the output of your program. Explain what could be improved (if 
> applicable).

## Scalability

> Give a concise analysis of the scalability of your solution. Identify in
> the various steps described previously how this affects the scalability.
>
> Example:
>   Steps A and B were initially performed the other way around, but 
>   since Step B performs a sort which involves a shuffle in the physical plan
>   of Spark, it was better to first apply the filter in Step A, and then sort.

## Performance

> Present performance measurements. You can obtain these from the Spark history
> server.

> Relate the measurements to your code and describe why specific steps take a
> relatively longer or shorter time w.r.t. the other steps.

