# Lab 1 Report

>"More people died in the struggle against water than in the struggle against men". 

As the Greek geographer Pytheas noted of the Low Countries, flood has been a serious issue that haunted people in the Low Land for hundreds of years.  Currently, as approximately two thirds of the land in the Netherlands is below sea level, it is still extremely vulnerable to flooding. 

This is the motivation of our work — Assume that all the flood control infrastratures in the Netherlands failed, and the sea level has rised a certain height. Residents living in the flooded areas needs to be evacuated into cities above the sea level. The goal of our project is to present a relocation plan for that situation.

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

