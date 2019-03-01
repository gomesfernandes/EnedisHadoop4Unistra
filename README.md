## EnedisHadoop4Unistra

University of Strasbourg, M2 ILC 2019

Developers :
GOMES FERNANDES Caty, HARTMEYER Vincent


### Compile and Run

The project is split into two parts that answer two different questions using the same dataset:
* The first question distinguishes between the types of sectors and searches for the department with the most influence on the consumption for each each sector.
* The second question only looks at the consumption data for the residency sector and tries to analyse which factors contribute to a lower or higher consumption.

It is possible to compile and run everything at once, or to focus on a question using the commands described in the following subsections.

Both projects are cleaned with the following command : 

`make clean` : Remove existing outputs, classes and jar

### Compile and run everything together

`make` : Compile all java classes

`make run` : Run the project. The outputs are printed at the end of both runs, and can also be found in the `all_outputs` folder.

### Compile and run EnedisBySector

`make sector` : Compile the java classes related to this question

`make runSector` : Run the analysis for this question. The intermediate and final results can be found in `all_outputs/sector-output/` and `all_outputs/sector-output-final` respectively.

### Compile and run EnedisByConsumption

`make conso` : Compile the java classes related to this question

`make runConso` : Run the analysis for this question. The intermediate and final results can be found in `all_outputs/conso-output/` and `all_outputs/conso-output-final` respectively.