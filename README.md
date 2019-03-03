## EnedisHadoop4Unistra

University of Strasbourg, M2 ILC 2019

Developers :
GOMES FERNANDES Caty, HARTMEYER Vincent


**Please download the dataset from this link**: https://drive.google.com/file/d/11tEcNLQve1IJiWfkCie_zjKEHcNbli9Q/view?usp=sharing 

Original link: https://data.enedis.fr/explore/dataset/consommation-electrique-par-secteur-dactivite-commune/information/?sort=annee

### Quick run 

`make` : Compile all java classes

`make run` : Run all projects. The outputs are printed at the end of both runs, and can also be found in the `all_outputs` folder.

`make clean` : Remove existing outputs, classes and jar

### Seperate compilation and run

The project is split into two parts that answer two different questions using the same dataset:
* The first question distinguishes between the types of sectors and searches for the department with the most influence on the consumption for each sector.
* The second question focuses on the consumption data for the residency sector and tries to analyse which factors contribute to a lower or higher consumption.

Use the commands in the following sections to compile everything seperately.

#### EnedisBySector

`make sector` : Compile the java classes related to this question

`make runSector` : Run the analysis for this question. The intermediate and final results can be found in `all_outputs/sector-output/` and `all_outputs/sector-output-final` respectively.

#### EnedisByConsumption

`make conso` : Compile the java classes related to this question

`make runConso` : Run the analysis for this question. The intermediate and final results can be found in `all_outputs/conso-output/`,  `all_outputs/conso-minmax/` and `all_outputs/conso-output-final` respectively.
