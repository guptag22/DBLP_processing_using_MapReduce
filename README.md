# Processing of DBLP dataset using MapReduce

## Overview

In this assignment, I study the Hadoop framework to write MapReduce for processing the publicly available dataset of DBLP. This dataset is a collection of data about numerous publications like books, articles, thesis for phd and masters. Different MapReduce tasks performed on this dataset are - 
* For each venue, find top 10 authors who published at that venue.
* List of authors who published for at least 10 years without interruption.
* List of venue wise publications with only one author.
* List of venue wise publications with the highest number of authors.
* List of top 100 authors who published with maximum number of distinct co-authors.
* List of 100 authors who published with the least number of co-authors.


## Instruction to run

### Prerequisite

To run this project, install and configured the following on your system.
* SBT
* JVM (Use Java version 8 to run with hadoop)
* Hadoop

Alternatively, this project can be run on HortonWorks Sandbox. 

### Steps to run

* Clone and download this repository on a local machine. 
```sh
$ git clone https://ggupta22@bitbucket.org/cs441-fall2020/garima_gupta_cs441_fall2020_hw2.git
```
* Navigate to the project folder in a Command Prompt/Terminal.
```sh
$ cd Garima_Gupta_CS441_Fall2020_HW2
```

#### To run on JVM
* Build and run using SBT.
```sh
$ sbt clean compile 
$ sbt run input output
```

#### To run on system Hadoop
* Create a jar file
```sh
$ sbt clean compile assembly
```
The jar file gets created inside the project on path `Target/scala-<version>/`

* Start hadoop services. Locate hadoop start and stop shell scripts and run the start script.
```sh
$ cd /usr/local/Cellar/hadoop/3.3.0/libexec/sbin
$ ./start-all.sh
```

* Create an input directory in hadoop file system
```shell script
$ hadoop fs -mkdir /input
```

* Put the xml input file into hdfs
```shell script
$ hadoop fs -put <inputPath>/input/dblp.xml /input
```

* Run the jar file on hadoop
```shell script
$ hadoop jar <jarFilePath>/CS441-HW2-assembly-0.1.jar /input/ /output/
```

* Outputs of the tasks are stored in `/output` on hdfs

## MapReduce tasks

#### Task 1 - Top 10 published authors at each venue

For each publication, we extract the venue according to the type of publication. For each venue, we find the authors who have published at that venue and calculate the number of publications they have made at tha venue. Tha Mapper for this task reads each publication and extracts the venue and list of authors of that publication. It parses the xml and sends (\<venue>,\<author>) as key value pair to the Reducer. Reducer get the venue as key, and a list of all authors who have published at that venue. It then finds the top 10 authors who have made the most contribution at that venue.

#### Task 2 - Authors published for more than 10 years

In this task, we find the authors who have been consistently publishing for at-least 10 years. For each publication, we read the xml and parse it to extract the year it was published and authors associated with the publication. Mapper writes the (\<author>,\<year>) as key value pair for each author and sends it to the Reducer. The reducer receives a list of years for each author (key) when the author has made a publication and checks for the existence of a 10 year consistency. It write the author for output if a consistency is found.

#### Task 3 - Publications with only one author

For each publication, we extract the venue and list of authors. If the number of authors is only one, then we read the name of the publication and write it to context for the reducer. Reducer gets a list of all the publications for a venue which have only one author. It writes the (\<venue>,\<publication>) pair to the output.

#### Task 4 - Publications with the most authors

For each venue, we need a list of publications which have been published by maximum number of authors for tha venue. Mapper reads the venue, publication title, and list of authors, and sends them to the reducer for further processing. For each venue, reducer gets a list of all the publications and their associated authors. Reducer then finds the maximum number of authors who have published at that venue as co-authors of a single publication. Reducer then filters out all the publications which have been authored by maximum number of authors. The venue and filtered publications are then written for output.

#### Task 5 - Top 100 and bottom 100 co-authors

For this task, we need to find top 100 authors who have published with the maximum number of co-authors, and 100 authors who have published with least number of co-authors (zero or more). The mapper reads the input publication ad extracts the list of authors. It then creates pairs of authors who are co-authors to each other and writes them as key-value pairs both ways. Example, if a publication has 2 authors Auth1 and Auth2, the mapper writes (\<Auth1>,\<Auth2>) as well as (\<Auth2>,\<Auth1>) in the mapper output. For each author (key), the reducer receives a list of authors as input values who the key author has made publications with. Reducer find distinct co-authors from this list and calculates the number of co-authors for the key author. This number along with the key author is written in a global map which keeps track of all the authors and the number of co-authors they are associated with. The cleanup method runs after all the reducers for this job have completed and the global map consists data for all the authors. It sorts this map wrt to the number of co-authors in descending order. It extracts the top 100 and bottom 100 authors and writes them to the output. 


## Output
The Outputs for all the tasks are be default stored in the `output/` directory of the project. The directly currently consists of output for a smaller test file `input/dblp-test2.xml`. Individual output files of each task have been converted to `.csv` and save in a separate directory `CSV_Outputs`.