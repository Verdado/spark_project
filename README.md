#Pre-requisites:
\
Note: Make sure to configure below environment variables on your machine.
- SPARK_HOME
- HADOOP_HOME
- JAVA_HOME

##Project Overview:
####Config path:
- spark_project/table.json
####Final output path:
- spark_project/target/final_dataset/*.csv
####Transformation path:
- spark_project/helper/DataWranglinghelper.py
####Input files path:
- spark_project/resources/example_data_2019_v1.csv

##How to run:
<Script_location>/spark_project/main.py

##Expectation:
- Output file will be written in "Final output path".
- It will print the final dataframe at the end.

##Note: 
Developed in windows machine, please change the directory format as per your OS.