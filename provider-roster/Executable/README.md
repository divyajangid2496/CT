## Description:
A spark application that can be used to Spark application to analyze provider visits and their distribution over time. To execute this spark job, follow the instructions defined below.

## Execution Instruction:
1. The current directory is called "Executable". Inside, you'll find the input_data and output_data folders, along with a JAR file that includes all the necessary classes and executables
2. The entry point of the application, or the main method is in com.availity.spark.provider.ProviderRoaster file
3. This app expects the input in certain format. Say you have an entity named providers, inside the input_data directory, create another directory named "providers" and place all the csv files within this entity folder. This allows user to input N-number of files for a particular entity (i.e.: "Executable/input_data/<entity_name>/data.csv")
4. To execute the spark job, navigate to the  Executable directory using cd command  
    #### "cd <project_path>/Executable"
5. Execute below spark submit command  
     #### "spark-submit --class com.availity.spark.provider.ProviderRoster  --master local provider_2.12-0.1.0-SNAPSHOT.jar"
6. If you have a different input or output file format, then edit the com.availity.spark.config.AppConfig file in the project. Rebuild the jar and copy the jar file in the "Executable" directory and follow step 4 and 5
    
