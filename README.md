# SkewDebug
SkewDebug is a tool to detect source code locations in a Spark Pipeline making easier to fix Data, Memory and Computation Skews.

The tool is created as a class file in src/main/scala/SkewDetection/

Steps to use the tool:

1. Import the class

2. Create the SkewDebug Object

3. Pass your SparkContext as a constructor to the SkewDebug class during the object creation

4. After your implementation of the pipeline you can simply call the printlog function from the SkewDebug Object

5. Run your pipeline

Example of the Pipeline and the working of the tool is mentioned in: src/main/scala/hc/PipeLine

The location of the dataset needs to be changed as we are using the ticket_flights.csv file which is in the data folder.

