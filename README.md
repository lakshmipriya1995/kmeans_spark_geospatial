# write a spark application in python to implement K-means algorithm to calculate K-means for the device 
location (each location has a latitude and longitude) in the file that is prepared by the previous step. That 
is to say, the file in /devicestatus_etl on HDFS is the input of your program. Review Figure 1 to see the 
format of the data. You cannot use K-means in MLib of Spark to solve the problem.
In your code, 
• Set the number of means (center points) K = 5.
• You also need a variable convergeDist to decide when the K-means calculation is done – when 
the amount the locations of the means changes between iterations is less than convergeDist. Set 
the variable = 0.1.
• To take a random sample of K location points as starting center points, you can use takeSample(), 
which is used to return a fixed-size sample subset of an RDD. For example, 
data.takeSample(False, 5, 34) takes a random sample of 5 location points from the RDD “data” as 
starting center points and it returns an array of length 5, where “False” means no replacement, 
and 34 is the value of seed. In your code, use the same values as the arguments (i.e. False, 5, 34)
of this function to take 5 starting center points.
• When parsing the input file, only include known locations (that is, filter out (0, 0) locations).
• You need to persist an RDD if necessary. Which RDD needs to be persisted?
Your program should produce the following final K center points (K = 5), if you use the same settings as 
listed above (i.e. K = 5, convergeDist = 0.1, etc.). In the output, the 1st field is latitude, and the 2nd field is 
longitude. 
