import sys
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
import shutil
import numpy as np
import statistics as stats

#pick the starting points in the following function
def pick_centroids(dataset, k):
    initial_centroids = dataset.takeSample(False, k,34)
    return initial_centroids

#find the squared distance of two points
def distancing(p, q):
    return np.sqrt((p[0]-q[0])**2 + (p[1]-q[1])**2) 

#caluclate the sum of points 
def get_mean(a,b):
    sum1=0
    sum2=0
    for i in b:
        sum1+=i[0]
        sum2+=i[1]
    return (a, [sum1/len(b), sum2/len(b)])       
 
#assigning the closest centroid 
def give_centroids(p):
    min_dist = float("inf")
    centroids = centroids_broadcast.value
    nearest_centroid = 0
    for i in range(len(centroids)):
        #calculating the squared distance of two points
        distance = distancing(p, centroids[i])
        if(distance < min_dist):
            min_dist = distance
            nearest_centroid = i
    return (nearest_centroid, p)


#here we are checking the distance current and new center points
def stop_criteria(new_centroids, convergeDist):
    old_centroids = centroids_broadcast.value
    for i in range(len(old_centroids)):
        check = distancing(old_centroids[i], new_centroids[i]) <= convergeDist
        if check == False:
            return False
    return True 

if __name__ == "__main__":


    if len(sys.argv) != 3:
        print("Number of arguments not valid!")
        sys.exit(1)

    parameters = {"distance": 2, "k": 5, "convergeDist": 0.1, "maxiteration": 50}

    INPUT_PATH = str(sys.argv[1])
    OUTPUT_PATH = str(sys.argv[2])

    #reading csv file from the previous step 
    spark = SparkSession.builder.appName("kmeans").master("local[1]").getOrCreate()
    sc = spark.sparkContext
    df_pyspark=spark.read.csv(INPUT_PATH)
    #get latitude and longitude
    df1 = df_pyspark.select(['_c3','_c4'])
    df2 = df1.filter((df1['_c3']!=0) & (df1['_c4']!=0))
    df2.coalesce(1).write.csv('filteredData')
  

    sc.setLogLevel("ERROR")

    print("\n***START****\n")

    inputPoints = sc.textFile('filteredData').map(lambda x: [float(k) for k in x.split(",")])
    points = inputPoints.collect()


    #pick the starting points
    initial_centroids = pick_centroids(inputPoints, k=parameters["k"])
    distance_broadcast = sc.broadcast(parameters["distance"])
    centroids_broadcast = sc.broadcast(initial_centroids)

    cluster_assignment = [give_centroids(k) for k in points]




    stop, n = False, 0
    while True:
        print("--Iteration n. {itr:d}".format(itr=n+1), end="\r", flush=True)
        cluster_assignment = [give_centroids(k) for k in points]

        cluster_assignment_rdd = sc.parallelize(cluster_assignment)

        #find the closest centroid
        centroids_rdd = cluster_assignment_rdd.groupByKey().map(lambda a: get_mean(a[0],a[1])).sortByKey(ascending=True)

        new_centroids = [item[1] for item in centroids_rdd.collect()]
        #to check if distance is less than convergence distance i.e., convergence distance
        stop = stop_criteria(new_centroids,parameters["convergeDist"])

        n += 1

        #loop until the total distance between the new center points and the current center points is less than convergence ditance
        if(stop == False and n < parameters["maxiteration"]):
            centroids_broadcast = sc.broadcast(new_centroids)
        else:
            break

    with open(OUTPUT_PATH, "w") as f:
        for centroid in new_centroids:
            f.write(str(centroid) + "\n")
    print("completed")
    shutil.rmtree('filteredData')
