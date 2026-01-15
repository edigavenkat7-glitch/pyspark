import os
from pyspark import SparkContext

os.environ["PYSPARK_PYTHON"] = "C:/Users/abhig/Documents/Python/Python/Python37/python.exe"

sc = SparkContext("local[*]", "Spark-programming")

rdd1=sc.textFile("C:/Users/abhig/Documents/entain.txt")
rdd2=rdd1.flatMap(lambda x: x.split(" "))
rdd3=rdd2.map(lambda x : (x,1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)
rdd5 = rdd4.collect()

for i in rdd5:
    print(i)
