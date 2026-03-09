from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('My app').setMaster('local')


print(conf.get("spark.master"),
    conf.get("spark.app.name"))
sc = SparkContext(conf=conf)

print(sc.master,
    sc.appName,
    sc.sparkHome is None)