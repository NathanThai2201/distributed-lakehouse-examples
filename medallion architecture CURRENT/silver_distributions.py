from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("GoldLayer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.100.66:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)

if table_exists("silver_catalog.default.yellow_taxi"):
    yellow_df = spark.read.table("silver_catalog.default.yellow_taxi")

    # Convert to Pandas (only safe if data fits in memory)
    #pdf = yellow_df.toPandas() # dangerous

    pdf = yellow_df.sample(fraction=0.01).toPandas()
    #pdf = yellow_df.select("trip_distance").toPandas()

    # Matplotlib settings
    plt.rc('font', size=14)
    plt.rc('axes', labelsize=14, titlesize=14)
    plt.rc('legend', fontsize=14)
    plt.rc('xtick', labelsize=10)
    plt.rc('ytick', labelsize=10)

    # Plot histogram
    pdf.hist(bins=50, figsize=(12, 8))

    plt.show()