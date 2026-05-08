import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, NumericType
from pyspark.sql import functions as F

from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA

# ---------- CONFIG ----------
MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"

TABLE = "silver_catalog.default.yellow_taxi"


# ---------- SPARK ----------
def build_spark():
    return SparkSession.builder \
        .appName("PCA_Analysis") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


# ---------- HELPERS ----------
def get_numeric_cols(df):
    return [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]


def cast_to_double(df, cols):
    return df.select(*[F.col(c).cast(DoubleType()).alias(c) for c in cols])


# ---------- MAIN ----------
def run_pca(sample_fraction=0.2, k=5):
    spark = build_spark()

    print("Loading data...")
    df = spark.table(TABLE)

    print("Running PCA on yellow taxi trips...")

    print("Selecting numeric columns...")
    num_cols = get_numeric_cols(df)
    df = cast_to_double(df, num_cols)

    print(f"Sampling {sample_fraction*100}%...")
    df = df.sample(fraction=sample_fraction, seed=42).dropna()

    print("Assembling features...")
    assembler = VectorAssembler(inputCols=num_cols, outputCol="features")
    df_vec = assembler.transform(df).select("features")

    print("Scaling features...")
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    scaler_model = scaler.fit(df_vec)
    df_scaled = scaler_model.transform(df_vec)

    print(f"Running PCA with k={k}...")
    pca = PCA(k=k, inputCol="scaled_features", outputCol="pca_features")
    pca_model = pca.fit(df_scaled)

    df_pca = pca_model.transform(df_scaled)

    print("\nExplained Variance:")
    print(pca_model.explainedVariance)

    print("\nSample PCA Output:")
    df_pca.select("pca_features").show(5, truncate=False)

    spark.stop()


# ---------- ENTRY ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=float, default=0.2)
    parser.add_argument("--k", type=int, default=5)
    args = parser.parse_args()

    run_pca(sample_fraction=args.sample, k=args.k)