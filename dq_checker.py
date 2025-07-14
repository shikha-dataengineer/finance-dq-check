from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan

# Initialize Spark session
def main():
    spark = SparkSession.builder \
        .appName("Finance Data Quality Checker") \
        .master("local[*]") \
        .getOrCreate()

    csv_path = "finance_data.csv"
    df = spark.read.option("header", "true").csv(csv_path)

# Print schema
    print("\n=== Schema ===")
    df.printSchema()

#  Missing Value Check
    print("\n=== Missing Values ===")
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

#  Duplicate Check
    print("\n=== Duplicate Rows ===")
    total = df.count()
    distinct = df.dropDuplicates().count()
    print(f"Total rows: {total}")
    print(f"Unique rows: {distinct}")
    print(f"Duplicates: {total - distinct}")

# Schema Check (expected vs actual)
    expected_cols = {"date", "symbol", "open", "high", "low", "close", "volume"}
    actual_cols = set(df.columns)

    print("\n=== Schema Mismatch ===")
    print("Missing columns:", expected_cols - actual_cols)
    print("Unexpected columns:", actual_cols - expected_cols)

    spark.stop()

if __name__ == "__main__":
    main()
