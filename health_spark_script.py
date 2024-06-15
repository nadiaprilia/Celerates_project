from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL Job") \
    .getOrCreate()

# Extract data from OLTP
df_transactions = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/oltp_db") \
    .option("dbtable", "public.health_transactions") \
    .option("user", "nadiapril239") \
    .option("password", "Nadiagokil239*#") \
    .load()

df_references = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/oltp_db") \
    .option("dbtable", "public.disease_reference, public.doctor_reference, public.patient_reference, public.treatment_reference") \
    .option("user", "nadiapril239") \
    .option("password", "Nadiagokil239*#") \
    .load()

# Transform data as needed
# Contoh transformasi sederhana
df_transformed = df_transactions.join(df_references, df_transactions["disease_id"] == df_references["disease_id"])

# Load data into OLAP
df_transformed.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/olap_db") \
    .option("dbtable", "public.transformed_health_data") \
    .option("user", "nadiapril239") \
    .option("password", "Nadiagokil239*#") \
    .mode("append") \
    .save()

spark.stop()
