class WriteDF:
    def write(sourceDF):
        sourceDF.write \
            .format("com.databricks.spark.csv") \
            .option("header", True) \
            .option("delimiter", ",") \
            .mode("overwrite") \
            .option('nullValue', 'null') \
            .save("./target/final_dataset")
