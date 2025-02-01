from great_expectations.data_context import FileDataContext

from pyspark.sql import SparkSession



table_name = "my_table_name"

# get previously exported context
path_to_context = "/mnt/d/Projects/Python/pyspark-great-expectations-sandbox"
context = FileDataContext.create(project_root_dir=path_to_context)

print(context.list_datasources())

# get input data
spark = SparkSession.builder.appName('Fabric Tutorial').getOrCreate()
df = spark.read.csv(f"input_data/tables/{table_name}.csv", header=True, inferSchema=True)

data_asset = context.get_datasource(f"{table_name}_spark_datasource").get_asset(f"{table_name}_spark_dataframe")

data_asset.build_batch_request(dataframe=df)


# run checks again
checkpoint_run_results = context.run_checkpoint(checkpoint_name=f"{table_name}_checkpoint")
print(checkpoint_run_results)


