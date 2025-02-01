from great_expectations.data_context import FileDataContext

from pyspark.sql import SparkSession



# get previously exported context
path_to_context = "/mnt/d/Projects/Python/pyspark-great-expectations-sandbox"
context = FileDataContext.create(project_root_dir=path_to_context)

print(context.list_datasources())

# get input data
spark = SparkSession.builder.appName('Fabric Tutorial').getOrCreate()
df = spark.read.csv("input_data/my_data.csv", header=True, inferSchema=True)

data_asset = context.get_datasource("spark_data_source").get_asset("input_data_my_data_df")

data_asset.build_batch_request(dataframe=df)


# run checks again
checkpoint_run_results = context.run_checkpoint(checkpoint_name="my_checkopoint")
print(checkpoint_run_results)