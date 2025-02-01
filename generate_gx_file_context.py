import great_expectations as gx
from great_expectations.data_context import EphemeralDataContext


from pyspark.sql import SparkSession

def generate_gx_file_context(context: EphemeralDataContext, table_name: str, spark: SparkSession) -> None:
    # set up context
    
    suit_name = f"{table_name}_suite"
    datasource_name = f"{table_name}_spark_datasource"
    data_asset_name = f"{table_name}_spark_dataframe"
    checkpoint_name = f"{table_name}_checkpoint"

    context.add_or_update_expectation_suite(suit_name)

    # setup input datasource
    spark_datasource = context.sources.add_spark(datasource_name)
    data_asset = spark_datasource.add_dataframe_asset(data_asset_name)

    batch_request = data_asset.build_batch_request(
        dataframe=spark.read.csv(f"input_data/tables/{table_name}.csv", header=True, inferSchema=True)
    )
    # initialize validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suit_name
    )#


    # define validations
    # validator.expect_column_values_to_be_in_set("age", [24, 28, 21, 48])
    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_be_between("age", min_value=25, max_value=50)
    validator.expect_column_values_to_not_be_null("age")

    if table_name == "my_table_name":
        validator.expect_column_values_to_be_in_set("enum_column", ["cat", "dog"])
    else: 
        validator.expect_column_values_to_be_in_set("other_column", [1, 2])

    # save these expecations in Expectation Suite
    validator.save_expectation_suite(discard_failed_expectations=False)


    # define checkpoint 

    yaml_config = f"""
    name: {checkpoint_name}
    config_version: 1.0
    class_name: SimpleCheckpoint
    run_name_template: "check-{table_name}_%Y-%m-%d"
    validations:
        - batch_request:
            datasource_name: {datasource_name}
            data_asset_name: {data_asset_name}
    expectation_suite_name: {suit_name}
    """

    my_checkpoint = context.test_yaml_config(yaml_config)
    context.add_checkpoint(checkpoint=my_checkpoint)

    


if __name__ == "__main__":
    tables = ["my_table_name", "some_other_table_name"]
    spark = SparkSession.builder.appName('Fabric Tutorial').getOrCreate()

    context = gx.get_context()
    for table in tables:
        generate_gx_file_context(
            context=context,
            table_name=table,
            spark=spark
        )
    context.convert_to_file_context()
    