import great_expectations as gx

from pyspark.sql import SparkSession, DataFrame 



def test_great_expectations(df: DataFrame) -> None:
    # set up context
    context = gx.get_context()
    context.add_or_update_expectation_suite("spark_expectation_suite")

    # setup input datasource
    spark_data_source = context.sources.add_spark("spark_data_source")
    data_asset = spark_data_source.add_dataframe_asset("input_data_my_data_df")

    my_batch_request = data_asset.build_batch_request(dataframe=df)
    print(df.columns)
    # initialize validator
    validator = context.get_validator(
        batch_request=my_batch_request,
        expectation_suite_name="spark_expectation_suite"
    )


    # define validations
    # validator.expect_column_values_to_be_in_set("age", [24, 28, 21, 48])
    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_be_between("age", min_value=25, max_value=50)
    validator.expect_column_values_to_be_in_set("enum_column", ["cat", "dog"])

    # save these expecations in Expectation Suite
    validator.save_expectation_suite(discard_failed_expectations=False)


    # define checkpoint 
    my_checkopoint_name = "my_checkopoint"

    yaml_config = f"""
    name: {my_checkopoint_name}
    config_version: 1.0
    class_name: SimpleCheckpoint
    run_name_template: "my-run-name-template"
    validations:
        - batch_request:
            datasource_name: spark_data_source
            data_asset_name: input_data_my_data_df
    expectation_suite_name: spark_expectation_suite
    """

    my_checkopoint = context.test_yaml_config(yaml_config)
    context.add_checkpoint(checkpoint=my_checkopoint)

    checkpoint_run_results = context.run_checkpoint(my_checkopoint_name)

    print(checkpoint_run_results)

    print("so far so good")


def main() -> None:
    spark = SparkSession.builder.appName('Fabric Tutorial').getOrCreate()

    df = spark.read.csv("input_data/my_data.csv", header=True, inferSchema=True)
    df.printSchema()
    df.show()

    test_great_expectations(df)



if __name__ == '__main__':
    main()