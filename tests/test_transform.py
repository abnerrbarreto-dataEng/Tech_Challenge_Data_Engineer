import json

from pyspark.sql import SparkSession

from scripts import transform_silver, aggregate_gold


def create_spark_session():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_transform_silver_creates_parquet(tmp_path):
    # Arrange
    date = "2026-03-16"
    bronze_path = tmp_path / "bronze" / "breweries" / date
    bronze_path.mkdir(parents=True)

    sample_data = [{"id": "1", "name": "Brewery A", "state": "TX", "state_province": None}]
    with open(bronze_path / "breweries_raw.json", "w", encoding="utf-8") as f:
        json.dump(sample_data, f)

    transform_silver.BASE_PATH = str(tmp_path)

    spark = create_spark_session()

    # Act
    transform_silver.transform_to_silver(spark, date)

    # Assert
    silver_path = str(tmp_path / "silver" / "breweries" / date)
    df = spark.read.parquet(silver_path)
    result = df.filter(df.id == "1").collect()

    assert len(result) == 1
    assert result[0]["state_province"] == "TX"

    spark.stop()


def test_aggregate_gold_produces_expected_counts(tmp_path):
    date = "2026-03-16"
    silver_path = tmp_path / "silver" / "breweries" / date
    silver_path.mkdir(parents=True)

    sample_data = [
        {"id": "1", "brewery_type": "micro", "state_province": "TX"},
        {"id": "2", "brewery_type": "micro", "state_province": "TX"},
        {"id": "3", "brewery_type": "brewpub", "state_province": "CA"},
    ]

    spark = create_spark_session()
    spark.createDataFrame(sample_data).write.parquet(str(silver_path))

    aggregate_gold.BASE_PATH = str(tmp_path)

    aggregate_gold.aggregate_to_gold(spark, date)

    gold_path = str(tmp_path / "gold" / "brewery_summary" / date)
    df_gold = spark.read.parquet(gold_path)

    results = { (row.brewery_type, row.state_province): row.qtd_breweries for row in df_gold.collect() }
    assert results[("micro", "TX")] == 2
    assert results[("brewpub", "CA")] == 1

    spark.stop()
