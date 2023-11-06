from mylib.lib import (
    extract,
    load,
    query,
    data_transform,
    start_spark,
    end_spark,
)


def main():
    extract()
    spark = start_spark("Fifa22")
    df = load(spark)
    query(spark, df)
    data_transform(df)
    end_spark(spark)


if __name__ == "__main__":
    main()
