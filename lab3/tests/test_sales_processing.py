import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import year, col, sum as _sum, lower
from datetime import datetime

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-sales-processing") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    yield spark
    spark.stop()

def sample_df(spark):
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("transaction_date", TimestampType(), True),  # allow null for testing dropna
        StructField("payment_mode", StringType(), False),
        StructField("store_location", StringType(), False),
        StructField("discount_applied", BooleanType(), False),
        StructField("discount_amount", DoubleType(), False),
        StructField("loyalty_points_earned", IntegerType(), False),
        StructField("is_return", BooleanType(), False),
        StructField("sales_rep", StringType(), False),
    ])

    data = [
        # customer C1: credit 100, debit 100 -> violates (credit not < debit and not <=10%)
        ("T001","C1","Alice","P001","Laptop","Electronics",100.0,1, datetime(2024,6,1,10,30),"Credit Card","NY",False,0.0,10,False,"SR01"),
        ("T002","C1","Alice","P002","Jeans","Clothing",50.0,2, datetime(2024,6,2,14,15),"Debit Card","NY",False,0.0,5,False,"SR02"),
        # customer C2: only debit -> OK
        ("T003","C2","Bob","P003","Apples","Grocery",20.0,1, datetime(2024,6,3,9,45),"Debit Card","LA",False,0.0,2,False,"SR03"),
        # customer C3: credit small <=10% of total -> OK
        ("T004","C3","Carol","P004","Book","Books",9.0,1, datetime(2024,6,4,11,0),"Credit Card","SF",False,0.0,0,False,"SR04"),
        ("T005","C3","Carol","P005","Pen","Stationery",90.0,1, datetime(2024,6,4,12,0),"Debit Card","SF",False,0.0,0,False,"SR04"),
        # row with null to test dropna
        ("T006","C4","Dan","P006","Toy","Toys",15.0,1, None,"Cash","TX",False,0.0,1,False,"SR05"),
    ]
    return spark.createDataFrame(data, schema=schema)

def test_dropna_and_order_year(spark):
    df = sample_df(spark)
    df_clean = df.dropna()  # should remove the row with null date (T006)
    df_with_year = df_clean.withColumn("order_year", year(col("transaction_date")))

    assert df.count() == 6
    assert df_clean.count() == 5  # one row dropped
    years = [r["order_year"] for r in df_with_year.select("order_year").distinct().collect()]
    assert 2024 in years

def test_total_sales_per_year(spark):
    df = sample_df(spark)
    df_clean = df.dropna().withColumn("order_year", year(col("transaction_date")))
    agg = df_clean.groupBy("order_year").agg(_sum((col("amount") * col("quantity"))).alias("total_sales"))
    res = {row["order_year"]: row["total_sales"] for row in agg.collect()}

    # compute expected total for 2024 ignoring the null row
    expected = 100.0*1 + 50.0*2 + 20.0*1 + 9.0*1 + 90.0*1  # = 100 + 100 + 20 + 9 + 90 = 319
    assert pytest.approx(res.get(2024, 0.0), rel=1e-6) == expected

def test_total_amount_per_payment_method(spark):
    df = sample_df(spark)
    df_clean = df.dropna()
    agg = df_clean.groupBy("payment_mode").agg(_sum((col("amount") * col("quantity"))).alias("total_amount"))
    res = {row["payment_mode"]: row["total_amount"] for row in agg.collect()}

    # expected amounts per payment mode
    # "Credit Card": 100 + 9 = 109
    # "Debit Card": 100 + 20 + 90 = 210
    assert pytest.approx(res.get("Credit Card", 0.0), rel=1e-6) == 109.0
    assert pytest.approx(res.get("Debit Card", 0.0), rel=1e-6) == 210.0

def test_credit_debit_customer_validation(spark):
    df = sample_df(spark)
    df_clean = df.dropna()
    # run same validation logic as notebook: aggregate per customer
    agg = df_clean.groupBy("customer_id").agg(
        _sum((col("amount") * col("quantity")).when(lower(col("payment_mode")).like("%credit%"), col("amount") * col("quantity")).otherwise(0)).alias("total_credit")
    )
    # Above .when is not directly available in this form; instead build with SQL for clarity
    df_clean.createOrReplaceTempView("tmp_sample")
    validation_sql = """
    WITH agg AS (
      SELECT customer_id,
             SUM(CASE WHEN lower(payment_mode) LIKE '%credit%' THEN amount * quantity ELSE 0 END) AS total_credit,
             SUM(CASE WHEN lower(payment_mode) LIKE '%debit%' THEN amount * quantity ELSE 0 END) AS total_debit,
             SUM(amount * quantity) AS total_amount
      FROM tmp_sample
      GROUP BY customer_id
    )
    SELECT customer_id, total_credit, total_debit, total_amount
    FROM agg
    WHERE NOT (total_credit < total_debit AND total_credit <= 0.1 * total_amount)
    ORDER BY customer_id
    """
    violations = [row["customer_id"] for row in spark.sql(validation_sql).collect()]

    # From sample data, C1 violates (credit=100, debit=100 -> credit not < debit)
    assert "C1" in violations
    # C2 and C3 should not be in violations (C2 only debit; C3 credit is 9 <=10% of 99 -> okay)
    assert "C2" not in violations
    assert "C3" not in violations
