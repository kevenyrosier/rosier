
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import array_contains
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import array
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import concat, col
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import concat, lit, col, size, split
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import upper, lower, col
from pyspark.sql import functions as f
from pyspark.sql.functions import substring, length, col, expr
import os
import time
import datetime
from pyspark.sql.functions import *
import string
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import current_date, datediff
import pytest
#import adding
import pyspark.sql.functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.utils import PythonException


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.getOrCreate()

input_list = [Row("14682567488545865180", "1617558612", 1, "desktop", "Privacy", 6),
        Row("15710216700719891235", "1617530317", 1, "desktop", "Privacy", 7)]
input_schema = StructType([
            StructField('fullvisitorid', StringType()),
            StructField('visitid', StringType()),
            StructField('visitnumber', IntegerType()),
            StructField('devicecategory', StringType()),
            StructField('eventcategory', StringType()),
            StructField('eventaction', IntegerType())
        ])

def test_here_put_the_test_description(spark_session):
    test_input = spark_session.createDataFrame(input_list,
    schema = input_schema)
    return test_input


    expected_result = test_input.select([Row("fullvisitorid").count()>= test_result
    ])

    test_result = test_here_put_the_test_description(test_input)
    actual_result = test_result.collect()

    assert (expected_result == actual_result)