import os

from flask import Flask
from flask import request
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType

spark = SparkSession.builder.getOrCreate()

app = Flask(__name__)

print('      ___           ___           ___           ___           ___           ___           ___      ')
print('     /\  \         /\__\         /\  \         /\  \         /\  \         /\  \         /\__\     ')
print('    /::\  \       /:/  /        /::\  \       /::\  \        \:\  \       /::\  \       /:/  /     ')
print('   /:/\:\  \     /:/__/        /:/\:\  \     /:/\:\  \        \:\  \     /:/\:\  \     /:/__/      ')
print('  /:/  \:\  \   /::\  \ ___   /::\~\:\  \   /::\~\:\  \       /::\  \   /::\~\:\  \   /::\  \ ___  ')
print(' /:/__/ \:\__\ /:/\:\  /\__\ /:/\:\ \:\__\ /:/\:\ \:\__\     /:/\:\__\ /:/\:\ \:\__\ /:/\:\  /\__\ ')
print(' \:\  \  \/__/ \/__\:\/:/  / \:\~\:\ \/__/ \:\~\:\ \/__/    /:/  \/__/ \/__\:\/:/  / \/__\:\/:/  / ')
print('  \:\  \            \::/  /   \:\ \:\__\    \:\ \:\__\     /:/  /           \::/  /       \::/  /  ')
print('   \:\  \           /:/  /     \:\ \/__/     \:\ \/__/     \/__/            /:/  /        /:/  /   ')
print('    \:\__\         /:/  /       \:\__\        \:\__\                       /:/  /        /:/  /    ')
print('     \/__/         \/__/         \/__/         \/__/                       \/__/         \/__/     ')
print('                                COMP4442 Project     Version: 0.9.1                                ')

def produce_pi(scale):
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()
    n = 100000 * scale

    def f(_):
        from random import random
        x = random()
        y = random()
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(
        xrange(1, n + 1), scale).map(f).reduce(lambda x, y: x + y)
    spark.stop()
    pi = 4.0 * count / n
    return pi


@app.route("/")
def index():
    return "Python Flask SparkPi server running. Add the 'sparkpi' route to this URL to invoke the app."


@app.route("/sparkpireadtest")
def sparkpi():
    customSchema = StructType([
    StructField("driverID", StringType(), True),        
    StructField("carPlateNumber", StringType(), True),
    StructField("Latitude", StringType(), True),
    StructField("Longtitude", StringType(), True),
    StructField("Speed", StringType(), True),
    StructField("Direction", StringType(), True),
    StructField("siteName", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("isRapidlySpeedup", StringType(), True),
    StructField("isRapidlySlowdown", StringType(), True),
    StructField("isNeutralSlide", StringType(), True),
    StructField("isNeutralSlideFinished", StringType(), True),
    StructField("neutralSlideTime", StringType(), True),
    StructField("isOverspeed", StringType(), True),
    StructField("isOverspeedFinished", StringType(), True),
    StructField("overspeedTime", StringType(), True),
    StructField("isFatigueDriving", StringType(), True),
    StructField("isHthrottleStop", StringType(), True),
    StructField("isOilLeak", StringType(), True)
    ])

    df = spark.read\
        .option('header', True)\
        .option('escape', '"')\
        .schema(customSchema)\
        .csv('/home/comp4442/Downloads/detail-records/detail_record_2017_01_02_08_00_00')
    #scale = int(request.args.get('scale', 2))
    #pi = produce_pi(scale)
    test = df._jdf.schema().treeString()
    #response = str(test)
    response = df.select('*').collect()
    return response


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7777))
    app.run(host='0.0.0.0', port=port)

