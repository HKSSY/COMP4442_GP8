import os

from flask import Flask
from flask import request
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import unix_timestamp

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
print('                                COMP4442 Project     Version: 0.9.2                                ')

# Dataset location settings
dataset_directory = r"/home/comp4442/Downloads/detail-records/"

def load_dataset():
    global dataset_dataframe
    customSchema = StructType([ # Set a custom schema for the dataset
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

    try:
        search_directory = dataset_directory
        dataset_full_path_list = []

        for file in os.listdir(os.path.join(search_directory)):
            dataset_file_path = os.path.join(search_directory, file)
            dataset_full_path_list.append(dataset_file_path)

        #print(dataset_full_path_list)

        dataset_dataframe = spark.read\
                .option('header', False)\
                .option('escape', '"')\
                .schema(customSchema)\
                .csv(dataset_full_path_list)
        
        dataset_dataframe.cache()

        #list_dataset = df.select('*').collect()
        #test = df.select('driverID').collect()
        print("Message: Read dataset successfully")
        print(*dataset_full_path_list, sep = "\n")
    except FileNotFoundError:
        print("Initialization error: The dataset directory does not exist")
        exit()

    #return message


@app.route("/")
def index():
    #test = dataset_dataframe.select('driverID').collect()
    #test = dataset_dataframe.where("Time = '2017-01-01'").collect()
    test = dataset_dataframe.where("carPlateNumber = '华AVM936'").collect()
    print(dataset_dataframe.is_cached)
    return str(test)


@app.route("/about")
def sparkpi():
    #scale = int(request.args.get('scale', 2))
    #pi = produce_pi(scale)
    #test = load_dataset.df._jdf.schema().treeString()
    #response = str(test)
    #command = ".select('*').collect()"
    response = load_dataset()
    return response


if __name__ == "__main__":
    load_dataset()
    port = int(os.environ.get("PORT", 7777))
    app.run(host='0.0.0.0', port=port)