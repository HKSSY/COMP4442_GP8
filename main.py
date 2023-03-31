import os
import socket
import pandas

from pathlib import Path
from flask import Flask, render_template
from flask import request
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import count, countDistinct, col, max as sparkMax

spark = SparkSession.builder.getOrCreate()


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
print('                                COMP4442 Project     Version: 0.9.4                                ')

# Check whether the port is open. if it is used by other application, it will switch the other listen port
host = "localhost"
port = 3500
for i in range(0,10):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if sock.connect_ex((host, port)) == 0:
        port += 1
        print("The current port is: " + str(port))
        sock.close()
    else:
        print("The current port is: " + str(port))
        break

# Dataset location settings
current_path = Path(__file__).absolute().parent
dataset_directory = current_path / "detail-records/"

def load_dataset():
    global dataset_dataframe
    customSchema = StructType([ # Set a custom schema for the dataset
    StructField("driverID", StringType(), True),
    StructField("carPlateNumber", StringType(), True),
    StructField("Latitude", StringType(), True),
    StructField("Longtitude", StringType(), True),
    StructField("Speed", IntegerType(), True),
    StructField("Direction", StringType(), True),
    StructField("siteName", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("isRapidlySpeedup", StringType(), True),
    StructField("isRapidlySlowdown", StringType(), True),
    StructField("isNeutralSlide", StringType(), True),
    StructField("isNeutralSlideFinished", StringType(), True),
    StructField("neutralSlideTime", IntegerType(), True),
    StructField("isOverspeed", StringType(), True),
    StructField("isOverspeedFinished", StringType(), True),
    StructField("overspeedTime", IntegerType(), True),
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
        dataset_dataframe.createOrReplaceTempView("RECORD")
        dataset_dataframe.cache()

        #list_dataset = df.select('*').collect()
        #test = df.select('driverID').collect()
        print("Message: Read dataset successfully")
        print(*dataset_full_path_list, sep = "\n")
    except FileNotFoundError:
        print("Initialization error: The dataset directory does not exist")
        exit()

    #return message

app = Flask(__name__, static_url_path='/static', template_folder='static/') # add path for the HTML files

@app.route("/", methods=['GET'])
def index():
    sql_query = "driverID AS `Driver ID`, ROUND(AVG(Speed), 1) AS `AVG Speed`, COUNT(isRapidlySpeedup) AS `Rapidly Speedup`, \
    COUNT(isRapidlySlowdown) AS `Rapidly Slowdown`, COUNT(isNeutralSlide) AS `Neutral Slide`, SUM(neutralSlideTime) AS `Total Neutral Slide`, \
    COUNT(isOverspeed) AS `Overspeed`, SUM(overspeedTime) AS `Total Overspeed`, COUNT(isFatigueDriving) AS `Fatigue Driving`, \
    COUNT(isHthrottleStop) AS `Throttle Stop`, COUNT(isOilLeak) AS `Oil Leak`"
    sql_groupby_query = "GROUP BY driverID"
    output_summary = spark.sql("SELECT " + sql_query + " FROM RECORD " + sql_groupby_query).toPandas()
    response = output_summary
    #print(response)
    return render_template('index.html', tables=[response.to_html(classes='data', index=False)], titles=response.columns.values)
    #return app.send_static_file('index.html')

@app.route("/test", methods=['GET', 'POST'])
def sparkpi():
    #test = dataset_dataframe.select('driverID').collect()
    #test = dataset_dataframe.where("Time = '2017-01-01'").collect()
    #test = dataset_dataframe.where("isHthrottleStop = 'NaN'").collect()
    #print(test)
    #count1 = dataset_dataframe.filter((col('driverID') == 'haowei1000008')).count()
    #print(count1)
    #count2 = dataset_dataframe.filter((col('driverID') == 'haowei1000008') & (col('isHthrottleStop').isNotNull())).count()
    #print(count2)
    #count = dataset_dataframe.filter((col('driverID') == 'haowei1000008') & (col('isHthrottleStop').isNull())).count()
    #print(count)
    #count3 = dataset_dataframe.groupBy('driverID').count().show()
    #count3 = dataset_dataframe.select((col('driverID') == 'haowei1000008') & sparkMax(col('Speed'))).show()
    #spark.sql("SELECT * FROM RECORD WHERE driverID == 'haowei1000008'").show(5) # Show first 5 record
    #spark.sql("SELECT MAX(Speed) FROM RECORD WHERE driverID == 'haowei1000008'").show(5) # Show first 5 record
    sql_query = "driverID"
    sql_groupby_query = "GROUP BY driverID"
    output_driver_id = spark.sql("SELECT " + sql_query + " FROM RECORD " + sql_groupby_query).rdd.map(lambda x : x[0]).collect()
    print(output_driver_id)
    sql_query = "carPlateNumber"
    sql_groupby_query = "GROUP BY carPlateNumber"
    output_car_plate_number = spark.sql("SELECT " + sql_query + " FROM RECORD " + sql_groupby_query).rdd.map(lambda x : x[0]).collect()
    print(output_car_plate_number)
    response = output_driver_id
    #print(response)
    return render_template('test.html', option_driver_id=output_driver_id, opt_car_plate_number=output_car_plate_number)

@app.errorhandler(500)
def handle_bad_request(e):
    return 'Internal Server Error!', 500

@app.errorhandler(404)
def handle_bad_request(e):
    return 'Page not found!', 404

if __name__ == "__main__":
    load_dataset()
    app.run(host, port)