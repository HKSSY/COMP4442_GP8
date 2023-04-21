import os
import socket

from pathlib import Path
from flask import Flask, render_template, request, jsonify
from flask import request
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import count, countDistinct, col, max as sparkMax
from pyspark.sql.functions import monotonically_increasing_id

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
print('                                COMP4442 Project     Version: 0.9.9                                ')

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
    StructField("Time", TimestampType(), True),
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
        dataset_dataframe = dataset_dataframe.withColumn('id', monotonically_increasing_id())
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


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/fetch_data', methods=['POST'])
def fetch_data():
    start = int(request.form['start'])
    length = int(request.form['length'])
    draw = int(request.form['draw'])
    search_value = request.form['search[value]']
    
    # Add search condition
    search_condition = ""
    if search_value:
        search_condition = f"""WHERE (
        LOWER(driverID) LIKE LOWER('%{search_value}%') OR
        LOWER(carPlateNumber) LIKE LOWER('%{search_value}%') OR
        LOWER(Latitude) LIKE LOWER('%{search_value}%') OR
        LOWER(Longtitude) LIKE LOWER('%{search_value}%') OR
        LOWER(Speed) LIKE LOWER('%{search_value}%') OR
        LOWER(siteName) LIKE LOWER('%{search_value}%') OR
        LOWER(Time) LIKE LOWER('%{search_value}%')
        )"""

    # Get the ordering information from the request
    order_column = request.form['order[0][column]']
    order_dir = request.form['order[0][dir]']
    order_column_name = request.form[f'columns[{order_column}][data]']

    records_total = spark.sql("SELECT COUNT(*) as count FROM RECORD").collect()[0]["count"]

    # Update records_filtered based on search condition
    if search_condition:
        records_filtered = spark.sql(f"SELECT COUNT(*) as count FROM RECORD {search_condition}").collect()[0]["count"]
    else:
        records_filtered = records_total

    sql_results = spark.sql(f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(ORDER BY {order_column_name} {order_dir}) as row_num FROM RECORD {search_condition}
        ) tmp
        WHERE tmp.row_num > {start} AND tmp.row_num <= {start + length}
    """).collect()

    data = [{
        "id": sql_result["id"],
        "driverID": sql_result["driverID"],
        "carPlateNumber": sql_result["carPlateNumber"],
        "Latitude": sql_result["Latitude"],
        "Longtitude": sql_result["Longtitude"],
        "Speed": sql_result["Speed"],
        "siteName": sql_result["siteName"],
        "Time": sql_result["Time"],
        "isRapidlySpeedup": sql_result["isRapidlySpeedup"],
        "isRapidlySlowdown": sql_result["isRapidlySlowdown"],
        "isNeutralSlide": sql_result["isNeutralSlide"],
        "isNeutralSlideFinished": sql_result["isNeutralSlideFinished"],
        "neutralSlideTime": sql_result["neutralSlideTime"],
        "isRapidlySlowdown": sql_result["isRapidlySlowdown"],
        "isOverspeed": sql_result["isOverspeed"],
        "isOverspeedFinished": sql_result["isOverspeedFinished"],
        "overspeedTime": sql_result["overspeedTime"],
        "isFatigueDriving": sql_result["isFatigueDriving"],
        "isHthrottleStop": sql_result["isHthrottleStop"],
        "isOilLeak": sql_result["overspeedTime"]
    } for sql_result in sql_results]

    response = {
        "draw": draw,
        "recordsTotal": records_total,
        "recordsFiltered": records_filtered,
        "data": data
    }

    return jsonify(response)

@app.route("/summary", methods=['GET'])
def summary():
    sql_query = "driverID AS `Driver ID`, ROUND(AVG(Speed), 1) AS `AVG Speed`, COUNT(isRapidlySpeedup) AS `Rapidly Speedup`, \
    COUNT(isRapidlySlowdown) AS `Rapidly Slowdown`, COUNT(isNeutralSlide) AS `Neutral Slide`, SUM(neutralSlideTime) AS `Total Neutral Slide`, \
    COUNT(isOverspeed) AS `Overspeed`, SUM(overspeedTime) AS `Total Overspeed`, COUNT(isFatigueDriving) AS `Fatigue Driving`, \
    COUNT(isHthrottleStop) AS `Throttle Stop`, COUNT(isOilLeak) AS `Oil Leak`"
    sql_groupby_query = "GROUP BY driverID"
    output_summary = spark.sql("SELECT " + sql_query + " FROM RECORD " + sql_groupby_query).toPandas()
    response = output_summary
    #print(response)
    return render_template('summary.html', tables=[response.to_html(classes='g--12 g-s--12 card', index=False).replace('<tr style="text-align: right;">', '<tr class="table-header">')], titles=response.columns.values)
    #return app.send_static_file('index.html')

@app.route("/car_speed_monitor", methods=['GET', 'POST'])
def car_speed_monitor():
    sql_query = "driverID, DATE(Time) AS `Date`, ROUND(AVG(Speed), 1) AS `AVG Speed`"
    sql_groupby_query = "GROUP BY driverID, DATE(Time)"
    sql_orderby_query = "ORDER BY driverID, Date"
    output_summary = spark.sql("SELECT " + sql_query + " FROM RECORD " + sql_groupby_query + " " + sql_orderby_query).collect()

    # Prepare the data for the graph
    drivers_data = {}
    for row in output_summary:
        driver_id = row['driverID']
        date = row['Date'].strftime("%Y-%m-%d")
        avg_speed = row['AVG Speed']

        if driver_id not in drivers_data:
            drivers_data[driver_id] = {
                'dates': [],
                'avg_speeds': [],
            }

        drivers_data[driver_id]['dates'].append(date)
        drivers_data[driver_id]['avg_speeds'].append(avg_speed)

    # Return the components to the HTML template
    return render_template(
        template_name_or_list='car_speed_monitor.html',
        drivers_data=drivers_data,
    )

@app.route("/about", methods=['GET'])
def about_page():
    return app.send_static_file('about.html')

@app.errorhandler(500)
def handle_bad_request(e):
    return 'Internal Server Error!', 500

@app.errorhandler(404)
def handle_bad_request(e):
    return 'Page not found!', 404

if __name__ == "__main__":
    load_dataset()
    app.run(host, port, debug=True)