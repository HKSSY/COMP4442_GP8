# COMP4442-Project
This is a PolyU COMP 4442 project to design a web service based on Spark SQL.

## Objective
As an important part of an intelligent transportation system, driving behavior analysis helps us to identify dangerous driving scenarios to avoid traffic accidents. The project is to develop a website for driver behavior analysis.

## Goals
The website should support the following functions: 
- a) Generate a summary to show the driving behavior of each driver. 
- b) Monitor the driving speed of each driver in real-time.

## Dataset Information
There are 10 files in the dataset, each describing the drivers’ driving behavior one day. In each file, each row represents a record that describes the driving-related features. The features are separated by commas. There are 19 features in total. The detailed information on the dataset will be shown in the table below:
<table>
    <thead>
        <tr>
            <th>Feature</th>
            <th>Comments</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>driverID</td>
            <td>N/A</td>
        </tr>
        <tr>
            <td>carPlateNumber</td>
            <td>N/A</td>
        </tr>
        <tr>
            <td>Latitude</td>
            <td>N/A</td>
        </tr>
        <tr>
            <td>Longtitude</td>
            <td>N/A</td>
        </tr>
        <tr>
            <td>Speed</td>
            <td>Km/h</td>
        </tr>
        <tr>
            <td>Direction</td>
            <td>N/A</td>
        </tr>
        <tr>
            <td>siteName</td>
            <td>N/A</td>
        </tr>
        <tr>
            <td>Time</td>
            <td>The time reporting the record</td>
        </tr>
        <tr>
            <td>isRapidlySpeedup</td>
            <td>1 indicates the presence of the behavior</td>
        </tr>
        <tr>
            <td>isRapidlySlowdown</td>
            <td>1 indicates the presence of the behavior</td>
        </tr>
        <tr>
            <td>isNeutralSlide</td>
            <td>1 indicates the presence of the behavior</td>
        </tr>
        <tr>
            <td>isNeutralSlideFinished</td>
            <td>1 indicates the end of the behavior</td>
        </tr>
        <tr>
            <td>neutralSlideTime</td>
            <td>The  duration  of  a  neutral  slide  behavior (seconds)</td>
        </tr>
        <tr>
            <td>isOverspeed</td>
            <td>1 indicates the presence of the behavior</td>
        </tr>
        <tr>
            <td>isOverspeedFinished</td>
            <td>1 indicates the end of the behavior</td>
        </tr>
        <tr>
            <td>overspeedTime</td>
            <td>The  duration  of  an  overspeed  behavior (seconds)</td>
        </tr>
        <tr>
            <td>isFatigueDriving</td>
            <td>1 indicates the presence of the behavior</td>
        </tr>
        <tr>
            <td>isHthrottleStop</td>
            <td>1 indicates the presence of the behavior</td>
        </tr>
        <tr>
            <td>isOilLeak</td>
            <td>1 indicates the presence of the behavior</td>
        </tr>
    </tbody>
</table>

## Designed Pages
<table>
    <thead>
        <tr>
            <th>Name</th>
            <th>Feature</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>index.html</td>
            <td>Included a DataTables with a data filtering function</td>
        </tr>
        <tr>
            <td>summary.html</td>
            <td>Included a table for displaying each driverID summary data from the dataset</td>
        </tr>
        <tr>
            <td>car_speed_monitor.html</td>
            <td>Draw the average speed graph by day for each driverID</td>
        </tr>
        <tr>
            <td>about.html</td>
            <td>Show the group information</td>
        </tr>
    </tbody>
</table>
        
## Group Members
* [Dicky Shek](https://github.com/HKSSY)
* PY Yuk 
* Walter Yip
* Roland Mok
* Sam Kan

## Credit
CSS framework: [Surface](https://github.com/mildrenben/surface)
