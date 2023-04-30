# COMP4442-Project
This is a PolyU COMP 4442 project to design a web service based on Spark SQL.

## Objective
As an important part of an intelligent transportation system, driving behavior analysis helps us to identify dangerous driving scenarios to avoid traffic accidents. The project is to develop a website for driver behavior analysis.

## Goals
The website should support the following functions: 
a) Generate a summary to show the driving behavior of each driver. 
b) Monitor the driving speed of each driver in real-time.

## Dataset Information
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

## Group Members
* [Dicky Shek](https://github.com/HKSSY)
* PY Yuk 
* Walter Yip
* Roland Mok
* Sam Kan
