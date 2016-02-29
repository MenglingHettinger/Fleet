create table sensors as 
select * from fleet_daily_buckets where (sensor_key='Idle TimeAVERAGE' or 
sensor_key='Engine Coolant TemperatureAVERAGE' or sensor_key='Engine SpeedAVERAGE' or
sensor_key='Battery VoltageAVERAGE' or sensor_key='Vehicle SpeedAVERAGE');



select * from fleet_daily_buckets where (sensor_key='IgnitionTOTAL_COUNT' or 
sensor_key='IgnitionON_COUNT' or sensor_key='Idle TimeTOTAL_COUNT' or
sensor_key='Idle TimeBELOW_LIMIT_COUNT' );
	865242
	865242


set mapred.job.map.memory.mb=8192;
set mapred.job.reduce.memory.mb=8192;

add jar /tmp/adaltas-hive-udf-0.0.1-SNAPSHOT.jar;   

CREATE TEMPORARY FUNCTION to_map as 'com.adaltas.UDAFToMap';  
create table  cont_sensors as
select vehicle_id,ddmmyy,
group_map['Idle TimeAVERAGE'] as Idle_TimeAVERAGE,
group_map['Engine Coolant TemperatureAVERAGE'] as Engine_Coolant_TemperatureAVERAGE,
group_map['Engine SpeedAVERAGE'] as Engine_SpeedAVERAGE,
group_map['Battery VoltageAVERAGE'] as Battery_VoltageAVERAGE,
group_map['Vehicle SpeedAVERAGE'] as Vehicle_SpeedAVERAGE
from(
select vehicle_id, ddmmyy, to_map(sensor_key,sensor_value) as group_map
from fleet_daily_buckets
group by vehicle_id, ddmmyy
)gm;



add file /home/mh564g/select_date.py;
from fleet_daily_buckets
select transform(vehicle_id,bucket_time,sensor_key,sensor_value,ddmmyy)
using 'select_daily.py'
as vehicle_id,bucket_time,sensor_key,sensor_value,ddmmyy;


create table weather_data (
snowfall FLOAT,
sfcPresAvg FLOAT,
dewPtAvg FLOAT,
timestamp STRING,
precip FLOAT,
wetBulbAvg FLOAT,
relHumAvg FLOAT,
windSpdAvg FLOAT,
tempAvg FLOAT,
cldCvrAvg FLOAT,
spcHumAvg FLOAT,
postal_code STRING)
row format delimited
fields terminated by ','
stored as textfile;


load data inpath '/fleet/weather/weather_small.csv' into table weather_data;

 LOCATION '/fleet/weather‘


create table demand_data (
Vehicle STRING,
JobDate STRING,
DetailsDate STRING,
Mileage FLOAT,
Battery_Age FLOAT,
Delta_Mileage FLOAT,
BRR INT,
VehGroup INT,
DateInservice STRING,
SpecID STRING,
FuelType STRING,
State STRING,
GarageID STRING,
Zip STRING)
row format delimited
fields terminated by ','
stored as textfile;


load data inpath '/fleet/demand/veh_with_age.csv' into table demand_data;


sed 's/T00:00:00-08:00//g' weather_2010_small > weather_2010_small2.csv

create table weather_2010 (
snowfall float,
sfcPresAvg FLOAT,
dewPtAvg FLOAT,
timestamp date,
precip FLOAT,
wetBulbAvg FLOAT,
relHumAvg FLOAT,
windSpdAvg FLOAT,
tempAvg FLOAT,
cldCvrAvg FLOAT,
spcHumAvg FLOAT,
postal_code STRING)
row format delimited
fields terminated by ','
stored as textfile;


load data inpath '/fleet/weather/weather_2010_small.csv' into table weather_2010 IGNORE 1 LINES;


create table weather_7days as 
SELECT timestamp, postal_code, mean(snowfall), mean(sfcPresAvg), mean(dewPtAvg),mean(precip), mean(wetBulbAvg),
mean(relHumAvg), mean(windSpdAvg), mean(tempAvg), mean(cldCvrAvg), mean(spcHumAvg) 
OVER (PARTITION BY (postal_code) ORDER BY timestamp ROWS BETWEEN 7 FOLLOWING and CURRENT ROW) FROM weather_2010;


create table snowfall_7days as 
SELECT timestamp, postal_code, AVG(snowfall)
OVER (PARTITION BY postal_code ORDER BY timestamp ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) FROM weather_2010;



,sum(sfcPresAvg), sum(dewPtAvg),sum(precip), sum(wetBulbAvg),
sum(relHumAvg), sum(windSpdAvg), sum(tempAvg), sum(cldCvrAvg), sum(spcHumAvg) 

