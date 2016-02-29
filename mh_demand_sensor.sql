set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set mapred.map.tasks=240;
set mapred.reduce.tasks=60;


select * from fleet_queue_buckets where sensor_name like '%Malfun%' limit 3000;


create external table demand_data (
JobType STRING,
Vehicle STRING,
GarageID STRING,
JobNo STRING,
JobDate string,
DetailDate string,
TaskID string,
TaskName string,
Hours string,
Charges string,
PersonID STRING,
Mileage BIGINT,
TaskFam STRING,
TaskType STRING,
TaskMon SMALLINT,
TaskYr SMALLINT,
State STRING,
Status STRING,
VehicleGroup STRING,
DateInservice string,
SpecID STRING,
MakeID STRING,
Model STRING,
FuelType STRING)
row format delimited
fields terminated by ','
stored as textfile;

load data local inpath 'JULDEC2014.csv' overwrite into table demand_data;


create table mh_demand_test as
select * from demand_data where 
taskname = "Battery R&R" or
taskname = "Wiring/harness R&R" or
taskname = "Alternator R&R" or
taskname = "Spark Plugs R&R" or
taskname = "Starter R&R" or
taskname = "Engine system OTH" or
taskname = "Serpentine Belt - R&R" or
taskname = "Intake Manifold Gasket R&R" or
taskname = "Disc Brakes Front R&R" or
taskname = "Brake Rotor R&R" or
taskname = "Tire Pressure Adjust" or
taskname = "Replace 1 Tire" or
taskname = "Balance 1 Tire" or
taskname = "Replace 2 Tires " or
taskname = "Replace 4 Tires " or
taskname = "Tire Repair-Each" or
taskname = "Air Filter R&R" or
taskname = "Fuel Pump Electr Intank R&R" or
taskname = "Fuel Pump Electrical R&R" or
taskname = "EVACUATE&RECHARGE - R&R" or
taskname = "Heating/AC System OTH" or
taskname = "Compressor/clutcha/c R&R" or
taskname = "Blower Motor R&R" or
taskname = "Oxygen Sensor R&R" or
taskname = "Emission/Exhaust System OTH" or
taskname = "Catalytic Converter R&R";


drop table mh_demand_test;

create table mh_demand_test2 as
select *, to_date(jobdate) as jobdate_date from demand_data;

drop table mh_demand_test2;



Create table mh_sensor_malfun as
select vehicle_id, bucket_time,
CASE 
when sensor_name like '02_Sensor_Circuit%ON_COUNT' then '02_Sensor_Circuit'
when sensor_name like '%Camshaft_Position%_ON_COUNT' then 'Camshaft_Position' 
when sensor_name like 'Catalyst_System_Efficiency_Below%_ON_COUNT' then 'Catalyst_System_Efficiency_Below'
when sensor_name like 'Crankshaft_Position_Sensor%ON_COUNT' then 'Crankshaft_Position_Sensor'
when sensor_name like '%Misfire%_Detected%ON_COUNT' then 'Misfire_Detected'
when sensor_name like 'Engine_Coolant_Temperature%LIMIT_COUNT' then 'Engine_Coolant_Temperature'
when sensor_name like 'Evaporative_Emission_Control_System%ON_COUNT' then 'Evaporative_Emission_Control_System'
when sensor_name like 'Exhaust_Gas_Recirculation%_ON_COUNT' then 'Exhaust_Gas_Recirculation'
when sensor_name like 'HO2S_Heater_Control_Circuit%_ON_COUNT' then 'HO2S_Heater_Control_Circuit'
when sensor_name like 'Heated_Catalyst%Below_Threshold%_ON_COUNT' then 'Heated_Catalyst_Below_Threshold'
when sensor_name like 'Ignition_Coil%Malfunction_ON_COUNT' then 'Ignition_Coil_Malfunction'
when sensor_name like 'Injection_Pump_Fuel%Malfunction%ON_COUNT' then "Injection_Pump_Fuel_Malfunction"
when sensor_name like 'Injector_Circuit_Malfunction%ON_COUNT' then "Injector_Circuit_Malfunction"
when sensor_name like 'Insufficient_Coolant_Temperature%ON_COUNT' then "Insufficient_Coolant_Temperature"
when sensor_name like 'Internal_Control_Module%ON_COUNT' then "Internal_Control_Module"
when sensor_name like 'Knock_Sensor%ON_COUNT' then "Knock_Sensor"
when sensor_name like 'Malfunction_Indicator_Lamp%Malfunction_ON_COUNT' then "Malfunction_Indicator_Lamp"
when sensor_name like 'Mass_or_Volume_Air_Flow_Circuit%_ON_COUNT' then 'Mass_or_Volume_Air_Flow_Circuit'
when sensor_name like 'Power_Steering_Pressure_Sensor%_ON_COUNT' then 'Power_Steering_Pressure_Sensor'
when sensor_name like 'Secondary_Air_Injection%ON_COUNT	' then 'Secondary_Air_Injection'
when sensor_name like 'Shift%Solenoid%ON_COUNT' then 'Shift_Solenoid'
when sensor_name like 'System_too%_ON_COUNT' then 'System_too'
when sensor_name like 'Throttle%Position_ON_COUNT' then 'Throttle_Position'
when sensor_name like 'Tire%BELOW_LIMIT_COUNT' then 'Tire_Below_LIMIT'
when sensor_name like 'Transmission%Malfunction_ON_COUNT' then 'Transmission_Malfunction'
END as sensor_group
,count(*) as sensorgroup_count
from fleet_queue_buckets
group by vehicle_id, bucket_time,
CASE 
when sensor_name like '02_Sensor_Circuit%ON_COUNT' then '02_Sensor_Circuit'
when sensor_name like 'Battery_Voltage%LIMIT_COUNT' then 'Battery_Voltage'
when sensor_name like 'Engine_Speed%LIMIT_COUNT' then 'Engine_Speed'
when sensor_name like 'Oxygen_Sensor%_TOTAL_COUNT' then 'Oxygen_Sensor'
when sensor_name like '%Camshaft_Position%_ON_COUNT' then 'Camshaft_Position' 
when sensor_name like 'Catalyst_System_Efficiency_Below%_ON_COUNT' then 'Catalyst_System_Efficiency_Below'
when sensor_name like 'Crankshaft_Position_Sensor%ON_COUNT' then 'Crankshaft_Position_Sensor'
when sensor_name like '%Misfire%_Detected%ON_COUNT' then 'Misfire_Detected'
when sensor_name like 'Engine_Coolant_Temperature%LIMIT_COUNT' then 'Engine_Coolant_Temperature'
when sensor_name like 'Evaporative_Emission_Control_System%ON_COUNT' then 'Evaporative_Emission_Control_System'
when sensor_name like 'Exhaust_Gas_Recirculation%_ON_COUNT' then 'Exhaust_Gas_Recirculation'
when sensor_name like 'HO2S_Heater_Control_Circuit%_ON_COUNT' then 'HO2S_Heater_Control_Circuit'
when sensor_name like 'Heated_Catalyst%Below_Threshold%_ON_COUNT' then 'Heated_Catalyst_Below_Threshold'
when sensor_name like 'Ignition_Coil%Malfunction_ON_COUNT' then 'Ignition_Coil_Malfunction'
when sensor_name like 'Injection_Pump_Fuel%Malfunction%ON_COUNT' then "Injection_Pump_Fuel_Malfunction"
when sensor_name like 'Injector_Circuit_Malfunction%ON_COUNT' then "Injector_Circuit_Malfunction"
when sensor_name like 'Insufficient_Coolant_Temperature%ON_COUNT' then "Insufficient_Coolant_Temperature"
when sensor_name like 'Internal_Control_Module%ON_COUNT' then "Internal_Control_Module"
when sensor_name like 'Knock_Sensor%ON_COUNT' then "Knock_Sensor"
when sensor_name like 'Malfunction_Indicator_Lamp%Malfunction_ON_COUNT' then "Malfunction_Indicator_Lamp"
when sensor_name like 'Mass_or_Volume_Air_Flow_Circuit%_ON_COUNT' then 'Mass_or_Volume_Air_Flow_Circuit'
when sensor_name like 'Power_Steering_Pressure_Sensor%_ON_COUNT' then 'Power_Steering_Pressure_Sensor'
when sensor_name like 'Secondary_Air_Injection%ON_COUNT	' then 'Secondary_Air_Injection'
when sensor_name like 'Shift%Solenoid%ON_COUNT' then 'Shift_Solenoid'
when sensor_name like 'System_too%_ON_COUNT' then 'System_too'
when sensor_name like 'Throttle%Position_ON_COUNT' then 'Throttle_Position'
when sensor_name like 'Tire%BELOW_LIMIT_COUNT' then 'Tire_Below_LIMIT'
when sensor_name like 'Transmission%Malfunction_ON_COUNT' then 'Transmission_Malfunction'  
END;

Create table mh_sensor_malfunction as
select * from mh_sensor_malfun where sensor_group !="NULL";


add jar adaltas-hive-udf-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION to_map AS 'com.adaltas.UDAFToMap';
create table mh_sensor_malfunction_transpose as 
select 
    vehicle_id, bucket_time,
    group_map['02_Sensor_Circuit'] as 02_Sensor_Circuit,
    group_map['Catalyst_System_Efficiency_Below'] as Catalyst_System_Efficiency_Below,
    group_map['Crankshaft_Position_Sensor'] as Crankshaft_Position_Sensor
    from ( select
        vehicle_id, bucket_time,
        to_map(sensor_group,cast(sensorgroup_count as string)) as group_map 
        from mh_sensor_malfunction
        group by vehicle_id, bucket_time
    ) gm;



select distinct sensor_group from mh_demand_sensor_before30 order by sensor_group;
Catalyst_System_Efficiency_Below
Engine_Coolant_Temperature_LIMIT_COUNT
Tire_Below_LIMIT

select distinct sensor_group from mh_demand_sensor_before60 order by sensor_group;
02_Sensor_Circuit
Catalyst_System_Efficiency_Below
Engine_Coolant_Temperature_LIMIT_COUNT
HO2S_Heater_Control_Circuit
Tire_Below_LIMIT

select distinct sensor_group from mh_demand_sensor_after30 order by sensor_group;
Engine_Coolant_Temperature_LIMIT_COUNT
Misfire_Detected
Tire_Below_LIMIT

select * from mh_demand_sensor_before60 where sensor_group ='HO2S_Heater_Control_Circuit' limit 1000;



Create table mh_demand_sensor_before60 as
select mh_demand_test2.vehicle,mh_demand_test2.jobtype,mh_demand_test2.garageid,mh_demand_test2.jobno,mh_demand_test2.jobdate,mh_demand_test2.detaildate,
mh_demand_test2.taskid,mh_demand_test2.taskname,mh_demand_test2.personid,mh_demand_test2.mileage,mh_demand_test2.taskfam,
mh_demand_test2.tasktype,mh_demand_test2.taskmon,mh_demand_test2.taskyr,mh_demand_test2.state,mh_demand_test2.status,mh_demand_test2.vehiclegroup,
mh_demand_test2.dateinservice,mh_demand_test2.specid,mh_demand_test2.makeid,mh_demand_test2.fueltype,mh_sensor_malfunction.*, 
TO_DATE(mh_demand_test2.jobdate) as jobdate_date,TO_DATE(mh_demand_test2.detaildate) as detaildate_date,
DATEDIFF(TO_DATE(mh_demand_test2.jobdate),TO_DATE(mh_sensor_malfunction.bucket_time)) as job_bucket_diff,
DATEDIFF(TO_DATE(mh_demand_test2.detaildate),TO_DATE(mh_sensor_malfunction.bucket_time)) as detail_bucket_diff from mh_demand_test2 left join mh_sensor_malfunction
where 
mh_demand_test2.vehicle = mh_sensor_malfunction.vehicle_id and 
(DATEDIFF(TO_DATE(mh_demand_test2.jobdate),TO_DATE(mh_sensor_malfunction.bucket_time))<60 and
DATEDIFF(TO_DATE(mh_demand_test2.jobdate),TO_DATE(mh_sensor_malfunction.bucket_time))>0);


Create table mh_demand_sensor_after30 as
select mh_demand_test2.jobtype,mh_demand_test2.garageid,mh_demand_test2.jobno,mh_demand_test2.jobdate,mh_demand_test2.detaildate,
mh_demand_test2.taskid,mh_demand_test2.taskname,mh_demand_test2.personid,mh_demand_test2.mileage,mh_demand_test2.taskfam,
mh_demand_test2.tasktype,mh_demand_test2.taskmon,mh_demand_test2.taskyr,mh_demand_test2.state,mh_demand_test2.status,mh_demand_test2.vehiclegroup,
mh_demand_test2.dateinservice,mh_demand_test2.specid,mh_demand_test2.makeid,mh_demand_test2.fueltype,mh_sensor_malfunction.*, 
TO_DATE(mh_demand_test2.jobdate) as jobdate_date,TO_DATE(mh_demand_test2.detaildate) as detaildate_date,
DATEDIFF(TO_DATE(mh_demand_test2.jobdate),TO_DATE(mh_sensor_malfunction.bucket_time)) as job_bucket_diff,
DATEDIFF(TO_DATE(mh_demand_test2.detaildate),TO_DATE(mh_sensor_malfunction.bucket_time)) as detail_bucket_diff from mh_demand_test2 left join mh_sensor_malfunction
where 
mh_demand_test2.vehicle = mh_sensor_malfunction.vehicle_id and 
(DATEDIFF(TO_DATE(mh_demand_test2.detaildate),TO_DATE(mh_sensor_malfunction.bucket_time))>-30 and 
DATEDIFF(TO_DATE(mh_demand_test2.detaildate),TO_DATE(mh_sensor_malfunction.bucket_time))<0);

Create table mh_demand_sensor_before0107 as 
select mh_demand_sensor_before30.vehicle,mh_demand_sensor_before30.jobtype,mh_demand_sensor_before30.garageid,
mh_demand_sensor_before30.jobno,mh_demand_sensor_before30.jobdate,mh_demand_sensor_before30.detaildate,
mh_demand_sensor_before30.taskid, mh_demand_sensor_before30.taskname, mh_demand_sensor_before30.personid,
mh_demand_sensor_before30.mileage, mh_demand_sensor_before30.taskfam, mh_demand_sensor_before30.tasktype,
mh_demand_sensor_before30.taskmon, mh_demand_sensor_before30.taskyr, mh_demand_sensor_before30.state,
mh_demand_sensor_before30.status, mh_demand_sensor_before30.vehiclegroup, mh_demand_sensor_before30.dateinservice,
mh_demand_sensor_before30.specid, mh_demand_sensor_before30.makeid, mh_demand_sensor_before30.fueltype,
mh_demand_sensor_before30.bucket_time, mh_demand_sensor_before30.sensor_group, mh_demand_sensor_before30.sensorgroup_count,
sum(sensorgroup_count) group by vehicle,sensor_group;

Create table mh_demand_sensor_before0107 as 
select vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group,
sum(sensorgroup_count) as sum_count,count(distinct detail_bucket_diff) from mh_demand_sensor_before30 
where job_bucket_diff <=7 and job_bucket_diff >0
group by vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group;

Create table mh_demand_sensor_before0814 as 
select vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group,
sum(sensorgroup_count) as sum_count,count(distinct detail_bucket_diff) from mh_demand_sensor_before30 
where job_bucket_diff <=14 and job_bucket_diff >7
group by vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group;

Create table mh_demand_sensor_before1521 as 
select vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group,
sum(sensorgroup_count) as sum_count,count(distinct detail_bucket_diff) from mh_demand_sensor_before30 
where job_bucket_diff <= 21 and job_bucket_diff >14
group by vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group;

Create table mh_demand_sensor_before2228 as 
select vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group,
sum(sensorgroup_count) as sum_count,count(distinct detail_bucket_diff) as num_days from mh_demand_sensor_before30 
where job_bucket_diff <=28 and job_bucket_diff >21
group by vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group;


Create table mh_demand_sensor_after0107 as
select vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group,
sum(sensorgroup_count) as sum_count,count(distinct detail_bucket_diff) as num_days from mh_demand_sensor_after30 
where detail_bucket_diff >=-7 and detail_bucket_diff <0 
group by vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group;

Create table mh_demand_sensor_after0814 as
select vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group,
sum(sensorgroup_count) as sum_count,count(distinct detail_bucket_diff) as num_days from mh_demand_sensor_after30 
where detail_bucket_diff >= -14 and detail_bucket_diff < -7 
group by vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group;

Create table mh_demand_sensor_after1521 as
select vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group,
sum(sensorgroup_count) as sum_count,count(distinct detail_bucket_diff) as num_days  from mh_demand_sensor_before30 
where detail_bucket_diff >= -21 and detail_bucket_diff < -14
group by vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group;

Create table mh_demand_sensor_after2228 as
select vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group,
sum(sensorgroup_count) as sum_count,count(distinct detail_bucket_diff) as num_days from mh_demand_sensor_before30 
where detail_bucket_diff >= -28 and detail_bucket_diff < -21
group by vehicle_id,jobtype, garageid,jobno,jobdate_date,detaildate_date, taskid,taskname, personid,
mileage, taskfam, tasktype, taskmon, taskyr, state, status, vehiclegroup, dateinservice, specid,
makeid, fueltype, bucket_time, sensor_group;




create table mh_demand_sensor as
select mh_demand_test.*, fleet_daily_bucket_transposed.* from mh_demand_test join fleet_daily_bucket_transposed 
on mh_demand_test.vehicle =  fleet_daily_bucket_transposed.vehicle_id and
to_date(mh_demand_test.jobdate) = fleet_daily_bucket_transposed.bucket_time;




