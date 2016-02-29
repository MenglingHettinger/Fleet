create table mh_01072015_queue_odo as
select * from fleet_queue_sensors where sensor_name = 'Odometer';

create table mh_01072015_queue_spd as
select * from fleet_queue_sensors where sensor_name = 'Speed';

create table mh_01072015_queue_dec as
select * from fleet_queue_sensors where sensor_name = 'Max Deceleration';

create table mh_01072015_queue_spd5 as 
select * from mh_01072015_queue_spd where cast(sensor_value as double) > 5;

create table mh_01072015_queue_dec1 as 
select * from mh_01072015_queue_dec where cast(sensor_value as double) > 1;

create table mh_01082015_queue_odo_daily_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as odo_cnt, 
avg(cast(sensor_value as double)) as avg_odo,
min(cast(sensor_value as double)) as min_odo, max(cast(sensor_value as double)) as max_odo, ddmmyy from mh_01072015_queue_odo group by vehicle_id,ddmmyy;

create table mh_01082015_queue_spd5_daily_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as spd_cnt, 
avg(cast(sensor_value as double)) as avg_spd,
stddev_pop(cast(sensor_value as double)) as st_spd, ddmmyy from mh_01072015_queue_odo group by vehicle_id,ddmmyy;

create table mh_01082015_queue_dec1_daily_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as dec_cnt, 
avg(cast(sensor_value as double)) as avg)_dec,
stddev_pop(cast(sensor_value as double) as st_dec, ddmmyy from mh_01072015_queue_odo group by vehicle_id,ddmmyy;

create table mh_01082015_queue_dec1odo_daily_info as
select mh_01072015_queue_dec1_daily_info.vehicle_id, mh_01072015_queue_dec1_daily_info.count as dec_cnt, mh_01072015_queue_dec1_daily_info.avg_dec, mh_01072015_queue_dec1_daily_info.sd_dec, mh_01072015_queue_dec1_daily_info.ddmmyy, mh_01082015_queue_odo_daily_info.odo_cnt, mh_01082015_queue_odo_daily_info.avg_odo, mh_01082015_queue_odo_daily_info.min_odo, mh_01082015_queue_odo_daily_info.max_odo from
mh_01072015_queue_dec1_daily_info left join mh_01082015_queue_odo_daily_info 
on (mh_01072015_queue_dec1_daily_info.vehicle_id = mh_01082015_queue_odo_daily_info.vehicle_id and
mh_01072015_queue_dec1_daily_info.ddmmyy = mh_01082015_queue_odo_daily_info.ddmmyy);

create table mh_01082015_queue_dec1odospd5_daily_info as
select mh_01082015_queue_dec1_odo_daily_info.*, mh_01072015_queue_spd5_daily_info.avg_spd,mh_01072015_queue_spd5_daily_info.sd_spd from mh_01082015_queue_dec1_odo_daily_info left join mh_01072015_queue_spd5_daily_info on
(mh_01082015_queue_dec1_odo_daily_info.vehicle_id = mh_01072015_queue_spd5_daily_info.vehicle_id and
mh_01082015_queue_dec1_odo_daily_info.ddmmyy = mh_01072015_queue_spd5_daily_info.ddmmyy);


#############
create table mh_01082015_engspd as
select * from fleet_queue_sensors where sensor_name = 'Engine Speed';


create table mh_01082015_batvol as
select * from fleet_queue_sensors where sensor_name = 'Battery Voltage';


#########
create table mh_raw_odo as 
select * from fleet_raw_sensors where sensor_name = 'Odometer';

create table mh_raw_odo_ddmmyy as
select *,cast(bucket_time as date) as bucket_date from mh_raw_odo;

create table mh_raw_spd as 
select * from fleet_raw_sensors where sensor_name = 'Odometer';

create table mh_raw_odo_ddmmyy as
select *,cast(bucket_time as date) as bucket_date from mh_raw_odo;

create table mh_01082015_raw_odo_daily_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as odo_cnt, 
avg(cast(sensor_value as double)) as avg_odo,
min(cast(sensor_value as double)) as min_odo, max(cast(sensor_value as double)) as max_odo, ddmmyy from mh_01072015_raw_odo group by vehicle_id,ddmmyy;


############
create table mh_01072015_queue_spd as
select * from fleet_queue_sensors where sensor_name = 'Speed';

create table mh_01132015_queue_spd_not0 as 
select * from mh_01052015_queue_speed where cast(sensor_value as double) > 0;

create table mh_01132015_queue_spd0 as 
select * from mh_01052015_queue_speed where cast(sensor_value as double) = 0;

create table mh_01132015_queue_spd_not0_daily_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as spd_cnt, 
avg(cast(sensor_value as double)) as avg_spd,
stddev_pop(cast(sensor_value as double)) as st_spd, ddmmyy from mh_01132015_queue_spd_not0 group by vehicle_id,ddmmyy;

create table mh_q01132015_queue_spd_not0_date as
select *, Year(bucket_time) as yr, month(bucket_time) as mon from mh_01132015_queue_spd_not0;

create table mh_01132015_queue_spd_not0_month_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as spd_cnt, 
avg(cast(sensor_value as double)) as avg_spd,
stddev_pop(cast(sensor_value as double)) as st_spd, yr, mon from mh_q01132015_queue_spd_not0_date group by vehicle_id,yr, mon;

create table mh_01132015_queue_spd_0_date as
select *, Year(bucket_time) as yr, month(bucket_time) as mon from mh_01132015_queue_spd0;

create table mh_01132015_queue_spd_0_month_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as spd_cnt, 
yr, mon from  mh_01132015_queue_spd_0_date group by vehicle_id,yr, mon;

##########

mh_01052015_raw_speed
create table mh_01132015_raw_spd_not0 as 
select * from mh_01052015_raw_speed where cast(sensor_value as double) > 0;

create table mh_01132015_rawspd0 as 
select * from mh_01052015_raw_speed where cast(sensor_value as double) = 0;

create table mh_01132015_raw_spd_not0_date as
select *, Year(bucket_time) as yr, month(bucket_time) as mon from mh_01132015_raw_spd_not0;

create table mh_01132015_raw_spd_not0_month_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as spd_cnt, 
avg(cast(sensor_value as double)) as avg_spd,
stddev_pop(cast(sensor_value as double)) as st_spd, yr, mon from  mh_01132015_raw_spd_not0_date group by vehicle_id,yr, mon;


create table mh_01132015_raw_spd_0_date as
select *, Year(bucket_time) as yr, month(bucket_time) as mon from mh_01132015_rawspd0;

create table mh_01132015_raw_spd_0_month_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as spd_cnt, 
yr, mon from  mh_01132015_raw_spd_0_date group by vehicle_id,yr, mon;

#############
create table mh_01152015_queue_max_acceleration as
select * from fleet_queue_sensors where sensor_name = 'Max Acceleration';  

create table mh_01152015_raw_max_acceleration as
select * from fleet_raw_sensors where sensor_name = 'Max Acceleration'; 

create table mh_01152015_queue_spd_acc as
select mh_01052015_queue_speed.*, mh_01152015_queue_max_acceleration.sensor_name as sensor_name_2, mh_01152015_queue_max_acceleration.sensor_value as sensor_value_2 from mh_01052015_queue_speed join mh_01152015_queue_max_acceleration on
mh_01052015_queue_speed.vehicle_id = mh_01152015_queue_max_acceleration.vehicle_id and
mh_01052015_queue_speed.bucket_time = mh_01152015_queue_max_acceleration.bucket_time;

create table mh_01152015_queue_spdg0_acc as 
select * from mh_01152015_queue_spd_acc where cast(sensor_value as double) >0;

create table mh_01152015_queue_spdg0_acc_date as
select *, Year(bucket_time) as yr, month(bucket_time) as mon from mh_01152015_queue_spdg0_acc;

create table mh_01152015_queue_spdg0_acc_date_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as acc_cnt, 
avg(cast(sensor_value_2 as double)) as avg_acc,
stddev_pop(cast(sensor_value_2 as double)) as st_acc, yr, mon from mh_01152015_queue_spdg0_acc_date group by vehicle_id,yr, mon;



create table mh_01152015_raw_max_acceleration as
select * from fleet_raw_sensors where sensor_name = 'Max Acceleration'; 

create table mh_01152015_raw_spd_acc as
select mh_01052015_raw_speed.*, mh_01152015_raw_max_acceleration.sensor_name as sensor_name_2, mh_01152015_raw_max_acceleration.sensor_value as sensor_value_2 from mh_01052015_raw_speed join mh_01152015_raw_max_acceleration on
mh_01052015_raw_speed.vehicle_id = mh_01152015_raw_max_acceleration.vehicle_id and
mh_01052015_raw_speed.bucket_time = mh_01152015_raw_max_acceleration.bucket_time;

create table mh_01152015_raw_spdg0_acc as 
select * from mh_01152015_raw_spd_acc where cast(sensor_value as double) >0;

create table mh_01152015_raw_spdg0_acc_date as
select *, Year(bucket_time) as yr, month(bucket_time) as mon from mh_01152015_raw_spdg0_acc;

create table mh_01152015_raw_spdg0_acc_date_info as
select vehicle_id,min(bucket_time) as min_time, max(bucket_time) as max_time, count(distinct bucket_time) as spd_cnt, 
avg(cast(sensor_value_2 as double)) as avg_acc,
stddev_pop(cast(sensor_value_2 as double)) as st_acc, yr, mon from mh_01152015_raw_spdg0_acc_date group by vehicle_id,yr, mon;


