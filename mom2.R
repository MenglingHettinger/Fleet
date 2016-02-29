# The following two commands remove any previously installed H2O packages for R.
#if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
#if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
# Next, we download, install and initialize the H2O package for R.
#install.packages("h2o")
library(h2o)
#localH2O=h2o.init(ip="10.0.28.126",port=54321, startH2O=T)
localH2O=h2o.init(ip="127.0.0.1",port=54321,Xmx="8G",nthreads=-1)
#install.packages(sqldf)
library(sqldf)
#install.packages("data.table")
library(data.table)
library(dummies)
## get data from hdfs and join with vehicle data to get zip code
system("hadoop fs -getmerge /user/bf976q/fleet/MOM /users/mh564g/MOM/mom_train.csv")
system("hadoop fs -getmerge /user/bf976q/fleet/MOM_Prediction_Data_format /users/mh564g/MOM/mom_test.csv")
train <- fread("mom_train.csv",stringsAsFactors=F, header=F)
test <- fread("mom_test.csv",stringsAsFactors=F, header=F)
names(train) <- c("Vehicle","ComplaintText","VehicleWeight","JobDesc","Model","SpecID","JobNo","Mileage","VehicleGroup","TaskYr",
                 "DateInService","JobCategory","FuelType","State","MakeID","Status","TagWeight","TaskFam","TaskType",
                 "TaskMon","TaskName","JobDate","STAR_Valve_Train_DIA_AGE","Wiring_harness_RnR_AGE","Fuel_Filter_RnR_AGE",
                 "STAR_Check_Engine_Light_DIA_AGE","Replace_4_Tires_AGE","Alignment_ADJ_AGE","Radiator_Cap_RnR_AGE",
                 "Coolant_Replace_Flush_RnR_AGE","Oxygen_Sensor_RnR_AGE","Steering_suspension_Sys_OTH_AGE","Disc_Brakes_Rear_RnR_AGE",
                 "Spark_Plugs_RnR_AGE","Battery_RnR_AGE","Disc_Brakes_Front_RnR_AGE","Thermostat_Housing_RnR_AGE","Coolant_FilterRnR_AGE",
                 "Cooling_System_DIA_AGE","STAR_Transmissionmanual_auto_DIA_AGE","Ignition_System_DIA_AGE","STAR_Tankfuel_DIA_AGE",
                 "Air_Filter_RnR_AGE","Engine_System_DIA_AGE","Serpentine_Belt_RnR_AGE","Emission_Exhaust_System_DIA_AGE",
                 "Compressor_clutcha_c_RnR_AGE","Power_Steering_Hose_RnR_AGE","Tires_Wheels_Hubs_DIA_AGE","Cooling_Fan_Engine_RnR_AGE",
                 "Engine_system_OTH_AGE","Intake_Manifold_Gasket_RnR_AGE","Charging_starting_System_DIA_AGE","Fuel_Pump_Electrical_RnR_AGE",
                 "Transmission_DIA_AGE","PM_DOT_Inspection_All_Pass_Cars_n_Trucks_AGE","Automatic_Complete_RnR_AGE","Starter_RnR_AGE",
                 "Transmission_OTH_AGE","Brake_System_DIA_AGE","STAR_Air_Conditioning_System_DIA_AGE","STAR_Exhaust_System_DIA_AGE",
                 "STAR_Engine_Assembly_DIA_AGE","Alternator_RnR_AGE","STAR_Electrical_System_DIA_AGE","STAR_Steering_suspension_Sys_DIA_AGE",
                 "Tire_Pressure_Adjust_AGE","Fuel_System_DIA_AGE","STAR_Fuel_System_DIA_AGE","Electrical_System_DIA_AGE",
                 "Shock_Absorbers2_RnR_AGE","Catalytic_Converter_RnR_AGE","Power_Steering_Pump_RnR_AGE","Replace_2_Tires_AGE",
                 "Steering_suspension_Sys_DIA_AGE","Radiator_RnR_AGE","Transmission_Linkage_RnR_AGE","Balance_1_Tire_AGE",
                 "Brake_Pedal_Pad_RnR_AGE","Emission_Exhaust_System_OTH_AGE","Replace_1_Tire_AGE","Tire_RepairEach_AGE",
                 "Transmission_Service_Filter_RnR_AGE","STAR_Cooling_System_DIA_AGE","Mount_RnR_AGE","Fuel_Pump_Electr_Intank_RnR_AGE",
                 "Cooling_System_OTH_AGE","Coolant_Recovery_Tank_RnR_AGE","Heating_AC_System_OTH_AGE","STAR_Hydraulic_Brake_System_DIA_AGE",
                 "Blower_Motor_RnR_AGE","Radiator_Hose_RnR_AGE","Heating_AC_System_DIA_AGE","STAR_Heater_System_DIA_AGE","Brake_Rotor_RnR_AGE",
                 "Abs_Light_DIA_AGE","Thermostat_gasket_RnR_AGE")


train$JobMon <- format(as.Date(train$JobDate,"%Y-%m-%d"), "%m")
train$VehAge <- as.numeric(Sys.Date() - as.Date(train$DateInService,"%Y-%m-%d"))
names(test) <- c("Vehicle","ComplaintText","VehicleWeight","JobDesc","Model","SpecID","JobNo","Mileage","VehicleGroup","TaskYr",
                 "DateInService","DetailDate","JobCategory","FuelType","State","MakeID","Status","TagWeight","TaskFam","TaskType",
                 "TaskMon","TaskName","JobDate","STAR_Valve_Train_DIA_AGE","Wiring_harness_RnR_AGE","Fuel_Filter_RnR_AGE",
                 "STAR_Check_Engine_Light_DIA_AGE","Replace_4_Tires_AGE","Alignment_ADJ_AGE","Radiator_Cap_RnR_AGE",
                 "Coolant_Replace_Flush_RnR_AGE","Oxygen_Sensor_RnR_AGE","Steering_suspension_Sys_OTH_AGE","Disc_Brakes_Rear_RnR_AGE",
                 "Spark_Plugs_RnR_AGE","Battery_RnR_AGE","Disc_Brakes_Front_RnR_AGE","Thermostat_Housing_RnR_AGE","Coolant_FilterRnR_AGE",
                 "Cooling_System_DIA_AGE","STAR_Transmissionmanual_auto_DIA_AGE","Ignition_System_DIA_AGE","STAR_Tankfuel_DIA_AGE",
                 "Air_Filter_RnR_AGE","Engine_System_DIA_AGE","Serpentine_Belt_RnR_AGE","Emission_Exhaust_System_DIA_AGE",
                 "Compressor_clutcha_c_RnR_AGE","Power_Steering_Hose_RnR_AGE","Tires_Wheels_Hubs_DIA_AGE","Cooling_Fan_Engine_RnR_AGE",
                 "Engine_system_OTH_AGE","Intake_Manifold_Gasket_RnR_AGE","Charging_starting_System_DIA_AGE","Fuel_Pump_Electrical_RnR_AGE",
                 "Transmission_DIA_AGE","PM_DOT_Inspection_All_Pass_Cars_n_Trucks_AGE","Automatic_Complete_RnR_AGE","Starter_RnR_AGE",
                 "Transmission_OTH_AGE","Brake_System_DIA_AGE","STAR_Air_Conditioning_System_DIA_AGE","STAR_Exhaust_System_DIA_AGE",
                 "STAR_Engine_Assembly_DIA_AGE","Alternator_RnR_AGE","STAR_Electrical_System_DIA_AGE","STAR_Steering_suspension_Sys_DIA_AGE",
                 "Tire_Pressure_Adjust_AGE","Fuel_System_DIA_AGE","STAR_Fuel_System_DIA_AGE","Electrical_System_DIA_AGE",
                 "Shock_Absorbers2_RnR_AGE","Catalytic_Converter_RnR_AGE","Power_Steering_Pump_RnR_AGE","Replace_2_Tires_AGE",
                 "Steering_suspension_Sys_DIA_AGE","Radiator_RnR_AGE","Transmission_Linkage_RnR_AGE","Balance_1_Tire_AGE",
                 "Brake_Pedal_Pad_RnR_AGE","Emission_Exhaust_System_OTH_AGE","Replace_1_Tire_AGE","Tire_RepairEach_AGE",
                 "Transmission_Service_Filter_RnR_AGE","STAR_Cooling_System_DIA_AGE","Mount_RnR_AGE","Fuel_Pump_Electr_Intank_RnR_AGE",
                 "Cooling_System_OTH_AGE","Coolant_Recovery_Tank_RnR_AGE","Heating_AC_System_OTH_AGE","STAR_Hydraulic_Brake_System_DIA_AGE",
                 "Blower_Motor_RnR_AGE","Radiator_Hose_RnR_AGE","Heating_AC_System_DIA_AGE","STAR_Heater_System_DIA_AGE","Brake_Rotor_RnR_AGE",
                 "Abs_Light_DIA_AGE","Thermostat_gasket_RnR_AGE")
test <- as.data.frame(test)
test <- test[,-12]
test$JobDate <- Sys.Date()
test$JobMon <- format(as.Date(test$JobDate,"%Y-%m-%d"), "%m")
test$VehAge <- as.numeric(Sys.Date() - as.Date(test$DateInService,"%Y-%m-%d"))
# get most recent vehicle master data
path_vehicle <- system('java -jar parseVehiclePath.jar', wait=T, intern=T)
system(paste("hadoop fs -copyToLocal", path_vehicle, sep=" "))
system("cp veh* Vehicle.csv")
system("rm veh*")
veh <- fread("Vehicle.csv",sep=",",header=T,stringsAsFactors=F)
veh$Zip <- substr(veh$Zip,start=1,stop=5)
train_veh <- sqldf("select train.*, veh.Zip, veh.Horsepower, veh.EngineSize, veh.AirConditioning, 
                  veh.Cylinders,veh.PrimaryCapacity,veh.Status as CurrentStatus from train left join veh on train.Vehicle=veh.Vehicle")
names(train_veh)[8] <- "Odometer"
test_veh <- sqldf("select test.*, veh.Zip, veh.Horsepower, veh.EngineSize, veh.Odometer as Odometer, veh.AirConditioning, 
                  veh.Cylinders,veh.PrimaryCapacity,veh.Status as CurrentStatus from test left join veh on test.Vehicle=veh.Vehicle")
write.csv(train_veh,"mom_train_veh.csv",quote=F,row.names=F)
write.csv(test_veh,"mom_test_veh.csv",quote=F,row.names=F)
## join train and test data with weather data
system("pig weather.pig")
system("hive -f weather.sql")
system("hadoop fs -getmerge /apps/hive/warehouse/mh_mom_weather_train /users/mh564g/MOM/mom_train_weather.csv")
system("hadoop fs -getmerge /apps/hive/warehouse/mh_mom_weather_test /users/mh564g/MOM/mom_test_weather.csv")
## generate train and test data
mom <- fread("mom_train_weather.csv",stringsAsFactors=F,header=F,sep="\001")
names(mom) = c("vehicle","vehicleweight","model","specid","jobno","odometer","vehiclegroup","taskyr","dateinservice","jobcategory",
"fueltype","state","makeid","status","tagweight","taskfam","tasktype","taskmon","taskname","jobdate",
"star_valve_train_dia_age","wiring_harness_rnr_age","fuel_filter_rnr_age","star_check_engine_light_dia_age",	
"replace_4tires_age","alignment_adj_age","radiator_cap_rnr_age","coolant_replace_flush_rnr_age","oxygen_sensor_rnr_age",
"steering_suspension_sys_oth_age","disc_brakes_rear_rnr_age","spark_plugs_rnr_age","battery_rnr_age",
"disc_brakes_front_rnr_age","thermostat_housing_rnr_age","coolant_filterrnr_age","cooling_system_dia_age",
"star_transmissionmanual_auto_dia_age","ignition_system_dia_age","star_tankfuel_dia_age","air_filter_rnr_age",
"engine_system_dia_age","serpentine_belt_rnr_age","emission_exhaust_system_dia_age","compressor_clutcha_c_rnr_age",
"power_steering_hose_rnr_age","tires_wheels_hubs_dia_age","cooling_fan_engine_rnr_age","engine_system_oth_age",
"intake_manifold_gasket_rnr_age","charging_starting_system_dia_age","fuel_pump_electrical_rnr_age",
"transmission_dia_age","pm_dot_inspection_all_pass_cars_n_trucks_age","automatic_complete_rnr_age","starter_rnr_age",
"transmission_oth_age","brake_system_dia_age","star_air_conditioning_system_dia_age","star_exhaust_system_dia_age",
"star_engine_assembly_dia_age","alternator_rnr_age","star_electrical_system_dia_age","star_steering_suspension_sys_dia_age",
"tire_pressure_adjust_age","fuel_system_dia_age","star_fuel_system_dia_age","electrical_system_dia_age","shock_absorbers_rnr_age",
"catalytic_converter_rnr_age","power_steering_pump_rnr_age","replace_2tires_age","steering_suspension_sys_dia_age","radiator_rnr_age",
"transmission_linkage_rnr_age","balance_tire_age","brake_pedal_pad_rnr_age","emission_exhaust_system_oth_age","replace_1tire_age",
"tire_repaireach_age","transmission_service_filter_rnr_age","star_cooling_system_dia_age","mount_rnr_age","fuel_pump_electr_intank_rnr_age",
"cooling_system_oth_age","coolant_recovery_tank_rnr_age","heating_ac_system_oth_age","star_hydraulic_brake_system_dia_age","blower_motor_rnr_age",
"radiator_hose_rnr_age","heating_ac_system_dia_age","star_heater_system_dia_age","brake_rotor_rnr_age","abs_light_dia_age","thermostat_gasket_rnr_age",
"jobmon","vehage","zip","horsepower","enginesize","airconditioning","cylinders",
"primarycapacity","currentstatus","snowfall_avg","snowfall_sd","precip_avg","precip_sd","relhumavg_avg","relhumavg_sd","tempavg_avg","tempavg_sd")

mom <- as.data.frame(mom)
mom$taskmon <- factor(mom$taskmon)
mom_dummy <- dummy(mom$taskfam)
mom_all <- cbind(mom,mom_dummy)
fam01_ratio <- sum(mom_all$taskfam1)/dim(mom_all)[1]
fam02_ratio <- sum(mom_all$taskfam2)/dim(mom_all)[1]
fam03_ratio <- sum(mom_all$taskfam3)/dim(mom_all)[1]
fam04_ratio <- sum(mom_all$taskfam4)/dim(mom_all)[1]
fam06_ratio <- sum(mom_all$taskfam6)/dim(mom_all)[1]
fam07_ratio <- sum(mom_all$taskfam7)/dim(mom_all)[1]
fam08_ratio <- sum(mom_all$taskfam8)/dim(mom_all)[1]
fam09_ratio <- sum(mom_all$taskfam9)/dim(mom_all)[1]
fam11_ratio <- sum(mom_all$taskfam11)/dim(mom_all)[1]
fam12_ratio <- sum(mom_all$taskfam12)/dim(mom_all)[1]

write.csv(mom_all,"mom_train_all.csv",quote=F,row.names=F)
mom_noPM <- subset(mom_all, taskfam!=80)
write.csv(mom_noPM,"mom_train.csv",quote=F,row.names=F)

mom_pred <- fread("mom_test_weather.csv",,stringsAsFactors=F,header=F,sep="\001")
names(mom_pred) = c("vehicle","vehicleweight","model","specid","jobno","odometer","vehiclegroup","taskyr","dateinservice","jobcategory",
               "fueltype","state","makeid","status","tagweight","taskfam","tasktype","taskmon","taskname","jobdate",
               "star_valve_train_dia_age","wiring_harness_rnr_age","fuel_filter_rnr_age","star_check_engine_light_dia_age",
               "replace_4tires_age","alignment_adj_age","radiator_cap_rnr_age","coolant_replace_flush_rnr_age","oxygen_sensor_rnr_age",
               "steering_suspension_sys_oth_age","disc_brakes_rear_rnr_age","spark_plugs_rnr_age","battery_rnr_age",
               "disc_brakes_front_rnr_age","thermostat_housing_rnr_age","coolant_filterrnr_age","cooling_system_dia_age",
               "star_transmissionmanual_auto_dia_age","ignition_system_dia_age","star_tankfuel_dia_age","air_filter_rnr_age",
               "engine_system_dia_age","serpentine_belt_rnr_age","emission_exhaust_system_dia_age","compressor_clutcha_c_rnr_age",
              "power_steering_hose_rnr_age","tires_wheels_hubs_dia_age","cooling_fan_engine_rnr_age","engine_system_oth_age",
              "intake_manifold_gasket_rnr_age","charging_starting_system_dia_age","fuel_pump_electrical_rnr_age",
              "transmission_dia_age","pm_dot_inspection_all_pass_cars_n_trucks_age","automatic_complete_rnr_age","starter_rnr_age",
              "transmission_oth_age","brake_system_dia_age","star_air_conditioning_system_dia_age","star_exhaust_system_dia_age",
              "star_engine_assembly_dia_age","alternator_rnr_age","star_electrical_system_dia_age","star_steering_suspension_sys_dia_age",
             "tire_pressure_adjust_age","fuel_system_dia_age","star_fuel_system_dia_age","electrical_system_dia_age","shock_absorbers_rnr_age",
          "catalytic_converter_rnr_age","power_steering_pump_rnr_age","replace_2tires_age","steering_suspension_sys_dia_age","radiator_rnr_age",
"transmission_linkage_rnr_age","balance_tire_age","brake_pedal_pad_rnr_age","emission_exhaust_system_oth_age","replace_1tire_age",
"tire_repaireach_age","transmission_service_filter_rnr_age","star_cooling_system_dia_age","mount_rnr_age","fuel_pump_electr_intank_rnr_age",
"cooling_system_oth_age","coolant_recovery_tank_rnr_age","heating_ac_system_oth_age","star_hydraulic_brake_system_dia_age","blower_motor_rnr_age",
"radiator_hose_rnr_age","heating_ac_system_dia_age","star_heater_system_dia_age","brake_rotor_rnr_age","abs_light_dia_age","thermostat_gasket_rnr_age","jobmon","vehage","zip","horsepower","enginesize","airconditioning","cylinders",
"primarycapacity","currentstatus","snowfall_avg","snowfall_sd","precip_avg","precip_sd","relhumavg_avg","relhumavg_sd","tempavg_avg","tempavg_sd")



mom_pred <-as.data.frame(mom_pred)
write.csv(mom_pred,"mom_test.csv",quote=F,row.names=F)

## train model base on task family and make predictions
train_path_model = "mom_train.csv"
test_path_model ="mom_test.csv"
#test_path_model = system.file("extdata", "mom_test.csv", package = "h2o")
#train_data = h2o.importFile(localH2O, path=train_path_model, key="train_model.hex")
#test_data = h2o.importFile(localH2O, path=test_path_model, key="test_model.hex")
train_data = h2o.uploadFile(localH2O, path = train_path_model, key = "train_model.hex")
test_data = h2o.uploadFile(localH2O, path=test_path_model, key="test_model.hex")

Features = c("model","odometer","vehiclegroup","fueltype","state","makeid","tagweight",
               "star_valve_train_dia_age","wiring_harness_rnr_age","fuel_filter_rnr_age","star_check_engine_light_dia_age",
             "replace_4tires_age","alignment_adj_age","radiator_cap_rnr_age","coolant_replace_flush_rnr_age","oxygen_sensor_rnr_age",
             "steering_suspension_sys_oth_age","disc_brakes_rear_rnr_age","spark_plugs_rnr_age","battery_rnr_age",
               "disc_brakes_front_rnr_age","thermostat_housing_rnr_age","coolant_filterrnr_age","cooling_system_dia_age",
               "star_transmissionmanual_auto_dia_age","ignition_system_dia_age","star_tankfuel_dia_age","air_filter_rnr_age",	
               "engine_system_dia_age","serpentine_belt_rnr_age","emission_exhaust_system_dia_age","compressor_clutcha_c_rnr_age",
             "power_steering_hose_rnr_age","tires_wheels_hubs_dia_age","cooling_fan_engine_rnr_age","engine_system_oth_age",
             "intake_manifold_gasket_rnr_age","charging_starting_system_dia_age","fuel_pump_electrical_rnr_age",
             "transmission_dia_age","pm_dot_inspection_all_pass_cars_n_trucks_age","automatic_complete_rnr_age","starter_rnr_age",
             "transmission_oth_age","brake_system_dia_age","star_air_conditioning_system_dia_age","star_exhaust_system_dia_age",
             "star_engine_assembly_dia_age","alternator_rnr_age","star_electrical_system_dia_age","star_steering_suspension_sys_dia_age",
             "tire_pressure_adjust_age","fuel_system_dia_age","star_fuel_system_dia_age","electrical_system_dia_age","shock_absorbers_rnr_age",
             "catalytic_converter_rnr_age","power_steering_pump_rnr_age","replace_2tires_age","steering_suspension_sys_dia_age","radiator_rnr_age",
             "transmission_linkage_rnr_age","balance_tire_age","brake_pedal_pad_rnr_age","emission_exhaust_system_oth_age","replace_1tire_age",
             "tire_repaireach_age","transmission_service_filter_rnr_age","star_cooling_system_dia_age","mount_rnr_age","fuel_pump_electr_intank_rnr_age",
             "cooling_system_oth_age","coolant_recovery_tank_rnr_age","heating_ac_system_oth_age","star_hydraulic_brake_system_dia_age","blower_motor_rnr_age",
             "radiator_hose_rnr_age","heating_ac_system_dia_age","star_heater_system_dia_age","brake_rotor_rnr_age","abs_light_dia_age","thermostat_gasket_rnr_age",
             "jobmon","vehage","zip","horsepower","enginesize","airconditioning","cylinders",
             "primarycapacity","currentstatus","snowfall_avg","snowfall_sd","precip_avg","precip_sd","relhumavg_avg","relhumavg_sd","tempavg_avg",
             "tempavg_sd")

## Train model and test model
rf_model_family = function(taskfamily,train_data,features){
  train_name <- paste("rf_model",taskfamily,sep="") 
  train_name <- h2o.randomForest(x=features,y=taskfamily,data=train_data,classification=TRUE, ntree=50, 
                                 depth=15, sample.rate=0.67,nbins=1024,seed=-1, importance=TRUE, 
                                 nodesize=1,balance.classes=T)
}
rf_pred_family =  function(taskfamily,model,test_data){
  test_name <- paste("rf_pred",taskfamily,sep="") 
  test_name = h2o.predict(object=model, newdata=test_data)
  #h2o.performance(data, reference, measure = "accuracy", thresholds, gains = TRUE, ...)
  names(test_name) <- c(paste("Prediction",taskfamily,sep=""),paste("Pred",taskfamily,"0",sep="_"),paste("Pred",taskfamily,"1",sep="_"))
  return(test_name)
}

#rf_fit_02 <- rf_model_family("TaskFam2",train_data,Features)
#rf_pred_02 <- rf_pred_family("TaskFam2",rf_fit_02,test_data)

taskfam = c("taskfam1","taskfam2","taskfam3","taskfam4","taskfam6","taskfam7","taskfam8","taskfam9","taskfam11","taskfam12")
taskfam = c("taskfam11","taskfam12")
for(i in taskfam){
  trainname <- paste("rf_fit",i,sep="_")
  file_name <- paste("prediction/pred_",i,".csv",sep="")
  trainname <- rf_model_family(i,train_data,Features)
  testname <- paste("rf_pred",i,sep="_")
  testname <- rf_pred_family(i,trainname,test_data)
  testname1 <- as.data.frame(testname)
  write.csv(testname1,file_name,row.names=F)
}

## combine all the results with test data
multcbind = function(mypath){
  filenames=list.files(path=mypath, full.names=TRUE)
  datalist = lapply(filenames, function(x){read.csv(file=x,header=T)})
  Reduce(function(x,y) {cbind(x,y)}, datalist)}

results <- multcbind("prediction")
write.csv(results,"PRED/rf_pred_table.csv",row.names=F)
results <- read.csv("PRED/rf_pred_table.csv",stringsAsFactors=F)
## calculate the risk score = probability * age
fam01_jobs <- c("alignment_adj_age","radiator_cap_rnr_age","radiator_rnr_age","automatic_complete_rnr_age","shock_absorbers_rnr_age","power_steering_pump_rnr_age")
fam02_jobs <- c("disc_brakes_front_rnr_age","disc_brakes_rear_rnr_age","brake_pedal_pad_rnr_age","brake_rotor_rnr_age")
fam03_jobs <- c("replace_4tires_age","tire_pressure_adjust_age","replace_2tires_age","balance_tire_age",
                "replace_1tire_age","tire_repaireach_age")
fam04_jobs <- c("transmission_oth_age","transmission_linkage_rnr_age","transmission_service_filter_rnr_age","mount_rnr_age")
fam06_jobs <- c("battery_rnr_age","wiring_harness_rnr_age","alternator_rnr_age","spark_plugs_rnr_age",
                "starter_rnr_age")
fam07_jobs <- c("oxygen_sensor_rnr_age","emission_exhaust_system_oth_age","catalytic_converter_rnr_age")
fam08_jobs <- c("engine_system_oth_age","serpentine_belt_rnr_age","intake_manifold_gasket_rnr_age")
fam09_jobs <- c("air_filter_rnr_age","fuel_filter_rnr_age","fuel_pump_electr_intank_rnr_age","fuel_pump_electrical_rnr_age")
fam11_jobs <- c("radiator_cap_rnr_age","radiator_rnr_age","radiator_hose_rnr_age","thermostat_housing_rnr_age",
                "thermostat_gasket_rnr_age","coolant_replace_flush_rnr_age","cooling_system_oth_age","cooling_fan_engine_rnr_age",
                "coolant_recovery_tank_rnr_age","coolant_filterrnr_age")
fam12_jobs <- c("heating_ac_system_oth_age","compressor_clutcha_c_rnr_age","blower_motor_rnr_age")
fam80_jobs <- c("pm_dot_inspection_age")

results <- as.data.frame(results)
test_data <- read.csv("mom_test.csv",stringsAsFactors=F)
test_data$fam01_min <- apply(test_data[,fam01_jobs],1,min)
test_data$fam02_min <- apply(test_data[,fam02_jobs],1,min)
test_data$fam03_min <- apply(test_data[,fam03_jobs],1,min)
test_data$fam04_min <- apply(test_data[,fam04_jobs],1,min)
test_data$fam06_min <- apply(test_data[,fam06_jobs],1,min)
test_data$fam07_min <- apply(test_data[,fam07_jobs],1,min)
test_data$fam08_min <- apply(test_data[,fam08_jobs],1,min)
test_data$fam09_min <- apply(test_data[,fam09_jobs],1,min)
test_data$fam11_min <- apply(test_data[,fam11_jobs],1,min)
test_data$fam12_min <- apply(test_data[,fam12_jobs],1,min)


test_result <- cbind(test_data, results)
test_result$risk_fam01 <- as.numeric(test_result$Pred_taskfam1_1)*as.numeric(test_result$fam01_min)
test_result$risk_fam02 <- as.numeric(test_result$Pred_taskfam2_1)*as.numeric(test_result$fam02_min)
test_result$risk_fam03 <- as.numeric(test_result$Pred_taskfam3_1)*as.numeric(test_result$fam03_min)
test_result$risk_fam04 <- as.numeric(test_result$Pred_taskfam4_1)*as.numeric(test_result$fam04_min)
test_result$risk_fam06 <- as.numeric(test_result$Pred_taskfam6_1)*as.numeric(test_result$fam06_min)
test_result$risk_fam07 <- as.numeric(test_result$Pred_taskfam7_1)*as.numeric(test_result$fam07_min)
test_result$risk_fam08 <- as.numeric(test_result$Pred_taskfam8_1)*as.numeric(test_result$fam08_min)
test_result$risk_fam09 <- as.numeric(test_result$Pred_taskfam9_1)*as.numeric(test_result$fam09_min)
test_result$risk_fam11 <- as.numeric(test_result$Pred_taskfam11_1)*as.numeric(test_result$fam11_min)
test_result$risk_fam12 <- as.numeric(test_result$Pred_taskfam12_1)*as.numeric(test_result$fam12_min)
## get the predictions based on the ratio of number of taskfam in the training data
length <- as.numeric(Sys.Date() - as.Date("2010-01-01"))
test_result$fam01_prob <- ifelse(test_result$risk_fam01 > quantile(test_result$risk_fam01,1-fam01_ratio),test_result$risk_fam01/length,0)
test_result$fam02_prob <- ifelse(test_result$risk_fam02 > quantile(test_result$risk_fam02,1-fam02_ratio),test_result$risk_fam02/length,0)
test_result$fam03_prob <- ifelse(test_result$risk_fam03 > quantile(test_result$risk_fam03,1-fam03_ratio),test_result$risk_fam03/length,0)
test_result$fam04_prob <- ifelse(test_result$risk_fam04 > quantile(test_result$risk_fam04,1-fam04_ratio),test_result$risk_fam04/length,0)
test_result$fam06_prob <- ifelse(test_result$risk_fam06 > quantile(test_result$risk_fam06,1-fam06_ratio),test_result$risk_fam06/length,0)
test_result$fam07_prob <- ifelse(test_result$risk_fam07 > quantile(test_result$risk_fam07,1-fam07_ratio),test_result$risk_fam07/length,0)
test_result$fam08_prob <- ifelse(test_result$risk_fam08 > quantile(test_result$risk_fam08,1-fam08_ratio),test_result$risk_fam08/length,0)
test_result$fam09_prob <- ifelse(test_result$risk_fam09 > quantile(test_result$risk_fam09,1-fam09_ratio),test_result$risk_fam09/length,0)
test_result$fam11_prob <- ifelse(test_result$risk_fam11 > quantile(test_result$risk_fam11,1-fam11_ratio),test_result$risk_fam11/length,0)
test_result$fam12_prob <- ifelse(test_result$risk_fam12 > quantile(test_result$risk_fam12,1-fam12_ratio),test_result$risk_fam12/length,0)


## get the most recent 15 days sensor data for each vehicle
system("hive -f mom.sql")
system("hadoop fs -copyToLocal /apps/hive/warehouse/mh_sensor_malfunction_transpose")
system("cat mh_sensor_malfunction_transpose/0* > sensor.csv")
system("rm mh_sensor_malfunction_transpose/0*")
system("sed 's/\\\\N/0/g' sensor.csv > sensor_0.csv")
sensor <- fread("sensor_0.csv",stringsAsFactors=F,sep="\001")
names(sensor) <- c(
  "vehicle","battery_voltage","engine_speed","oxygen_sensor","ambient_air_temperature","transmission_fluid_temperature",
  "coolant_thermostat_coolant_temperature_below","camshaft_position","catalyst_system_efficiency_below",
  "crankshaft_position_sensor","misfire_detected","engine_coolant_temperature","engine_position_system_performance",
  "evaporative_emission","exhaust_gas_recirculation","ho2s_heater","heated_catalyst_below_threshold",
  "ignition_coil_malfunction","injection_pump_fuel_malfunction","injector_circuit_malfunction",
  "insufficient_coolant_temperature","internal_control_module","intake_air_temperature","idle_control",
  "knock_sensor","malfunction_indicator_lamp","mass_or_volume_air_flow_circuit",
  "power_steering_pressure_sensor","secondary_air","shift_solenoid","system_too","throttle_position",
  "tire_below_limit","transmission_malfunction","turbocharger_boost_sensor","transmission_range_sensor",
  "02_sensor_circuit","02_sensor_heater")


## join ages with sensors
age_sensor <- merge(x=test_result, y=sensor,by="vehicle", all.x=T)
age_sensor[is.na(age_sensor)] <- 0

## for each sensor shows up more than 100 time in the past 15 days, we add a weight of 0.1 to the vehicle
sensor_weight <- ifelse(age_sensor[,c(173:209)]<100,0,0.056)
age_sensor_weight <- cbind(age_sensor[,c(1:172)],sensor_weight)
age_sensor_weight$pred_weight <- rowSums(age_sensor_weight[,c(c(163:169),c(171:172))], na.rm = FALSE, dims = 1)
age_sensor_weight$sensor_weight <- rowSums(age_sensor_weight[,c(173:209)], na.rm = FALSE, dims = 1)
mean(age_sensor_weight$pred_weight)
mean(age_sensor_weight$sensor_weight)
age_sensor_weight$total_weight <- age_sensor_weight$pred_weight+age_sensor_weight$sensor_weight
age_sensor_w_order <- age_sensor_weight[order(-age_sensor_weight$total_weight),]

## group sensor by subsystems

##
FileName <- paste("PRED/Prediction_age_sensor_",Sys.Date(),".csv",sep="")
write.csv(age_sensor_w_order,FileName,row.names=F)
pred <- age_sensor_w_order
names(veh)[1] <-"CurrentStatus"
pred_veh <- sqldf("select pred.*, veh.CurrentStatus from pred left join veh on pred.Vehicle=veh.Vehicle")
pred_sub <- subset(pred_veh,CurrentStatus=="A")
VehGroupStart <- substr(pred_sub$vehiclegroup, start=1,stop=1)
pred_sub1 <- subset(pred_sub,substr(pred_sub$vehiclegroup, start=1, stop=1)!= 5)
pred_sub2 <- subset(pred_sub1,pm_dot_inspection_all_pass_cars_n_trucks_age>160)
FileNameFilter <- paste("PRED/Prediction_age_sensor_filter_",Sys.Date(),".csv",sep="")
write.csv(pred_sub2,FileNameFilter,row.names=F)
pred_small <- pred_sub2[,c(c(1:20),c(163:172),c(173:213))]
FileNameFilterSmall <- paste("PRED/Prediction_age_sensor_filter_Small_",Sys.Date(),".csv",sep="")
write.csv(pred_small,FileNameFilterSmall,row.names=F)
h2o.shutdown(localH2O)
