remove_outliers <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  y
}

data.bf$parts_age__days[as.numeric(as.character(data.bf$parts_age__days)) == -9] <- NA

data.test$parts_age__days <- rm.outlier(as.numeric(as.character(data.test$parts_age__days)),fill=T)

data.test$parts_age__days[as.numeric(as.character(data.test$parts_age__days)) < 0] <- NA

data.test$parts_age__days[as.numeric(as.character(data.test$parts_age__days)) > 1824] <- NA

datav20balanced <- balancedstratification(as.matrix(datav20), strata=c(rep(0,1200),rep(1,1200)), pik=inclusionprobabilities(datav20$BF, 1200))



indexes = sample(1:nrow(datav20.test.shuffle), size=0.2*nrow(datav20.test.shuffle))
test = datav20.test.shuffle[indexes,]
train = data20.test.shuffle[-indexes,]


train.small <- train[, c(2,5,9,c(11:30),252,253,256)]
test.small <- test[, c(2,5,9,c(11:30),252,253,256)]

data.1 <- data.clean.sample[, c(c(1:10),c(11:30),c(251:256))]

write.csv(train.small, file ="train_small.csv")
write.csv(test.small, file ="test_small.csv")

data.balanced <- read.csv("datav20_balanced.csv")
data.balanced <- data.balanced[,-1]

data1 <- data.balanced[, c(c(1:11), c(11:30),c(251:256))]
write.csv(data1, file ="data1.csv")

data.clean.2 <- subset(data.clean, as.numeric(as.character(data.clean$parts_age__days)) != 1825)

#split data into buckets and use for model test
data.clean1 <- data.clean.2[, c(2,5,9,c(11:30),256)]
data.clean2 <- data.clean.2[, c(2,5,9,c(31:50),256)]
data.clean3 <- data.clean.2[, c(2,5,9,c(51:70),256)]
data.clean4 <- data.clean.2[, c(2,5,9,c(71:90),256)]
data.clean7 <- data.clean.2[, c(2,5,9,c(131:150),256)]
data.clean12 <- data.clean.2[, c(2,5,9,c(231:250),256)]

write.csv(data.clean1, file="clean1.csv")
write.csv(data.clean2, file="clean2.csv")
write.csv(data.clean3, file="clean3.csv")
write.csv(data.clean4, file="clean4.csv")
write.csv(data.clean7, file="clean7.csv")
write.csv(data.clean12, file="clean12.csv")


#compare sensor data cross different buckets
compare <-ifelse(as.numeric(as.character(data.clean.2$telematics__SecondAirSystemMonitor10))==as.numeric(as.character(data.clean.2$telematics__SecondAirSystemMonitor)),1,0)
compare <-ifelse(as.numeric(as.character(data.clean.2$telematics__OxygenHeaterMonitor))==as.numeric(as.character(data.clean.2$telematics__OxygenHeaterMonitor12)),1,0)
#compare accuracy results between different buckets using only sensor data
compare3 <-ifelse(as.numeric(as.character(prediction.test$BF_3))==as.numeric(as.character(prediction.test$predict_3)),1,0)
compare4 <-ifelse(as.numeric(as.character(prediction.test$BF_4))==as.numeric(as.character(prediction.test$predict_4)),1,0)
compare7 <-ifelse(as.numeric(as.character(prediction.test$BF_7))==as.numeric(as.character(prediction.test$predict_7)),1,0)
compare12 <-ifelse(as.numeric(as.character(prediction.test$BF_12))==as.numeric(as.character(prediction.test$predict_12)),1,0)

#split data from original data using buckets
data1 <- data[, c(2,5,9,c(11:30),256)]
data2 <- data[, c(2,5,9,c(31:50),256)]
data3 <- data[, c(2,5,9,c(51:70),256)]
data4 <- data[, c(2,5,9,c(71:90),256)]
data7 <- data[, c(2,5,9,c(131:150),256)]
data12 <- data[, c(2,5,9,c(231:250),256)]

write.csv(data1, file = "data1.csv")
write.csv(data2, file = "data2.csv")
write.csv(data3, file = "data3.csv")
write.csv(data4, file = "data4.csv")
write.csv(data7, file = "data7.csv")
write.csv(data12, file = "data12.csv")

bucketage15_12 <- read.table("bucketAge15_12.csv", header=T, sep="|", nrow=100000)


# test "parallel" package in R
workerFunc <- function(n){return(n^2)}
values <- c(1:1000000)
head(lapply(values, workerFunc))
sertime <- system.time(lapply(values, workerFunc))
head(parLapply(cl, values, workerFunc))
partime <-system.time(parLapply(cl, values, workerFunc))
# test "foreach" package in R
head(foreach(i=1:100) %do% i^2)
eachtime <- system.time(foreach(i=1:1000) %do% sqrt(i))

#for SMOTE sampling
library(DMwR)

#preprocess data for SVM_perfect

train.data <- data[c(1:30000), c(256,2,5,9, c(11:30))]
test.data <- data[c(30001:60000), c(256,2,5,9, c(11:30))]
train.data$BF[train.data$BF == 0] <- -1
test.data$BF[test.data$BF == 0] <- -1
names(train.data) <- c("BF",c(1:23))
names(test.data) <- c("BF",c(1:23))
train.kv.1 <- paste("1",train.data$"1",sep=":")
train.kv.2 <- paste("2",train.data$"2",sep=":")
train.kv.3 <- paste("3",train.data$"3",sep=":")
train.kv.4 <- paste("4",train.data$"4",sep=":")
train.kv.5 <- paste("5",train.data$"5",sep=":")
train.kv.6 <- paste("6",train.data$"6",sep=":")
train.kv.7 <- paste("7",train.data$"7",sep=":")
train.kv.8 <- paste("8",train.data$"8",sep=":")
train.kv.9 <- paste("9",train.data$"9",sep=":")
train.kv.10 <- paste("10",train.data$"10",sep=":")
train.kv.11 <- paste("11",train.data$"11",sep=":")
train.kv.12 <- paste("12",train.data$"12",sep=":")
train.kv.13 <- paste("13",train.data$"13",sep=":")
train.kv.14 <- paste("14",train.data$"14",sep=":")
train.kv.15 <- paste("15",train.data$"15",sep=":")
train.kv.16 <- paste("16",train.data$"16",sep=":")
train.kv.17 <- paste("17",train.data$"17",sep=":")
train.kv.18 <- paste("18",train.data$"18",sep=":")
train.kv.19 <- paste("19",train.data$"19",sep=":")
train.kv.20 <- paste("20",train.data$"20",sep=":")
train.kv.21 <- paste("21",train.data$"21",sep=":")
train.kv.22 <- paste("22",train.data$"22",sep=":")
train.kv.23 <- paste("23",train.data$"23",sep=":")

train.kv <- cbind(train.data$BF, train.kv.1,train.kv.3,train.kv.4,train.kv.5,train.kv.6,train.kv.7,train.kv.8,train.kv.9,train.kv.10,train.kv.11,train.kv.12,train.kv.13,train.kv.14,train.kv.15,train.kv.16,train.kv.17,train.kv.18, train.kv.19,train.kv.20, train.kv.21, train.kv.22, train.kv.23)

write.table(train.kv, file="trainkv.csv", append=F, quote=F, sep=" ", row.names=F, col.names=F)

#May 13
#daily data
data428 <- read.csv("formatted_raw_daily/42800_data.csv",sep="|")
#battery R&R data
bat428 <- data428[which(data428$Task.Name=="Battery R&R"),]
bat428s <- bat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)]
bat428min <- bat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-3]
bat428median <- bat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-1]

bat428m <- as.matrix(bat428smin)
bat428m <- t(bat428m)
bat428ts <- ts(bat428m)
plot.ts(bat428ts[,c(51:56)])

bat428m <- as.matrix(bat428s)
bat428m <- t(bat428m)
bat428ts <- ts(bat428m)
plot.ts(bat428ts[,c(51:56)])



#non battery R&R data
nobat428 <- data428[which(data428$Task.Name!="Battery R&R"),]
nobat428s <- nobat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)]
nobat428min <- nobat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-3]
nobat428median <- nobat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-1]
nobat428m <- as.matrix(nobat428s)
nobat428m <- t(nobat428m)
nobat428ts <- ts(nobat428m)
plot.ts(nobat428ts[,c(151:160)])

#check misfire monitor sensor data

mf428 <- bat428[, c(161-83,347-83, 533-83,719-83,905-83,1091-83,1277-83,1463-83,1649-83,1835-83,2021-83,2207-83,2393-83,2579-83,2765-83,2951-83,3137-83,3323-83,3509-83,3695-83)]
mf428s <- bat428[, c(161-84,161-83,347-84,347-83,533-84,533-83,719-84,719-83,905-84,905-83,1091-84,1091-83,1277-84,1277-83,1463-84,1463-83,1649-84,1649-83,1835-84,1835-83,2021-84,2021-83,2207-84,2207-83,2393-84,2393-83,2579-84,2579-83,2765-84,2765-83,2951-84,2951-83,3137-84,3137-83,3323-84,3323-83,3509-84,3509-83,3695-84,3695-83)]
mf428s.test <- c(as.numeric(as.character(mf428s[,2]))/as.numeric(as.character(mf428s[,1])))
mf428m <- as.matrix(mf428s)
mf428m <- t(mf428m)
mf428ts <- ts(mf428m)
plot.ts(mf428ts[,c(21:26)])

nomf428 <- nobat428[, c(161-81,347-81, 533-81,719-81,905-81,1091-81,1277-81,1463-81,1649-81,1835-81,2021-81,2207-81,2393-81,2579-81,2765-81,2951-81,3137-81,3323-81,3509-81,3695-81)-2]
nomf428m <- as.matrix(nomf428s)
nomf428m <- t(nomf428m)
nomf428ts <- ts(nomf428m)
plot.ts(nomf428ts[,c(21:26)])


#check engine speed sensor data
ep428s <- bat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-28]
noep428s <- nobat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-28]
ep428m <- as.matrix(ep428s)
ep428m <- t(ep428m)
ep428ts <- ts(ep428m)
plot.ts(ep428ts[,c(11:20)])

noep428m <- as.matrix(noep428s)
noep428m <- t(noep428m)
noep428ts <- ts(noep428m)
plot.ts(noep428ts[,c(21:26)])

#check heated catelyst sensor data
hc428s <- bat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-81]
nohc428s <- nobat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-81]
hc428m <- as.matrix(hc428s)
hc428m <- t(hc428m)
hc428ts <- ts(hc428m)
plot.ts(hc428ts[,c(21:27)])

nohc428m <- as.matrix(nohc428s)
nohc428m <- t(nohc428m)
nohc428ts <- ts(nohc428m)
plot.ts(nohc428ts[,c(41:46)])

#check ignition sensor data
ig428s <- bat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-142]
noig428s <- nobat428[, c(161,347, 533,719,905,1091,1277,1463,1649,1835,2021,2207,2393,2579,2765,2951,3137,3323,3509,3695)-142]
ig428m <- as.matrix(ig428s)
ig428m <- t(ig428m)
ig428ts <- ts(ig428m)
plot.ts(ig428ts[,c(151:160)])

noig428m <- as.matrix(noig428s)
noig428m <- t(noig428m)
noig428ts <- ts(noig428m)
plot.ts(noig428ts[,c(41:46)])

#calculate means for each day exclude zeros and NAs
means <- function(data,n=20, zeros.rm=T){
  meanval <- c(rep(0,n))
  if (zeros.rm==T){
    for (i in c(1:n)){
      meanval[i] <- mean(data[,i][data[,i] != 0], na.rm=T)
    }
    return(meanval)
  }


  else if (zeros.rm==F){
    for (i in c(1:n)){
      
      meanval[i] <- mean(data[,i], na.rm=T)
      #return(meanval[i])
    }
    return(meanval)
  }
}



#test count/on see if they are same
test <- data428[data428[,108] !=data428[,109],]
test <- subset(data428, data428[,79]!=data428[,80])



# calculate ages/mileages from demand data
demand_11_14 <- rbind(demand_11_13c, demand_13_14c)
dup <- duplicated(demand_11_14)
demand1114c <- demand_11_14[!duplicated(demand_11_14),]

q <- sqldf("select * from demand_bat order by Vehicle,jobDate ")


diff <- function(data){
  row = 1
  if (data[row,1]== data[row+1,1]){
    diffday <- data[row+1,3]-data[row,3]
    diffmil <- data[row+1,5]-data[row,5]
    row = row + 1
  }
  data <- cbind(data, diffday, diffmil)
}

diff <- function(data){

  #diffday <- c(0)
  diffday <- c(rep(NA, nrow(data)))

  for (row in c(1:nrow(data))){
    if (data[row,1] == data[row+1,1]){
      diffday[row] <-TRUE
    }
  }
  return(diffday)
}



diff <- function(data){
  
  #diffday <- c(0)
  #diffday <- c(rep(0, 12))
  row =1
  while(row <12){
    if (data[row,1] == data[row+1,1]){
      return(data.table(cbind(data,as.numeric(data[row+1,3]-data[row,3]))))
      inc(1) <-row
    }
  }
  
  return(diffday)
}
library(Hmisc)

library(data.table)
diff <- function(data){
  
  #diffday <- c(0)
  diffday <- c(rep(0, nrow(data)))
  
  row = 1
  if (data[row,1] == data[row+1,1]){
    diffday[row] <- as.numeric(data[row+1,3]-data[row,3])
    #diffmil <- data[row+1,5]-data[row,5]
  }
  inc(row) <- 1

  return(diffday)
}


# May 14 get battery milage and age

data1113 <- read.table("dr_data//2011_2013_Demand.txt",header= T, sep=",")
data1113c <- data1113[, c(2,3,5,8,16)]
rm(data1113)
data1314 <- read.table("dr_data//1311_1403_Demand.txt",header=T, sep=",")
data1314c <- data1314[,c(2,3,5,8,16)]
rm(data1314)
data1114 <- rbind(data1113c, data1314c)
data1114$JobDate <- as.Date(data1114$JobDate, "%m/%d/%Y")
#subset battery R&R data
batdata <- data1114[which(data1114$TaskName =="Battery R&R"),]

library(sqldf)

ord.data <- sqldf("select * from batdata order by Vehicle, JobDate")


x <- diff(ord.data)

selrows <- function(data){
  
}

library(data.table)
findMatch <- function(data){
  
}



# battery ages and mileages
batt_age <- read.csv("battery_age_days_processed.csv")
#163417
names(batt_age) <- c("Vehicle","JobDate","BatteryAge")
batt_zeros <- batt_age[which(batt_age$BatteryAge==0),]
#11525
batt_normal <-batt_age[which(batt_age$BatteryAge>0),]
#85700
unique(factor(as.character(batt_normal$Vehicle)))
#41053 level
hist(as.numeric(as.character(batt_normal$BatteryAge)))
batt_mil <- read.csv("battery_age_mileage_processed.csv")
#105037
names(batt_mil) <- c("Vehicle","JobDate","BatteryAge","BatteryMileage")
dim(battmil_normal)
#42120

# subset mileages (0:30000 miles) and put into bin with binwithd 1000
batmiltest <- battmil_normal
batmiltest <- batmiltest[which(batmiltest$BatteryMileage <30000),]
g <- factor(round(batmiltest$BatteryMileage/1000))
hist(as.numeric(as.character(g)))

# test the mileage/days see if it makes sense
milperday <- battmil_normal$BatteryMileage/battmil_normal$BatteryAge
summary(milperday)
milperday.c <- milperday[milperday <100]
hist(milperday.c)
g <- factor(round(milperday/5))



# calculate battery used age and milesge for the non Battery R&R jobs
batt_age <- read.csv("battery_age_days_processed.csv")
# na data
batt_age_clean.na <- subset(batt_age,is.na(BatteryAge))
batt_age_clean.na$JobDate <- as.Date(as.character(batt_age_clean.na$JobDate))
battage <- as.numeric(Sys.Date() - batt_age_clean.na$JobDate)
hist(as.numeric(as.character(batt_age_clean.na$BatteryAge)))
hist(as.numeric(as.character(batt_age_clean.na$BatteryAge)))
BRR <- c(rep(0, nrow(batt_age_clean.na)))
battage_na <- cbind(batt_age_clean.na, BRR) 
battage_na$JobDate <- as.character(battage_na$JobDate)
#nonzero nonna data
batt_age_normal <- subset(batt_age, batt_age$BatteryAge >0)
BRR <- c(rep(1, nrow(batt_age_normal)))
battage_normal <-cbind(batt_age_normal, BRR)

batt_age <- rbind(battage_normal, battage_na)
write.csv(batt_age, "batt_age_BRR.csv")

#Join battery data with vehicle data
batt_age <- read.csv("batt_age_BRR.csv")
veh_data <- read.table("2_Vehicle_Spec_Data_for_Abhay_With_Zip_Code.txt", header=T, sep=",")
library(sqldf)

data <- sqldf("SELECT veh_data.Vehicle,Veh_data.VehGroup,Veh_data.SpecID, Veh_data.FuelType, 
              veh_data.QUE_FLT_VEHEXT01_State, batt_age.JobDate, batt_age.BatteryAge, 
              batt_age.BRR FROM batt_age LEFT JOIN veh_data ON veh_data.Vehicle = batt_age.Vehicle")
write.csv(data, file="Veh_Batt.csv")



#May 15th
batt_mil <- read.csv("battery_age_mileage_processed.csv")
names(batt_mil) <- c("Vehicle","JobDate","BatteryAge","BatteryMileage")
battmil_normal <- batt_mil[which(as.numeric(as.character(batt_mil$BatteryMileage))>0),]
# produce battery age 5 days ago and battery mileage 5 days ago by subtracting the present mileage by 5 days milperday
BatteryAge5 <- battmil_normal$BatteryAge - 5
BatteryMileage5 <- battmil_normal$BatteryMileage - round(5*(battmil_normal$BatteryMileage/battmil_normal$BatteryAge))
BatteryAge10 <- battmil_normal$BatteryAge - 10
BatteryMileage10 <- battmil_normal$BatteryMileage - round(10*(battmil_normal$BatteryMileage/battmil_normal$BatteryAge))
BatteryAge15 <- battmil_normal$BatteryAge - 15
BatteryMileage15 <- battmil_normal$BatteryMileage - round(15*(battmil_normal$BatteryMileage/battmil_normal$BatteryAge))
BatteryAge20 <- battmil_normal$BatteryAge - 20
BatteryMileage20 <- battmil_normal$BatteryMileage - round(20*(battmil_normal$BatteryMileage/battmil_normal$BatteryAge))
BatteryAge25 <- battmil_normal$BatteryAge - 25
BatteryMileage25 <- battmil_normal$BatteryMileage - round(25*(battmil_normal$BatteryMileage/battmil_normal$BatteryAge))
BatteryAge30 <- battmil_normal$BatteryAge - 30
BatteryMileage30 <- battmil_normal$BatteryMileage - round(30*(battmil_normal$BatteryMileage/battmil_normal$BatteryAge))


# take a look at new daily data with 0 replaced by NA
daily <- read.table("dailyformatted2.csv",header=T, nrow=10000, sep="|")
daily <- daily[, c(1:200)]
hist(daily$Battery.Voltage_average_1,xlim=range(8,15),breaks=200,ylim=range(0,1000))
lowbet1<- subset(daily, daily$Battery.Voltage_average_1>5&daily$Battery.Voltage_average_1<12)
dim(lowbet1)
hist(daily$Battery.Voltage_average_1,xlim=range(10,12),breaks=100,ylim=range(0,50))
lowbet2<- subset(daily, daily$Battery.Voltage_average_2>5&daily$Battery.Voltage_average_2<12)
dim(lowbet2)
lowbet10<- subset(daily, daily$Battery.Voltage_average_10>5&daily$Battery.Voltage_average_10<12)
dim(lowbet10)
lowbet20<- subset(daily, daily$Battery.Voltage_average_20>5&daily$Battery.Voltage_average_20<12)
dim(lowbet20)
hist(daily$Battery.Voltage_average_20,xlim=range(10,12),breaks=100,ylim=range(0,50))
hist(daily$Battery.Voltage_average_10,xlim=range(10,12),breaks=100,ylim=range(0,50))


# Demand data with both ages and mileages
demand <- read.csv("fleet_data/battery_age_mileage_processed.csv")
names(demand) <- c("Vehicle","JobDate","BatteryAge","BatteryMileage")
demand_normal <- subset(demand, demand$BatteryMileage >0)
demand_NA <- subset(demand, is.na(demand$BatteryMileage))
demand_NA$BatteryAge <- as.numeric(as.Date(as.character("03/24/14"),"%m/%d/%y")-as.Date(as.character(demand_NA$JobDate),"%m/%d/%y"))
write.csv(demand_normal,"batt_age_mileage_BRR1.csv")
write.csv(demand_NA,"batt_age_mileage_BRR0.csv")
bucketdate <- read.table("subset_bucket.csv",header=T, sep="|")
BRR = c(rep(1, nrow(demand_normal)))
demand_normal <- cbind(demand_normal,BRR)
BRR = c(rep(0, nrow(demand_NA)))
demand_NA <- cbind(demand_NA,BRR)
batt_age <- rbind(demand_normal, demand_NA)
library(sqldf)
veh_data$DateInservice <- as.Date(veh_data$DateInservice,"%m/%d/%Y")
data <- sqldf("SELECT veh_data.Vehicle,Veh_data.VehGroup,Veh_data.DateInservice,Veh_data.SpecID, 
              Veh_data.FuelType, veh_data.QUE_FLT_VEHEXT01_State, batt_age.JobDate, batt_age.BatteryAge, 
              batt_age.BatteryMileage, batt_age.BRR FROM batt_age LEFT JOIN veh_data ON veh_data.Vehicle = batt_age.Vehicle")
write.csv(data, file="Veh_Batt_am.csv")
data.small <- data[which(as.Date(as.character(data$JobDate),"%m/%d/%y")> as.Date(as.character("06/01/13"),"%m/%d/%y")),]
#calculate battery mileage based on battery age


#join sensor data with veh_battery data
veh_batt <- read.csv("fleet_data/Veh_Batt_am_rev.csv")
sensor <- read.table("subset2.csv",header=T, sep="|")
sendata <- sensor[which(sensor$TaskName =="Battery R&R\t"),]
if(veh_batt$BatteryMileage==NA){
  veh_batt$BatteryMileage <- veh_batt$BatteryAge*10
}

#May 16
dim(veh_batt)
summary(factor(veh_batt$BRR))
veh_batt_0 <- subset(veh_batt, veh_batt$BRR == 0)
veh_batt_1 <- subset(veh_batt, veh_batt$BRR == 1)
write.csv(veh_batt_1, file="daily_mileage_dirtycar.csv")
head(veh_batt_0)
vehJobDate0 <- as.Date(as.character(veh_batt_0$JobDate),"%m/%d/%Y")
vehJobDate1 <- as.Date(as.character(veh_batt_1$JobDate),"%m/%d/%Y")

veh_batt_0$JobDate <- vehJobDate0
veh_batt_1$JobDate <- vehJobDate1

demand11<- read.csv("dr_data/2011_2013_Demand.txt",sep=",",header=T)
demand11 <-demand11[,c(-17)]
demand13<- read.csv("dr_data/1311_1403_Demand.txt",sep=",",header=T)
demand <- rbind(demand11, demand13)
rm(demand11)
rm(demand13)
names(demand)
demandJobDate <- as.Date(as.character(demand$JobDate),"%m/%d/%Y")
demand$JobDate <- demandJobDate
demandDetailDate <- as.Date(as.character(demand$DetailDate),"%m/%d/%Y")
demand$DetailDate <- demandDetailDate

veh_clean <- read.table("fleet_data/cleanvehicles.csv",header= T, sep="|")
names(veh_clean)[1:30]
veh_clean <- veh_clean[,c(1,2,3,5,7,9,15,16)]

dt1 <- sqldf("SELECT veh_batt_0.Vehicle, veh_batt_0.VehGroup, veh_batt_0.SpecID, demand.TaskID, veh_batt_0.FuelType,
             veh_batt_0.QUE_FLT_VEHEXT01_State, veh_batt_0.JobDate, veh_batt_0.BatteryAge, 
             veh_batt_0.BatteryMileage, veh_batt_0.VehicleAge,veh_batt_0.BRR, demand.Mileage as BatteryInitMileage, demand.DetailDate
             FROM veh_batt_0 LEFT JOIN demand ON (veh_batt_0.Vehicle=demand.Vehicle AND veh_batt_0.JobDate=demand.JobDate)")

dt1_test <- sqldf("SELECT veh_batt_0.Vehicle, veh_batt_0.VehGroup, veh_batt_0.SpecID, demand.TaskID, veh_batt_0.FuelType,
             veh_batt_0.QUE_FLT_VEHEXT01_State, veh_batt_0.JobDate, veh_batt_0.BatteryAge, 
             veh_batt_0.BatteryMileage, veh_batt_0.BRR, demand.Mileage as BatteryInitMileage, demand.DetailDate
             FROM veh_batt_0 LEFT JOIN demand ON (veh_batt_0.Vehicle=demand.Vehicle AND veh_batt_0.JobDate=demand.JobDate)")

dt1 <- unique(dt1)
dt2 <- sqldf("SELECT dt1.Vehicle, veh_clean.UnitID,dt1.VehGroup, dt1.TaskID,dt1.SpecID, dt1.QUE_FLT_VEHEXT01_State, dt1.JobDate, 
             dt1.BatteryAge, dt1.BatteryMileage, dt1.BRR, dt1.BatteryInitMileage,
             veh_clean.State, veh_clean.DemandDate, veh_clean.TaskName, dt1.VehicleAge,veh_clean.Odometer_min_5, 
            veh_clean.Odometer_max_5
             FROM dt1 LEFT JOIN veh_clean ON (dt1.Vehicle=veh_clean.VehicleID)")

write.csv(dt2,"daily_mileage_wna.csv")

dt2_test <- sqldf("SELECT dt1.Vehicle, veh_clean.UnitID,dt1.VehGroup, dt1.SpecID, dt1.QUE_FLT_VEHEXT01_State, dt1.JobDate, 
             dt1.BatteryAge, dt1.BatteryMileage, dt1.BRR, dt1.BatteryInitMileage,
             veh_clean.State, veh_clean.DemandDate, veh_clean.TaskName, dt1.VehicleAge,veh_clean.Odometer_min_5, veh_clean.Odometer_max_5
             FROM dt1 LEFT JOIN veh_clean ON dt1.Vehicle=veh_clean.VehicleID AND dt1.DetailDate=veh_clean.DemandDate")


head(dt2)
dt2 <- unique(dt2)
write.csv(dt2, "veh_batt_dt2.csv")
dt2_clean <- dt2[which(dt2$Odometer_max_5 >0),]
dt2_clean_test <- dt2[which(dt2_test$Odometer_max_5 >0),]
write.csv(dt2_clean, "veh_batt_dt2_clean.csv")

write.csv(data1,"batt_odo_battMil.csv")
data_odo <- read.csv("fleet_data/batt_odo_battMil.csv")
  
#time series 


battvol_min_00_ts <- ts(battvol_min_00[1,])
battvol_min_00_ts <- ts(battvol_min_00,frequency=20)
plot.ts(battvol_min_00_ts[1:10])
#May 17

# data00
data00 <- read.table("data_daily/data00.csv",sep="|",header=T)
battvol_min_00 <- data00[,c(1,9,22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
battvol_min_00_1 <- battvol_min_00[which(battvol_min_00$TaskName=="Battery R&R\t"),]
battvol_min_00_0 <- battvol_min_00[which(battvol_min_00$TaskName!="Battery R&R\t"),]
mean_battmin_1 <- sapply(battvol_min_00_1, function(x) mean(as.numeric(x),na.rm=T))
mean_battmin_1_ts <- ts(mean_battmin_1)
plot.ts(mean_battmin_1[3:22])
battvol_min_00_0 <- battvol_min_00_0[c(1:192),]
mean_battmin_0 <- sapply(battvol_min_00_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_battmin_0[3:22])

# data01
data01 <- read.table("data_daily/data01.csv",sep="|",header=T)
battvol_min_01 <- data01[,c(1,9,22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
battvol_min_01_1 <- battvol_min_01[which(battvol_min_01$TaskName=="Battery R&R\t"),]
battvol_min_01_0 <- battvol_min_01[which(battvol_min_01$TaskName!="Battery R&R\t"),]
mean_battmin_1 <- sapply(battvol_min_01_1, function(x) mean(as.numeric(x),na.rm=T))
mean_battmin_1_ts <- ts(mean_battmin_1)
plot.ts(mean_battmin_1[3:22],ylim=range(12.7,13.7))
battvol_min_01_0 <- battvol_min_01_0[c(1:192),]
mean_battmin_0 <- sapply(battvol_min_01_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_battmin_0[3:22],ylim=range(12.7,13.7))
plot.ts(mean_battmin_1[3:22],ylim=range(12.7,13.7))
# data02
data02 <- read.table("data_daily/data02.csv",sep="|",header=T)
battvol_min_02 <- data02[,c(1,9,22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
battvol_min_02_1 <- battvol_min_02[which(battvol_min_02$TaskName=="Battery R&R\t"),]
battvol_min_02_0 <- battvol_min_02[which(battvol_min_02$TaskName!="Battery R&R\t"),]
mean_battmin_1 <- sapply(battvol_min_02_1, function(x) mean(as.numeric(x),na.rm=T))
mean_battmin_1_ts <- ts(mean_battmin_1)
battvol_min_02_0 <- battvol_min_02_0[c(1:192),]
mean_battmin_0 <- sapply(battvol_min_02_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_battmin_0[3:22],ylim=range(12.7,13.7))
plot.ts(mean_battmin_1[3:22],ylim=range(12.7,13.7))

#data 20
data20 <- read.table("data_daily/data20.csv",sep="|",header=T)
battvol_min_20 <- data20[,c(1,9,22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
battvol_min_20_1 <- battvol_min_20[which(battvol_min_20$TaskName=="Battery R&R\t"),]
battvol_min_20_0 <- battvol_min_20[which(battvol_min_20$TaskName!="Battery R&R\t"),]
mean_battmin_1 <- sapply(battvol_min_20_1, function(x) mean(as.numeric(x),na.rm=T))
mean_battmin_1_ts <- ts(mean_battmin_1)
battvol_min_20_0 <- battvol_min_20_0[c(1:192),]
mean_battmin_0 <- sapply(battvol_min_20_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_battmin_0[3:22],ylim=range(12.7,13.7))
plot.ts(mean_battmin_1[3:22],ylim=range(12.7,13.7))
#data 40
data40 <- read.table("data_daily/data40.csv",sep="|",header=T)
battvol_min_40 <- data40[,c(1,9,22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
battvol_min_40_1 <- battvol_min_40[which(battvol_min_40$TaskName=="Battery R&R\t"),]
battvol_min_40_0 <- battvol_min_40[which(battvol_min_40$TaskName!="Battery R&R\t"),]
mean_battmin_1 <- sapply(battvol_min_40_1, function(x) mean(as.numeric(x),na.rm=T))
mean_battmin_1_ts <- ts(mean_battmin_1)
battvol_min_40_0 <- battvol_min_40_0[c(1:192),]
mean_battmin_0 <- sapply(battvol_min_40_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_battmin_0[3:22],ylim=range(12.7,13.7))
plot.ts(mean_battmin_1[3:22],ylim=range(12.7,13.7))

# data 30 engine speed
data30 <- read.csv("data_daily/data30.csv",sep="|",header=T)
engspeed_min_30 <- data30[,c(1,9,40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
engspeed_min_30_1 <- engspeed_min_30[which(engspeed_min_30$TaskName=="Battery R&R\t"),]
engspeed_min_30_0 <- engspeed_min_30[which(engspeed_min_30$TaskName!="Battery R&R\t"),]
mean_engspeed_1 <- sapply(engspeed_min_30_1, function(x) mean(as.numeric(x),na.rm=T))
mean_engspeed_1_ts <- ts(mean_engspeed_1)
engspeed_min_30_0 <- engspeed_min_30_0[c(1:192),]
mean_engspeed_0 <- sapply(engspeed_min_30_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_engspeed_0[3:22],ylim=range(450,700))
plot.ts(mean_engspeed_1[3:22],ylim=range(450,700))
# data 20 engine speed
engspeed_min_20 <- data20[,c(1,9,40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
engspeed_min_20_1 <- engspeed_min_20[which(engspeed_min_20$TaskName=="Battery R&R\t"),]
engspeed_min_20_0 <- engspeed_min_20[which(engspeed_min_20$TaskName!="Battery R&R\t"),]
mean_engspeed_1 <- sapply(engspeed_min_20_1, function(x) mean(as.numeric(x),na.rm=T))
mean_engspeed_1_ts <- ts(mean_engspeed_1)
engspeed_min_20_0 <- engspeed_min_20_0[c(1:192),]
mean_engspeed_0 <- sapply(engspeed_min_20_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_engspeed_0[3:22],ylim=range(450,700))
plot.ts(mean_engspeed_1[3:22],ylim=range(450,700))

# data 20 engine coolant temperature below limit
engcool_min_20 <- data20[,c(1,9,36,36+136,36+2*136,36+3*136,36+4*136,36+5*136,36+6*136,36+7*136,36+8*136,36+9*136,36+10*136,36+11*136,36+12*136,36+13*136,36+14*136,36+15*136,36+16*136,36+17*136,36+18*136,36+19*136)]
engcool_min_20_1 <- engcool_min_20[which(engcool_min_20$TaskName=="Battery R&R\t"),]
engcool_min_20_0 <- engcool_min_20[which(engcool_min_20$TaskName!="Battery R&R\t"),]
mean_engcool_1 <- sapply(engcool_min_20_1, function(x) mean(as.numeric(x),na.rm=T))
mean_engcool_1_ts <- ts(mean_engcool_1)
engcool_min_20_0 <- engcool_min_20_0[c(1:192),]
mean_engcool_0 <- sapply(engcool_min_20_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_engcool_0[3:22])
plot.ts(mean_engcool_1[3:22])


# check battery below limit counts
data01 <- read.csv("data_daily/data01.csv",sep="|",header=T)
battvol_min_01 <- data01[,c(1,9,22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
battvol_bt_01 <- data01[,c(1,9,26,26+136,26+2*136,26+3*136,26+4*136,26+5*136,26+6*136,26+7*136,26+8*136,26+9*136,
                            26+10*136,26+11*136,26+12*136,26+13*136,26+14*136,26+15*136,26+16*136,26+17*136,26+18*136,26+19*136)]
battvol_bt_01_1 <- battvol_bt_01[which(battvol_bt_01$TaskName=="Battery R&R\t"),]
sum_bt_1 <- sapply(battvol_bt_01_1, function(x) sum(as.numeric(x),na.rm=T))
battvol_bt_01_0 <- battvol_bt_01_0[c(1:350),]
sum_bt_0 <- sapply(battvol_bt_01_0, function(x) sum(as.numeric(x),na.rm=T))
plot.ts(sum_bt_0[3:22])
plot.ts(sum_bt_1[3:22])

#use larger datasets data00:data44
data00 <- read.csv("data_daily/data00.csv",sep="|",header=T)
battvol_min_00 <- data00[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_00 <- data00[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]

rm(data00)
data01 <- read.csv("data_daily/data01.csv",sep="|",header=T)
battvol_min_01 <- data01[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_01 <- data01[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]

rm(data01)

data02 <- read.csv("data_daily/data02.csv",sep="|",header=T)
battvol_min_02 <- data02[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_02 <- data02[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data02)

data03 <- read.csv("data_daily/data03.csv",sep="|",header=T)
battvol_min_03 <- data03[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_03 <- data03[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data03)

data04 <- read.csv("data_daily/data04.csv",sep="|",header=T)
battvol_min_04 <- data04[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_04 <- data04[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data04)

data05 <- read.csv("data_daily/data05.csv",sep="|",header=T)
battvol_min_05 <- data05[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_05 <- data05[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data05)


data06 <- read.csv("data_daily/data06.csv",sep="|",header=T)
battvol_min_06 <- data06[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_06 <- data06[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data06)

data07 <- read.csv("data_daily/data07.csv",sep="|",header=T)
battvol_min_07 <- data07[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_07 <- data07[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data07)

data08 <- read.csv("data_daily/data08.csv",sep="|",header=T)
battvol_min_08 <- data08[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_08 <- data08[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data08)

data09 <- read.csv("data_daily/data09.csv",sep="|",header=T)
battvol_min_09 <- data09[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_09 <- data09[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data09)

data10 <- read.csv("data_daily/data10.csv",sep="|",header=T)
battvol_min_10 <- data10[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_10 <- data10[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data10)

data11 <- read.csv("data_daily/data11.csv",sep="|",header=T)
battvol_min_11 <- data11[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_11 <- data11[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data11)

data12 <- read.csv("data_daily/data12.csv",sep="|",header=T)
battvol_min_12 <- data12[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_12 <- data12[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data12)

data13 <- read.csv("data_daily/data13.csv",sep="|",header=T)
battvol_min_13 <- data13[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_13 <- data13[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data13)

data14 <- read.csv("data_daily/data14.csv",sep="|",header=T)
battvol_min_14 <- data14[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_14 <- data14[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data14)

data15 <- read.csv("data_daily/data15.csv",sep="|",header=T)
battvol_min_15 <- data15[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_15 <- data15[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data15)

data16 <- read.csv("data_daily/data16.csv",sep="|",header=T)
battvol_min_16 <- data16[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_16 <- data16[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data16)

data17 <- read.csv("data_daily/data17.csv",sep="|",header=T)
battvol_min_17 <- data17[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_17 <- data17[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data17)

data18 <- read.csv("data_daily/data18.csv",sep="|",header=T)
battvol_min_18 <- data18[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_18 <- data18[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data18)

data19 <- read.csv("data_daily/data19.csv",sep="|",header=T)
battvol_min_19 <- data19[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_19 <- data19[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data19)

data20 <- read.csv("data_daily/data20.csv",sep="|",header=T)
battvol_min_20 <- data20[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_20 <- data20[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data20)

data21 <- read.csv("data_daily/data21.csv",sep="|",header=T)
battvol_min_21 <- data21[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_21 <- data21[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]

rm(data21)

data22 <- read.csv("data_daily/data22.csv",sep="|",header=T)
battvol_min_22 <- data22[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_22 <- data22[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data22)

data23 <- read.csv("data_daily/data23.csv",sep="|",header=T)
battvol_min_23 <- data23[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_23 <- data23[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data23)

data24 <- read.csv("data_daily/data24.csv",sep="|",header=T)
battvol_min_24 <- data24[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_24 <- data24[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data24)

data25 <- read.csv("data_daily/data25.csv",sep="|",header=T)
battvol_min_25 <- data25[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_25 <- data25[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data25)

data26 <- read.csv("data_daily/data26.csv",sep="|",header=T)
battvol_min_26 <- data26[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_26 <- data26[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data26)

data27 <- read.csv("data_daily/data27.csv",sep="|",header=T)
battvol_min_27 <- data27[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_27 <- data27[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data27)

data28 <- read.csv("data_daily/data28.csv",sep="|",header=T)
battvol_min_28 <- data28[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_28 <- data28[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data28)

data29 <- read.csv("data_daily/data29.csv",sep="|",header=T)
battvol_min_29 <- data29[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_29 <- data29[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data29)

data30 <- read.csv("data_daily/data30.csv",sep="|",header=T)
battvol_min_30 <- data30[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_30 <- data30[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data30)

data31 <- read.csv("data_daily/data31.csv",sep="|",header=T)
battvol_min_31 <- data31[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_31 <- data31[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data31)

data32 <- read.csv("data_daily/data32.csv",sep="|",header=T)
battvol_min_32 <- data32[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_32 <- data32[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data32)

data33 <- read.csv("data_daily/data33.csv",sep="|",header=T)
battvol_min_33 <- data33[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_33 <- data33[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data33)

data34 <- read.csv("data_daily/data34.csv",sep="|",header=T)
battvol_min_34 <- data34[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_34 <- data34[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data34)

data35 <- read.csv("data_daily/data35.csv",sep="|",header=T)
battvol_min_35 <- data35[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_35 <- data35[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data35)

data36 <- read.csv("data_daily/data36.csv",sep="|",header=T)
battvol_min_36 <- data36[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_36 <- data36[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data36)

data37 <- read.csv("data_daily/data37.csv",sep="|",header=T)
battvol_min_37 <- data37[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_37 <- data37[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data37)

data38 <- read.csv("data_daily/data38.csv",sep="|",header=T)
battvol_min_38 <- data38[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_38 <- data38[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data38)

data39 <- read.csv("data_daily/data39.csv",sep="|",header=T)
battvol_min_39 <- data39[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_39 <- data39[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data39)

data40 <- read.csv("data_daily/data40.csv",sep="|",header=T)
battvol_min_40 <- data40[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_40 <- data40[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data40)

data41 <- read.csv("data_daily/data41.csv",sep="|",header=T)
battvol_min_41 <- data41[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_41 <- data41[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data41)

data42 <- read.csv("data_daily/data42.csv",sep="|",header=T)
battvol_min_42 <- data42[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_42 <- data42[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data42)

data43 <- read.csv("data_daily/data43.csv",sep="|",header=T)
battvol_min_43 <- data43[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_43 <- data43[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data43)

data44 <- read.csv("data_daily/data44.csv",sep="|",header=T)
battvol_min_44 <- data44[,c(c(1:9),22,22+136,22+2*136,22+3*136,22+4*136,22+5*136,22+6*136,22+7*136,22+8*136,22+9*136,
                            22+10*136,22+11*136,22+12*136,22+13*136,22+14*136,22+15*136,22+16*136,22+17*136,22+18*136,22+19*136)]
engspeed_min_44 <- data44[,c(c(1:9),40,40+136,40+2*136,40+3*136,40+4*136,40+5*136,40+6*136,40+7*136,40+8*136,40+9*136,
                             40+10*136,40+11*136,40+12*136,40+13*136,40+14*136,40+15*136,40+16*136,40+17*136,40+18*136,40+19*136)]
rm(data44)


#Battery voltage min
battvol_min <- rbind(battvol_min_01,battvol_min_02,battvol_min_03,battvol_min_04, battvol_min_05,
                     battvol_min_06,battvol_min_07,battvol_min_08,battvol_min_09, battvol_min_10,
                     battvol_min_11,battvol_min_12,battvol_min_13,battvol_min_14, battvol_min_15,
                     battvol_min_16,battvol_min_17,battvol_min_18,battvol_min_19, battvol_min_20,
                     battvol_min_21,battvol_min_22,battvol_min_23,battvol_min_24, battvol_min_25,
                     battvol_min_26,battvol_min_27,battvol_min_28,battvol_min_29, battvol_min_30,
                     battvol_min_31,battvol_min_32,battvol_min_33,battvol_min_34, battvol_min_35,
                     battvol_min_36,battvol_min_37,battvol_min_38,battvol_min_39, battvol_min_40,
                     battvol_min_41,battvol_min_42,battvol_min_43,battvol_min_44)
battvol_min_1 <- battvol_min[which(battvol_min$TaskName=="Battery R&R\t"),]
dim(battvol_min_1)
hist(as.numeric(as.character(battvol_min_1[,c(4)])))
battvol_min_0 <- battvol_min[which(battvol_min$TaskName!="Battery R&R\t"),]
dim(battvol_min_0)
hist(as.numeric(as.character(battvol_min_0[,c(4)])))
mean_battmin_1 <- sapply(battvol_min_1, function(x) mean(as.numeric(x),na.rm=T))
battvol_min_0 <- battvol_min_0[sample(1:nrow(battvol_min_0), 15000, replace=F),]
battvol_min_0 <- battvol_min_0[c(120000:121683),]
mean_battmin_0 <- sapply(battvol_min_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_battmin_0[4:22],,ylim=range(12.7,13.7))
plot.ts(mean_battmin_1[4:22],ylim=range(12.7,13.7))

# engine speed min
engspeed_min <- rbind(engspeed_min_01,engspeed_min_02,engspeed_min_03,engspeed_min_04,engspeed_min_05,
                      engspeed_min_06,engspeed_min_07,engspeed_min_08,engspeed_min_09,engspeed_min_10,
                      engspeed_min_11,engspeed_min_12, engspeed_min_13, engspeed_min_14, engspeed_min_15,
                      engspeed_min_16,engspeed_min_17, engspeed_min_18, engspeed_min_19, engspeed_min_20,
                      engspeed_min_21,engspeed_min_22,engspeed_min_23,engspeed_min_24,engspeed_min_25,
                      engspeed_min_26,engspeed_min_27,engspeed_min_28,engspeed_min_29,engspeed_min_30,
                      engspeed_min_31,engspeed_min_32, engspeed_min_33, engspeed_min_34, engspeed_min_35,
                      engspeed_min_36,engspeed_min_37, engspeed_min_38, engspeed_min_39, engspeed_min_40,
                      engspeed_min_41,engspeed_min_42, engspeed_min_43, engspeed_min_44)
engspeed_min_1 <- engspeed_min[which(engspeed_min$TaskName=="Battery R&R\t"),]
dim(engspeed_min_1)
hist(as.numeric(as.character(engspeed_min_1[,c(4)])))
engspeed_min_0 <- engspeed_min[which(engspeed_min$TaskName!="Battery R&R\t"),]
dim(engspeed_min_0)
hist(as.numeric(as.character(engspeed_min_0[,c(4)])))
mean_engspeed_1 <- sapply(engspeed_min_1, function(x) mean(as.numeric(x),na.rm=T))
engspeed_min_0 <- engspeed_min_0[sample(1:nrow(engspeed_min_0), 15000, replace=F),]
engspeed_min_0 <- engspeed_min_0[c(1:7000),]
mean_engspeed_0 <- sapply(engspeed_min_0, function(x) mean(as.numeric(x),na.rm=T))
mean_engspeed_0[3:22]
mean_engspeed_1[3:22]
plot.ts(mean_engspeed_0[3:22],ylim=range(500,530))
plot.ts(mean_engspeed_1[3:22],ylim=range(500,530))


data <- cbind(battvol_min, engspeed_min)
data <- data[,-c(24:26)]
data$BattVol_10 <- rowMeans(data[,c(4:13)],na.rm=T)
data$BattVol_20 <- rowMeans(data[,c(14:23)],na.rm=T)
data$EngSpeed_10 <- rowMeans(data[,c(24:33)],na.rm=T)
data$EngSpeed_20 <- rowMeans(data[,c(34:43)],na.rm=T)
write.csv(data,file="20days_battvol_engspeed.csv")


#
batt_data <- read.csv("fleet_data/veh_batt_dt2_clean.csv")
sensor_data <- read.csv("20days_battvol_engspeed.csv")

# take mean of mileage age etc for each vehicle 
dt_group <-ddply(data, ~Vehicle, summarize,BatteryAge=mean(BatteryAge),
                 VehicleAge=mean(VehicleAge),MilPerDay=mean(MilPerDay),
                 BatteryMileage=mean(BatteryMileage))

data2 <- sqldf("SELECT data.Vehicle, data.VehGroup, data.SpecID, 
               data.QUE_FLT_VEHEXT01_State,data.JobDate,
               dt_group.BatteryAge, dt_group.VehicleAge, dt_group.MilPerDay,
               dt_group.BatteryMileage,data.BRR, data.BRRbool FROM data LEFT JOIN dt_group ON 
               (data.Vehicle= dt_group.Vehicle)")

data3 <- data2[unique(factor(data2$Vehicle)),]
write.csv(data3, file="unique_veh_batt.csv")

data4 <- sqldf("SELECT data.Vehicle, data.VehGroup, data.SpecID, data.QUE_FLT_VEHEXT01_State, data.JobDate, data.BatteryAge, 
               data.VehicleAge,data.MilPerDay, data.BatteryMileage, sensor_small.DemandDate, 
               sensor_small.BattVol_10, sensor_small.BattVol_20,sensor_small.EngSpeed_10, sensor_small.EngSpeed_20,data.BRR, data.BRRbool
               FROM data LEFT JOIN sensor_small on (data.Vehicle=sensor_small.VehicleID)")
# filter the ones that jobdate - demanddata < 2 days
data5 <- data4[which(abs(as.numeric(data4$JobDate-data4$DemandDate))<7),]


# 60 days data on battery voltage minimum
data6000 <- read.table("60days/data00.csv",header=T, sep="|")
data00_vol <- data6000[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                           22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                           22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                           22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                           22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                           22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]
rm(data6000)

data6010 <- read.table("60days/data10.csv",header=T, sep="|")
data10_vol <- data6010[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]
rm(data6010)

data6020 <- read.table("60days/data20.csv",header=T, sep="|")
data20_vol <- data6020[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]
rm(data6020)

data6030 <- read.table("60days/data30.csv",header=T, sep="|")
data30_vol <- data6030[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]
rm(data6030)

data6040 <- read.table("60days/data40.csv",header=T, sep="|")
data40_vol <- data6040[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)-1]
rm(data6040)


data6050 <- read.table("60days/data50.csv",header=T, sep="|")
data50_vol <- data6050[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]
rm(data6050)


data6060 <- read.table("60days/data60.csv",header=T, sep="|")
data60_vol <- data6060[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]
rm(data6060)

data6070 <- read.table("60days/data70.csv",header=T, sep="|")
data70_vol <- data6070[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]
rm(data6070)

data6070 <- read.table("60days/data70.csv",header=T, sep="|")
data70_vol <- data6070[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]
rm(data6070)

data6080 <- read.table("60days/data80.csv",header=T, sep="|")
data80_vol <- data6080[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]
rm(data6080)
#_____________
#_____________

data_vol <- rbind(data00_vol, data10_vol, data20_vol, data30_vol, data40_vol,data50_vol, data60_vol, data70_vol)
battvol_1 <- data_vol[which(data_vol$TaskName=="Battery R&R\t"),]
dim(battvol_1)
battvol_0 <- data_vol[which(data_vol$TaskName!="Battery R&R\t"),]
mean_battmin_1 <- sapply(battvol_1, function(x) mean(as.numeric(x),na.rm=T))
battvol_0 <- battvol_0[sample(1:nrow(battvol_0),1600, replace=FALSE),]
mean_battmin_0 <- sapply(battvol_0, function(x) mean(as.numeric(x),na.rm=T))
plot.ts(mean_battmin_0[6:63],ylim=range(12.7,13.7))
plot.ts(mean_battmin_1[6:63],ylim=range(12.7,13.7))


# join dates with battery age and mileage data
library(sqldf)
veh_dates <- read.csv("browse.csv")
veh_batt <- read.csv("fleet_data/batt_odo_battMil.csv")
jobdate <- as.Date(veh_dates$JobDate,"%m/%d/%Y")
detaildate <- as.Date(veh_dates$DetailDate,"%m/%d/%Y")
veh_dates$JobDate <- jobdate
veh_dates$DetailDate <- detaildate

batt_jobdate <- as.Date(veh_batt$JobDate,"%m/%d/%Y")
veh_batt$JobDate <- batt_jobdate

dates_jobdate <- as.Date(veh_dates$JobDate,"%m/%d/%Y")
veh_dates$JobDate <- dates_jobdate
dates_detaildate <- as.Date(veh_dates$DetailDate,"%m/%d/%Y")
veh_dates$DetailDate <- dates_detaildate

batt_join <- sqldf("SELECT veh_batt.Vehicle, veh_batt.VehGroup, veh_batt.SpecID, veh_batt.QUE_FLT_VEHEXT01_State,
                   veh_batt.JobDate, veh_batt.BatteryAge, veh_batt.VehicleAge, veh_batt.MilPerDay, veh_batt.BatteryMileage, 
                   veh_batt.BRR, veh_batt.BRRbool, veh_dates.JobDate, veh_dates.DetailDate FROM veh_batt LEFT JOIN veh_dates
                   ON (veh_batt.Vehicle=veh_dates.Vehicle AND veh_batt.JobDate=veh_dates.JobDate)")

write.csv(batt_join, "veh_batt_data.csv")
batt_join <- read.csv("veh_batt_data.csv")
jobdate <- as.Date(batt_join$JobDate)
batt_join$JobDate <- jobdate
detaildate <- as.Date(batt_join$DetailDate)
batt_join$DetailDate <- detaildate
sensor_data <- read.csv("20days_battvol_engspeed.csv")
sensor_small <- sensor_data[,c(2,3,4,45,46,47,48)]
sensordemanddate <- as.Date(sensor_small$DemandDate, "%m/%d/%Y")
sensor_small$DemandDate <- sensordemanddate
data <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
            batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
            batt_join.BRRbool, sensor_small.BattVol_10, sensor_small.BattVol_20, sensor_small.EngSpeed_10, sensor_small.EngSpeed_20
              FROM batt_join LEFT JOIN sensor_small ON (batt_join.Vehicle=sensor_small.VehicleID AND batt_join.JobDate=sensor_small.DemandDate)")

data_test <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
              batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
              batt_join.BRRbool, sensor_small.BattVol_10, sensor_small.BattVol_20, sensor_small.EngSpeed_10, sensor_small.EngSpeed_20
              FROM batt_join LEFT JOIN sensor_small ON (batt_join.Vehicle=sensor_small.VehicleID)")
write.csv(data, file="batt_am_sensor.csv")
read.csv("fleet_data/batt_am_sensor.csv")
data_comp <- data[complete.cases(data),]
data_comp_1 <- data_comp[which(data_comp$BRR==1),]
data_comp_0 <- data_comp[which(data_comp$BRR==0),]
data_comp_0 <- data_comp_0[sample(1:nrow(data_comp_0),300, replace=F),]
data_comp_test <- rbind(data_comp_1, data_comp_0)
write.csv(data_comp_test,file="batt_am_sensor_comp.csv" )
# 60 days data
data60 <- read.csv("at&t/out.csv",sep="|",header=T, 
                   colClasses=c(rep("character",63)))
data60_1 <- as.data.frame(lapply(data60[,c(4:33)],as.numeric),ncol=30)
data60_2 <- as.data.frame(lapply(data60[,c(34:63)],as.numeric),ncol=30)
class(data60[,c(4:33)])
sensor60_1 <- rowMeans(data60_1,na.rm=T)
sensor60_2 <- rowMeans(data60_2, na.rm=T)
sensor_small <- cbind(data60[,c(1:3)],sensor60_1,sensor60_2)

batt_data <- read.csv("batt_odo_battMil.csv")


#
data <- read.table("60days_131battery//data.csv",sep="|",header=T,colClasses=c(rep("character",1449)))
data_vol <- data[,c(1,7,9,22,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                          22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                          22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                          22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                          22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                          22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)]

data_eng <- data[,c(1,7,9,29,22+24,22+2*24,22+3*24,22+4*24,22+5*24,22+6*24,22+7*24,22+8*24,22+9*24,22+10*24,
                    22+11*24,22+12*24,22+13*24,22+14*24,22+15*24,22+16*24,22+17*24,22+18*24,22+19*24,22+20*24,
                    22+21*24,22+22*24,22+23*24,22+24*24,22+25*24,22+26*24,22+27*24,22+28*24,22+29*24,22+30*24,
                    22+31*24,22+32*24,22+33*24,22+34*24,22+35*24,22+36*24,22+37*24,22+38*24,22+39*24,22+40*24,
                    22+41*24,22+42*24,22+43*24,22+44*24,22+45*24,22+46*24,22+47*24,22+48*24,22+49*24,22+50*24,
                    22+51*24,22+52*24,22+53*24,22+54*24,22+55*24,22+56*24,22+57*24,22+58*24,22+59*24)+4]

data <- read.table("60day_corrected/data00.csv",sep="|",header=T, nrow=100)
batt_min <- read.table("60day_corrected/batt_min.csv",sep="|",header=T,nrow=100)

library(data.table)
data <- read.table("data.csv",header=T,sep="|")

vol_min <- read.table("summary data/batt_min.csv",header=T,sep="|")
engspd_min <- read.table("engspdmin.csv",header=T,sep="|")
class(vol_min_m[,4])
vol_min_m <- t(as.matrix(vol_min[4:63]))
vol_min_m <- as.numeric(vol_min_m,ncol=60)
plot.ts(vol_min_m[1:10])
vol_min_tf <- as.numeric(as.data.frame(vol_min))
as.matrix(as)



#May 21st
vol_min <- read.table("60day_corrected/summary data//battvolmin.csv",sep="|",header=T)
vol_count <- read.table("60day_corrected/summary data//battcount.csv",sep="|",header=T)
vol_low <- read.table("60day_corrected/summary data//batbelowlimit.csv",sep="|",header=T)
# battery min convert to mean for each 10 days
volmin_1 <- as.data.frame(lapply(vol_min[,c(4:18)],as.numeric),ncol=10)
volmin_2 <- as.data.frame(lapply(vol_min[,c(19:33)],as.numeric),ncol=10)
volmin_3 <- as.data.frame(lapply(vol_min[,c(34:48)],as.numeric),ncol=10)
volmin_4 <- as.data.frame(lapply(vol_min[,c(49:63)],as.numeric),ncol=10)

dim(volmin_1[,1])
volminmean_1 <- rowMeans(volmin_1,na.rm=T)
volminmean_2 <- rowMeans(volmin_2,na.rm=T)
volminmean_3 <- rowMeans(volmin_3,na.rm=T)
volminmean_4 <- rowMeans(volmin_4,na.rm=T)

volmindata <- cbind(vol_min[1:3],volminmean_1,volminmean_2,volminmean_3,volminmean_4)
write.csv(volmindata, "volmindata.csv")
volmindata <- read.csv("volmindata.csv")
# compute the low bettery percentage and assign to a factor 
vollowcount <- cbind(vol_low,vol_count)
lowvolper <- matrix(nrow=437646,ncol=60)
for(i in seq(1,437646)){
  fosummary(data$BRR)
r(j in seq(1,60)){
    lowvolper[i,j] <- as.numeric(as.character(vollowcount[i,j+3]))/as.numeric(as.character(vollowcount[i,j+66]))
  }
}
head(vollowcount,4)

write.csv(vollowcount,"vollowcount.csv")
write.csv(lowvolper,"lowvolper.csv")
lowvolper1 <- lowvolper
lowvolper1[as.character(lowvolper1)==NA] <-0
class(lowvolper[1,3])

vollowmean_1 <- rowMeans(lowvolper[,c(1:15)],na.rm=T)
vollowmean_2 <- rowMeans(lowvolper[,c(16:30)],na.rm=T)
vollowmean_3 <- rowMeans(lowvolper[,c(31:45)],na.rm=T)
vollowmean_4 <- rowMeans(lowvolper[,c(46:60)],na.rm=T)
vollowmax_1 <- apply(lowvolper[,c(1:15)],1,max,na.rm=T)
vollowmax_2 <- apply(lowvolper[,c(16:30)],1,max, na.rm=T)
vollowmax_3 <- apply(lowvolper[,c(31:45)],1,max,na.rm=T)
vollowmax_4 <- apply(lowvolper[,c(46:60)],1,max,na.rm=T)
vollowlimit <- cbind(vol_count[1:3],vollowmean_1,vollowmean_2,vollowmean_3,vollowmean_4,vollowmax_1,vollowmax_2,vollowmax_3,vollowmax_4) 
write.csv(vollowlimit,file="vollowlimit.csv")
# engine speed min
esd_min <- read.table("summary data/engspdmin.csv",sep="|",header=T)

# battery min convert to mean for each 10 days
esdmin_1 <- as.data.frame(lapply(esd_min[,c(4:18)],as.numeric),ncol=10)
esdmin_2 <- as.data.frame(lapply(esd_min[,c(19:33)],as.numeric),ncol=10)
esdmin_3 <- as.data.frame(lapply(esd_min[,c(34:48)],as.numeric),ncol=10)
esdmin_4 <- as.data.frame(lapply(esd_min[,c(49:63)],as.numeric),ncol=10)

dim(volmin_1[,1])
esdminmean_1 <- rowMeans(esdmin_1,na.rm=T)
esdminmean_2 <- rowMeans(esdmin_2,na.rm=T)
esdminmean_3 <- rowMeans(esdmin_3,na.rm=T)
esdminmean_4 <- rowMeans(esdmin_4,na.rm=T)

esdmindata <- cbind(esd_min[1:3],esdminmean_1,esdminmean_2,esdminmean_3,esdminmean_4)
write.csv(esdmindata, "esdmindata.csv")

#low speed engine count and factor
esd_count <- read.table("summary data/engspdcount.csv",sep="|",header=T)
esd_low <- read.table("summary data/engspeedbelow.csv",sep="|",header=T)
esd_count2 <- read.table("esdcount.csv",sep="|",header=T)
esd_low2 <- read.table("esdlow.csv",sep="|",header=T)
esdlowcount <- cbind(esd_low2,esd_count2)
lowesdper <- matrix(nrow=437646,ncol=60)
for(i in seq(1,437646)){
  for(j in seq(1,60)){
    lowesdper[i,j] <- as.numeric(as.character(esdlowcount[i,j+3]))/as.numeric(as.character(esdlowcount[i,j+66]))
  }
}
head(esdlowcount,4)

write.csv(esdlowcount,"esdlowcount.csv")
write.csv(lowesdper,"lowesdper.csv")

esdlowmean_1 <- rowMeans(lowesdper[,c(1:15)],na.rm=T)
esdlowmean_2 <- rowMeans(lowesdper[,c(16:30)],na.rm=T)
esdlowmean_3 <- rowMeans(lowesdper[,c(31:45)],na.rm=T)
esdlowmean_4 <- rowMeans(lowesdper[,c(46:60)],na.rm=T)
esdlowmax_1 <- apply(lowesdper[,c(1:15)],1,max,na.rm=T)
esdlowmax_2 <- apply(lowesdper[,c(16:30)],1,max, na.rm=T)
esdlowmax_3 <- apply(lowesdper[,c(31:45)],1,max,na.rm=T)
esdlowmax_4 <- apply(lowesdper[,c(46:60)],1,max,na.rm=T)
esdlowlimit <- cbind(esd_count[1:3],esdlowmean_1,esdlowmean_2,esdlowmean_3,esdlowmean_4,esdlowmax_1,esdlowmax_2,esdlowmax_3,esdlowmax_4) 
write.csv(esdlowlimit,file="esdlowlimit.csv")

# combine volmindata, vollowlimit, esdmindata, esdlowlimit
volmin <- read.csv("60day_corrected/summary data/volmindata.csv")
vollowlimit <- read.csv("60day_corrected/summary data/vollowlimit.csv")
esdmin <- read.csv("60day_corrected/summary data/esdmindata.csv")
esdlowlimit <- read.csv("60day_corrected/summary data/esdlowlimit.csv")
sensor_data_1 <- cbind(volmin[,2:4],volmin[,5],vollowlimit[,c(5,9)],esdmin[,5],esdlowlimit[,c(5,9)]) 
sensor_data_2 <- cbind(volmin[,2:4],volmin[,6],vollowlimit[,c(6,10)],esdmin[,6],esdlowlimit[,c(6,10)]) 
sensor_data_3 <- cbind(volmin[,2:4],volmin[,7],vollowlimit[,c(7,11)],esdmin[,7],esdlowlimit[,c(7,11)])
sensor_data_4 <- cbind(volmin[,2:4],volmin[,8],vollowlimit[,c(8,12)],esdmin[,8],esdlowlimit[,c(8,12)])
colnames1 <- c("Vehicle","DemandDate","TaskName","volmin1","vollowmean1","vollowmax1","esdmin1","esdlowmean1","esdlowmax1")
colnames2 <- c("Vehicle","DemandDate","TaskName","volmin2","vollowmean2","vollowmax2","esdmin2","esdlowmean2","esdlowmax2")
colnames3 <- c("Vehicle","DemandDate","TaskName","volmin3","vollowmean3","vollowmax3","esdmin3","esdlowmean3","esdlowmax3")
colnames4 <- c("Vehicle","DemandDate","TaskName","volmin4","vollowmean4","vollowmax4","esdmin4","esdlowmean4","esdlowmax4")
names(sensor_data_1) <- colnames1
names(sensor_data_2) <- colnames2
names(sensor_data_3) <- colnames3
names(sensor_data_4) <- colnames4

demanddate1 <- as.Date(sensor_data_1$DemandDate,"%m/%d/%Y")
sensor_data_1$DemandDate <- demanddate1
demanddate2 <- as.Date(sensor_data_2$DemandDate,"%m/%d/%Y")
sensor_data_2$DemandDate <- demanddate2
demanddate3 <- as.Date(sensor_data_3$DemandDate,"%m/%d/%Y")
sensor_data_3$DemandDate <- demanddate3
demanddate4 <- as.Date(sensor_data_4$DemandDate,"%m/%d/%Y")
sensor_data_4$DemandDate <- demanddate4
sensor_data <- cbind(sensor_data_1,sensor_data_2,sensor_data_3, sensor_data_4)
sensor_data <- sensor_data[,-c(10,11,12,19,20,21,28,29,30)]
write.csv(sensor_data,file="sensor_data_60days_15bucket.csv")

batt_join <- read.csv("veh_batt_data.csv")
jobdate <- as.Date(batt_join$JobDate)
batt_join$JobDate <- jobdate
detaildate <- as.Date(batt_join$DetailDate)
batt_join$DetailDate <- detaildate
library(sqldf)
data1 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
              batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
              batt_join.BRRbool, sensor_data_1.volmin, sensor_data_1.vollowmean, sensor_data_1.vollowmax, sensor_data_1.esdmin,
              sensor_data_1.esdlowmean, sensor_data_1.esdlowmax
              FROM batt_join LEFT JOIN sensor_data_1 ON (batt_join.Vehicle=sensor_data_1.Vehicle AND batt_join.DetailDate=sensor_data_1.DemandDate)")

data2 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
              batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
              batt_join.BRRbool, sensor_data_2.volmin, sensor_data_2.vollowmean, sensor_data_2.vollowmax, sensor_data_2.esdmin,
               sensor_data_2.esdlowmean, sensor_data_2.esdlowmax
               FROM batt_join LEFT JOIN sensor_data_2 ON (batt_join.Vehicle=sensor_data_2.Vehicle AND batt_join.DetailDate=sensor_data_2.DemandDate)")

data3 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
              batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
               batt_join.BRRbool, sensor_data_3.volmin, sensor_data_3.vollowmean, sensor_data_3.vollowmax, sensor_data_3.esdmin,
               sensor_data_3.esdlowmean, sensor_data_3.esdlowmax
               FROM batt_join LEFT JOIN sensor_data_3 ON (batt_join.Vehicle=sensor_data_3.Vehicle AND batt_join.DetailDate=sensor_data_3.DemandDate)")

data4 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
               batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
               batt_join.BRRbool, sensor_data_4.volmin, sensor_data_4.vollowmean, sensor_data_4.vollowmax, sensor_data_4.esdmin,
               sensor_data_4.esdlowmean, sensor_data_4.esdlowmax
               FROM batt_join LEFT JOIN sensor_data_4 ON (batt_join.Vehicle=sensor_data_4.Vehicle AND batt_join.DetailDate=sensor_data_4.DemandDate)")
data1 <- unique(data1)
data2 <- unique(data2)
data3 <- unique(data3)
data4 <- unique(data4)

data1_comp <- data1[complete.cases(data1),]
data1_comp_1 <- data1_comp[which(data1_comp$BRR==1),]


data2_comp <- data2[complete.cases(data2),]
dim(data2_comp)
data2_comp_1 <- data2_comp[which(data2_comp$BRR==1),]
dim(data2_comp_1)
data2_comp$BRR <- 0
data2_comp$BRRbool <- "N"
data2_comp$BatteryAge <- data2_comp$BatteryAge-15
data2_comp$VehicleAge <- data2_comp$VehicleAge-15
data2_comp$BatteryMileage <- data2_comp$BatteryMileage-15*as.numeric(data2_comp$MilPerDay)

data3_comp <- data3[complete.cases(data3),]
dim(data3_comp)
data3_comp_1 <- data3_comp[which(data3_comp$BRR==1),]
dim(data3_comp_1)
data3_comp$BRR <- 0
data3_comp$BRRbool <- "N"
data3_comp$BatteryAge <- data3_comp$BatteryAge-30
data3_comp$VehicleAge <- data3_comp$VehicleAge-30
data3_comp$BatteryMileage <- data3_comp$BatteryMileage-30*as.numeric(data3_comp$MilPerDay)

data4_comp <- data4[complete.cases(data4),]
dim(data4_comp)
data4_comp_1 <- data4_comp[which(data4_comp$BRR==1),]
dim(data4_comp_1)
data4_comp$BRR <- 0
data4_comp$BRRbool <- "N"
data4_comp$BatteryAge <- data4_comp$BatteryAge-45
data4_comp$VehicleAge <- data4_comp$VehicleAge-45
data4_comp$BatteryMileage <- data4_comp$BatteryMileage-45*as.numeric(data4_comp$MilPerDay)

# generate data w/o removing nas
data2$BRR <- 0
data2$BRRbool <- "N"
data2$BatteryAge <- data2$BatteryAge-15
data2$VehicleAge <- data2$VehicleAge-15
data2$BatteryMileage <- data2$BatteryMileage-15*as.numeric(data2$MilPerDay)

data3$BRR <- 0
data3$BRRbool <- "N"
data3$BatteryAge <- data3$BatteryAge-30
data3$VehicleAge <- data3$VehicleAge-30
data3$BatteryMileage <- data3$BatteryMileage-30*as.numeric(data3$MilPerDay)

data4$BRR <- 0
data4$BRRbool <- "N"
data4$BatteryAge <- data4$BatteryAge-45
data4$VehicleAge <- data4$VehicleAge-45
data4$BatteryMileage <- data4$BatteryMileage-45*as.numeric(data4$MilPerDay)

data_wna <- rbind(data1,data2,data3,data4)
data_wna_shuffle <- data_wna[sample(nrow(data_wna)),]
data_wna_shuffle_train <- data_wna_shuffle[c(1:30000),]
data_wna_shuffle_test <- data_wna_shuffle[c(30000:51000),]
write.csv(data_wna_shuffle,file="data_wna_shuffle.csv")
write.csv(data_wna_shuffle_train, file="data_wna_train.csv")
write.csv(data_wna_shuffle_test, file="data_wna_test.csv")
data_wna_train <- rbind(data1,data2,data3)
data_wna_test <- rbind(data4)
dim(data_wna)
dim(data_wna[which(data_wna$BRR==1),])
write.csv(data_wna, file="15day_chris_wna.csv")
write.csv(data_wna_train, file="15day_chris_wna_train.csv")
write.csv(data_wna_test, file="15day_chris_wna_test.csv")
#complete cases
data <- rbind(data1_comp, data2_comp, data3_comp, data4_comp)
data_brr1 <- data[which(data$BRR==1),]
write.csv(data,file="15day_chris_data.csv")

library(DMwR)
data <- read.csv("15day_chris_data.csv")
over =((0.6*11000)-241)/241
under=(0.4*11000)/(241*over)
over_perc = round(over,1)*100
under_perc = round(under,1)*100
data$BRR <- as.factor(data$BRR)
levels(data$BRR)
smotedata <- SMOTE(BRR~ .,data, perc.over=over_perc,k=5,perc.under=under_perc,learner=NULL)
table(smotedata$BRR)

smotedata_shuffle <- smotedata[sample(nrow(smotedata)),]
write.csv(smotedata_shuffle,"smotedata.csv")
smotedata_train <- smotedata_shuffle[c(1:6000),]
smotedata_test <- smotedata_shuffle[c(6001:10893),]
write.csv(smotedata_train,file="smote_train.csv")
write.csv(smotedata_test,file="smote_test.csv")

data79demanddata <- as.Date(data79$DemandDate,"%m/%d/%Y")
data79$DemandDate<- data79demanddata
batt_join$JobDate <- as.Date(batt_join$JobDate)
batt_join$DetailDate <- as.Date(batt_join$DetailDate)
data79day3 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
              batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
              batt_join.BRRbool,data79.Battery_Voltage_count_3,
              data79.Battery_Voltage_min_3,data79.Battery_Voltage_belowlimit_3,
              data79.Engine_Speed_count_3,data79.Engine_Speed_min_3,
              data79.Engine_Speed_belowlimit_3
              FROM batt_join LEFT JOIN data79 ON 
              (batt_join.Vehicle=data79.VehicleID AND 
              batt_join.DetailDate=data79.DemandDate)")
data79day3_comp <- data79day3[complete.cases(data79day3),]
write.csv(data79day3_comp, file="data79day3.csv")

data79day13 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
                    batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
                    batt_join.BRRbool,data79.Battery_Voltage_count_13,
                    data79.Battery_Voltage_min_13,data79.Battery_Voltage_belowlimit_13,
                    data79.Engine_Speed_count_13,data79.Engine_Speed_min_13,
                    data79.Engine_Speed_belowlimit_13
                    FROM batt_join LEFT JOIN data79 ON 
                    (batt_join.Vehicle=data79.VehicleID AND 
                    batt_join.DetailDate=data79.DemandDate)")
write.csv(data79day13, file="data79day13.csv")

data79day33 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
              batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
              batt_join.BRRbool,data79.Battery_Voltage_count_33,
                    data79.Battery_Voltage_min_33,data79.Battery_Voltage_belowlimit_33,
                    data79.Engine_Speed_count_33,data79.Engine_Speed_min_33,
                    data79.Engine_Speed_belowlimit_33
                    FROM batt_join LEFT JOIN data79 ON 
                    (batt_join.Vehicle=data79.VehicleID AND 
                    batt_join.DetailDate=data79.DemandDate)")
write.csv(data79day33,file="data79day33.csv")

data79day53 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
              batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
              batt_join.BRRbool,data79.Battery_Voltage_count_53,
                    data79.Battery_Voltage_min_53,data79.Battery_Voltage_belowlimit_53,
                    data79.Engine_Speed_count_53,data79.Engine_Speed_min_53,
                    data79.Engine_Speed_belowlimit_53
                    FROM batt_join LEFT JOIN data79 ON 
                    (batt_join.Vehicle=data79.VehicleID AND 
                    batt_join.DetailDate=data79.DemandDate)")
write.csv(data79day53, file="data79day53.csv")

# May 22
#test on a couple vehicles
vol_min <- read.table("summary data//battvolmin.csv",sep="|",header=T)
vol_count <- read.table("summary data//battcount.csv",sep="|",header=T)
vol_low <- read.table("summary data//batbelowlimit.csv",sep="|",header=T)
esd_min <- read.table("summary data/engspdmin.csv",sep="|",header=T)
esd_count <- read.table("esdcount.csv",sep="|",header=T)
esd_low2 <- read.table("esdlow.csv",sep="|",header=T)
lowvolper <- read.table("lowvolper.csv")
lowesdper <- read.table("lowesdper.csv")


volmintest_1 <- as.data.frame(lapply(vol_min[,c(13:27)],as.numeric),ncol=10)
volmintest_2 <- as.data.frame(lapply(vol_min[,c(28:42)],as.numeric),ncol=10)
volmintest_3 <- as.data.frame(lapply(vol_min[,c(43:57)],as.numeric),ncol=10)
volmintest_4 <- as.data.frame(lapply(vol_min[,c(8:22)],as.numeric),ncol=10)


volminmeantest_1 <- rowMeans(volmintest_1,na.rm=T)
volminmeantest_2 <- rowMeans(volmintest_2,na.rm=T)
volminmeantest_3 <- rowMeans(volmintest_3,na.rm=T)
volminmeantest_4 <- rowMeans(volmintest_4,na.rm=T)
volmintest <- cbind(vol_count[1:3],volminmeantest_1,volminmeantest_2,volminmeantest_3,volminmeantest_4)
vollowmeantest_1 <- rowMeans(lowvolper[,c(10:24)],na.rm=T)
vollowmeantest_2 <- rowMeans(lowvolper[,c(25:39)],na.rm=T)
vollowmeantest_3 <- rowMeans(lowvolper[,c(40:54)],na.rm=T)
vollowmeantest_4 <- rowMeans(lowvolper[,c(5:19)],na.rm=T)
vollowmaxtest_1 <- apply(lowvolper[,c(10:24)],1,max,na.rm=T)
vollowmaxtest_2 <- apply(lowvolper[,c(25:39)],1,max, na.rm=T)
vollowmaxtest_3 <- apply(lowvolper[,c(40:54)],1,max,na.rm=T)
vollowmaxtest_4 <- apply(lowvolper[,c(5:19)],1,max,na.rm=T)
vollowlimittest <- cbind(vol_count[1:3],vollowmeantest_1,vollowmeantest_2,vollowmeantest_3,vollowmeantest_4,vollowmaxtest_1,vollowmaxtest_2,vollowmaxtest_3,vollowmaxtest_4)
write.csv(vollowlimittest,file="vollowlimittest.csv")

esdmintest_1 <- as.data.frame(lapply(esd_min[,c(13:27)],as.numeric),ncol=10)
esdmintest_2 <- as.data.frame(lapply(esd_min[,c(28:42)],as.numeric),ncol=10)
esdmintest_3 <- as.data.frame(lapply(esd_min[,c(43:57)],as.numeric),ncol=10)
esdmintest_4 <- as.data.frame(lapply(esd_min[,c(8:22)],as.numeric),ncol=10)
esdminmeantest_1 <- rowMeans(esdmintest_1,na.rm=T)
esdminmeantest_2 <- rowMeans(esdmintest_2,na.rm=T)
esdminmeantest_3 <- rowMeans(esdmintest_3,na.rm=T)
esdminmeantest_4 <- rowMeans(esdmintest_4,na.rm=T)
esdmintest <- cbind(esd_count[1:3],esdminmeantest_1,esdminmeantest_2,esdminmeantest_3,esdminmeantest_4)

esdlowmeantest_1 <- rowMeans(lowesdper[,c(10:24)],na.rm=T)
esdlowmeantest_2 <- rowMeans(lowesdper[,c(25:39)],na.rm=T)
esdlowmeantest_3 <- rowMeans(lowesdper[,c(40:54)],na.rm=T)
esdlowmeantest_4 <- rowMeans(lowesdper[,c(5:19)],na.rm=T)
esdlowmaxtest_1 <- apply(lowesdper[,c(10:24)],1,max,na.rm=T)
esdlowmaxtest_2 <- apply(lowesdper[,c(25:39)],1,max, na.rm=T)
esdlowmaxtest_3 <- apply(lowesdper[,c(40:54)],1,max,na.rm=T)
esdlowmaxtest_4 <- apply(lowesdper[,c(5:19)],1,max,na.rm=T)
esdlowlimittest <- cbind(esd_count[1:3],esdlowmeantest_1,esdlowmeantest_2,esdlowmeantest_3,esdlowmeantest_4,esdlowmaxtest_1,esdlowmaxtest_2,esdlowmaxtest_3,esdlowmaxtest_4) 
write.csv(esdlowlimittest,file="esdlowlimittest.csv")
sensor_datatest_1 <- cbind(volmintest[,1:3],volmintest[,4],vollowlimittest[,c(4,8)],esdmintest[,4],esdlowlimittest[,c(4,8)]) 
sensor_datatest_2 <- cbind(volmintest[,1:3],volmintest[,5],vollowlimittest[,c(5,9)],esdmintest[,5],esdlowlimittest[,c(5,9)]) 
sensor_datatest_3 <- cbind(volmintest[,1:3],volmintest[,6],vollowlimittest[,c(6,10)],esdmintest[,6],esdlowlimittest[,c(6,10)])
sensor_datatest_4 <- cbind(volmintest[,1:3],volmintest[,7],vollowlimittest[,c(7,11)],esdmintest[,7],esdlowlimittest[,c(7,11)])


colnames <- c("Vehicle","DemandDate","TaskName","volmin","vollowmean","vollowmax","esdmin","esdlowmean","esdlowmax")
names(sensor_datatest_1) <- colnames
names(sensor_datatest_2) <- colnames
names(sensor_datatest_3) <- colnames
names(sensor_datatest_4) <- colnames

demanddate1 <- as.Date(sensor_datatest_1$DemandDate,"%m/%d/%Y")
sensor_datatest_1$DemandDate <- demanddate1
demanddate2 <- as.Date(sensor_datatest_2$DemandDate,"%m/%d/%Y")
sensor_datatest_2$DemandDate <- demanddate2
demanddate3 <- as.Date(sensor_datatest_3$DemandDate,"%m/%d/%Y")
sensor_datatest_3$DemandDate <- demanddate3
demanddate4 <- as.Date(sensor_datatest_4$DemandDate,"%m/%d/%Y")
sensor_datatest_4$DemandDate <- demanddate4

batt_join <- read.csv("veh_batt_data.csv")
jobdate <- as.Date(batt_join$JobDate)
batt_join$JobDate <- jobdate
detaildate <- as.Date(batt_join$DetailDate)
batt_join$DetailDate <- detaildate
library(sqldf)
datatest1 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
               batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
               batt_join.BRRbool, sensor_datatest_1.volmin, sensor_datatest_1.vollowmean, sensor_datatest_1.vollowmax, sensor_datatest_1.esdmin,
               sensor_datatest_1.esdlowmean, sensor_datatest_1.esdlowmax
               FROM batt_join LEFT JOIN sensor_datatest_1 ON (batt_join.Vehicle=sensor_datatest_1.Vehicle AND batt_join.DetailDate=sensor_datatest_1.DemandDate)")

datatest2 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
               batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
               batt_join.BRRbool, sensor_datatest_2.volmin, sensor_datatest_2.vollowmean, sensor_datatest_2.vollowmax, sensor_datatest_2.esdmin,
               sensor_datatest_2.esdlowmean, sensor_datatest_2.esdlowmax
               FROM batt_join LEFT JOIN sensor_datatest_2 ON (batt_join.Vehicle=sensor_datatest_2.Vehicle AND batt_join.DetailDate=sensor_datatest_2.DemandDate)")

datatest3 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
               batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
               batt_join.BRRbool, sensor_datatest_3.volmin, sensor_datatest_3.vollowmean, sensor_datatest_3.vollowmax, sensor_datatest_3.esdmin,
               sensor_datatest_3.esdlowmean, sensor_datatest_3.esdlowmax
               FROM batt_join LEFT JOIN sensor_datatest_3 ON (batt_join.Vehicle=sensor_datatest_3.Vehicle AND batt_join.DetailDate=sensor_datatest_3.DemandDate)")

datatest4 <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
               batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
               batt_join.BRRbool, sensor_datatest_4.volmin, sensor_datatest_4.vollowmean, sensor_datatest_4.vollowmax, sensor_datatest_4.esdmin,
               sensor_datatest_4.esdlowmean, sensor_datatest_4.esdlowmax
               FROM batt_join LEFT JOIN sensor_datatest_4 ON (batt_join.Vehicle=sensor_datatest_4.Vehicle AND batt_join.DetailDate=sensor_datatest_4.DemandDate)")
datatest1 <- unique(datatest1)
datatest2 <- unique(datatest2)
datatest3 <- unique(datatest3)
datatest4 <- unique(datatest4)

datatest1_comp <- datatest1[complete.cases(datatest1),]
datatest1_comp_1 <- datatest1_comp[which(datatest1_comp$BRR==1),]
datatest1_comp$BatteryAge <- datatest1_comp$BatteryAge-10
datatest1_comp$VehicleAge <- datatest1_comp$VehicleAge-10
datatest1_comp$BatteryMileage <- datatest1_comp$BatteryMileage-10*as.numeric(datatest1_comp$MilPerDay)

datatest2_comp <- datatest2[complete.cases(datatest2),]
dim(datatest2_comp)
datatest2_comp_1 <- datatest2_comp[which(datatest2_comp$BRR==1),]
dim(datatest2_comp_1)
datatest2_comp$BRR <- 0
datatest2_comp$BRRbool <- "N"
datatest2_comp$BatteryAge <- datatest2_comp$BatteryAge-25
datatest2_comp$VehicleAge <- datatest2_comp$VehicleAge-25
datatest2_comp$BatteryMileage <- datatest2_comp$BatteryMileage-25*as.numeric(datatest2_comp$MilPerDay)

datatest3_comp <- datatest3[complete.cases(datatest3),]
dim(datatest3_comp)
datatest3_comp_1 <- datatest3_comp[which(datatest3_comp$BRR==1),]
dim(datatest3_comp_1)
datatest3_comp$BRR <- 0
datatest3_comp$BRRbool <- "N"
datatest3_comp$BatteryAge <- datatest3_comp$BatteryAge-40
datatest3_comp$VehicleAge <- datatest3_comp$VehicleAge-40
datatest3_comp$BatteryMileage <- datatest3_comp$BatteryMileage-40*as.numeric(datatest3_comp$MilPerDay)

datatest4_comp <- datatest4[complete.cases(datatest4),]
dim(datatest4_comp)
datatest4_comp_1 <- datatest4_comp[which(data4_comp$BRR==1),]
dim(datatest4_comp_1)
datatest4_comp$BatteryAge <- datatest4_comp$BatteryAge-5
datatest4_comp$VehicleAge <- datatest4_comp$VehicleAge-5
datatest4_comp$BatteryMileage <- datatest4_comp$BatteryMileage-5*as.numeric(datatest4_comp$MilPerDay)

data_test <- rbind(datatest1_comp,datatest2_comp,datatest3_comp,datatest4_comp)
data_shuffletest <- data_test[sample(nrow(data_test)),]
table(factor(data_shuffletest$BRR))
data_shuffletest$BRR <- as.factor(data_shuffletest$BRR)
levels(data_shuffletest$BRR)
smotedata <- SMOTE(BRR~ .,data_shuffletest, perc.over=over_perc,k=5,perc.under=under_perc,learner=NULL)
table(smotedata$BRR)
write.csv(data_wna_shuffletest,file="data_shuffletest.csv")

write.csv(data_wna_shuffle_train, file="data_wna_train.csv")
write.csv(data_wna_shuffle_test, file="data_wna_test.csv")
data_wna_train <- rbind(data1,data2,data3)
data_wna_test <- rbind(data4)
dim(data_wna)
dim(data_wna[which(data_wna$BRR==1),])
write.csv(data_wna, file="15day_chris_wna.csv")
write.csv(data_wna_train, file="15day_chris_wna_train.csv")
write.csv(data_wna_test, file="15day_chris_wna_test.csv")
#complete cases
data <- rbind(data1_comp, data2_comp, data3_comp, data4_comp)
data_brr1 <- data[which(data$BRR==1),]
write.csv(data,file="15day_chris_data.csv")



#process sliding windows data
data1 <-read.csv("sliding_windows//output3.csv")

data1$TaskName <- as.factor(data1$TaskName)
data1 <- data1[,c(1:681)]
volmin <- data1[,c(14,14+1*15,14+2*15,14+3*15,14+4*15,14+5*15,14+6*15,14+7*15,14+8*15,14+9*15,14+10*15,
                   14+11*15,14+12*15,14+13*15,14+14*15,14+15*15,14+16*15,14+17*15,14+18*15,14+19*15,14+20*15,
                   14+21*15,14+22*15,14+23*15,14+24*15,14+25*15,14+26*15,14+27*15,14+28*15,14+29*15,14+30*15,
                   14+31*15,14+32*15,14+33*15,14+34*15,14+35*15,14+36*15,14+37*15,14+38*15,14+39*15,14+40*15,
                   14+41*15,14+42*15,14+43*15,14+44*15)]
volbelow<- data1[,c(16,16+1*15,16+2*15,16+3*15,16+4*15,16+5*15,16+6*15,16+7*15,16+8*15,16+9*15,16+10*15,
                   16+11*15,16+12*15,16+13*15,16+14*15,16+15*15,16+16*15,16+17*15,16+18*15,16+19*15,16+20*15,
                   16+21*15,16+22*15,16+23*15,16+24*15,16+25*15,16+26*15,16+27*15,16+28*15,16+29*15,16+30*15,
                   16+31*15,16+32*15,16+33*15,16+34*15,16+35*15,16+36*15,16+37*15,16+38*15,16+39*15,16+40*15,
                   16+41*15,16+42*15,16+43*15,16+44*15)]
volcount <- data1[,c(13,13+1*15,13+2*15,13+3*15,13+4*15,13+5*15,13+6*15,13+7*15,13+8*15,13+9*15,13+10*15,
                     13+11*15,13+12*15,13+13*15,13+14*15,13+15*15,13+16*15,13+17*15,13+18*15,13+19*15,13+20*15,
                     13+21*15,13+22*15,13+23*15,13+24*15,13+25*15,13+26*15,13+27*15,13+28*15,13+29*15,13+30*15,
                     13+31*15,13+32*15,13+33*15,13+34*15,13+35*15,13+36*15,13+37*15,13+38*15,13+39*15,13+40*15,
                     13+41*15,13+42*15,13+43*15,13+44*15)]

esdmin <- data1[,c(19,19+1*15,19+2*15,19+3*15,19+4*15,19+5*15,19+6*15,19+7*15,19+8*15,19+9*15,19+10*15,
                   19+11*15,19+12*15,19+13*15,19+14*15,19+15*15,19+16*15,19+17*15,19+18*15,19+19*15,19+20*15,
                   19+21*15,19+22*15,19+23*15,19+24*15,19+25*15,19+26*15,19+27*15,19+28*15,19+29*15,19+30*15,
                   19+31*15,19+32*15,19+33*15,19+34*15,19+35*15,19+36*15,19+37*15,19+38*15,19+39*15,19+40*15,
                   19+41*15,19+42*15,19+43*15,19+44*15)]
esdbelow<- data1[,c(21,21+1*15,21+2*15,21+3*15,21+4*15,21+5*15,21+6*15,21+7*15,21+8*15,21+9*15,21+10*15,
                    21+11*15,21+12*15,21+13*15,21+14*15,21+15*15,21+16*15,21+17*15,21+18*15,21+19*15,21+20*15,
                    21+21*15,21+22*15,21+23*15,21+24*15,21+25*15,21+26*15,21+27*15,21+28*15,21+29*15,21+30*15,
                    21+31*15,21+32*15,21+33*15,21+34*15,21+35*15,21+36*15,21+37*15,21+38*15,21+39*15,21+40*15,
                    21+41*15,21+42*15,21+43*15,21+44*15)]
esdcount <- data1[,c(18,18+1*15,18+2*15,18+3*15,18+4*15,18+5*15,18+6*15,18+7*15,18+8*15,18+9*15,18+10*15,
                     18+11*15,18+12*15,18+13*15,18+14*15,18+15*15,18+16*15,18+17*15,18+18*15,18+19*15,18+20*15,
                     18+21*15,18+22*15,18+23*15,18+24*15,18+25*15,18+26*15,18+27*15,18+28*15,18+29*15,18+30*15,
                     18+31*15,18+32*15,18+33*15,18+34*15,18+35*15,18+36*15,18+37*15,18+38*15,18+39*15,18+40*15,
                     18+41*15,18+42*15,18+43*15,18+44*15)]
lowvolper <- matrix(nrow=50000,ncol=45)
lowesdper <- matrix(nrow=50000,ncol=45)
for(i in seq(1,50000)){
  for(j in seq(1,45)){
    lowvolper[i,j] <- as.numeric(as.character(volbelow[i,j]))/as.numeric(as.character(volcount[i,j]))
  }
}
for(i in seq(1,50000)){
  for(j in seq(1,45)){
    lowesdper[i,j] <- as.numeric(as.character(esdbelow[i,j]))/as.numeric(as.character(esdcount[i,j]))
  }
}

volmin_1 <- as.data.frame(lapply(volmin[,c(1:15)],as.numeric),ncol=15)
volmin_2 <- as.data.frame(lapply(volmin[,c(16:30)],as.numeric),ncol=15)
volmin_3 <- as.data.frame(lapply(volmin[,c(31:45)],as.numeric),ncol=15)
volminmean_1 <- rowMeans(volmin_1,na.rm=T)
volminmean_2 <- rowMeans(volmin_2,na.rm=T)
volminmean_3 <- rowMeans(volmin_3,na.rm=T)

vollowmean_1 <- rowMeans(lowvolper[,c(1:15)],na.rm=T)
vollowmean_2 <- rowMeans(lowvolper[,c(16:30)],na.rm=T)
vollowmean_3 <- rowMeans(lowvolper[,c(31:45)],na.rm=T)
vollowmax_1 <- apply(lowvolper[,c(1:15)],1,max,na.rm=T)
vollowmax_2 <- apply(lowvolper[,c(16:30)],1,max, na.rm=T)
vollowmax_3 <- apply(lowvolper[,c(31:45)],1,max,na.rm=T)


esdmin_1 <- as.data.frame(lapply(esdmin[,c(1:15)],as.numeric),ncol=15)
esdmin_2 <- as.data.frame(lapply(esdmin[,c(16:30)],as.numeric),ncol=15)
esdmin_3 <- as.data.frame(lapply(esdmin[,c(31:45)],as.numeric),ncol=15)
esdminmean_1 <- rowMeans(esdmin_1,na.rm=T)
esdminmean_2 <- rowMeans(esdmin_2,na.rm=T)
esdminmean_3 <- rowMeans(esdmin_3,na.rm=T)

esdlowmean_1 <- rowMeans(lowesdper[,c(1:15)],na.rm=T)
esdlowmean_2 <- rowMeans(lowesdper[,c(16:30)],na.rm=T)
esdlowmean_3 <- rowMeans(lowesdper[,c(31:45)],na.rm=T)
esdlowmax_1 <- apply(lowesdper[,c(1:15)],1,max,na.rm=T)
esdlowmax_2 <- apply(lowesdper[,c(16:30)],1,max, na.rm=T)
esdlowmax_3 <- apply(lowesdper[,c(31:45)],1,max,na.rm=T)

data_sensor <- cbind(data1[,c(1:6)],volminmean_1,volminmean_2,volminmean_3,
                     vollowmean_1,vollowmean_2,vollowmean_3,
                     vollowmax_1,vollowmax_2,vollowmax_3,
                    esdminmean_1,esdminmean_2,esdminmean_3,
                    esdlowmean_1,esdlowmean_2,esdlowmean_3, 
                    esdlowmax_1,esdlowmax_2,esdlowmax_3)
write.csv(data_sensor, "sensordata3.csv")
sensordemanddata <- as.Date(data_sensor$DemandDate,"%d/%m/%y")
data_sensor$DemandDate <- sensordemanddata 
batt_join <- read.csv("fleet_data/veh_batt_data.csv")
jobdate <- as.Date(batt_join$JobDate)
batt_join$JobDate <- jobdate
detaildate <- as.Date(batt_join$DetailDate)
batt_join$DetailDate <- detaildate
data_join <- sqldf("SELECT batt_join.Vehicle, batt_join.VehGroup, batt_join.SpecID, batt_join.QUE_FLT_VEHEXT01_State, batt_join.JobDate,               
               batt_join.BatteryAge, batt_join.VehicleAge, batt_join.MilPerDay, batt_join.BatteryMileage, batt_join.BRR,
              batt_join.BRRbool, data_sensor.volminmean_1, data_sensor.volminmean_2, data_sensor.volminmean_3, 
              data_sensor.vollowmean_1, data_sensor.vollowmean_2, data_sensor.vollowmean_3, 
              data_sensor.vollowmax_1, data_sensor.vollowmax_2, data_sensor.vollowmax_3,
              data_sensor.esdminmean_1,data_sensor.esdminmean_2,data_sensor.esdminmean_3,
              data_sensor.esdlowmean_1,data_sensor.esdlowmean_2, data_sensor.esdlowmean_3,
              data_sensor.esdlowmax_1,data_sensor.esdlowmax_2, data_sensor.esdlowmax_3
              FROM batt_join LEFT JOIN data_sensor ON 
              (batt_join.Vehicle=data_sensor.Vehicle AND batt_join.DetailDate=data_sensor.DemandDate)")



data_join <- unique(data_join)
write.csv(data_join, file="sliding_windows/data_output1.csv")
data0 <- data[!complete.cases(data_join),]
data1 <- data[complete.cases(data_join),]
write.csv(data1, file="sliding_windows/data_output1_comp.csv")

class(data1$TaskName)
data1$TaskName <- as.factor(data1$TaskName)
write.csv(data, file="sliding_windows/mergeddata1.csv")
smotedata <- SMOTE(TaskName~ .,data1, perc.over=600,k=5,perc.under=100,learner=NULL)

write.csv(smotedata, file="sliding_windows/smotedata1")

# May 22 evening trying to fix the missing BRR=1 data problem
batt_age_mil <- read.csv("fleet_data/battery_age_mileage_processed.csv")
names(batt_age_mil) <- c("Vehicle","JobDate","BatteryAge","BatteryMileage")

two_dates <- read.csv("fleet_data/browse.csv")
two_dates$JobDate <- as.Date(two_dates$JobDate,"%m/%d/%Y")
two_dates$DetailDate <- as.Date(two_dates$DetailDate,"%m/%d/%Y")
batt_age_mil$JobDate <- as.Date(batt_age_mil$JobDate,"%m/%d/%Y")
library(sqldf)
# join batt_age_mil with the broswer data to get Detaildate for each job
batt_age_mil_join <- sqldf("SELECT batt_age_mil.Vehicle, batt_age_mil.JobDate, two_dates.DetailDate,batt_age_mil.BatteryAge,
                            batt_age_mil.BatteryMileage FROM batt_age_mil LEFT JOIN two_dates ON 
                            (batt_age_mil.Vehicle=two_dates.Vehicle AND batt_age_mil.JobDate=two_dates.JobDate)")

batt_age_mil_join <- unique(batt_age_mil_join)
#split batt_age_mil_join into BRR=0 and BRR=1
veh_batt_BRR1 <- batt_age_mil_join[which(batt_age_mil_join$BatteryAge >0),] #43814
veh_batt_BRR0 <- subset(batt_age_mil_join, is.na(BatteryAge))  #53650

names(batt_age_mil_join)
#battery mileage and age join with browser (jobdate and detaildate) data 
write.csv(batt_age_mil_join, file="data_for_trail/batt_age_mil_BRR0and1.csv")
write.csv(veh_batt_BRR1, file="data_for_trail/batt_age_mil_BRR1.csv")
write.csv(veh_batt_BRR0, file="data_for_trail/batt_age_mil_BRR0.csv")

data_mil_cal <- read.table("60day_corrected/summary data/data_odometer_10.csv",sep="|",header=T)  #437646
library(data.table)
data_mil_cal2 <- fread("60day_corrected/summary data/data_odometer_10.csv",sep="|",header=T) #663856
data_mil_cal3 <- fread("60day_corrected/Odomax.csv",sep="|",header=T)
data_mil_cal_brr <- data_mil_cal3[which(data_mil_cal2$TaskName =="Battery R&R\t"),]  #20477
data_mil_cal_brr$DemandDate <- as.Date(data_mil_cal_brr$DemandDate,"%m/%d/%Y")

data_mil_cal3 <- as.data.frame(data_mil_cal3)
data_mil_cal_brr <- data_mil_cal3[which(data_mil_cal2$TaskName =="Battery R&R\t"),] 
data_mil_cal_brr$Odometer_10 <- apply(data_mil_cal_brr[,c(10:19)],1,max,na.rm=T)
data_mil_cal_brr$Odometer_20 <- apply(data_mil_cal_brr[,c(20:29)],1,max,na.rm=T)
data_mil_cal_brr <- data_mil_cal_brr[,c(1,2,3,4,5,6,7,8,9,70,71)]
names(data_mil_cal_brr) <- c("Vehicle","UnitID","VehicleGroup","VehicleMake","State","FuelType","DetailDate","TaskID","TaskName","Odometer_10","Odometer_20")


veh_join_with_batt <- sqldf("SELECT batt_age_mil_join.Vehicle, batt_age_mil_join.JobDate, batt_age_mil_join.DetailDate, 
                                  data_mil_cal_brr.VehicleGroup,data_mil_cal_brr.State,data_mil_cal_brr.FuelType, data_mil_cal_brr.TaskName, 
                                  data_mil_cal_brr.Odometer_min_1, data_mil_cal_brr.Odometer_min_5,data_mil_cal_brr.Odometer_min_10,
                                  batt_age_mil_join.BatteryAge, batt_age_mil_join.BatteryMileage FROM batt_age_mil_join LEFT JOIN data_mil_cal_brr ON
                                  (batt_age_mil_join.Vehicle=data_mil_cal_brr.Vehicle AND batt_age_mil_join.DetailDate=data_mil_cal_brr.DetailDate)")

veh_BRR0_with_batt <- sqldf("SELECT veh_batt_BRR0.Vehicle, veh_batt_BRR0.JobDate, veh_batt_BRR0.DetailDate, data_mil_cal_brr.DemandDate,
                           data_mil_cal_brr.VehicleGroup,data_mil_cal_brr.State,data_mil_cal_brr.FuelType, data_mil_cal_brr.TaskName, 
                           data_mil_cal_brr.Odometer_min_1, data_mil_cal_brr.Odometer_min_5,data_mil_cal_brr.Odometer_min_10,
                            veh_batt_BRR0.BatteryAge, veh_batt_BRR0.BatteryMileage FROM data_mil_cal_brr LEFT JOIN veh_batt_BRR0 ON
                            (veh_batt_BRR0.Vehicle=data_mil_cal_brr.Vehicle)")  #20796

veh_BRR0_with_batt2 <- sqldf("SELECT veh_batt_BRR0.Vehicle, veh_batt_BRR0.JobDate, veh_batt_BRR0.DetailDate,
                           data_mil_cal_brr.VehicleGroup,data_mil_cal_brr.State,data_mil_cal_brr.FuelType, data_mil_cal_brr.TaskName, 
                           data_mil_cal_brr.Odometer_10,data_mil_cal_brr.Odometer_20,
                            veh_batt_BRR0.BatteryAge, veh_batt_BRR0.BatteryMileage FROM veh_batt_BRR0 LEFT JOIN  data_mil_cal_brr ON
                            (veh_batt_BRR0.Vehicle=data_mil_cal_brr.Vehicle AND veh_batt_BRR0.DetailDate=data_mil_cal_brr.DetailDate)")  #53650

data_mil_cal_brr$DetailDate <- as.Date(data_mil_cal_brr$DetailDate,"%m/%d/%Y")
veh_BRR0_with_batt3 <- merge(x=veh_batt_BRR0,y=data_mil_cal_brr, by=c("Vehicle","DetailDate"),x.all=T) #16379
veh_with_batt_normal_BRR0 <- subset(veh_BRR0_with_batt3, (Odometer_10 >0 &Odometer_20 >0))  #5609
write.csv(veh_BRR0_with_batt, file="data_for_trail/veh_BRR0_with_batt.csv")
write.csv(veh_BRR0_with_batt2, file="data_for_trail/veh_BRR0_with_batt2.csv")
write.csv(veh_BRR0_with_batt3, file="data_for_trail/veh_BRR0_with_batt3.csv")



batt_BRR0_data <- read.csv("data_for_trail/veh_BRR0_with_batt3.csv")#16379
batt_BRR0_normal <- subset(batt_BRR0_data, Odometer_5>0 & Odometer_10 >0)
write.csv(veh_with_batt_normal_BRR0,"data_for_trail/batt_BRR0_normal.csv")  #BRR=0 battery and odometer data used to calculate the battery age/mileage
veh_batt_normal_BRR0 <- read.csv("data_for_trail/batt_BRR0_normal.csv")
#join master data with battery data to get vehicle age
master_data <- read.csv("30_Jan_Vehicle_master.csv",header=T, sep=",") #130568

veh_BRR1_data <- merge(x=veh_batt_BRR1, y=master_data, by="Vehicle", x.all=T)
veh_BRR0_data <- merge(x=veh_batt_normal_BRR0, y=master_data, by="Vehicle", x.all=T)
veh_BRR1_data$DateInservice <- as.Date(veh_BRR1_data$DateInservice, "%m/%d/%Y")
veh_BRR0_data$DateInservice <- as.Date(veh_BRR0_data$DateInservice, "%m/%d/%Y")
write.csv(veh_BRR1_data, file="data_for_trail/veh_BRR1_data.csv")
write.csv(veh_BRR0_data, file="data_for_trail/veh_BRR0_data.csv")


# read in date mileage data for abhay
mileage_data <- read.csv("Date_Odometer_for_Abhay.txt",sep=',', header=T)
mileage_data$Vehicle <- as.character(mileage_data$Vehicle)
mileage_data$TransDate <- as.Date(mileage_data$TransDate,"%m/%d/%Y")
mileage_data_sort <- mileage_data[order(mileage_data[,1],mileage_data[,2]),]
library(plyr)
mileage_data_most_recent <- ddply(mileage_data_sort, .(Vehicle),function(x) x[nrow(x),])


batt_age_mil <- read.csv("data_for_trail/batt_age_mil_BRR0and1.csv")
batt_age_mil <- batt_age_mil[,-1]
batt_age_mil$JobDate <- as.Date(batt_age_mil$JobDate)
batt_age_mil$DetailDate <- as.Date(batt_age_mil$DetailDate)
master_data <- read.csv("30_Jan_Vehicle_master.csv",header=T, sep=",") #130568
master_data$Vehicle <- as.character(master_data$Vehicle)
veh_data <- merge(x=batt_age_mil, y=master_data, by="Vehicle",x.all=T)
veh_data$DateInservice <- as.Date(veh_data$DateInservice, "%m/%d/%Y")
veh_data$DateRetired <- as.Date(veh_data$DateRetired, "%m/%d/%Y")

library(sqldf)
data <- merge(x=veh_data, y=mileage_data_sort, by="Vehicle", x.all=T)
write.csv(data,file="data_for_trail/comp_data_BRR0&1.csv")

data_small <- data[which(abs(as.numeric(data$TransDate-data$JobDate) < 15)),]
data_small <- subset(data, abs(as.numeric(data$TransDate-data$JobDate)) < 15)
data_smaller <- subset(data_small, as.numeric(data_small$TransDate-data_small$JobDate) >0)


# May 27th get mileage from bat_demand.csv
veh_batt_data <- read.csv("data_for_trail/batt_age_mil_BRR0and1.csv") #106039
veh_batt_BRR0 <- subset(veh_batt_data, is.na(BatteryAge)) #53650
veh_batt_BRR1 <- subset(veh_batt_data, BatteryAge >0) #43814
mil_data <- read.csv("bat_demand.csv", header=F, ) #179244 
names(mil_data) <- c("Vehicle","JobDate","TaskName","Mileage")

mil_data$JobDate <- as.Date(mil_data$JobDate, "%m/%d/%Y")
veh_batt_data$JobDate <- as.Date(veh_batt_data$JobDate)
veh_batt_mil <- merge(x=veh_batt_data,y=mil_data, by=c("Vehicle","JobDate"),x.all=T) #206178
veh_batt_mil <- unique(veh_batt_mil)  #95709
# merge with master_data to get vehicle information
master_data <- read.csv("30_Jan_Vehicle_master.csv",header=T, sep=",") #130568
veh_batt_mil_master <- merge(x=veh_batt_mil,y=master_data, by="Vehicle",x.all=T)  #95709 #important

veh_batt_mil_master$DateInservice <- as.Date(veh_batt_mil_master$DateInservice, "%m/%d/%Y")
veh_batt_mil_master$DateRetired <- as.Date(veh_batt_mil_master$DateRetired, "%m/%d/%Y")
# join with mileage data to get battery mileage for clean vehicles
mileage_data <- read.csv("Date_Odometer_for_Abhay.txt",sep=',', header=T)
mileage_data$Vehicle <- as.character(mileage_data$Vehicle)
mileage_data$TransDate <- as.Date(mileage_data$TransDate,"%m/%d/%Y")
mileage_data_sort <- mileage_data[order(mileage_data[,1],mileage_data[,2]),]
library(plyr)
mileage_data_most_recent <- ddply(mileage_data_sort, .(Vehicle),function(x) x[nrow(x),])
mileage_data_most_recent <- sqldf("select Vehicle,TransDate,max(Odometer),PrevOdom from mileage_data_sort 
                                  group by Vehicle ") # 82838

data <- merge(x=veh_batt_mil_master, y= mileage_data_most_recent, by="Vehicle", x.all=T) #92938
#remove the retired vehicles
data_BRR0 <- subset(data, is.na(BatteryAge) ) #42035 
data_BRR0_cur <- subset(data_BRR0, is.na(DateRetired)) #37782
data_BRR1 <- subset(data, BatteryAge>0 ) #42871
data_BRR1_cur <- subset(data_BRR1, is.na(DateRetired)) #39985
write.csv(data_BRR0_cur, file="data_for_trail/data_BRR0.csv")
write.csv(data_BRR1_cur, file="data_for_trail/data_BRR1.csv")
#shuffle data BRR=0 and BRR=1
data_BRR0_cur <- read.csv("data_for_trail/data_for_model_wo_sensor/data_BRR0_small.csv") #37782
data_BRR0_cur <- data_BRR0_cur[,-15]
data_BRR0_cur$BatteryAge <- as.numeric(as.character(data_BRR0_cur$BatteryAge))
data_BRR0_cur$MilPerDayBattery <- as.numeric(as.character(data_BRR0_cur$MilPerDayBattery))
data_BRR0_cur$BatteryMileage<- as.numeric(as.character(data_BRR0_cur$BatteryMileage))
data_BRR1_cur <- read.csv("data_for_trail/data_for_model_wo_sensor//data_BRR1_small.csv") #42871
data_BRR1_cur_normal <- subset(data_BRR1_cur, BatteryAge>90) #36795
data_cur <- rbind(data_BRR0_cur,data_BRR1_cur_normal) #74577



data_cur_shuffle <- data_cur[sample(1:nrow(data_cur)),] #74577
length(unique(as.character(data_cur_shuffle)))
data_cur_normal <- subset(data_cur_shuffle, BatteryMileage>0&BatteryAge >0) #72408
write.csv(data_cur_normal, file="data_for_trail/data_for_model_wo_sensor/data.csv")
indexes = sample(1:nrow(data_cur_normal), size=0.4*nrow(data_cur_normal))
test = data_cur_normal[indexes,] #29646
train = data_cur_normal[-indexes,] #44469

write.csv(train, file="data_for_trail/data_for_model_wo_sensor/train_data.csv")
write.csv(test, file="data_for_trail/data_for_model_wo_sensor/test_data.csv")


#merge with 60day_corrected data
library(data.table)
sensor_data_1 <- read.csv("sensor_data_60days_1.csv")
sensor_data_2 <- read.csv("sensor_data_60days_2.csv")
sensor_data <- fread("60day_corrected/summary data/sensor_data_60days_15bucket.csv",sep=",",header=TRUE) #437646
sensor_data_2 <- as.data.frame(sensor_data_2)
sensor_data <- rbind(sensor_data_1, sensor_data_2)
dim(sensor_data)


#join sensor data with battery age mileage vehicle age mileage data
names(sensor_data)[3] <- "DetailDate"
sensor_data$Vehicle <- as.character(sensor_data$Vehicle)
data_cur_normal$Vehicle <- as.character(data_cur_normal$Vehicle)
data_cur_normal$DetailDate <- as.Date(data_cur_normal$DetailDate,"%m/%d/%Y")
sensor_data$DetailDate <- as.Date(sensor_data$DetailDate)

sensor_data_brr <- sensor_data[which(sensor_data$TaskName=="Battery R&R\t"),]  #13401
data_cur_normal_DetailDate <- as.Date(data_cur_normal$DetailDate, "00%y-%m-%d" )
battery_sensor_data <- merge(x=data_cur_normal,y=sensor_data_brr, by=c("Vehicle"),x.all=T)  #24394
length(unique(battery_sensor_data$Vehicle)) #10046
battery_sensor_brr1 <- battery_sensor_data[which(battery_sensor_data$BRR==1),] #16411
battery_sensor_brr0 <- battery_sensor_data[which(battery_sensor_data$BRR==0),] #7983
write.csv(battery_sensor_data, file="data_for_trail/data_for_model_wo_sensor/Battery_sensor_data.csv")
write.csv(battery_sensor_brr0, file="data_for_trail/data_for_model_wo_sensor/Battery_sensor_brr0.csv")
write.csv(battery_sensor_brr1, file="data_for_trail/data_for_model_wo_sensor/Battery_sensor_brr1.csv")
indexes = sample(1:nrow(battery_sensor_data), size=0.4*nrow(battery_sensor_data))
battery_sensor_test = battery_sensor_data[indexes,]
battery_sensor_train = battery_sensor_data[-indexes,]

write.csv(battery_sensor_train, file="data_for_trail/data_for_model_wo_sensor/Battery_sensor_train.csv")
write.csv(battery_sensor_test, file="data_for_trail/data_for_model_wo_sensor/Battery_sensor_test.csv")

battery_sensor_comp <- battery_sensor_data[complete.cases(battery_sensor_data),]
dim(battery_sensor_comp) #11838
write.csv(battery_sensor_comp, file="data_for_trail/data_for_model_wo_sensor/Battery_sensor_comp.csv")



# May 28th :re-read in engine speed and battery voltage data using fread
#May 21st
library(data.table)
vol_min <- fread("60day_corrected/summary data//battvolmin.csv",sep="|",header=T)
vol_count <- fread("60day_corrected/summary data//battcount.csv",sep="|",header=T)
vol_low <- fread("60day_corrected/summary data//batbelowlimit.csv",sep="|",header=T)
# battery min convert to mean for each 15 days
vol_min <- as.data.frame(vol_min)
volmin_1 <- as.data.frame(lapply(vol_min[,c(4:18)],as.numeric),ncol=15)
volmin_2 <- as.data.frame(lapply(vol_min[,c(19:33)],as.numeric),ncol=15)
volmin_3 <- as.data.frame(lapply(vol_min[,c(34:48)],as.numeric),ncol=15)
volmin_4 <- as.data.frame(lapply(vol_min[,c(49:63)],as.numeric),ncol=15)

length(volmin_1[,1])
volminmean_1 <- rowMeans(volmin_1,na.rm=T)
volminmean_2 <- rowMeans(volmin_2,na.rm=T)
volminmean_3 <- rowMeans(volmin_3,na.rm=T)
volminmean_4 <- rowMeans(volmin_4,na.rm=T)

volmindata <- cbind(vol_min[,c(1:3)],volminmean_1,volminmean_2,volminmean_3,volminmean_4)
write.csv(volmindata, "volmindata.csv")
vol_low <- as.data.frame(vol_low)
vol_count <- as.data.frame(vol_count)
lowvolper <- matrix(nrow=663856,ncol=60)
for(i in seq(1,663856)){
  for(j in seq(1,60)){
    lowvolper[i,j] <- as.numeric(as.character(vol_low[i,j+3]))/as.numeric(as.character(vol_count[i,j+3]))
  }
}

write.csv(lowvolper, file="60day_corrected/summary data/lowvolper.csv")
lowvolper <- read.csv("60day_corrected/summary data/lowvolper.csv")
lowvolper1[as.character(lowvolper1)==NA] <-0
class(lowvolper[1,3])
vol_low <- as.data.frame(vol_low)
lowvolper_1 <- as.data.frame(lapply(lowvolper[,c(1:15)],as.numeric),ncol=15)
lowvolper_2 <- as.data.frame(lapply(lowvolper[,c(16:30)],as.numeric),ncol=15)
lowvolper_3 <- as.data.frame(lapply(lowvolper[,c(31:45)],as.numeric),ncol=15)
lowvolper_4 <- as.data.frame(lapply(lowvolper[,c(46:60)],as.numeric),ncol=15)
vollowmean_1 <- rowMeans(lowvolper[,c(1:15)],na.rm=T)
vollowmean_2 <- rowMeans(lowvolper[,c(16:30)],na.rm=T)
vollowmean_3 <- rowMeans(lowvolper[,c(31:45)],na.rm=T)
vollowmean_4 <- rowMeans(lowvolper[,c(46:60)],na.rm=T)
vollowmax_1 <- apply(lowvolper[,c(1:15)],1,function(x)max(!is.na(lowvolper[,c(1:15)])))
vollowmax_2 <- apply(lowvolper[,c(16:30)],1,function(x)max(!is.na(lowvolper[,c(16:30)])))
vollowmax_3 <- apply(lowvolper[,c(31:45)],1,function(x)max(!is.na(lowvolper[,c(31:45)])))
vollowmax_4 <- apply(lowvolper[,c(46:60)],1,function(x)max(!is.na(lowvolper[,c(46:60)])))
vollowlimit <- cbind(vol_count[,c(1:3)],vollowmean_1,vollowmean_2,vollowmean_3,vollowmean_4) 
write.csv(vollowlimit,file="vollowlimit.csv")
# engine speed min
bigdata_test <- read.csv("60day_corrected/bigdata.csv",sep="|",header=T,nrow=10)
esd_min <- fread("60day_corrected/summary data/engspdmin.csv",sep="|",header=T)
esd_count <- fread("60day_corrected/summary data/esdcount.csv",sep="|",header=T)
esd_low <- fread("60day_corrected/summary data/esdlow.csv",sep="|",header=T)
# battery min convert to mean for each 10 days
esd_min <- as.data.frame(esd_min)
esdmin_1 <- as.data.frame(lapply(esd_min[,c(4:18)],as.numeric),ncol=15)
esdmin_2 <- as.data.frame(lapply(esd_min[,c(19:33)],as.numeric),ncol=15)
esdmin_3 <- as.data.frame(lapply(esd_min[,c(34:48)],as.numeric),ncol=15)
esdmin_4 <- as.data.frame(lapply(esd_min[,c(49:63)],as.numeric),ncol=15)

dim(volmin_1[,1])
esdminmean_1 <- rowMeans(esdmin_1,na.rm=T)
esdminmean_2 <- rowMeans(esdmin_2,na.rm=T)
esdminmean_3 <- rowMeans(esdmin_3,na.rm=T)
esdminmean_4 <- rowMeans(esdmin_4,na.rm=T)

esdmindata <- cbind(esd_min[,c(1:3)],esdminmean_1,esdminmean_2,esdminmean_3,esdminmean_4)
write.csv(esdmindata, "esdmindata.csv")

#low speed engine count and factor

esd_low <- as.data.frame(esd_low)
esd_count <- as.data.frame(esd_count)

lowesdper <- matrix(nrow=663856,ncol=60)
for(i in seq(1,663856)){
  for(j in seq(1,60)){
    lowesdper[i,j] <- as.numeric(as.character(esd_low[i,j+3]))/as.numeric(as.character(esd_count[i,j+3]))
  }
}

write.csv(lowvolper, file="60day_corrected/summary data/lowvolper.csv")
lowesdper <- read.csv("60day_corrected/summary data/lowvolper.csv")
esdlow_1 <- as.data.frame(lapply(esd_low[,c(4:18)],as.numeric),ncol=15)
esdlow_2 <- as.data.frame(lapply(esd_low[,c(19:33)],as.numeric),ncol=15)
esdlow_3 <- as.data.frame(lapply(esd_low[,c(34:48)],as.numeric),ncol=15)
esdlow_4 <- as.data.frame(lapply(esd_low[,c(49:63)],as.numeric),ncol=15)
esdlowmean_1 <- rowMeans(lowesdper[,c(1:15)],na.rm=T)
esdlowmean_2 <- rowMeans(lowesdper[,c(16:30)],na.rm=T)
esdlowmean_3 <- rowMeans(lowesdper[,c(31:45)],na.rm=T)
esdlowmean_4 <- rowMeans(lowesdper[,c(46:60)],na.rm=T)
esdlowmax_1 <- apply(lowesdper[,c(1:15)],1,function(x)max(!is.na(lowesdper[,c(1:15)])))
esdlowmax_2 <- apply(lowesdper[,c(16:30)],1,function(x)max(!is.na(lowesdper[,c(16:30)])))
esdlowmax_2 <- apply(lowesdper[,c(16:30)],1,function(x)max(!is.na(lowesdper[,c(16:30)])))
esdlowmax_3 <- apply(lowesdper[,c(31:45)],1,max,na.rm=T)
esdlowmax_4 <- apply(lowesdper[,c(46:60)],1,max,na.rm=T)
esdlowlimit <- cbind(esd_min[,c(1:3)],esdlowmean_1,esdlowmean_2,esdlowmean_3,esdlowmean_4) 
write.csv(esdlowlimit,file="esdlowlimit.csv")
esdlowlimit<- read.csv(file="esdlowlimit.csv")
# combine volmindata, vollowlimit, esdmindata, esdlowlimit
volmin <- fread("60day_corrected/summary data/volmindata.csv")
vollowlimit <- read.csv("60day_corrected/summary data/vollowlimit.csv")
esdmin <- read.csv("60day_corrected/summary data/esdmindata.csv")
esdlowlimit <- read.csv("60day_corrected/summary data/esdlowlimit.csv")
vollowlimit <- vollowlimit[,-1]
sensor_data_1 <- cbind(volmindata[,c(1:3)],volmindata[,4],vollowlimit[,4],esdmindata[,4],esdlowlimit[,4]) 
sensor_data_2 <- cbind(volmindata[,c(1:3)],volmindata[,5],vollowlimit[,5],esdmindata[,5],esdlowlimit[,5]) 
sensor_data_3 <- cbind(volmindata[,c(1:3)],volmindata[,6],vollowlimit[,6],esdmindata[,6],esdlowlimit[,6])
sensor_data_4 <- cbind(volmindata[,c(1:3)],volmindata[,7],vollowlimit[,7],esdmindata[,7],esdlowlimit[,7])
colnames1 <- c("Vehicle","DemandDate","TaskName","volmin1","vollowmean1","esdmin1","esdlowmean1")
colnames2 <- c("Vehicle","DemandDate","TaskName","volmin2","vollowmean2","esdmin2","esdlowmean2")
colnames3 <- c("Vehicle","DemandDate","TaskName","volmin3","vollowmean3","esdmin3","esdlowmean3")
colnames4 <- c("Vehicle","DemandDate","TaskName","volmin4","vollowmean4","esdmin4","esdlowmean4")
names(sensor_data_1) <- colnames1
names(sensor_data_2) <- colnames2
names(sensor_data_3) <- colnames3
names(sensor_data_4) <- colnames4

demanddate1 <- as.Date(sensor_data_1$DemandDate,"%m/%d/%Y")
sensor_data_1$DemandDate <- demanddate1
demanddate2 <- as.Date(sensor_data_2$DemandDate,"%m/%d/%Y")
sensor_data_2$DemandDate <- demanddate2
demanddate3 <- as.Date(sensor_data_3$DemandDate,"%m/%d/%Y")
sensor_data_3$DemandDate <- demanddate3
demanddate4 <- as.Date(sensor_data_4$DemandDate,"%m/%d/%Y")
sensor_data_4$DemandDate <- demanddate4
sensor_data <- cbind(sensor_data_1,sensor_data_2,sensor_data_3, sensor_data_4) #663856
sensor_data <- sensor_data[,-c(8,9,10,15,16,17,22,23,24)]
write.csv(sensor_data,file="sensor_data_60days_15bucket.csv")
sensor_data <- read.csv("sensor_data_60days_15bucket.csv")
# use odometer data to calculate battery age/mileage 
clean_vehicle <- fread("data_for_trail/cleanvehicles.csv",sep="|",header=T)
clean_vehicle <- as.data.frame(clean_vehicle)  #14632
clean_veh <- clean_vehicle[,c(c(1:9),16,193)]
write.csv(clean_veh, "data_for_trail/clean_veh.csv")
clean_veh <- read.csv("data_for_trail/clean_veh.csv")
data_BRR0 <- read.csv("data_for_trail/data_for_model/data_BRR0_small.csv",sep=",",header=T) #37782
names(clean_veh) <- c("Vehicle","VehicleGroup","State","FuleType","MostRecentDate",
"TaskID","TaskName","Odometer_max_5","Odometer_max_10")

clean_veh_data <- merge(x=clean_veh, y=data_BRR0, by="Vehicle", x.all=T,y.all=T)
clean_veh_data <- sqldf("select clean_veh.Vehicle, clean_veh.VehicleGroup, clean_veh.State,
                              clean_veh.FuleType, clean_veh.MostRecentDate, clean_veh.TaskID,
                              clean_veh.TaskName, clean_veh.Odometer_max_5, clean_veh.Odometer_max_10,
                              data_BRR0.JobDate, data_BRR0.DetailDate, data_BRR0.TaskName, data_BRR0.JobDayMileage,
                              data_BRR0.SpecID, data_BRR0.DateInservice, data_BRR0.TransDate, data_BRR0.VehicleMileage,
                              data_BRR0.VehicleAge, data_BRR0.BatteryMileage, data_BRR0.BatteryAge, data_BRR0.MilPerDayBattery,
                              data_BRR0.MilPerDayVehicle, data_BRR0.BRR from clean_veh left join data_BRR0
                             on clean_veh.Vehicle=data_BRR0.Vehicle")  #14713


write.csv(clean_veh_data, file="data_for_trail/data_for_model/data_BRR0_odo.csv")
clean_veh_data <- read.csv("data_for_trail/data_for_model/data_BRR0_odo.csv")
veh_BRR0_data <- read.csv("data_for_trail/data_for_model/data_BRR0_odo.csv",sep=",",header=T) #14713
veh_BRR1_data <- read.csv("data_for_trail/data_for_model/data_BRR1_small.csv")   #42871
veh_BRR0_data_1 <- veh_BRR0_data[,c(1,10,11,15,2,17,4,3,18,8,9,13,12,14,26)] 
veh_BRR1_data_1 <- veh_BRR1_data[,c(1,2,3,4,6,7,8,9,11,13,14,15,16,17,19)]

names <- c("Vehicle","JobDate","DetailDate","TaskName","VehicleGroup","SpecID","FuleType","State",
           "DateInservice","VehicleMileage","VehicleAge","BatteryMileage","BatteryAge","MilPerDay","BRR")

names(veh_BRR0_data_1) <- names
names(veh_BRR1_data_1) <- names

veh_BRR0_data_1$BatteryMileage <- as.numeric(as.character(veh_BRR0_data_1$BatteryMileage))
veh_BRR0_data_1$BatteryAge <- as.numeric(as.character(veh_BRR0_data_1$BatteryAge))
veh_BRR0_data_1$MilPerDay <- as.numeric(as.character(veh_BRR0_data_1$MilPerDay))
veh_BRR1_data_1$BatteryMileage <- as.numeric(as.character(veh_BRR1_data_1$BatteryMileage))
veh_BRR1_data_1$BatteryAge <- as.numeric(as.character(veh_BRR1_data_1$BatteryAge))
veh_BRR1_data_1$MilPerDay <- as.numeric(as.character(veh_BRR1_data_1$MilPerDay))


veh_data <- rbind(veh_BRR0_data_1,veh_BRR1_data_1) #57584

veh_data_comp <- veh_data[complete.cases(veh_data),] #46072  #important

veh_data_comp$Vehicle <- as.character(veh_data_comp$Vehicle)
veh_data_comp$DetailDate <- as.Date(veh_data_comp$DetailDate,"%m/%d/%y")
veh_data_comp$JobDate <- as.Date(veh_data_comp$JobDate,"%m/%d/%y")

dim(sensor_data)  #663856 #important
names(sensor_data)[2] <- "DetailDate"
names(sensor_data_1)[2] <- "DetailDate"
names(sensor_data_2)[2] <- "DetailDate"
names(sensor_data_3)[2] <- "DetailDate"
names(sensor_data_4)[2] <- "DetailDate"
sensor_data$DetailDate <- as.Date(sensor_data$DetailDate)
sensor_data_1$DetailDate <- as.Date(sensor_data_1$DetailDate)
sensor_data_2$DetailDate <- as.Date(sensor_data_2$DetailDate)
sensor_data_3$DetailDate <- as.Date(sensor_data_3$DetailDate)
sensor_data_4$DetailDate <- as.Date(sensor_data_4$DetailDate)
data_1 <- merge(x=sensor_data_1, y=veh_data_comp, by=c("Vehicle","DetailDate"),y.all=T)

#data for 0 days ahead of the battery failure
data1 <- sqldf("select veh_data_comp.Vehicle, veh_data_comp.JobDate, veh_data_comp.DetailDate, veh_data_comp.TaskName,
               veh_data_comp.VehicleGroup, veh_data_comp.SpecID, veh_data_comp.FuleType, veh_data_comp.State,
               veh_data_comp.DateInservice,veh_data_comp.VehicleMileage,veh_data_comp.VehicleAge, 
               veh_data_comp.BatteryMileage, veh_data_comp.BatteryAge, veh_data_comp.MilPerDay,
               sensor_data_1.volmin1,sensor_data_1.vollowmean1,
               sensor_data_1.esdmin1, sensor_data_1.esdlowmean1, veh_data_comp.BRR
               from veh_data_comp left join sensor_data_1 
               on (veh_data_comp.Vehicle=sensor_data_1.Vehicle and veh_data_comp.DetailDate=sensor_data_1.DetailDate)")

data1 <- unique(data1)  #46072
write.csv(data1, file="data_for_trail/data_for_model/data_00days_batt&sensor.csv")
data1_comp <- data1[complete.cases(data1),]
write.csv(data1_comp, file="data_for_trail/data_for_model/data_00days_batt&sensorc_comp.csv")
# data for 15 days ahead of the battery failure
veh_data_comp2 <- veh_data_comp
veh_data_comp2$VehicleAge <- as.numeric(as.character(veh_data_comp2$VehicleAge))-15
veh_data_comp2$VehicleMileage <- as.numeric(as.character(veh_data_comp2$VehicleMileage))-15*as.numeric(as.character(veh_data_comp2$MilPerDay))

veh_data_comp2$BatteryAge <-  as.numeric(as.character(veh_data_comp2$BatteryAge))-15
veh_data_comp2$BatteryMileage <- as.numeric(as.character(veh_data_comp2$BatteryMileage))-15*as.numeric(as.character(veh_data_comp2$MilPerDay))
veh_data_comp2$BRR <- 0
data2 <- sqldf("select veh_data_comp2.Vehicle, veh_data_comp2.JobDate, veh_data_comp2.DetailDate, veh_data_comp2.TaskName,
               veh_data_comp2.VehicleGroup, veh_data_comp2.SpecID, veh_data_comp2.FuleType, veh_data_comp2.State,
               veh_data_comp2.DateInservice,veh_data_comp2.VehicleMileage,veh_data_comp2.VehicleAge, 
               veh_data_comp2.BatteryMileage, veh_data_comp2.BatteryAge, veh_data_comp2.MilPerDay,
               sensor_data_2.volmin2,sensor_data_2.vollowmean2,sensor_data_2.vollowmax2,
               sensor_data_2.esdmin2, sensor_data_2.esdlowmean2, sensor_data_2.esdlowmax2, veh_data_comp2.BRR
               from veh_data_comp2 left join sensor_data_2 
               on (veh_data_comp2.Vehicle=sensor_data_2.Vehicle and veh_data_comp2.DetailDate=sensor_data_2.DetailDate)")


data2 <- unique(data2)  #46074
write.csv(data2, file="data_for_trail/data_for_model/data_15days_batt&sensor.csv")

#data for 30 days ahead of the battery failure

veh_data_comp3 <- veh_data_comp
veh_data_comp3$VehicleAge <- as.numeric(as.character(veh_data_comp3$VehicleAge))-30
veh_data_comp3$VehicleMileage <- as.numeric(as.character(veh_data_comp3$VehicleMileage))-30*as.numeric(as.character(veh_data_comp3$MilPerDay))

veh_data_comp3$BatteryAge <-  as.numeric(as.character(veh_data_comp3$BatteryAge))-30
veh_data_comp3$BatteryMileage <- as.numeric(as.character(veh_data_comp3$BatteryMileage))-30*as.numeric(as.character(veh_data_comp3$MilPerDay))
veh_data_comp3$BRR <- 0
data3 <- sqldf("select veh_data_comp3.Vehicle, veh_data_comp3.JobDate, veh_data_comp3.DetailDate, veh_data_comp3.TaskName,
               veh_data_comp3.VehicleGroup, veh_data_comp3.SpecID, veh_data_comp3.FuleType, veh_data_comp3.State,
               veh_data_comp3.DateInservice,veh_data_comp3.VehicleMileage,veh_data_comp3.VehicleAge, 
               veh_data_comp3.BatteryMileage, veh_data_comp3.BatteryAge, veh_data_comp3.MilPerDay,
               sensor_data_3.volmin3,sensor_data_3.vollowmean3,sensor_data_3.vollowmax3,
               sensor_data_3.esdmin3, sensor_data_3.esdlowmean3, sensor_data_3.esdlowmax3, veh_data_comp3.BRR
               from veh_data_comp3 left join sensor_data_3
               on (veh_data_comp3.Vehicle=sensor_data_3.Vehicle and veh_data_comp3.DetailDate=sensor_data_3.DetailDate)")


data3 <- unique(data3)  #46074
write.csv(data3, file="data_for_trail/data_for_model/data_30days_batt&sensor.csv")


#data for 45 days ahead of the battery failure

veh_data_comp4 <- veh_data_comp
veh_data_comp4$VehicleAge <- as.numeric(as.character(veh_data_comp4$VehicleAge))-45
veh_data_comp4$VehicleMileage <- as.numeric(as.character(veh_data_comp4$VehicleMileage))-45*as.numeric(as.character(veh_data_comp4$MilPerDay))

veh_data_comp4$BatteryAge <-  as.numeric(as.character(veh_data_comp4$BatteryAge))-45
veh_data_comp4$BatteryMileage <- as.numeric(as.character(veh_data_comp4$BatteryMileage))-45*as.numeric(as.character(veh_data_comp4$MilPerDay))
veh_data_comp4$BRR <- 0
data4 <- sqldf("select veh_data_comp4.Vehicle, veh_data_comp4.JobDate, veh_data_comp4.DetailDate, veh_data_comp4.TaskName,
               veh_data_comp4.VehicleGroup, veh_data_comp4.SpecID, veh_data_comp4.FuleType, veh_data_comp4.State,
               veh_data_comp4.DateInservice,veh_data_comp4.VehicleMileage,veh_data_comp4.VehicleAge, 
               veh_data_comp4.BatteryMileage, veh_data_comp4.BatteryAge, veh_data_comp4.MilPerDay,
               sensor_data_4.volmin4,sensor_data_4.vollowmean4,sensor_data_4.vollowmax4,
               sensor_data_4.esdmin4, sensor_data_4.esdlowmean4, sensor_data_4.esdlowmax4, veh_data_comp4.BRR
               from veh_data_comp4 left join sensor_data_4
               on (veh_data_comp4.Vehicle=sensor_data_4.Vehicle and veh_data_comp4.DetailDate=sensor_data_4.DetailDate)")


data4 <- unique(data4)  #46071
write.csv(data4, file="data_for_trail/data_for_model/data_45days_batt&sensor.csv")

datanames <- c("Vehicle","JobDate","DetailDate","TaskName","VehicleGroup","SpecID","FuleType","State","DateInservice",
               "VehicleMileage","VehicleAge","BatteryMileage","BatteryAge","MilPerDay","volmin","vollowmean", 
              "vollowmax","esdmin","esdlowmean","esdlowmax","BRR")

names(data1) <- datanames
names(data2) <- datanames
names(data3) <- datanames
names(data4) <- datanames


sapply(data1, class)
data1$VehicleMileage <- as.numeric(as.character(data1$VehicleMileage))
data1$VehicleAge <- as.numeric(as.character(data1$VehicleAge))
data1$BRR <- as.numeric(as.character(data1$BRR))
sapply(data4, class)

batt_veh_sensor_data <- rbind(data1,data2,data3,data4)  #184291
bvsd_BRR1 <- subset(batt_veh_sensor_data, BRR==1)  #42841
bvsd_BRR0 <- subset(batt_veh_sensor_data, BRR==0)  #141450

bvsd_comp <- batt_veh_sensor_data[complete.cases(batt_veh_sensor_data),]  #16968

bvsd_comp_BRR1<- subset(bvsd_comp,BRR==1)  #2649
bvsd_comp_BRR0 <-subset(bvsd_comp,BRR==0) #14319
write.csv(bvsd_comp, file="data_for_trail/data_for_model/bvsd_comp.csv")
write.csv(bvsd_comp_BRR1, file="data_for_trail/data_for_model/bvsd_comp_BRR1.csv")
write.csv(bvsd_comp_BRR0, file="data_for_trail/data_for_model/bvsd_comp_BRR0.csv")

indexes = sample(1:nrow(bvsd_comp_BRR0), size=0.2*nrow(bvsd_comp_BRR0))
bvsd_comp_BRR0_train = bvsd_comp_BRR0[indexes,]
bvsd_comp_BRR0_test = bvsd_comp_BRR0[-indexes,]
bvsd_train <- rbind(bvsd_comp_BRR0_train, bvsd_comp_BRR1)
bvsd_test <- bvsd_comp_BRR0_test
write.csv(bvsd_train,file="data_for_trail/data_for_model/bvsd_train_random.csv")
write.csv(bvsd_test,file="data_for_trail/data_for_model/bvsd_test_random.csv")

# 0-30 as training 31-60 as testing
bvsd_train_1 <- rbind(data1, data2)
bvsd_train_1 <- read.csv("data_for_trail/data_for_model/bvsd_train_data1&2.csv")
bvsd_test_1 <- rbind(data3, data4)
bvsd_test_1 <- read.csv("data_for_trail/data_for_model/bvsd_test_data3&4.csv")
data4 <- read.csv("data_for_trail/data_for_model/data_45days_batt&sensor.csv")
data4 <- data4[complete.cases(data4),]
bvsd_train_comp <- bvsd_train_1[complete.cases(bvsd_train_1),] #9137
bvsd_test_comp <- bvsd_test_1[complete.cases(bvsd_test_1),]#7601
write.csv(bvsd_train_comp,file="data_for_trail/data_for_model/bvsd_train_data1&2.csv")
write.csv(bvsd_test_comp,file="data_for_trail/data_for_model/bvsd_test_data3&4.csv")
write.csv(bvsd_test_comp,file="data_for_trail/data_for_model/bvsd_test_data3&4.csv")
write.csv(data4, file="data_for_trail/data_for_model/data_45days_comp.csv")
#Smote to oversample
library(DMwR)
BRR1 <- 2649
BRR0 <- 14089
over =((0.6*BRR0)-BRR1)/BRR1
under=(0.4*BRR0)/(BRR1*over)
over_perc = round(over,1)*100
under_perc = round(under,1)*100
bvsd_comp$BRR <- as.factor(bvsd_comp$BRR)
bvsd_comp_smote <- SMOTE(bvsd_comp$BRR~ .,bvsd_comp, perc.over=500,k=5,perc.under=100,learner=NULL)

#problem
bvsd_comp <- read.csv("data_for_trail/data_for_model/bvsd_comp.csv")
bvsd_comp_BRR1 <- read.csv("data_for_trail/data_for_model/bvsd_comp_BRR1.csv")



# make the list for trail
sensor <- read.table("data_for_trail/data_for_prediction/15day.csv",sep="|",header=T)
sensor_small <- sensor[,c(1,2,3,4,5,7,8,13,14,19,20,24,26,27,31)]
names(sensor_small) <- c("Vehicle","VehicleGroup","SpecID","State","FuleType","UnitId","Date","Odometer_min",
                         "Odometer_max","Battery_Voltage_count","Battery_Voltage_min","Battery_Voltage_Belowlimit",
                         "Engine_Speed_count","Engine_Speed_min","Engine_Speed_belowlimit")
lowvolper <- data.frame(nrow=2968)
for(i in seq(1,2968)){
    lowvolper[i,1] <- as.numeric(as.character(sensor_small$Battery_Voltage_Belowlimit[i]))/as.numeric(as.character(sensor_small$Battery_Voltage_count[i]))
  }

lowesdper <- data.frame(nrow=2968)
for(i in seq(1,2968)){
  lowesdper[i,1] <- as.numeric(as.character(sensor_small$Engine_Speed_belowlimit[i]))/as.numeric(as.character(sensor_small$Engine_Speed_count[i]))
}
sensor_data <- cbind(sensor_small[,c(c(1:9),11,14)], lowvolper,lowesdper)  #2968


names(sensor_data) <- c("Vehicle","VehicleGroup","SpecID","State","FuleType","UnitId","Date",
                        "Odometer_min","Odometer_max","volmin","esdmin","vollowmean","esdlowmean")
write.csv(sensor_data, "data_for_trail/data_for_model/sensor_for_5Garages.csv")
demand_data <- read.csv("data_for_trail/data_for_prediction/age_mileage.csv")  #76831
sensor_data$Vehicle <- as.character(sensor_data$Vehicle)
demand_data$Vehicle <- as.character(demand_data$Vehicle)

test_data <- merge(x=sensor_data, y=demand_data, by="Vehicle",x.all=T)

library(sqldf)
test_data <- sqldf("select sensor_data.Vehicle, sensor_data.VehicleGroup, sensor_data.SpecID, sensor_data.State, sensor_data.FuleType,
                        sensor_data.Date, demand_data.max_JobDate, demand_data.Mileage,sensor_data.Odometer_min, sensor_data.Odometer_max, sensor_data.volmin, sensor_data.vollowmean, 
                        sensor_data.esdmin, sensor_data.esdlowmean
                      from sensor_data left join demand_data on sensor_data.Vehicle=demand_data.Vehicle") #2968
test_data$Date <- as.Date(test_data$Date, "%m/%d/%Y")
test_data$max_JobDate <- as.Date(test_data$max_JobDate,"%Y-%m-%d")
write.csv(test_data,file="data_for_prediction/test_data.csv")
test_data_comp <- test_data[complete.cases(test_data),] #933
write.csv(test_data_comp, file="data_for_prediction/test_data_comp.csv")



# daily data from May 12-May 27
daily <- read.table("data_for_trail/data_for_prediction/daily_512_527.csv",sep="|",header=T)

daily_data <- daily[,c(c(1:7),c(14,19,20,24,26,27,31),(c(14,19,20,24,26,27,31)+1*42),
                       (c(14,19,20,24,26,27,31)+2*42),(c(14,19,20,24,26,27,31)+3*42),
                       (c(14,19,20,24,26,27,31)+4*42),(c(14,19,20,24,26,27,31)+5*42),
                       (c(14,19,20,24,26,27,31)+6*42),(c(14,19,20,24,26,27,31)+7*42),
                       (c(14,19,20,24,26,27,31)+8*42),(c(14,19,20,24,26,27,31)+9*42),
                       (c(14,19,20,24,26,27,31)+10*42),(c(14,19,20,24,26,27,31)+11*42),
                       (c(14,19,20,24,26,27,31)+12*42),(c(14,19,20,24,26,27,31)+13*42),
                       (c(14,19,20,24,26,27,31)+14*42))]
Odometer_max <- daily[,c(14,14+1*42,14+2*42,14+3*42,14+4*42,14+5*42,14+6*42,14+7*42,14+8*42,
                         14+9*42,14+10*42,14+11*42,14+12*42,14+13*42,14+14*42)]
Battery_Vol_count <- daily[,c(14,14+1*42,14+2*42,14+3*42,14+4*42,14+5*42,14+6*42,14+7*42,14+8*42,
                         14+9*42,14+10*42,14+11*42,14+12*42,14+13*42,14+14*42)+5]
Battery_Vol_min <- daily[,c(14,14+1*42,14+2*42,14+3*42,14+4*42,14+5*42,14+6*42,14+7*42,14+8*42,
                              14+9*42,14+10*42,14+11*42,14+12*42,14+13*42,14+14*42)+6]
Battery_Vol_below <- daily[,c(14,14+1*42,14+2*42,14+3*42,14+4*42,14+5*42,14+6*42,14+7*42,14+8*42,
                              14+9*42,14+10*42,14+11*42,14+12*42,14+13*42,14+14*42)+10]
Engine_Speed_count <- daily[,c(14,14+1*42,14+2*42,14+3*42,14+4*42,14+5*42,14+6*42,14+7*42,14+8*42,
                              14+9*42,14+10*42,14+11*42,14+12*42,14+13*42,14+14*42)+12]
Engine_Speed_min <- daily[,c(14,14+1*42,14+2*42,14+3*42,14+4*42,14+5*42,14+6*42,14+7*42,14+8*42,
                            14+9*42,14+10*42,14+11*42,14+12*42,14+13*42,14+14*42)+13]
Engine_Speed_below <- daily[,c(14,14+1*42,14+2*42,14+3*42,14+4*42,14+5*42,14+6*42,14+7*42,14+8*42,
                              14+9*42,14+10*42,14+11*42,14+12*42,14+13*42,14+14*42)+17]

volminmean <- rowMeans(Battery_Vol_min,na.rm=T)
lowvolper <- data.frame(nrow=2843,ncol=15 )
for(i in seq(1,2843)){
  for(j in seq(1,15)){
    lowvolper[i,j] <- as.numeric(as.character(Battery_Vol_below[i,j]))/as.numeric(as.character(Battery_Vol_count[i,j]))
  }
}

vollowmean <- rowMeans(lowvolper,na.rm=T)
vollowmax <- apply(lowvolper,1,max,na.rm=T)

#low speed engine count and factor
esdminmean <- rowMeans(Engine_Speed_min,na.rm=T)
lowesdper <- data.frame(nrow=2843,ncol=15 )
for(i in seq(1,2843)){
  for(j in seq(1,15)){
    lowesdper[i,j] <- as.numeric(as.character(Engine_Speed_below[i,j]))/as.numeric(as.character(Engine_Speed_count[i,j]))
  }
}

esdlowmean <- rowMeans(lowesdper[,c(1:15)],na.rm=T)
esdlowmax <- apply(lowesdper,1,function(x)max(!is.na(lowesdper)))
odometermax <- apply(Odometer_max,1,max, na.rm=T)
odometermin <- apply(Odometer_max,1,min,na.rm=T)
sensor_data <- cbind(daily[,c(1:7)],odometermax,odometermin,volminmean,vollowmean, esdminmean,esdlowmean)
library(sqldf)
names(sensor_data)[1] <-"Vehicle"
demand_data <- battery_age_mil_master
sensor_data$Vehicle <- as.character(sensor_data$Vehicle)
demand_data$Vehicle <- as.character(demand_data$Vehicle)
test_data <- sqldf("select sensor_data.Vehicle, sensor_data.VehicleGroup, sensor_data.SpecID, sensor_data.State, sensor_data.FuelType,
                    sensor_data.odometermax, sensor_data.odometermin, demand_data.MilPerDay,demand_data.BatteryAge,demand_data.BatteryAgeInYears,
                    demand_data.BatteryMileage, demand_data.BatteryMileageThousand,demand_data.VehicleAge, demand_data.VehicleAgeInYears,
                    sensor_data.volminmean, sensor_data.vollowmean,
                    sensor_data.esdminmean, sensor_data.esdlowmean,demand_data.BRR
                   from sensor_data left join demand_data on sensor_data.Vehicle=demand_data.Vehicle") #2843

test_data$Date <- as.Date(test_data$Date, "%m/%d/%Y")
test_data$max_JobDate <- as.Date(test_data$max_JobDate,"%Y-%m-%d")
write.csv(test_data,file="data_for_trail/data_for_prediction/test_data_revised.csv")
test_data_comp <- test_data[complete.cases(test_data),] #1442
write.csv(test_data_comp, file="data_for_trail/data_for_prediction/test_data_comp_revised.csv")



#May 30th start from the very beginning the second day of trail
battery_age_mileage <- read.csv("data_for_trail/data_for_model/battery_age_mileage_processed.csv",header=T, sep=",")  # 76830
battery_age_mil_BRR1 <- subset(battery_age_mileage, BRR==1)  $41911
#summary(battery_age_mil_BRR1$BatteryAge)
#Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
#1.0   245.0   427.0   477.9   666.0  1597.0 
battery_age_mil_BRR0 <- subset(battery_age_mileage, BRR==0)  #34919
summary(battery_age_mil_BRR0$BatteryAge)
#Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
#2.0   300.0   661.0   718.1  1110.0  1605.0

battery_age_mil_master <- read.csv("data_for_trail/data_step_by_step/battery_age_mileage_merged_master.csv") #67097
battery_age_mil_master$VehGroup <- factor(battery_age_mil_master$VehGroup)
battery_age_mil_master_BRR1 <- subset(battery_age_mil_master, BRR==1)#41597
battery_age_mil_master_BRR0 <- subset(battery_age_mil_master, BRR==0)#25500

battage_by_vehgroup <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$VehGroup, summary)
batteryage_mean_by_group <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$VehGroup, mean)
batteryage_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$VehGroup, sd)
batteryage_mean_by_group <- cbind(names(batteryage_mean_by_group),batteryage_mean_by_group)
batteryage_mean_by_group <- as.data.frame(batteryage_mean_by_group)
batteryage_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$VehGroup, sd)
batteryage_sd_by_group <- cbind(names(batteryage_sd_by_group),batteryage_sd_by_group)
batteryage_sd_by_group <- as.data.frame(batteryage_sd_by_group)
names(batteryage_mean_by_group) <- c("VehGroup","mean_by_group")
names(batteryage_sd_by_group) <- c("VehGroup","sd_by_group")
library(sqldf)

data <- merge(x = battery_age_mil_master,y=batteryage_mean_by_group, by="VehGroup",x.all=T)
data2 <- merge(x = data,y=batteryage_sd_by_group, by="VehGroup",x.all=T)  #67097
write.csv(data2, file="data_for_trail/data_step_by_step/battery_by_group.csv")


battage_by_fueltype <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$FuelType, summary)
table(battery_age_mil_master$FuelType)
battery_age_mil_master_BRR1$ModelYear <- factor(battery_age_mil_master_BRR1$ModelYear)
battage_by_modelyear <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$ModelYear,summary)
names(battage_by_modelyear)

battage_by_modelyear <- as.data.frame(battage_by_modelyear)
write.table(battage_by_modelyear,"data_for_trail/data_step_by_step/battery_by_modelyear.csv")
battage_by_modelyear <- read.csv("data_for_trail/data_step_by_step/battery_by_modelyear.csv")

#  read gas refill data
mileage_data <- read.csv("Date_Odometer_for_Abhay.txt",sep=',', header=T)
length(unique(mileage_data$Vehicle))  #82840
sqldf("select count(distinct(Vehicle)) from mileage_data")
mileage_data$Vehicle <- as.character(mileage_data$Vehicle)
mileage_data$TransDate <- as.Date(mileage_data$TransDate,"%m/%d/%Y")
mileage_data_sort <- mileage_data[order(mileage_data[,1],mileage_data[,2]),]
library(plyr)
mileage_data_most_recent <- ddply(mileage_data_sort, .(Vehicle),function(x) x[c(nrow(x)-1,nrow(x)),])
mileage_data_most_recent <- ddply(mileage_data_sort, .(Vehicle),function(x) x[nrow(x),])
mileage_data_most_recent <- sqldf("select Vehicle,TransDate,max(Odometer),PrevOdom from mileage_data_sort 
                                  group by Vehicle ") # 82838

write.csv(mileage_data_most_recent, file="data_for_trail/data_step_by_step/gas_fill_mileage_data_most_recent.csv")
battery_age_mil_gas <- merge(x=battery_age_mil_master,y=mileage_data_most_recent, by="Vehicle",x.all=T)  #61123
battery_age_mil_gas$DateInservice <- as.Date(battery_age_mil_gas$DateInservice, "%m/%d/%Y")
battery_age_mil_gas$JobDate <- as.Date(battery_age_mil_gas$JobDate, "%Y-%m-%d")
write.csv(battery_age_mil_gas, file="data_for_trail/data_step_by_step/battery_age_mil_gas.csv")
battery_age_mil_gas_BRR1 <- subset(battery_age_mil_gas, BRR==1) #40025
battery_age_mil_gas_BRR0 <- subset(battery_age_mil_gas, BRR==0) #21098
write.csv(battery_age_mil_gas_BRR0, file="data_for_trail/data_step_by_step/battery_age_mil_gas_BRR0.csv")
write.csv(battery_age_mil_gas_BRR1, file="data_for_trail/data_step_by_step/battery_age_mil_gas_BRR1.csv")

BRR0 <- read.csv("data_for_trail/data_step_by_step/battery_age_mil_gas_BRR0.csv")
BRR1 <- read.csv("data_for_trail/data_step_by_step/battery_age_mil_gas_BRR1.csv")

brr0 <- BRR0[,c(2,4,5,10,11,12,27,28,30,31,23)]
brr1 <- BRR1[,c(2,4,5,10,11,12,27,21,22,29,23)]
names(brr0)[8]<-"BatteryAge"
brr1$BatteryMileage <- factor(brr1$BatteryMileage)
data <- rbind(brr0,brr1)
data2 <- data[sample(1:nrow(data)),]
write.csv(data2, file="data_for_trail/data_step_by_step/battery_data.csv")
test_data <- subset(data2, GarageID=="MO0006"|GarageID=="GA111"|GarageID=="TX0022"|GarageID=="TX050"|GarageID=="TX040")
train_data <- subset(data2, GarageID!="MO0006"&GarageID!="GA111"&GarageID!="TX0022"&GarageID!="TX050"&GarageID!="TX040")
write.csv(test_data, file="data_for_trail/data_for_prediction/test_data.csv")
write.csv(train_data, file="data_for_trail/data_for_prediction/train_data.csv")
batt_more_features <- read.csv("data_for_trail/battery_age_more_features.csv")
test_data_more_features<- subset(batt_more_features, GarageID=="MO0006"|GarageID=="GA111"|GarageID=="TX0022"|GarageID=="TX050"|GarageID=="TX040")
train_data_more_features <- subset(batt_more_features,GarageID!="MO0006"&GarageID!="GA111"&GarageID!="TX0022"&GarageID!="TX050"&GarageID!="TX040")
write.csv(test_data_more_features, file="data_for_trail/data_for_prediction/test_data_more_features.csv")
write.csv(train_data_more_features, file="data_for_trail/data_for_prediction/train_data_more_features.csv")


# standarize battery age and mileage by group  May 31
battery_age_mil_master <- read.csv("data_for_trail/data_step_by_step/battery_data.csv")
battery_age_mil_master_BRR1 <- subset(data, BRR==1) #40025
battery_age_mil_master_BRR0 <- subset #21098
battage_by_vehgroup <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$VehGroup, summary)
batteryage_mean_by_group <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$VehGroup, mean)
batteryage_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$VehGroup, sd)
batteryage_mean_by_group <- cbind(names(batteryage_mean_by_group),batteryage_mean_by_group)
batteryage_mean_by_group <- as.data.frame(batteryage_mean_by_group)
batteryage_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryAge, battery_age_mil_master_BRR1$VehGroup, sd)
batteryage_sd_by_group <- cbind(names(batteryage_sd_by_group),batteryage_sd_by_group)
batteryage_sd_by_group <- as.data.frame(batteryage_sd_by_group)
names(batteryage_mean_by_group) <- c("VehGroup","mean_by_group")
names(batteryage_sd_by_group) <- c("VehGroup","sd_by_group")
library(sqldf)

data <- merge(x = battery_age_mil_master,y=batteryage_mean_by_group, by="VehGroup",x.all=T)
data2 <- merge(x = data,y=batteryage_sd_by_group, by="VehGroup",x.all=T)  #67097

battmil_by_vehgroup <- tapply(battery_age_mil_master_BRR1$BatteryMileage, battery_age_mil_master_BRR1$VehGroup, summary)
battery_age_mil_master_BRR1$BatteryMileage <- as.numeric(as.character(battery_age_mil_master_BRR1$BatteryMileage))
batterymil_mean_by_group <- tapply(battery_age_mil_master_BRR1$BatteryMileage, battery_age_mil_master_BRR1$VehGroup, mean)
batterymil_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryMileage, battery_age_mil_master_BRR1$VehGroup, sd)
batterymil_mean_by_group <- cbind(names(batterymil_mean_by_group),batterymil_mean_by_group)
batterymil_mean_by_group <- as.data.frame(batterymil_mean_by_group)
batterymil_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryMileage, battery_age_mil_master_BRR1$VehGroup, sd)
batterymil_sd_by_group <- cbind(names(batterymil_sd_by_group),batterymil_sd_by_group)
batterymil_sd_by_group <- as.data.frame(batterymil_sd_by_group)
names(batterymil_mean_by_group) <- c("VehGroup","mean_by_group_mil")
names(batterymil_sd_by_group) <- c("VehGroup","sd_by_group_mil")

data3 <- merge(x = data2,y=batterymil_mean_by_group, by="VehGroup",x.all=T)
data4 <- merge(x = data3,y=batterymil_sd_by_group, by="VehGroup",x.all=T)  #67097

vehage_by_vehgroup <- tapply(battery_age_mil_master_BRR1$VehicleAge, battery_age_mil_master_BRR1$VehGroup, summary)
battery_age_mil_master_BRR1$VehicleAge <- as.numeric(as.character(battery_age_mil_master_BRR1$VehicleAge))
vehage_mean_by_group <- tapply(battery_age_mil_master_BRR1$VehicleAge, battery_age_mil_master_BRR1$VehGroup, mean)
vehage_sd_by_group <- tapply(battery_age_mil_master_BRR1$VehicleAge, battery_age_mil_master_BRR1$VehGroup, sd)
vehage_mean_by_group <- cbind(names(vehage_mean_by_group),vehage_mean_by_group)
vehage_mean_by_group <- as.data.frame(vehage_mean_by_group)
vehage_sd_by_group <- tapply(battery_age_mil_master_BRR1$VehicleAge, battery_age_mil_master_BRR1$VehGroup, sd)
vehage_sd_by_group <- cbind(names(vehage_sd_by_group),vehage_sd_by_group)
vehage_sd_by_group <- as.data.frame(vehage_sd_by_group)
names(vehage_mean_by_group) <- c("VehGroup","mean_by_group_vehage")
names(vehage_sd_by_group) <- c("VehGroup","sd_by_group_vehage")

data5 <- merge(x = data4,y=vehage_mean_by_group, by="VehGroup",x.all=T)
data6 <- merge(x = data5,y=vehage_sd_by_group, by="VehGroup",x.all=T)  #67097
write.csv(data6, file="data_for_trail/data_step_by_step/battery_by_group_standarized.csv")


# split data into traning and testing based on garages
data <- read.csv("data_for_trail/data_step_by_step/battery_by_group_standarized.csv")
test_data <- subset(data, GarageID=="MO0006"|GarageID=="GA111"|GarageID=="TX0022"|GarageID=="TX050"|GarageID=="TX040")
train_data <- subset(data, GarageID!="MO0006"&GarageID!="GA111"&GarageID!="TX0022"&GarageID!="TX050"&GarageID!="TX040")

write.csv(test_data,"data_for_trail/data_step_by_step/battery_by_group_standarized_test.csv")
write.csv(train_data,"data_for_trail/data_step_by_step/battery_by_group_standarized_train.csv")


# June 1 combine sensor data with battery_Vehicle data 
sensor_data <- read.csv("sensor_data_60days_15bucket.csv")
vehbatt_data <- read.csv("data_for_trail/data_step_by_step/battery_by_group_standarized.csv")
sensor_data$Vehicle <- as.character(sensor_data$Vehicle)
vehbatt_data$Vehicle <- as.character(vehbatt_data$Vehicle)
sensor <- sensor_data
sensor_data <- subset(sensor, TaskName=="Battery R&R\t")
sensor_data1 <- sqldf("select Vehicle, max(DemandDate),volmin1, vollowmean1, esdmin1,esdlowmean1 from sensor_data group by Vehicle")
data1 <- merge(x=vehbatt_data, y=sensor_data1,by="Vehicle",x.all=T)
write.csv(data1, "data_for_trail/data_step_by_step/data1.csv")

sensor_data2 <- sqldf("select Vehicle, volmin2, vollowmean2, esdmin2,esdlowmean2 from sensor_data group by Vehicle")
data2 <- merge(x=vehbatt_data, y=sensor_data2,by="Vehicle",x.all=T)
write.csv(data2, "data_for_trail/data_step_by_step/data2.csv")

sensor_data3 <- sqldf("select Vehicle, volmin3, vollowmean3, esdmin3,esdlowmean3 from sensor_data group by Vehicle")
data3 <- merge(x=vehbatt_data, y=sensor_data3,by="Vehicle",x.all=T)
write.csv(data3, "data_for_trail/data_step_by_step/data3.csv")

sensor_data4 <- sqldf("select Vehicle, volmin4, vollowmean4, esdmin4,esdlowmean4 from sensor_data group by Vehicle")
data4 <- merge(x=vehbatt_data, y=sensor_data4,by="Vehicle",x.all=T)
write.csv(data4, "data_for_trail/data_step_by_step/data4.csv")
library(sqldf)
#sensor_data.DemandDate, sensor_data.TaskName, 
data <- sqldf("select vehbatt_data.Vehicle, vehbatt_data.VehGroup, vehbatt_data.SpecID, vehbatt_data.FuelType, 
              vehbatt_data.State, vehbatt_data.GarageID, 
              vehbatt_data.MilPerDay, vehbatt_data.BatteryAgeInYears,vehbatt_data.BatteryMileageTenthousand,
              vehbatt_data.VehicleAgeInYears, vehbatt_data.BattAgeStand,vehbatt_data.BattMilStand, 
              vehbatt_data.VehAgeStand, vehbatt_data.AgeRatio, vehbatt_data.BattAgeSq, 
              vehbatt_data.BattMilSq, vehbatt_data.BattAgeTimesMile, 
              sensor_data.volmin1,sensor_data.vollowmean1, sensor_data.esdmin1, sensor_data.esdlowmean1, 
              sensor_data.volmin2,  sensor_data.vollowmean2, sensor_data.esdmin2, sensor_data.esdlowmean2,
              sensor_data.volmin3, sensor_data.vollowmean3,sensor_data.esdmin3, sensor_data.esdlowmean3,
              sensor_data.volmin4, sensor_data.vollowmean4, sensor_data.esdmin4, sensor_data.esdlowmean4,
              vehbatt_data.BRR from vehbatt_data left join sensor_data on (vehbatt_data.Vehicle=sensor_data.Vehicle) ")


# standarize the test data from the 5 garages

battery_age_mil_master <- read.csv("data_for_trail/data_step_by_step/battery_data.csv")
battery_age_mil_master_BRR1 <- subset(data, BRR==1) #40025
battery_age_mil_master_BRR0 <- subset(data, BRR==0) #21098
battage_by_vehgroup <- tapply(battery_age_mil_master_BRR1$BatteryAgeInYears, battery_age_mil_master_BRR1$VehGroup, summary)
batteryage_mean_by_group <- tapply(battery_age_mil_master_BRR1$BatteryAgeInYears, battery_age_mil_master_BRR1$VehGroup, mean)
batteryage_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryAgeInYears, battery_age_mil_master_BRR1$VehGroup, sd)
batteryage_mean_by_group <- cbind(names(batteryage_mean_by_group),batteryage_mean_by_group)
batteryage_mean_by_group <- as.data.frame(batteryage_mean_by_group)
batteryage_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryAgeInYears, battery_age_mil_master_BRR1$VehGroup, sd)
batteryage_sd_by_group <- cbind(names(batteryage_sd_by_group),batteryage_sd_by_group)
batteryage_sd_by_group <- as.data.frame(batteryage_sd_by_group)
names(batteryage_mean_by_group) <- c("VehGroup","mean_by_group")
names(batteryage_sd_by_group) <- c("VehGroup","sd_by_group")
library(sqldf)
test_data <- read.csv("data_for_trail/data_for_prediction/test_data_comp_revised.csv")
data <- merge(x = test_data,y=batteryage_mean_by_group, by="VehGroup",x.all=T)
data2 <- merge(x = data,y=batteryage_sd_by_group, by="VehGroup",x.all=T)  #67097

battmil_by_vehgroup <- tapply(battery_age_mil_master_BRR1$BatteryMileageTenthousand, battery_age_mil_master_BRR1$VehGroup, summary)
battery_age_mil_master_BRR1$BatteryMileageTenthousand <- as.numeric(as.character(battery_age_mil_master_BRR1$BatteryMileageTenthousand))
batterymil_mean_by_group <- tapply(battery_age_mil_master_BRR1$BatteryMileageTenthousand, battery_age_mil_master_BRR1$VehGroup, mean)
batterymil_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryMileageTenthousand, battery_age_mil_master_BRR1$VehGroup, sd)
batterymil_mean_by_group <- cbind(names(batterymil_mean_by_group),batterymil_mean_by_group)
batterymil_mean_by_group <- as.data.frame(batterymil_mean_by_group)
batterymil_sd_by_group <- tapply(battery_age_mil_master_BRR1$BatteryMileageTenthousand, battery_age_mil_master_BRR1$VehGroup, sd)
batterymil_sd_by_group <- cbind(names(batterymil_sd_by_group),batterymil_sd_by_group)
batterymil_sd_by_group <- as.data.frame(batterymil_sd_by_group)
names(batterymil_mean_by_group) <- c("VehGroup","mean_by_group_mil")
names(batterymil_sd_by_group) <- c("VehGroup","sd_by_group_mil")

data3 <- merge(x = data2,y=batterymil_mean_by_group, by="VehGroup",x.all=T)
data4 <- merge(x = data3,y=batterymil_sd_by_group, by="VehGroup",x.all=T)  #67097

vehage_by_vehgroup <- tapply(battery_age_mil_master_BRR1$VehicleAgeInYears, battery_age_mil_master_BRR1$VehGroup, summary)
battery_age_mil_master_BRR1$VehicleAge <- as.numeric(as.character(battery_age_mil_master_BRR1$VehicleAgeInYears))
vehage_mean_by_group <- tapply(battery_age_mil_master_BRR1$VehicleAge, battery_age_mil_master_BRR1$VehGroup, mean)
vehage_sd_by_group <- tapply(battery_age_mil_master_BRR1$VehicleAge, battery_age_mil_master_BRR1$VehGroup, sd)
vehage_mean_by_group <- cbind(names(vehage_mean_by_group),vehage_mean_by_group)
vehage_mean_by_group <- as.data.frame(vehage_mean_by_group)
vehage_sd_by_group <- tapply(battery_age_mil_master_BRR1$VehicleAge, battery_age_mil_master_BRR1$VehGroup, sd)
vehage_sd_by_group <- cbind(names(vehage_sd_by_group),vehage_sd_by_group)
vehage_sd_by_group <- as.data.frame(vehage_sd_by_group)
names(vehage_mean_by_group) <- c("VehGroup","mean_by_group_vehage")
names(vehage_sd_by_group) <- c("VehGroup","sd_by_group_vehage")

data5 <- merge(x = data4,y=vehage_mean_by_group, by="VehGroup",x.all=T)
data6 <- merge(x = data5,y=vehage_sd_by_group, by="VehGroup",x.all=T)  #67097
write.csv(data6, file="data_for_trail/data_step_by_step/5garage_test_data_standarized.csv") #912

# June 1
data1 <-read.csv("data_for_trail/data_step_by_step/data1.csv")
test <- read.csv("data_for_trail/data_step_by_step/5garage_test_data_standarized.csv")
#######################
veh_data <- read.csv("data_for_trail/data_for_model/tr_data_with_mileage 3.csv") #67146
veh_data_JobData <- as.Date(veh_data$JobDate,"%m/%d/%y")
veh_data$JobDate <- veh_data_JobData
veh_data_DetailsData <- as.Date(veh_data$DetailsDate,"%m/%d/%y")
veh_data$DetailsDate <- veh_data_DetailsData
veh_data_DateInservice <- as.Date(veh_data$DateInservice,"%m/%d/%Y")
veh_data$DateInservice <- veh_data_DateInservice
veh_data$Vehicle <- as.character(veh_data$Vehicle)
veh_data_0 <- subset(veh_data,BRR==0)  #67146
veh_data_1 <- subset(veh_data, BRR==1) #41650





write.csv(veh_data_0,"data_for_trail/data_step_by_step/veh_data_0_june2.csv")
write.csv(veh_data_1,"data_for_trail/data_step_by_step/veh_data_1_june2.csv")

odom_data <- read.csv("Date_Odometer_for_Abhay.txt")
odom_data$Vehicle <- as.character(odom_data$Vehicle)
odom_data_TransDate <- as.Date(odom_data$TransDate, "%m/%d/%Y")
odom_data$TransDate <- odom_data_TransDate
veh_data <- veh_data[,-1]
library(sqldf)
last_odom <- sqldf("select Vehicle, TransDate, max(Odometer) from odom_data group by Vehicle")
veh_data_odom <- merge(x=veh_data, y=last_odom, by="Vehicle",x.all=T)
veh_data_odom_1 <- subset(veh_data_odom, BRR==1)  #40078
veh_data_odom_0 <- subset(veh_data_odom, BRR==0)  #61167
write.csv(veh_data_odom_1, "data_for_trail/data_step_by_step/veh_data_odom_1_0602.csv")
write.csv(veh_data_odom_0, "data_for_trail/data_step_by_step/veh_data_odom_0_0602.csv")
# calculate battery age and mileage in excel
veh_data_odom_1<- read.csv( "data_for_trail/data_step_by_step/veh_data_odom_1_0602.csv")
veh_data_odom_0<- read.csv( "data_for_trail/data_step_by_step/veh_data_odom_0_0602.csv")
veh_data_odom_0 <- veh_data_odom_0[,c(1,2,3,4,6,7,10,11,12,13,14,8,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32)]
veh_data_small_0 <- veh_data_odom_0[,c(1,2,3,6,7,9,11,13,15,17,20,21,22)]
veh_data_small_1 <- veh_data_odom_1[,c(1,2,3,6,7,10,12,14,16,18,21,22,23)]
battery_age_mil_master_BRR1<-veh_data_small_1
battery_age_mil_master_BRR0<-veh_data_small_0
battery_age_mil_master_BRR0$MilPerDay <- as.numeric(as.character(battery_age_mil_master_BRR0$MilPerDay))
battery_age_mil_master_BRR1$MilPerDay <- as.numeric(as.character(battery_age_mil_master_BRR1$MilPerDay))
battery_age_mil_master_BRR0$BattAgeInYears <- as.numeric(as.character(battery_age_mil_master_BRR0$BattAgeInYears))
battery_age_mil_master_BRR1$BattAgeInYears <- as.numeric(as.character(battery_age_mil_master_BRR1$BattAgeInYears))
battery_age_mil_master_BRR0$VehAgeInYears <- as.numeric(as.character(battery_age_mil_master_BRR0$VehAgeInYears))
battery_age_mil_master_BRR1$VehAgeInYears<- as.numeric(as.character(battery_age_mil_master_BRR1$VehAgeInYears))
battery_age_mil_master_BRR0$BattMilIn10000 <- as.numeric(as.character(battery_age_mil_master_BRR0$BattMilIn10000))
battery_age_mil_master_BRR1$BattMilIn10000 <- as.numeric(as.character(battery_age_mil_master_BRR1$BattMilIn10000))
battery_age_mil_master<-rbind(battery_age_mil_master_BRR0,battery_age_mil_master_BRR1)


battage_by_vehgroup <- tapply(battery_age_mil_master_BRR1$BattAgeInYears, battery_age_mil_master_BRR1$VehGroup, summary)
batteryage_mean_by_group <- tapply(battery_age_mil_master_BRR1$BattAgeInYears, battery_age_mil_master_BRR1$VehGroup, mean)
batteryage_mean_by_group <- cbind(names(batteryage_mean_by_group),batteryage_mean_by_group)
batteryage_mean_by_group <- as.data.frame(batteryage_mean_by_group)
batteryage_sd_by_group <- tapply(battery_age_mil_master_BRR1$BattAgeInYears, battery_age_mil_master_BRR1$VehGroup, sd)
batteryage_sd_by_group <- cbind(names(batteryage_sd_by_group),batteryage_sd_by_group)
batteryage_sd_by_group <- as.data.frame(batteryage_sd_by_group)
names(batteryage_mean_by_group) <- c("VehGroup","mean_by_group_battage")
names(batteryage_sd_by_group) <- c("VehGroup","sd_by_group_battage")


data <- merge(x = battery_age_mil_master,y=batteryage_mean_by_group, by="VehGroup",x.all=T)
data2 <- merge(x = data,y=batteryage_sd_by_group, by="VehGroup",x.all=T)  #67097

battmil_by_vehgroup <- tapply(battery_age_mil_master_BRR1$BattMilIn10000, battery_age_mil_master_BRR1$VehGroup, summary)
batterymil_mean_by_group <- tapply(battery_age_mil_master_BRR1$BattMilIn10000, battery_age_mil_master_BRR1$VehGroup, mean)
batterymil_mean_by_group <- cbind(names(batterymil_mean_by_group),batterymil_mean_by_group)
batterymil_mean_by_group <- as.data.frame(batterymil_mean_by_group)
batterymil_sd_by_group <- tapply(battery_age_mil_master_BRR1$BattMilIn10000, battery_age_mil_master_BRR1$VehGroup, sd)
batterymil_sd_by_group <- cbind(names(batterymil_sd_by_group),batterymil_sd_by_group)
batterymil_sd_by_group <- as.data.frame(batterymil_sd_by_group)
names(batterymil_mean_by_group) <- c("VehGroup","mean_by_group_battmil")
names(batterymil_sd_by_group) <- c("VehGroup","sd_by_group_battmil")

data3 <- merge(x = data2,y=batterymil_mean_by_group, by="VehGroup",x.all=T)
data4 <- merge(x = data3,y=batterymil_sd_by_group, by="VehGroup",x.all=T)  #101245
write.csv(data4, file="data_for_trail/data_step_by_step/veh_data_standarized_0602.csv")
dim(data4)
# training and testing
data <- read.csv("data_for_trail/data_step_by_step/veh_data_standarized_0602.csv")

test_data <- subset(data, GarageID=="MO0006"|GarageID=="GA111"|GarageID=="TX0022"|GarageID=="TX050"|GarageID=="TX040")
train_data <- subset(data, GarageID!="MO0006"&GarageID!="GA111"&GarageID!="TX0022"&GarageID!="TX050"&GarageID!="TX040")
write.csv(test_data,"data_for_trail/data_step_by_step/veh_data_standarized_test_0602.csv")
write.csv(train_data,"data_for_trail/data_step_by_step/veh_data_standarized_train_0602.csv")

# join with sensor data
library(data.table)
sensor_data_1 <- read.csv("sensor_data_60days_15bucket.csv") #663856
names(sensor_data)[3]<-"DetailsDate"
sensor_data_BRR <- subset(sensor_data, TaskName=="Battery R&R\t") #20477
sensor_data_noBRR <-subset(sensor_data, TaskName!="Battery R&R\t")
pred_0602 <- read.csv("data_for_trail/data_for_prediction/Pred_0602.csv")
pred_0602$Vehicle <- as.character(pred_0602$Vehicle)
pred_0602_JobDate <- as.Date(pred_0602$JobDate,"%m/%d/%y")
pred_0602$JobDate <- pred_0602_JobDate
pred_0602_DetailsDate <- as.Date(pred_0602$DetailsDate,"%m/%d/%y")
pred_0602$DetailsDate <- pred_0602_DetailsDate

test_pred_0602 <- subset(pred_0602, GarageID=="MO0006"|GarageID=="GA111"|GarageID=="TX0022"|GarageID=="TX050"|GarageID=="TX040")
train_pred_0602 <- subset(pred_0602, GarageID!="MO0006"&GarageID!="GA111"&GarageID!="TX0022"&GarageID!="TX050"&GarageID!="TX040")

sensor_data$Vehicle <- as.character(sensor_data$Vehicle)
sensor_data_DetailsDate <- as.Date(sensor_data$DetailsDate)
sensor_data$DemandDate <- sensor_data_DemandDate

data_train <- merge(x=train_pred_0602,y=sensor_data_BRR, by=c("Vehicle","DetailsDate"),x.all=T)

sensor_for_5garages <- read.csv("data_for_trail/data_for_model/sensor_for_5Garages.csv")
sensor_for_5garages$Vehicle <- as.character(sensor_for_5garages$Vehicle)
test_data <- merge(x=sensor_for_5garages,y=test_sensor_data, by="Vehicle",x.all=T)
write.csv(test_data, "data_for_trail/data_for_model/test_data_after_first_model_2.csv")


data_train <- merge(x=train_pred_0602,y=sensor_data_BRR, by=c("Vehicle","DetailsDate"),x.all=T)
test_data <- merge(x=test_pred_0602,y=sensor_for_5garages, by="Vehicle",y.all=T)

library(sqldf)
data_train <- sqldf("select train_pred_0602.Vehicle, train_pred_0602.BattAgeTimesProb, train_pred_0602.FuelType, train_pred_0602.QUE_FLT_VEHEXT01_State,
                    train_pred_0602.DetailsDate, sensor_data_BRR.volmin1 as volmin, sensor_data_BRR.vollowmean1 as vollowmean, 
                    sensor_data_BRR.esdmin1 as esdmin, sensor_data_BRR.esdlowmean1 as esdlowmean,train_pred_0602.BRR
                    from train_pred_0602 left join sensor_data_BRR on 
                    ( train_pred_0602.Vehicle=sensor_data_BRR.Vehicle and train_pred_0602.DetailsDate=sensor_data_BRR.DetailsDate)")
data_test <- sqldf("select test_pred_0602.Vehicle, test_pred_0602.BattAgeTimesProb, test_pred_0602.FuelType, test_pred_0602.QUE_FLT_VEHEXT01_State,
                    test_pred_0602.DetailsDate, sensor_for_5garages.volmin, sensor_for_5garages.vollowmean, 
                    sensor_for_5garages.esdmin, sensor_for_5garages.esdlowmean, test_pred_0602.BRR
                    from test_pred_0602 left join sensor_for_5garages on 
                    ( test_pred_0602.Vehicle=sensor_for_5garages.Vehicle)")
data_test_1 <- subset(data_test, BRR==0)
write.csv(data_train,"data_for_trail/data_for_model/train_data_for_second_model.csv")
write.csv(data_test_1,"data_for_trail/data_for_model/test_data_for_second_model.csv")


# June 4th
sensor_data <- read.csv("data_for_trail/data_step_by_step/sensor_data_60days_15bucket.csv")  #663856
sensor_BRR <- subset(sensor_data, TaskName=="Battery R&R\t") #20477
sensor_BRR$DemandDate <- as.Date(sensor_BRR$DemandDate)
length(unique(sensor_BRR$Vehicle)) #16367
sensor_BRR_comp <- sensor_BRR[complete.cases(sensor_BRR),] #9634
length(unique(sensor_BRR_comp$Vehicle))  #8245
#vehicle battery data
veh_data <- read.csv("data_for_trail/data_step_by_step/veh_data_standarized_0602.csv")  #55895
veh_BRR0 <- subset(veh_data,BRR==0) #19956
veh_BRR1 <- subset(veh_data,BRR==1)
veh_BRR0_BattAgeInYear <- veh_BRR0$BattAgeInYears-0.16
veh_BRR0$BattAgeInYears <- veh_BRR0_BattAgeInYear
veh_BRR0_BattMilIn10000 <- veh_BRR0$MilPerDay*veh_BRR0$BattAgeInYears
veh_BRR0$BattMilIn10000 <- veh_BRR0_BattMilIn10000
veh_BRR0_VehAgeInYear <- veh_BRR0$VehAgeInYears-0.16
veh_BRR0$VehAgeInYears <- veh_BRR0_VehAgeInYear
veh_BRR0_normal <- subset(veh_BRR0, BattAgeInYears>0) 19360
veh_data <- rbind(veh_BRR0_normal, veh_BRR1)
vehdata_5garages <- subset(veh_data, GarageID=="MO0006"|GarageID=="GA111"|GarageID=="TX0022"|GarageID=="TX050"|GarageID=="TX040") #3280
vehdata_rest <- subset(veh_data, GarageID!="MO0006"&GarageID!="GA111"&GarageID!="TX0022"&GarageID!="TX050"&GarageID!="TX040") #52560
indexes = sample(1:nrow(vehdata_rest), size=0.4*nrow(vehdata_rest))
veh_test_1 = vehdata_rest[indexes,] #21024
veh_train = vehdata_rest[-indexes,] #31212
veh_test <- rbind(veh_test_1,vehdata_5garages) #24087
#training and testing data for 1st model
write.csv(veh_train, file="data_for_trail/data_for_model/veh_train_for_1st_model_0604.csv")
write.csv(veh_test, file="data_for_trail/data_for_model/veh_test_for_1st_model_0604.csv")
#training and testing data for 2nd model
data_2 <- read.csv("data_for_trail/data_for_model/0604/Pred_1s_0604.csv") #24087
data_2$DetailsDate <- as.Date(data_2$DetailsDate,"%m/%d/%y")
data_test_2 <- subset(data_2, GarageID=="MO0006"|GarageID=="GA111"|GarageID=="TX0022"|GarageID=="TX050"|GarageID=="TX040")  #3280
data_train_2 <- subset(data_2, GarageID!="MO0006"&GarageID!="GA111"&GarageID!="TX0022"&GarageID!="TX050"&GarageID!="TX040") #20807

library(sqldf)
data_train_2_0015 <- sqldf("select data_train_2.Vehicle, data_train_2.BattAgeSqTimesProb, data_train_2.VehGroup,data_train_2.DetailsDate,data_train_2.BattAgeInYears,
                    data_train_2.FuelType,data_train_2.QUE_FLT_VEHEXT01_State, sensor_BRR.volmin1,sensor_BRR.vollowmean1, sensor_BRR.esdmin1,sensor_BRR.esdlowmean1
                    from data_train_2 left join sensor_BRR on 
                    ( data_train_2.Vehicle=sensor_BRR.Vehicle and data_train_2.DetailsDate=sensor_BRR.DemandDate)")

data_train_2_1630 <- sqldf("select data_train_2.Vehicle, data_train_2.BattAgeSqTimesProb, data_train_2.VehGroup,data_train_2.DetailsDate,data_train_2.BattAgeInYears,
                          data_train_2.FuelType,data_train_2.QUE_FLT_VEHEXT01_State, sensor_BRR.volmin2,sensor_BRR.vollowmean2, sensor_BRR.esdmin2,sensor_BRR.esdlowmean2
                          from data_train_2 left join sensor_BRR on 
                          ( data_train_2.Vehicle=sensor_BRR.Vehicle and data_train_2.DetailsDate=sensor_BRR.DemandDate)")

data_train_2_015 <- sqldf("select data_train_2.Vehicle, data_train_2.BattAgeSqTimesProb, data_train_2.VehGroup,data_train_2.DetailsDate,data_train_2.BattAgeInYears,
                          data_train_2.FuelType,data_train_2.QUE_FLT_VEHEXT01_State, sensor_BRR.volmin1,sensor_BRR.vollowmean1, sensor_BRR.esdmin1,sensor_BRR.esdlowmean1
                          from data_train_2 left join sensor_BRR on 
                          ( data_train_2.Vehicle=sensor_BRR.Vehicle and data_train_2.DetailsDate=sensor_BRR.DemandDate)")

data_train_2_015 <- sqldf("select data_train_2.Vehicle, data_train_2.BattAgeSqTimesProb, data_train_2.VehGroup,data_train_2.DetailsDate,data_train_2.BattAgeInYears,
                          data_train_2.FuelType,data_train_2.QUE_FLT_VEHEXT01_State, sensor_BRR.volmin1,sensor_BRR.vollowmean1, sensor_BRR.esdmin1,sensor_BRR.esdlowmean1
                          from data_train_2 left join sensor_BRR on 
                          ( data_train_2.Vehicle=sensor_BRR.Vehicle and data_train_2.DetailsDate=sensor_BRR.DemandDate)")
data_test <- sqldf("select test_pred_0602.Vehicle, test_pred_0602.BattAgeTimesProb, test_pred_0602.FuelType, test_pred_0602.QUE_FLT_VEHEXT01_State,
                   test_pred_0602.DetailsDate, sensor_for_5garages.volmin, sensor_for_5garages.vollowmean, 
                   sensor_for_5garages.esdmin, sensor_for_5garages.esdlowmean, test_pred_0602.BRR
                   from test_pred_0602 left join sensor_for_5garages on 
                   ( test_pred_0602.Vehicle=sensor_for_5garages.Vehicle)")


# June 4th parallel modeling
# data for model one based on battery age mileage etc. static variables
data_1 <- read.csv("data_for_trail/data_step_by_step/veh_data_standarized_0602.csv")  # 55895
battage_by_state <- tapply(data_1$BattAgeInYears, list(data_1$VehGroup,data_1$QUE_FLT_VEHEXT01_State), median,na.rm=T)
with(data_1, tapply(BattAgeInYears, list(QUE_FLT_VEHEXT01_State, VehGroup), summary))
data_test_1 <- subset(data_1, GarageID=="MO0006"|GarageID=="GA111"|GarageID=="TX0022"|GarageID=="TX050"|GarageID=="TX040") #3335
data_train_1 <- subset(data_1, GarageID!="MO0006"&GarageID!="GA111"&GarageID!="TX0022"&GarageID!="TX050"&GarageID!="TX040") #52560
write.csv(data_test_1, "data_for_trail/data_for_model/0604/data_test_1st_model_0604.csv")
write.csv(data_train_1, "data_for_trail/data_for_model/0604/data_train_1st_model_0604.csv")

# data for model two on sensor data
data_2 <- read.csv("data_for_trail/data_step_by_step/sensor_data_60days_15bucket.csv") #663856
data_2_BRR1 <- subset(data_2, TaskName=="Battery R&R\t")  #20477
data_2_BRR0 <- subset(data_2, TaskName=="Tires/Wheels/Hubs DIA\t"| TaskName=="Balance 2 Tires\t"
                      |TaskName=="Replace 1 Tire\t"|TaskName=="Door Lock R&R\t"|TaskName=="Wiper Blade,refill R&R\t") #31504 records, 18124 unique vehicles
data_2_BRR1_15days <- data_2_BRR1[,c(2,3,4,5,6,7,8)]
data_2_BRR1_15days$BRR <- 1
data_2_BRR0_15days <- data_2_BRR0[,c(2,3,4,5,6,7,8)]
data_2_BRR0_15days$BRR <- 0

data_modified_2 <- rbind(data_2_BRR0_15days,data_2_BRR1_15days)
data_modified_2$Vehicle <- as.character(data_modified_2$Vehicle)
data_test_1$Vehicle <- as.character(data_test_1$Vehicle)
data_test_2 <- sqldf("select data_modified_2.Vehicle,data_modified_2.DemandDate,data_modified_2.TaskName,data_modified_2.volmin1, data_modified_2.vollowmean1,
                     data_modified_2.esdmin1, data_modified_2.esdlowmean1,data_modified_2.BRR from data_test_1 left join data_modified_2 
                     on data_modified_2.Vehicle=data_test_1.Vehicle")
data_test_2 <- unique(data_test_2) #3090
data_train_2 <- sqldf("select data_modified_2.Vehicle,data_modified_2.DemandDate,data_modified_2.TaskName,data_modified_2.volmin1, data_modified_2.vollowmean1,
                     data_modified_2.esdmin1, data_modified_2.esdlowmean1,data_modified_2.BRR from data_train_1 left join data_modified_2 
                     on data_modified_2.Vehicle=data_train_1.Vehicle")
data_train_2 <- unique(data_train_2)  #39539
data_train_2_comp <- data_train_2[complete.cases(data_train_2),] #27292
write.csv(data_test_2, "data_for_trail/data_for_model/0604/data_test_2nd_model_0604.csv")
write.csv(data_train_2, "data_for_trail/data_for_model/0604/data_train_2nd_model_0604.csv")
write.csv(data_train_2_comp, "data_for_trail/data_for_model/0604/data_train_2nd_model_comp_0604.csv")
# join two model results
result1 <- read.csv("data_for_trail/data_for_model/0604/Prediction_1st_model_0604.csv")
result2 <- read.csv("data_for_trail/data_for_model/0604/Prediction_2nd_model_0604.csv")

result1$Vehicle <- as.character(result1$Vehicle)
result2$Vehicle <- as.character(result2$Vehicle)
result1_predtrue <- subset(result1, result=="1")  #490
result <- merge(x=result1,y=result2, by="Vehicle")
result2_predtrue <- subset(result2, result=="1") #723
result_predtrue <- merge(x=result1_predtrue, y=result2_predtrue, by="Vehicle")  #107

# 5 garages
sensor_5garages <- read.csv("data_for_trail/data_for_prediction/test_data_comp.csv")
names(data_modified_2)[c(4:7)] <-c("volmin","vollowmean","esdmin","esdlowmean")
data_modified_2 <- data_modified_2[,c(1,4,5,6,7,8)]
write.csv(data_modified_2,"data_for_trail/data_for_prediction/data_train_2nd_model_modified_0604.csv")
sensor_5garages <- sensor_5garages[,c(2,13,14,15,16)]
write.csv(sensor_5garages,"data_for_trail/data_for_prediction/data_test_2nd_model_modified_0604.csv")


#June 5
data_test <- read.table("60day_corrected/bigdata.csv",header=T,sep="|",nrow=10000)
volmin <- read.csv("data_for_trail/data_step_by_step/sensor_data/5days/volmin5days.csv",sep="|",header=T,colClasses=c(rep("character",9),rep("numeric",5)))
volmax <- read.csv("data_for_trail/data_step_by_step/sensor_data/5days/volmax5days.csv",sep="|",header=T)
volcount <- read.csv("data_for_trail/data_step_by_step/sensor_data/5days/volcount5days.csv",sep="|",header=T)
volbelow <- read.csv("data_for_trail/data_step_by_step/sensor_data/5days/volbelow5days.csv",sep="|",header=T)
volmax_comp <- volmax[complete.cases(volmax),]
volmin_comp <- volmin[complete.cases(volmin),]
volcount_comp <- volcount[complete.cases(volcount),]
volbelow_comp <- volbelow[complete.cases(volbelow),]

vol <- cbind(volmin,volmax,volcount,volbelow)
vol <- vol[,-c(c(15:23),c(29:37),c(43:51))]
vol_comp_1 <- vol[complete.cases(vol[,c(10,15)]),]


#extract BRR=1
BRRtrue <- c("Alternator Belt R&R\t","Alternator Belt ADJ\t", "Battery R&R\t", "Battery ADJ\t", "Charging/starting System DIA\t", "Electrical System DIA\t", 
             "Alternator R&R\t",
             "Alternator - REP\t", "Battery/hold Down R&R\t", "Battery Cable R&R\t", "Battery Cable REP\t", "Battery, Charge REP\t", "Voltage Regulator R&R\t", 
             "Electrical System DIA\t","Electrical System OTH\t", "Battery,egr Warning Lite R&R\t", "Battery Aux Engine Aerial R&R\t", "Battery Mpu/forklift R&R\t",
             "Alternator Mpu R&R\t", "Battery Vpu R&R\t","Jump Start Vehicle OTH\t", "Battery,auxillary Engine R&R\t", "12 volt Battery, hvy duty auxiliary R&R\t")


BRR1 <- subset(vol_comp_1, TaskName=="Alternator Belt R&R\t"|
                 TaskName=="Alternator Belt ADJ\t"|
                 TaskName=="Battery R&R\t"|
                 TaskName=="Battery ADJ\t"|
                 TaskName=="Charging/starting System DIA\t"|
                 TaskName=="Electrical System DIA\t"|
                 TaskName=="Alternator R&R\t"|
                 TaskName=="Alternator - REP\t"|
                 TaskName=="Battery/hold Down R&R\t"|
                 TaskName=="Battery Cable R&R\t"|
                 TaskName=="Battery Cable REP\t"|
                 TaskName== "Battery, Charge REP\t"|
                 TaskName=="Voltage Regulator R&R\t"|
                 TaskName=="Electrical System OTH\t"|
                 TaskName=="Battery,egr Warning Lite R&R\t"|
                 TaskName=="Battery Aux Engine Aerial R&R\t"|
                 TaskName=="Battery Mpu/forklift R&R\t"|
                 TaskName=="Battery Aux Engine Aerial R&R\t"|
                 TaskName=="Alternator Mpu R&R\t"|
                 TaskName=="Battery Vpu R&R\t"|
                 TaskName=="Jump Start Vehicle OTH\t"|
                 TaskName=="Battery,auxillary Engine R&R\t"|
                 TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                 TaskName=="Starter R&R\t"|
                 TaskName=="Spark Plugs R&R\t"|
                 TaskName=="Wiring/harness R&R\t"|
                 TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                 TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                 TaskName=="Jump Start Vehicle\t"|
                 TaskName=="Aerial/Electrical System DIA\t"|
                 TaskName=="Aerial/Electrical system OTH\t"|
                 TaskName=="EVACUATE & RECHARGE - R&R\t"|
                 TaskName=="Engine  system OTH\t"|
                 TaskName=="Engine System DIA\t")

BRR0 <- subset(vol_comp_1, TaskName!="Alternator Belt R&R\t"&
                 TaskName!="Alternator Belt ADJ\t"&
                 TaskName!="Battery R&R\t"&
                 TaskName!="Battery ADJ\t"&
                 TaskName!="Charging/starting System DIA\t"&
                 TaskName!="Electrical System DIA\t"&
                 TaskName!="Alternator R&R\t"&
                 TaskName!="Alternator - REP\t"&
                 TaskName!="Battery/hold Down R&R\t"&
                 TaskName!="Battery Cable R&R\t"&
                 TaskName!="Battery Cable REP\t"&
                 TaskName!= "Battery, Charge REP\t"&
                 TaskName!="Voltage Regulator R&R\t"&
                 TaskName!="Electrical System OTH\t"&
                 TaskName!="Battery,egr Warning Lite R&R\t"&
                 TaskName!="Battery Aux Engine Aerial R&R\t"&
                 TaskName!="Battery Mpu/forklift R&R\t"&
                 TaskName!="Battery Aux Engine Aerial R&R\t"&
                 TaskName!="Alternator Mpu R&R\t"&
                 TaskName!="Battery Vpu R&R\t"&
                 TaskName!="Jump Start Vehicle OTH\t"&
                 TaskName!="Battery,auxillary Engine R&R\t"&
                 TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                 TaskName!="Starter R&R\t"&
                 TaskName!="Spark Plugs R&R\t"&
                 TaskName!="Wiring/harness R&R\t"&
                 TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                 TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                 TaskName!="Jump Start Vehicle\t"&
                 TaskName!="Aerial/Electrical System DIA\t"&
                  TaskName!="Aerial/Electrical system OTH\t"&
                 TaskName!="EVACUATE & RECHARGE - R&R\t"&
                 TaskName!="Engine  system OTH\t"&
                 TaskName!="Engine System DIA\t")

BRR0$BRR <-0
BRR1$BRR <-1
sensor_5days <- rbind(BRR1,BRR0)
write.csv(sensor_5days, "data_for_trail/data_step_by_step/sensor_data/sensor_5days_0605.csv")
  

Aerial/Electrical System DIA\t
Aerial/Electrical system OTH\t 
Ignition Switch R&R\t
Jump Start Vehicle\t
Ignition System DIA\t 
EVACUATE & RECHARGE - R&R\t
Engine  system OTH\t
Engine System DIA\t

#run association rule mining

count.fields("data_for_trail/data_step_by_step/association_processed.csv",sep=",")
no_col <- max(count.fields("data_for_trail/data_step_by_step/association_processed.csv",sep=","))
data <- read.table("data_for_trail/data_step_by_step/association_processed.csv",sep=",",fill=T,,col.names=1:no_col)
for(i in 1:nrow(data)) {
  data[i,] <- data[i,][!duplicated(data[i,])]
}
dim(data)
library(arules)
data_small.tr <- as(data_small,"transactions")
write.csv(data, "data_for_trail/data_step_by_step/association_processed_dupremoved.csv")
rules <- apriori(data_small.tr, parameter=list(support=0.5, confidence=0.5))

# bucket sensor data into 10 days
library(data.table)

volmin <-read.csv("data_for_trail/data_step_by_step/sensor_data/volmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
volmax <- read.csv("data_for_trail/data_step_by_step/sensor_data/volmax.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmin <- read.csv("data_for_trail/data_step_by_step/sensor_data/esdmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmax <- read.csv("data_for_trail/data_step_by_step/sensor_data/esdmax.csv",sep="|",header=T,stringsAsFactors=FALSE)
volmin <- as.data.frame(volmin)
volmax <- as.data.frame(volmax)
esdmin <- as.data.frame(esdmin)
esdmax <- as.data.frame(esdmax)

volmin_1 <- as.data.frame(lapply(volmin[,c(10:19)],as.numeric),ncol=10)
volmin_2 <- as.data.frame(lapply(volmin[,c(20:29)],as.numeric),ncol=10)
volmin_3 <- as.data.frame(lapply(volmin[,c(30:39)],as.numeric),ncol=10)
volmin_4 <- as.data.frame(lapply(volmin[,c(40:49)],as.numeric),ncol=10)
volmin_5 <- as.data.frame(lapply(volmin[,c(50:59)],as.numeric),ncol=10)
volmin_6 <- as.data.frame(lapply(volmin[,c(60:69)],as.numeric),ncol=10)
volminmean_1 <- rowMeans(volmin_1,na.rm=T)
volminmean_2 <- rowMeans(volmin_2,na.rm=T)
volminmean_3 <- rowMeans(volmin_3,na.rm=T)
volminmean_4 <- rowMeans(volmin_4,na.rm=T)
volminmean_5 <- rowMeans(volmin_5,na.rm=T)
volminmean_6 <- rowMeans(volmin_6,na.rm=T)

volmax_1 <- as.data.frame(lapply(volmax[,c(10:19)],as.numeric),ncol=10)
volmax_2 <- as.data.frame(lapply(volmax[,c(20:29)],as.numeric),ncol=10)
volmax_3 <- as.data.frame(lapply(volmax[,c(30:39)],as.numeric),ncol=10)
volmax_4 <- as.data.frame(lapply(volmax[,c(40:49)],as.numeric),ncol=10)
volmax_5 <- as.data.frame(lapply(volmax[,c(50:59)],as.numeric),ncol=10)
volmax_6 <- as.data.frame(lapply(volmax[,c(60:69)],as.numeric),ncol=10)
volmaxmean_1 <- rowMeans(volmax_1,na.rm=T)
volmaxmean_2 <- rowMeans(volmax_2,na.rm=T)
volmaxmean_3 <- rowMeans(volmax_3,na.rm=T)
volmaxmean_4 <- rowMeans(volmax_4,na.rm=T)
volmaxmean_5 <- rowMeans(volmax_5,na.rm=T)
volmaxmean_6 <- rowMeans(volmax_6,na.rm=T)


esdmin_1 <- as.data.frame(lapply(esdmin[,c(10:19)],as.numeric),ncol=10)
esdmin_2 <- as.data.frame(lapply(esdmin[,c(20:29)],as.numeric),ncol=10)
esdmin_3 <- as.data.frame(lapply(esdmin[,c(30:39)],as.numeric),ncol=10)
esdmin_4 <- as.data.frame(lapply(esdmin[,c(40:49)],as.numeric),ncol=10)
esdmin_5 <- as.data.frame(lapply(esdmin[,c(50:59)],as.numeric),ncol=10)
esdmin_6 <- as.data.frame(lapply(esdmin[,c(60:69)],as.numeric),ncol=10)
esdminmean_1 <- rowMeans(esdmin_1,na.rm=T)
esdminmean_2 <- rowMeans(esdmin_2,na.rm=T)
esdminmean_3 <- rowMeans(esdmin_3,na.rm=T)
esdminmean_4 <- rowMeans(esdmin_4,na.rm=T)
esdminmean_5 <- rowMeans(esdmin_5,na.rm=T)
esdminmean_6 <- rowMeans(esdmin_6,na.rm=T)

esdmax_1 <- as.data.frame(lapply(esdmax[,c(10:19)],as.numeric),ncol=10)
esdmax_2 <- as.data.frame(lapply(esdmax[,c(20:29)],as.numeric),ncol=10)
esdmax_3 <- as.data.frame(lapply(esdmax[,c(30:39)],as.numeric),ncol=10)
esdmax_4 <- as.data.frame(lapply(esdmax[,c(40:49)],as.numeric),ncol=10)
esdmax_5 <- as.data.frame(lapply(esdmax[,c(50:59)],as.numeric),ncol=10)
esdmax_6 <- as.data.frame(lapply(esdmax[,c(60:69)],as.numeric),ncol=10)
esdmaxmean_1 <- rowMeans(esdmax_1,na.rm=T)
esdmaxmean_2 <- rowMeans(esdmax_2,na.rm=T)
esdmaxmean_3 <- rowMeans(esdmax_3,na.rm=T)
esdmaxmean_4 <- rowMeans(esdmax_4,na.rm=T)
esdmaxmean_5 <- rowMeans(esdmax_5,na.rm=T)
esdmaxmean_6 <- rowMeans(esdmax_6,na.rm=T)


sensordata_1 <- cbind(volmin[,c(1:9)],volminmean_1, volmaxmean_1,esdminmean_1,esdmaxmean_1)
sensordata_2 <- cbind(volmin[,c(1:9)],volminmean_2, volmaxmean_2,esdminmean_2,esdmaxmean_2)
sensordata_3 <- cbind(volmin[,c(1:9)],volminmean_3, volmaxmean_3,esdminmean_3,esdmaxmean_3)
sensordata_4 <- cbind(volmin[,c(1:9)],volminmean_4, volmaxmean_4,esdminmean_4,esdmaxmean_4)
sensordata_5 <- cbind(volmin[,c(1:9)],volminmean_5, volmaxmean_5,esdminmean_5,esdmaxmean_5)
sensordata_6 <- cbind(volmin[,c(1:9)],volminmean_6, volmaxmean_6,esdminmean_6,esdmaxmean_6)

# day 1 to day 10
sensorcomp_1 <- sensordata_1[complete.cases(sensordata_1),] #428903

sensor1_BRR1 <- subset(sensorcomp_1, TaskName=="Alternator Belt R&R\t"|
                 TaskName=="Alternator Belt ADJ\t"|
                 TaskName=="Battery R&R\t"|
                 TaskName=="Battery ADJ\t"|
                 TaskName=="Charging/starting System DIA\t"|
                 TaskName=="Electrical System DIA\t"|
                 TaskName=="Alternator R&R\t"|
                 TaskName=="Alternator - REP\t"|
                 TaskName=="Battery/hold Down R&R\t"|
                 TaskName=="Battery Cable R&R\t"|
                 TaskName=="Battery Cable REP\t"|
                 TaskName== "Battery, Charge REP\t"|
                 TaskName=="Voltage Regulator R&R\t"|
                 TaskName=="Electrical System OTH\t"|
                 TaskName=="Battery,egr Warning Lite R&R\t"|
                 TaskName=="Battery Aux Engine Aerial R&R\t"|
                 TaskName=="Battery Mpu/forklift R&R\t"|
                 TaskName=="Battery Aux Engine Aerial R&R\t"|
                 TaskName=="Alternator Mpu R&R\t"|
                 TaskName=="Battery Vpu R&R\t"|
                 TaskName=="Jump Start Vehicle OTH\t"|
                 TaskName=="Battery,auxillary Engine R&R\t"|
                 TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                 TaskName=="Starter R&R\t"|
                 TaskName=="Spark Plugs R&R\t"|
                 TaskName=="Wiring/harness R&R\t"|
                 TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                 TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                 TaskName=="Jump Start Vehicle\t"|
                 TaskName=="Aerial/Electrical System DIA\t"|
                 TaskName=="Aerial/Electrical system OTH\t"|
                 TaskName=="EVACUATE & RECHARGE - R&R\t"|
                 TaskName=="Engine  system OTH\t"|
                 TaskName=="Engine System DIA\t")  #75056

sensor1_BRR0 <- subset(sensorcomp_1, TaskName!="Alternator Belt R&R\t"&
                 TaskName!="Alternator Belt ADJ\t"&
                 TaskName!="Battery R&R\t"&
                 TaskName!="Battery ADJ\t"&
                 TaskName!="Charging/starting System DIA\t"&
                 TaskName!="Electrical System DIA\t"&
                 TaskName!="Alternator R&R\t"&
                 TaskName!="Alternator - REP\t"&
                 TaskName!="Battery/hold Down R&R\t"&
                 TaskName!="Battery Cable R&R\t"&
                 TaskName!="Battery Cable REP\t"&
                 TaskName!= "Battery, Charge REP\t"&
                 TaskName!="Voltage Regulator R&R\t"&
                 TaskName!="Electrical System OTH\t"&
                 TaskName!="Battery,egr Warning Lite R&R\t"&
                 TaskName!="Battery Aux Engine Aerial R&R\t"&
                 TaskName!="Battery Mpu/forklift R&R\t"&
                 TaskName!="Battery Aux Engine Aerial R&R\t"&
                 TaskName!="Alternator Mpu R&R\t"&
                 TaskName!="Battery Vpu R&R\t"&
                 TaskName!="Jump Start Vehicle OTH\t"&
                 TaskName!="Battery,auxillary Engine R&R\t"&
                 TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                 TaskName!="Starter R&R\t"&
                 TaskName!="Spark Plugs R&R\t"&
                 TaskName!="Wiring/harness R&R\t"&
                 TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                 TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                 TaskName!="Jump Start Vehicle\t"&
                 TaskName!="Aerial/Electrical System DIA\t"&
                 TaskName!="Aerial/Electrical system OTH\t"&
                 TaskName!="EVACUATE & RECHARGE - R&R\t"&
                 TaskName!="Engine  system OTH\t"&
                 TaskName!="Engine System DIA\t") #353847

sensor1_BRR0$BRR <-0
sensor1_BRR1$BRR <-1

# day 11-day 20
sensorcomp_2 <- sensordata_2[complete.cases(sensordata_2),] #400386
sensor2_BRR1 <- subset(sensorcomp_2, TaskName=="Alternator Belt R&R\t"|
                         TaskName=="Alternator Belt ADJ\t"|
                         TaskName=="Battery R&R\t"|
                         TaskName=="Battery ADJ\t"|
                         TaskName=="Charging/starting System DIA\t"|
                         TaskName=="Electrical System DIA\t"|
                         TaskName=="Alternator R&R\t"|
                         TaskName=="Alternator - REP\t"|
                         TaskName=="Battery/hold Down R&R\t"|
                         TaskName=="Battery Cable R&R\t"|
                         TaskName=="Battery Cable REP\t"|
                         TaskName== "Battery, Charge REP\t"|
                         TaskName=="Voltage Regulator R&R\t"|
                         TaskName=="Electrical System OTH\t"|
                         TaskName=="Battery,egr Warning Lite R&R\t"|
                         TaskName=="Battery Aux Engine Aerial R&R\t"|
                         TaskName=="Battery Mpu/forklift R&R\t"|
                         TaskName=="Battery Aux Engine Aerial R&R\t"|
                         TaskName=="Alternator Mpu R&R\t"|
                         TaskName=="Battery Vpu R&R\t"|
                         TaskName=="Jump Start Vehicle OTH\t"|
                         TaskName=="Battery,auxillary Engine R&R\t"|
                         TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                         TaskName=="Starter R&R\t"|
                         TaskName=="Spark Plugs R&R\t"|
                         TaskName=="Wiring/harness R&R\t"|
                         TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                         TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                         TaskName=="Jump Start Vehicle\t"|
                         TaskName=="Aerial/Electrical System DIA\t"|
                         TaskName=="Aerial/Electrical system OTH\t"|
                         TaskName=="EVACUATE & RECHARGE - R&R\t"|
                         TaskName=="Engine  system OTH\t"|
                         TaskName=="Engine System DIA\t")  #66944

sensor2_BRR0 <- subset(sensorcomp_2, TaskName!="Alternator Belt R&R\t"&
                         TaskName!="Alternator Belt ADJ\t"&
                         TaskName!="Battery R&R\t"&
                         TaskName!="Battery ADJ\t"&
                         TaskName!="Charging/starting System DIA\t"&
                         TaskName!="Electrical System DIA\t"&
                         TaskName!="Alternator R&R\t"&
                         TaskName!="Alternator - REP\t"&
                         TaskName!="Battery/hold Down R&R\t"&
                         TaskName!="Battery Cable R&R\t"&
                         TaskName!="Battery Cable REP\t"&
                         TaskName!= "Battery, Charge REP\t"&
                         TaskName!="Voltage Regulator R&R\t"&
                         TaskName!="Electrical System OTH\t"&
                         TaskName!="Battery,egr Warning Lite R&R\t"&
                         TaskName!="Battery Aux Engine Aerial R&R\t"&
                         TaskName!="Battery Mpu/forklift R&R\t"&
                         TaskName!="Battery Aux Engine Aerial R&R\t"&
                         TaskName!="Alternator Mpu R&R\t"&
                         TaskName!="Battery Vpu R&R\t"&
                         TaskName!="Jump Start Vehicle OTH\t"&
                         TaskName!="Battery,auxillary Engine R&R\t"&
                         TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                         TaskName!="Starter R&R\t"&
                         TaskName!="Spark Plugs R&R\t"&
                         TaskName!="Wiring/harness R&R\t"&
                         TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                         TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                         TaskName!="Jump Start Vehicle\t"&
                         TaskName!="Aerial/Electrical System DIA\t"&
                         TaskName!="Aerial/Electrical system OTH\t"&
                         TaskName!="EVACUATE & RECHARGE - R&R\t"&
                         TaskName!="Engine  system OTH\t"&
                         TaskName!="Engine System DIA\t") #333442

sensor2_BRR0$BRR <-0
sensor2_BRR1$BRR <-1


# day 21 to day 30
sensor1_BRR0 <- subset(sensorcomp_1, TaskName!="Alternator Belt R&R\t"&
                         TaskName!="Alternator Belt ADJ\t"&
                         TaskName!="Battery R&R\t"&
                         TaskName!="Battery ADJ\t"&
                         TaskName!="Charging/starting System DIA\t"&
                         TaskName!="Electrical System DIA\t"&
                         TaskName!="Alternator R&R\t"&
                         TaskName!="Alternator - REP\t"&
                         TaskName!="Battery/hold Down R&R\t"&
                         TaskName!="Battery Cable R&R\t"&
                         TaskName!="Battery Cable REP\t"&
                         TaskName!= "Battery, Charge REP\t"&
                         TaskName!="Voltage Regulator R&R\t"&
                         TaskName!="Electrical System OTH\t"&
                         TaskName!="Battery,egr Warning Lite R&R\t"&
                         TaskName!="Battery Aux Engine Aerial R&R\t"&
                         TaskName!="Battery Mpu/forklift R&R\t"&
                         TaskName!="Battery Aux Engine Aerial R&R\t"&
                         TaskName!="Alternator Mpu R&R\t"&
                         TaskName!="Battery Vpu R&R\t"&
                         TaskName!="Jump Start Vehicle OTH\t"&
                         TaskName!="Battery,auxillary Engine R&R\t"&
                         TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                         TaskName!="Starter R&R\t"&
                         TaskName!="Spark Plugs R&R\t"&
                         TaskName!="Wiring/harness R&R\t"&
                         TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                         TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                         TaskName!="Jump Start Vehicle\t"&
                         TaskName!="Aerial/Electrical System DIA\t"&
                         TaskName!="Aerial/Electrical system OTH\t"&
                         TaskName!="EVACUATE & RECHARGE - R&R\t"&
                         TaskName!="Engine  system OTH\t"&
                         TaskName!="Engine System DIA\t") #353847

sensor1_BRR0$BRR <-0
sensor1_BRR1$BRR <-1

# day 11-day 20
sensorcomp_3 <- sensordata_3[complete.cases(sensordata_3),] #394440
sensor3_BRR1 <- subset(sensorcomp_3, TaskName=="Alternator Belt R&R\t"|
                         TaskName=="Alternator Belt ADJ\t"|
                         TaskName=="Battery R&R\t"|
                         TaskName=="Battery ADJ\t"|
                         TaskName=="Charging/starting System DIA\t"|
                         TaskName=="Electrical System DIA\t"|
                         TaskName=="Alternator R&R\t"|
                         TaskName=="Alternator - REP\t"|
                         TaskName=="Battery/hold Down R&R\t"|
                         TaskName=="Battery Cable R&R\t"|
                         TaskName=="Battery Cable REP\t"|
                         TaskName== "Battery, Charge REP\t"|
                         TaskName=="Voltage Regulator R&R\t"|
                         TaskName=="Electrical System OTH\t"|
                         TaskName=="Battery,egr Warning Lite R&R\t"|
                         TaskName=="Battery Aux Engine Aerial R&R\t"|
                         TaskName=="Battery Mpu/forklift R&R\t"|
                         TaskName=="Battery Aux Engine Aerial R&R\t"|
                         TaskName=="Alternator Mpu R&R\t"|
                         TaskName=="Battery Vpu R&R\t"|
                         TaskName=="Jump Start Vehicle OTH\t"|
                         TaskName=="Battery,auxillary Engine R&R\t"|
                         TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                         TaskName=="Starter R&R\t"|
                         TaskName=="Spark Plugs R&R\t"|
                         TaskName=="Wiring/harness R&R\t"|
                         TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                         TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                         TaskName=="Jump Start Vehicle\t"|
                         TaskName=="Aerial/Electrical System DIA\t"|
                         TaskName=="Aerial/Electrical system OTH\t"|
                         TaskName=="EVACUATE & RECHARGE - R&R\t"|
                         TaskName=="Engine  system OTH\t"|
                         TaskName=="Engine System DIA\t")  #65578

sensor3_BRR0 <- subset(sensorcomp_3, TaskName!="Alternator Belt R&R\t"&
                         TaskName!="Alternator Belt ADJ\t"&
                         TaskName!="Battery R&R\t"&
                         TaskName!="Battery ADJ\t"&
                         TaskName!="Charging/starting System DIA\t"&
                         TaskName!="Electrical System DIA\t"&
                         TaskName!="Alternator R&R\t"&
                         TaskName!="Alternator - REP\t"&
                         TaskName!="Battery/hold Down R&R\t"&
                         TaskName!="Battery Cable R&R\t"&
                         TaskName!="Battery Cable REP\t"&
                         TaskName!= "Battery, Charge REP\t"&
                         TaskName!="Voltage Regulator R&R\t"&
                         TaskName!="Electrical System OTH\t"&
                         TaskName!="Battery,egr Warning Lite R&R\t"&
                         TaskName!="Battery Aux Engine Aerial R&R\t"&
                         TaskName!="Battery Mpu/forklift R&R\t"&
                         TaskName!="Battery Aux Engine Aerial R&R\t"&
                         TaskName!="Alternator Mpu R&R\t"&
                         TaskName!="Battery Vpu R&R\t"&
                         TaskName!="Jump Start Vehicle OTH\t"&
                         TaskName!="Battery,auxillary Engine R&R\t"&
                         TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                         TaskName!="Starter R&R\t"&
                         TaskName!="Spark Plugs R&R\t"&
                         TaskName!="Wiring/harness R&R\t"&
                         TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                         TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                         TaskName!="Jump Start Vehicle\t"&
                         TaskName!="Aerial/Electrical System DIA\t"&
                         TaskName!="Aerial/Electrical system OTH\t"&
                         TaskName!="EVACUATE & RECHARGE - R&R\t"&
                         TaskName!="Engine  system OTH\t"&
                         TaskName!="Engine System DIA\t") #328862

sensor3_BRR0$BRR <-0
sensor3_BRR1$BRR <-1
# day 31-60 train as brr=0 for all cases
#BRR=1 use day 0-30
names(sensor1_BRR1)[10:13] <-c("volminmean","volmaxmean","esdminmean","esdmaxmean")
names(sensor2_BRR1)[10:13] <-c("volminmean","volmaxmean","esdminmean","esdmaxmean")
names(sensor3_BRR1)[10:13] <-c("volminmean","volmaxmean","esdminmean","esdmaxmean")
BRR1 <- rbind(sensor1_BRR1, sensor2_BRR1,sensor3_BRR1) #207578
write.csv(BRR1, "data_for_trail/data_for_model/0605/sensorBRR1.csv", )

#BRR=0 use day 0-60
sensorcomp_4 <- sensordata_4[complete.cases(sensordata_4),]#387030
sensorcomp_5 <- sensordata_5[complete.cases(sensordata_5),] #382305
sensorcomp_6 <- sensordata_6[complete.cases(sensordata_6),] #374378

names(sensor1_BRR0)[10:13] <-c("volminmean","volmaxmean","esdminmean","esdmaxmean")
names(sensor2_BRR0)[10:13] <-c("volminmean","volmaxmean","esdminmean","esdmaxmean")
names(sensor3_BRR0)[10:13] <-c("volminmean","volmaxmean","esdminmean","esdmaxmean")
names(sensorcomp_4)[10:13] <-c("volminmean","volmaxmean","esdminmean","esdmaxmean")
names(sensorcomp_5)[10:13] <-c("volminmean","volmaxmean","esdminmean","esdmaxmean")
names(sensorcomp_6)[10:13] <-c("volminmean","volmaxmean","esdminmean","esdmaxmean")

sensorcomp_4$BRR <-0
sensorcomp_5$BRR <-0
sensorcomp_6$BRR <-0
BRR0 <- rbind(sensor1_BRR0, sensor2_BRR0,sensor3_BRR0, 
              sensorcomp_4, sensorcomp_5, sensorcomp_6) #2159864

BRR0_sample <- BRR0[sample(1:nrow(BRR0), 200000,
                          replace=FALSE),] 

write.csv(BRR0_sample, "data_for_trail/data_for_model/0605/sensorBRR0_sample.csv")
write.csv(BRR0, "data_for_trail/data_for_model/0605/sensorBRR0.csv" )

sensor <- rbind(BRR1, BRR0_sample)
write.csv(sensor, "data_for_trail/data_for_model/0605/sensor.csv" )


# train sensor data in daily bases
library(data.table)
volmin <- read.csv("data_for_trail/data_step_by_step/sensor_data/volmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
volmax <- read.csv("data_for_trail/data_step_by_step/sensor_data/volmax.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmin <- read.csv("data_for_trail/data_step_by_step/sensor_data/esdmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmax <- read.csv("data_for_trail/data_step_by_step/sensor_data/esdmax.csv",sep="|",header=T,stringsAsFactors=FALSE)
volmin <- as.data.frame(volmin)
volmax <- as.data.frame(volmax)
esdmin <- as.data.frame(esdmin)
esdmax <- as.data.frame(esdmax)


sensor_05 <- cbind(volmin[,c(1:9)],volmin[,c(10:14)],volmax[,c(10:14)],esdmin[,c(10:14)],esdmax[,c(10:14)])
sensor_10 <- cbind(volmin[,c(1:9)],volmin[,c(15:19)],volmax[,c(15:19)],esdmin[,c(15:19)],esdmax[,c(15:19)])
sensor_15 <- cbind(volmin[,c(1:9)],volmin[,c(20:24)],volmax[,c(20:24)],esdmin[,c(20:24)],esdmax[,c(20:24)])
sensor_20 <- cbind(volmin[,c(1:9)],volmin[,c(25:29)],volmax[,c(25:29)],esdmin[,c(25:29)],esdmax[,c(25:29)])
sensor_25 <- cbind(volmin[,c(1:9)],volmin[,c(30:34)],volmax[,c(30:34)],esdmin[,c(30:34)],esdmax[,c(30:34)])
sensor_30 <- cbind(volmin[,c(1:9)],volmin[,c(35:39)],volmax[,c(35:39)],esdmin[,c(35:39)],esdmax[,c(35:39)])
sensor_35 <- cbind(volmin[,c(1:9)],volmin[,c(40:44)],volmax[,c(40:44)],esdmin[,c(40:44)],esdmax[,c(40:44)])
sensor_40 <- cbind(volmin[,c(1:9)],volmin[,c(45:49)],volmax[,c(45:49)],esdmin[,c(45:49)],esdmax[,c(45:49)])
sensor_45 <- cbind(volmin[,c(1:9)],volmin[,c(50:54)],volmax[,c(50:54)],esdmin[,c(50:54)],esdmax[,c(50:54)])
sensor_50 <- cbind(volmin[,c(1:9)],volmin[,c(55:59)],volmax[,c(55:59)],esdmin[,c(55:59)],esdmax[,c(55:59)])
sensor_55 <- cbind(volmin[,c(1:9)],volmin[,c(60:64)],volmax[,c(60:64)],esdmin[,c(60:64)],esdmax[,c(60:64)])
sensor_60 <- cbind(volmin[,c(1:9)],volmin[,c(65:69)],volmax[,c(65:69)],esdmin[,c(65:69)],esdmax[,c(65:69)])


sensor_05_comp <- sensor_05[complete.cases(sensor_05),] 
sensor_10_comp <- sensor_10[complete.cases(sensor_10),] 
sensor_15_comp <- sensor_15[complete.cases(sensor_15),] 
sensor_20_comp <- sensor_20[complete.cases(sensor_20),] 
sensor_25_comp <- sensor_25[complete.cases(sensor_25),] 
sensor_30_comp <- sensor_30[complete.cases(sensor_30),] 
sensor_35_comp <- sensor_35[complete.cases(sensor_35),] 
sensor_40_comp <- sensor_40[complete.cases(sensor_40),] 
sensor_45_comp <- sensor_45[complete.cases(sensor_45),] 
sensor_50_comp <- sensor_50[complete.cases(sensor_50),] 
sensor_55_comp <- sensor_55[complete.cases(sensor_55),] 
sensor_60_comp <- sensor_60[complete.cases(sensor_60),] 

sensor_05[,c(10:29)] <- sapply(sensor_05[,c(10:29)] , as.numeric)
sensor_05_volminmean <- rowMeans(sensor_05[,c(10:14)],na.rm=T)
sensor_05_volmaxmean <- rowMeans(sensor_05[,c(15:19)],na.rm=T)
sensor_05_esdminmean <- rowMeans(sensor_05[,c(20:24)],na.rm=T)
sensor_05_esdmaxmean <- rowMeans(sensor_05[,c(25:29)],na.rm=T)
sensor_05_mean <- cbind(sensor_05[,c(1:9)],sensor_05_volminmean,sensor_05_volmaxmean,
                        sensor_05_esdminmean,sensor_05_esdmaxmean)
#extract BRR=1
sensor_05_BRR1 <- subset(sensor_05_mean, TaskName=="Alternator Belt R&R\t"|
                 TaskName=="Alternator Belt ADJ\t"|
                 TaskName=="Battery R&R\t"|
                 TaskName=="Battery ADJ\t"|
                 TaskName=="Charging/starting System DIA\t"|
                 TaskName=="Electrical System DIA\t"|
                 TaskName=="Alternator R&R\t"|
                 TaskName=="Alternator - REP\t"|
                 TaskName=="Battery/hold Down R&R\t"|
                 TaskName=="Battery Cable R&R\t"|
                 TaskName=="Battery Cable REP\t"|
                 TaskName== "Battery, Charge REP\t"|
                 TaskName=="Voltage Regulator R&R\t"|
                 TaskName=="Electrical System OTH\t"|
                 TaskName=="Battery,egr Warning Lite R&R\t"|
                 TaskName=="Battery Aux Engine Aerial R&R\t"|
                 TaskName=="Battery Mpu/forklift R&R\t"|
                 TaskName=="Battery Aux Engine Aerial R&R\t"|
                 TaskName=="Alternator Mpu R&R\t"|
                 TaskName=="Battery Vpu R&R\t"|
                 TaskName=="Jump Start Vehicle OTH\t"|
                 TaskName=="Battery,auxillary Engine R&R\t"|
                 TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                 TaskName=="Starter R&R\t"|
                 TaskName=="Spark Plugs R&R\t"|
                 TaskName=="Wiring/harness R&R\t"|
                 TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                 TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                 TaskName=="Jump Start Vehicle\t"|
                 TaskName=="Aerial/Electrical System DIA\t"|
                 TaskName=="Aerial/Electrical system OTH\t"|
                 TaskName=="EVACUATE & RECHARGE - R&R\t"|
                 TaskName=="Engine  system OTH\t"|
                 TaskName=="Engine System DIA\t")  #13096

sensor_05_BRR0 <- subset(sensor_05_mean, TaskName!="Alternator Belt R&R\t"&
                 TaskName!="Alternator Belt ADJ\t"&
                 TaskName!="Battery R&R\t"&
                 TaskName!="Battery ADJ\t"&
                 TaskName!="Charging/starting System DIA\t"&
                 TaskName!="Electrical System DIA\t"&
                 TaskName!="Alternator R&R\t"&
                 TaskName!="Alternator - REP\t"&
                 TaskName!="Battery/hold Down R&R\t"&
                 TaskName!="Battery Cable R&R\t"&
                 TaskName!="Battery Cable REP\t"&
                 TaskName!= "Battery, Charge REP\t"&
                 TaskName!="Voltage Regulator R&R\t"&
                 TaskName!="Electrical System OTH\t"&
                 TaskName!="Battery,egr Warning Lite R&R\t"&
                 TaskName!="Battery Aux Engine Aerial R&R\t"&
                 TaskName!="Battery Mpu/forklift R&R\t"&
                 TaskName!="Battery Aux Engine Aerial R&R\t"&
                 TaskName!="Alternator Mpu R&R\t"&
                 TaskName!="Battery Vpu R&R\t"&
                 TaskName!="Jump Start Vehicle OTH\t"&
                 TaskName!="Battery,auxillary Engine R&R\t"&
                 TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                 TaskName!="Starter R&R\t"&
                 TaskName!="Spark Plugs R&R\t"&
                 TaskName!="Wiring/harness R&R\t"&
                 TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                 TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                 TaskName!="Jump Start Vehicle\t"&
                 TaskName!="Aerial/Electrical System DIA\t"&
                 TaskName!="Aerial/Electrical system OTH\t"&
                 TaskName!="EVACUATE & RECHARGE - R&R\t"&
                 TaskName!="Engine  system OTH\t"&
                 TaskName!="Engine System DIA\t")  #77182

sensor_05_BRR0$BRR <-0
sensor_05_BRR1$BRR <-1



sensor_10[,c(10:29)] <- sapply(sensor_10[,c(10:29)] , as.numeric)
sensor_10_volminmean <- rowMeans(sensor_10[,c(10:14)],na.rm=T)
sensor_10_volmaxmean <- rowMeans(sensor_10[,c(15:19)],na.rm=T)
sensor_10_esdminmean <- rowMeans(sensor_10[,c(20:24)],na.rm=T)
sensor_10_esdmaxmean <- rowMeans(sensor_10[,c(25:29)],na.rm=T)
sensor_10_mean <- cbind(sensor_10[,c(1:9)],sensor_10_volminmean,sensor_10_volmaxmean,sensor_10_esdminmean,sensor_10_esdmaxmean)
sensor_10_BRR1 <- subset(sensor_10_mean, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #15223

sensor_10_BRR0 <- subset(sensor_10_mean, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #86046

sensor_10_BRR0$BRR <-0
sensor_10_BRR1$BRR <-1



sensor_15[,c(10:29)] <- sapply(sensor_15[,c(10:29)] , as.numeric)
sensor_15_volminmean <- rowMeans(sensor_15[,c(10:14)],na.rm=T)
sensor_15_volmaxmean <- rowMeans(sensor_15[,c(15:19)],na.rm=T)
sensor_15_esdminmean <- rowMeans(sensor_15[,c(20:24)],na.rm=T)
sensor_15_esdmaxmean <- rowMeans(sensor_15[,c(25:29)],na.rm=T)
sensor_15_mean <- cbind(sensor_15[,c(1:9)],sensor_15_volminmean,sensor_15_volmaxmean,
                        sensor_15_esdminmean,sensor_15_esdmaxmean)
sensor_15_BRR1 <- subset(sensor_15_mean, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #15223

sensor_15_BRR0 <- subset(sensor_15_mean, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #86046

sensor_15_BRR0$BRR <-0
sensor_15_BRR1$BRR <-1


names(sensor_05_BRR0)[10:13] <- c("volminmean1","volmaxmean1","esdminmean1","esdmaxmean1")
names(sensor_05_BRR1)[10:13] <- c("volminmean1","volmaxmean1","esdminmean1","esdmaxmean1")
names(sensor_10_BRR0)[10:13] <- c("volminmean2","volmaxmean2","esdminmean2","esdmaxmean2")
names(sensor_10_BRR1)[10:13] <- c("volminmean2","volmaxmean2","esdminmean2","esdmaxmean2")
names(sensor_15_BRR0)[10:13] <- c("volminmean3","volmaxmean3","esdminmean3","esdmaxmean3")
names(sensor_15_BRR1)[10:13] <- c("volminmean3","volmaxmean3","esdminmean3","esdmaxmean3")

sensor_05 <- rbind(sensor_05_BRR0,sensor_05_BRR1)
sensor_10 <- rbind(sensor_10_BRR0,sensor_10_BRR1)
sensor_15 <- rbind(sensor_15_BRR0,sensor_15_BRR1)
train_1 <- cbind(sensor_05,sensor_10,sensor_15)
train_1 <- train_1[complete.cases(train_1),]  #350263





sensor_15_comp[,c(10:29)] <- sapply(sensor_15_comp[,c(10:29)] , as.numeric)
sensor_15_volminmean <- rowMeans(sensor_15_comp[,c(10:14)],na.rm=T)
sensor_15_volmaxmean <- rowMeans(sensor_15_comp[,c(15:19)],na.rm=T)
sensor_15_esdminmean <- rowMeans(sensor_15_comp[,c(20:24)],na.rm=T)
sensor_15_esdmaxmean <- rowMeans(sensor_15_comp[,c(25:29)],na.rm=T)
sensor_15_mean <- cbind(sensor_15_comp[,c(1:9)],sensor_15_volminmean,sensor_15_volmaxmean,sensor_15_esdminmean,sensor_15_esdmaxmean)
sensor_15_BRR1 <- subset(sensor_15_mean, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #13096

sensor_15_BRR0 <- subset(sensor_15_mean, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #77182

sensor_15_BRR0$BRR <-0
sensor_15_BRR1$BRR <-1


sensor_20_comp[,c(10:29)] <- sapply(sensor_20_comp[,c(10:29)] , as.numeric)
sensor_20_volminmean <- rowMeans(sensor_20_comp[,c(10:14)],na.rm=T)
sensor_20_volmaxmean <- rowMeans(sensor_20_comp[,c(15:19)],na.rm=T)
sensor_20_esdminmean <- rowMeans(sensor_20_comp[,c(20:24)],na.rm=T)
sensor_20_esdmaxmean <- rowMeans(sensor_20_comp[,c(25:29)],na.rm=T)
sensor_20_mean <- cbind(sensor_20_comp[,c(1:9)],sensor_20_volminmean,sensor_20_volmaxmean,sensor_20_esdminmean,sensor_20_esdmaxmean)
sensor_20_BRR1 <- subset(sensor_20_mean, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #15223

sensor_20_BRR0 <- subset(sensor_20_mean, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #86046

sensor_20_BRR0$BRR <-0
sensor_20_BRR1$BRR <-1



sensor_25_comp[,c(10:29)] <- sapply(sensor_25_comp[,c(10:29)] , as.numeric)
sensor_25_volminmean <- rowMeans(sensor_25_comp[,c(10:14)],na.rm=T)
sensor_25_volmaxmean <- rowMeans(sensor_25_comp[,c(15:19)],na.rm=T)
sensor_25_esdminmean <- rowMeans(sensor_25_comp[,c(20:24)],na.rm=T)
sensor_25_esdmaxmean <- rowMeans(sensor_25_comp[,c(25:29)],na.rm=T)
sensor_25_mean <- cbind(sensor_25_comp[,c(1:9)],sensor_25_volminmean,sensor_25_volmaxmean,
                        sensor_25_esdminmean,sensor_25_esdmaxmean)
sensor_25_BRR1 <- subset(sensor_25_mean, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #13096

sensor_25_BRR0 <- subset(sensor_25_mean, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #77182

sensor_25_BRR0$BRR <-0
sensor_25_BRR1$BRR <-1


sensor_30_comp[,c(10:29)] <- sapply(sensor_30_comp[,c(10:29)] , as.numeric)
sensor_30_volminmean <- rowMeans(sensor_30_comp[,c(10:14)],na.rm=T)
sensor_30_volmaxmean <- rowMeans(sensor_30_comp[,c(15:19)],na.rm=T)
sensor_30_esdminmean <- rowMeans(sensor_30_comp[,c(20:24)],na.rm=T)
sensor_30_esdmaxmean <- rowMeans(sensor_30_comp[,c(25:29)],na.rm=T)
sensor_30_mean <- cbind(sensor_30_comp[,c(1:9)],sensor_30_volminmean,sensor_30_volmaxmean,sensor_30_esdminmean,sensor_30_esdmaxmean)
sensor_30_BRR1 <- subset(sensor_30_mean, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #15223

sensor_30_BRR0 <- subset(sensor_30_mean, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #86046

sensor_30_BRR0$BRR <-0
sensor_30_BRR1$BRR <-1

sensor_35_comp$BRR <-0
sensor_40_comp$BRR <-0
sensor_45_comp$BRR <-0
sensor_50_comp$BRR <-0
sensor_55_comp$BRR <-0
sensor_60_comp$BRR <-0


names(sensor_05_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_05_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_10_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_10_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_15_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_15_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_20_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_20_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_25_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_25_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_30_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_30_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")

names(sensor_35_comp)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_40_comp)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_45_comp)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_50_comp)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_55_comp)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_60_comp)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")

train_data <- rbind(sensor_05_BRR0,sensor_05_BRR1,sensor_10_BRR0,sensor_10_BRR1,
               sensor_15_BRR0,sensor_15_BRR1,sensor_20_BRR0,sensor_20_BRR1,
               sensor_25_BRR0,sensor_25_BRR1,sensor_30_BRR0,sensor_30_BRR1,
               sensor_35_comp,sensor_45_comp,sensor_55_comp)
test_data <- rbind(sensor_40_comp,sensor_50_comp,sensor_60_comp)
write.csv(train_data, "data_for_trail/data_for_model/0606/sensor_data_training.csv")
write.csv(test_data, "data_for_trail/data_for_model/0606/sensor_data_testing.csv")
write.csv(sensor_40_comp, "data_for_trail/data_for_model/0606/sensor_data_testing_40days.csv")
write.csv(sensor_50_comp, "data_for_trail/data_for_model/0606/sensor_data_testing_50days.csv")
write.csv(sensor_60_comp, "data_for_trail/data_for_model/0606/sensor_data_testing_60days.csv")


# June 6 create test dataset to 
volmin <-fread("data_for_trail/data_step_by_step/sensor_data/volmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
volmax <- fread("data_for_trail/data_step_by_step/sensor_data/volmax.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmin <- fread("data_for_trail/data_step_by_step/sensor_data/esdmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmax <- fread("data_for_trail/data_step_by_step/sensor_data/esdmax.csv",sep="|",header=T,stringsAsFactors=FALSE)
volmin <- as.data.frame(volmin)
volmax <- as.data.frame(volmax)
esdmin <- as.data.frame(esdmin)
esdmax <- as.data.frame(esdmax)

sensor_07 <- cbind(volmin[,c(1:9)],volmin[,c(12:16)],volmax[,c(12:16)],esdmin[,c(12:16)],esdmax[,c(12:16)])
sensor_12 <- cbind(volmin[,c(1:9)],volmin[,c(17:21)],volmax[,c(17:21)],esdmin[,c(17:21)],esdmax[,c(17:21)])
sensor_17 <- cbind(volmin[,c(1:9)],volmin[,c(22:26)],volmax[,c(22:26)],esdmin[,c(22:26)],esdmax[,c(22:26)])
sensor_22 <- cbind(volmin[,c(1:9)],volmin[,c(27:31)],volmax[,c(27:31)],esdmin[,c(27:31)],esdmax[,c(27:31)])

sensor_07_comp <- sensor_07[complete.cases(sensor_07),] #105356
sensor_12_comp <- sensor_12[complete.cases(sensor_12),] #68788
sensor_17_comp <- sensor_17[complete.cases(sensor_17),] #100309
sensor_22_comp <- sensor_22[complete.cases(sensor_22),] #101117

#extract BRR=1
sensor_07_BRR1 <- subset(sensor_07_comp, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #13096

sensor_07_BRR0 <- subset(sensor_07_comp, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #77182

sensor_07_BRR0$BRR <-0
sensor_07_BRR1$BRR <-1

sensor_12_BRR1 <- subset(sensor_12_comp, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #10253

sensor_12_BRR0 <- subset(sensor_12_comp, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #58535

sensor_12_BRR0$BRR <-0
sensor_12_BRR1$BRR <-1
sensor_17_BRR1 <- subset(sensor_17_comp, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #13096

sensor_17_BRR0 <- subset(sensor_17_comp, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #77182

sensor_17_BRR0$BRR <-0
sensor_17_BRR1$BRR <-1

sensor_22_BRR1 <- subset(sensor_22_comp, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #10253

sensor_22_BRR0 <- subset(sensor_22_comp, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #58535
sensor_22_BRR0$BRR <- 0
sensor_22_BRR1$BRR <- 1
names(sensor_07_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_07_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_12_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_12_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_17_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_17_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_22_BRR0)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
names(sensor_22_BRR1)[10:29] <-c("volminmean1","volminmean2","volminmean3","volminmean4","volminmean5",
                                 "volmaxmean1","volmaxmean2","volmaxmean3","volmaxmean4","volmaxmean5",
                                 "esdminmean1","esdminmean2","esdminmean3","esdminmean4","esdminmean5",
                                 "esdmaxmean1","esdmaxmean2","esdmaxmean3","esdmaxmean4","esdmaxmean5")
BRR0 <- rbind(sensor_07_BRR0,sensor_12_BRR0,sensor_17_BRR0,sensor_22_BRR0)  #317854
BRR0_sample <- BRR0[sample(1:nrow(BRR0), 60000,replace=FALSE),] 
BRR1 <- rbind(sensor_07_BRR1,sensor_12_BRR1,sensor_17_BRR1, sensor_22_BRR1) #57716
sensor_data <- rbind(BRR0_sample, BRR1)
#sensor data day 7
sensor_data_07 <- rbind(sensor_07_BRR0, sensor_07_BRR1)
write.csv(sensor_data_07, "data_for_trail/data_for_model/0605/sensor_data_5days_test_earlyprediction_07.csv")
#sensor data day 12
sensor_data_12 <- rbind(sensor_12_BRR0, sensor_12_BRR1)
write.csv(sensor_data_12, "data_for_trail/data_for_model/0605/sensor_data_5days_test_earlyprediction_12.csv")
#sensor data day 17
sensor_data_17 <- rbind(sensor_17_BRR0, sensor_17_BRR1)
write.csv(sensor_data_17, "data_for_trail/data_for_model/0605/sensor_data_5days_test_earlyprediction_17.csv")
#sensor data day 22
sensor_data_22 <- rbind(sensor_22_BRR0, sensor_22_BRR1)
write.csv(sensor_data_22, "data_for_trail/data_for_model/0605/sensor_data_5days_test_earlyprediction_22.csv")


# sensor data day 60

sensor_60 <- cbind(volmin[,c(1:9)],volmin[,c(65:69)],volmax[,c(65:69)],esdmin[,c(65:69)],esdmax[,c(65:69)])

sensor_60_comp <- sensor_60[complete.cases(sensor_60),] #101117
sensor_60_BRR1 <- subset(sensor_60_comp, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #13096

sensor_60_BRR0 <- subset(sensor_60_comp, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #77182

sensor_60_BRR0$BRR <-0
sensor_60_BRR1$BRR <-1
sensor_data_60 <- rbind(sensor_60_BRR0, sensor_60_BRR1)
write.csv(sensor_data_60, "data_for_trail/data_for_model/0605/sensor_data_5days_test_earlyprediction_60.csv")

# test on day 50
sensor_50 <- cbind(volmin[,c(1:9)],volmin[,c(55:59)],volmax[,c(55:59)],esdmin[,c(55:59)],esdmax[,c(55:59)])

sensor_50_comp <- sensor_50[complete.cases(sensor_50),] #101117
sensor_50_BRR1 <- subset(sensor_50_comp, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #13096

sensor_50_BRR0 <- subset(sensor_50_comp, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #77182

sensor_50_BRR0$BRR <-0
sensor_50_BRR1$BRR <-1
sensor_data_50 <- rbind(sensor_50_BRR0, sensor_50_BRR1)
write.csv(sensor_data_50, "data_for_trail/data_for_model/0605/sensor_data_5days_test_earlyprediction_50.csv")

# test on day 40
sensor_40 <- cbind(volmin[,c(1:9)],volmin[,c(45:49)],volmax[,c(45:49)],esdmin[,c(45:49)],esdmax[,c(45:49)])

sensor_40_comp <- sensor_40[complete.cases(sensor_40),] #101117
sensor_40_BRR1 <- subset(sensor_40_comp, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #13096

sensor_40_BRR0 <- subset(sensor_40_comp, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #77182

sensor_40_BRR0$BRR <-0
sensor_40_BRR1$BRR <-1
sensor_data_40 <- rbind(sensor_40_BRR0, sensor_40_BRR1)
write.csv(sensor_data_40, "data_for_trail/data_for_model/0605/sensor_data_5days_test_earlyprediction_40.csv")

# association rule minig
library(arules)
tran_data<- read.transactions("data_for_trail/data_for_model/0606/association_processed.csv",format="basket",sep=",")
tran_matrix <- as(tran_data,"matrix")
write.csv(tran_matrix,"data_for_trail/data_for_model/0606/tran_matrix.csv")


#make 5 days buckets


volmin <- read.csv("data_for_trail/data_step_by_step/sensor_data/volmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
volmax <- read.csv("data_for_trail/data_step_by_step/sensor_data/volmax.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmin <- read.csv("data_for_trail/data_step_by_step/sensor_data/esdmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmax <- read.csv("data_for_trail/data_step_by_step/sensor_data/esdmax.csv",sep="|",header=T,stringsAsFactors=FALSE)

sensor_05 <- cbind(volmin[,c(1:9)],volmin[,c(10:14)],volmax[,c(10:14)],esdmin[,c(10:14)],esdmax[,c(10:14)])
sensor_10 <- cbind(volmin[,c(1:9)],volmin[,c(15:19)],volmax[,c(15:19)],esdmin[,c(15:19)],esdmax[,c(15:19)])
sensor_15 <- cbind(volmin[,c(1:9)],volmin[,c(20:24)],volmax[,c(20:24)],esdmin[,c(20:24)],esdmax[,c(20:24)])
sensor_20 <- cbind(volmin[,c(1:9)],volmin[,c(25:29)],volmax[,c(25:29)],esdmin[,c(25:29)],esdmax[,c(25:29)])
sensor_25 <- cbind(volmin[,c(1:9)],volmin[,c(30:34)],volmax[,c(30:34)],esdmin[,c(30:34)],esdmax[,c(30:34)])
sensor_30 <- cbind(volmin[,c(1:9)],volmin[,c(35:39)],volmax[,c(35:39)],esdmin[,c(35:39)],esdmax[,c(35:39)])
sensor_35 <- cbind(volmin[,c(1:9)],volmin[,c(40:44)],volmax[,c(40:44)],esdmin[,c(40:44)],esdmax[,c(40:44)])
sensor_40 <- cbind(volmin[,c(1:9)],volmin[,c(45:49)],volmax[,c(45:49)],esdmin[,c(45:49)],esdmax[,c(45:49)])
sensor_45 <- cbind(volmin[,c(1:9)],volmin[,c(50:54)],volmax[,c(50:54)],esdmin[,c(50:54)],esdmax[,c(50:54)])
sensor_50 <- cbind(volmin[,c(1:9)],volmin[,c(55:59)],volmax[,c(55:59)],esdmin[,c(55:59)],esdmax[,c(55:59)])
sensor_55 <- cbind(volmin[,c(1:9)],volmin[,c(60:64)],volmax[,c(60:64)],esdmin[,c(60:64)],esdmax[,c(60:64)])
sensor_60 <- cbind(volmin[,c(1:9)],volmin[,c(65:69)],volmax[,c(65:69)],esdmin[,c(65:69)],esdmax[,c(65:69)])


sensor_05[,c(10:29)] <- sapply(sensor_05[,c(10:29)] , as.numeric)
sensor_05_volminmean <- rowMeans(sensor_05[,c(10:14)],na.rm=T)
sensor_05_volmaxmean <- rowMeans(sensor_05[,c(15:19)],na.rm=T)
sensor_05_esdminmean <- rowMeans(sensor_05[,c(20:24)],na.rm=T)
sensor_05_esdmaxmean <- rowMeans(sensor_05[,c(25:29)],na.rm=T)
sensor_05_mean <- cbind(sensor_05[,c(1:9)],sensor_05_volminmean,sensor_05_volmaxmean,
                        sensor_05_esdminmean,sensor_05_esdmaxmean)
sensor_10[,c(10:29)] <- sapply(sensor_10[,c(10:29)] , as.numeric)
sensor_10_volminmean <- rowMeans(sensor_10[,c(10:14)],na.rm=T)
sensor_10_volmaxmean <- rowMeans(sensor_10[,c(15:19)],na.rm=T)
sensor_10_esdminmean <- rowMeans(sensor_10[,c(20:24)],na.rm=T)
sensor_10_esdmaxmean <- rowMeans(sensor_10[,c(25:29)],na.rm=T)
sensor_10_mean <- cbind(sensor_10[,c(1:9)],sensor_10_volminmean,sensor_10_volmaxmean,
                        sensor_10_esdminmean,sensor_10_esdmaxmean)
sensor_15[,c(10:29)] <- sapply(sensor_15[,c(10:29)] , as.numeric)
sensor_15_volminmean <- rowMeans(sensor_15[,c(10:14)],na.rm=T)
sensor_15_volmaxmean <- rowMeans(sensor_15[,c(15:19)],na.rm=T)
sensor_15_esdminmean <- rowMeans(sensor_15[,c(20:24)],na.rm=T)
sensor_15_esdmaxmean <- rowMeans(sensor_15[,c(25:29)],na.rm=T)
sensor_15_mean <- cbind(sensor_15[,c(1:9)],sensor_15_volminmean,sensor_15_volmaxmean,
                        sensor_15_esdminmean,sensor_15_esdmaxmean)

train_1 <- cbind(sensor_05_mean,sensor_10_mean, sensor_15_mean)
train_1 <- train_1[,-c(c(14:22),c(27:35))]


train_1_BRR1 <- subset(train_1, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  

train_1_BRR0 <- subset(train_1, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #86046

train_1_BRR0$BRR <-0
train_1_BRR1$BRR <-1

train_1_BRR1_comp <- train_1_BRR1[complete.cases(train_1_BRR1),]  #57339
train_1_BRR1_unique <- train_1_BRR1_comp[!duplicated(train_1_BRR1_comp[,c(10:21)]),]#38456
train_1_BRR0_comp <- train_1_BRR0[complete.cases(train_1_BRR0),]  #292924
train_1_BRR0_unique <- train_1_BRR0_comp[!duplicated(train_1_BRR0_comp[,c(10:21)]),]
train_1_unique <- rbind(train_1_BRR1_unique, train_1_BRR0_unique)
train_1_final <- sqldf("select * from train_1_unique group by VehicleID order by BRR ASC limit 1")

names(sensor_05_BRR0)[10:13] <- c("volminmean1","volmaxmean1","esdminmean1","esdmaxmean1")
names(sensor_05_BRR1)[10:13] <- c("volminmean1","volmaxmean1","esdminmean1","esdmaxmean1")
names(sensor_10_BRR0)[10:13] <- c("volminmean2","volmaxmean2","esdminmean2","esdmaxmean2")
names(sensor_10_BRR1)[10:13] <- c("volminmean2","volmaxmean2","esdminmean2","esdmaxmean2")
names(sensor_15_BRR0)[10:13] <- c("volminmean3","volmaxmean3","esdminmean3","esdmaxmean3")
names(sensor_15_BRR1)[10:13] <- c("volminmean3","volmaxmean3","esdminmean3","esdmaxmean3")

sensor_05 <- rbind(sensor_05_BRR0,sensor_05_BRR1)
sensor_10 <- rbind(sensor_10_BRR0,sensor_10_BRR1)
sensor_15 <- rbind(sensor_15_BRR0,sensor_15_BRR1)
train_1 <- cbind(sensor_05,sensor_10,sensor_15)
train_1 <- train_1[complete.cases(train_1),]  #350263

#June 7th

volmin <- read.csv("data_for_trail/data_step_by_step/sensor_data/volmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
volmax <- read.csv("data_for_trail/data_step_by_step/sensor_data/volmax.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmin <- read.csv("data_for_trail/data_step_by_step/sensor_data/esdmin.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmax <- read.csv("data_for_trail/data_step_by_step/sensor_data/esdmax.csv",sep="|",header=T,stringsAsFactors=FALSE)

sensor_data <- cbind(volmin,volmax[,c(10:69)],esdmin[,c(10:69)],esdmax[,c(10:69)])

sensor_BRR1 <- subset(sensor_data, TaskName=="Alternator Belt R&R\t"|
                           TaskName=="Alternator Belt ADJ\t"|
                           TaskName=="Battery R&R\t"|
                           TaskName=="Battery ADJ\t"|
                           TaskName=="Charging/starting System DIA\t"|
                           TaskName=="Electrical System DIA\t"|
                           TaskName=="Alternator R&R\t"|
                           TaskName=="Alternator - REP\t"|
                           TaskName=="Battery/hold Down R&R\t"|
                           TaskName=="Battery Cable R&R\t"|
                           TaskName=="Battery Cable REP\t"|
                           TaskName== "Battery, Charge REP\t"|
                           TaskName=="Voltage Regulator R&R\t"|
                           TaskName=="Electrical System OTH\t"|
                           TaskName=="Battery,egr Warning Lite R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Battery Mpu/forklift R&R\t"|
                           TaskName=="Battery Aux Engine Aerial R&R\t"|
                           TaskName=="Alternator Mpu R&R\t"|
                           TaskName=="Battery Vpu R&R\t"|
                           TaskName=="Jump Start Vehicle OTH\t"|
                           TaskName=="Battery,auxillary Engine R&R\t"|
                           TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                           TaskName=="Starter R&R\t"|
                           TaskName=="Spark Plugs R&R\t"|
                           TaskName=="Wiring/harness R&R\t"|
                           TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                           TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                           TaskName=="Jump Start Vehicle\t"|
                           TaskName=="Aerial/Electrical System DIA\t"|
                           TaskName=="Aerial/Electrical system OTH\t"|
                           TaskName=="EVACUATE & RECHARGE - R&R\t"|
                           TaskName=="Engine  system OTH\t"|
                           TaskName=="Engine System DIA\t")  #13096

sensor_BRR0<- subset(sensor_data, TaskName!="Alternator Belt R&R\t"&
                           TaskName!="Alternator Belt ADJ\t"&
                           TaskName!="Battery R&R\t"&
                           TaskName!="Battery ADJ\t"&
                           TaskName!="Charging/starting System DIA\t"&
                           TaskName!="Electrical System DIA\t"&
                           TaskName!="Alternator R&R\t"&
                           TaskName!="Alternator - REP\t"&
                           TaskName!="Battery/hold Down R&R\t"&
                           TaskName!="Battery Cable R&R\t"&
                           TaskName!="Battery Cable REP\t"&
                           TaskName!= "Battery, Charge REP\t"&
                           TaskName!="Voltage Regulator R&R\t"&
                           TaskName!="Electrical System OTH\t"&
                           TaskName!="Battery,egr Warning Lite R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Battery Mpu/forklift R&R\t"&
                           TaskName!="Battery Aux Engine Aerial R&R\t"&
                           TaskName!="Alternator Mpu R&R\t"&
                           TaskName!="Battery Vpu R&R\t"&
                           TaskName!="Jump Start Vehicle OTH\t"&
                           TaskName!="Battery,auxillary Engine R&R\t"&
                           TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                           TaskName!="Starter R&R\t"&
                           TaskName!="Spark Plugs R&R\t"&
                           TaskName!="Wiring/harness R&R\t"&
                           TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                           TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                           TaskName!="Jump Start Vehicle\t"&
                           TaskName!="Aerial/Electrical System DIA\t"&
                           TaskName!="Aerial/Electrical system OTH\t"&
                           TaskName!="EVACUATE & RECHARGE - R&R\t"&
                           TaskName!="Engine  system OTH\t"&
                           TaskName!="Engine System DIA\t")  #77182




sensor_BRR1_unique <- sensor_BRR1[!duplicated(sensor_BRR1[,c(10:21)]),]  #46834
sensor_BRR0_unique <- sensor_BRR0[!duplicated(sensor_BRR0[,c(10:21)]),] #184680
sensor_BRR0_subset <- sqldf("select * from  sensor_BRR0_unique where VehicleID not in 
                            (select VehicleID from sensor_BRR1_unique)") #38336

sensor_BRR0_final  <- sensor_BRR0_subset
sensor_BRR1_final <- sensor_BRR1_unique

sensor_BRR0_final$BRR <-0
sensor_BRR1_final$BRR <-1


write.csv(sensor_BRR0_final, "data_for_trail/data_for_model/0607/sensor_BRR0_final.csv")
write.csv(sensor_BRR1_final, "data_for_trail/data_for_model/0607/sensor_BRR1_final.csv")
# explore the mean of BRR=1 and BRR=0
sensor_BRR1_final[,c(10:249)] <- sapply(sensor_BRR1_final[,c(10:249)] , as.numeric)
sensor_BRR0_final[,c(10:249)] <- sapply(sensor_BRR0_final[,c(10:249)] , as.numeric)
sensor_BRR1_mean <- colMeans(sensor_BRR1_final[,c(10:249)],na.rm=T)
sensor_BRR0_mean <- colMeans(sensor_BRR0_final[,c(10:249)],na.rm=T)
#sensor_BRR1_mod_7 <- colMeans(sensor_BRR1_final[,(c(10:249))%%7],na.rm=T)
#sensor_BRR0_mod_7 <- colMeans(sensor_BRR0_final[,c(10:249)%%7],na.rm=T)
# plot voltage min
plot.ts(sensor_BRR1_mean[c(1:60)],ylim=range(12.5,13.3),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean[c(1:60)],ylim=range(12.5,13.3),col="blue")

# plot voltage max
plot.ts(sensor_BRR1_mean[(c(61:120))],ylim=range(14.1,14.4),col="red")
par(new=T)
plot.ts(sensor_BRR1_mean[(c(61:120))],ylim=range(14.1,14.4),col="red")

# plot engine speed min
plot.ts(sensor_BRR1_mean[c(121:180)],ylim=range(480,525),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean[c(121:180)],ylim=range(480,525),col="blue")
# plot engine speed max
plot.ts(sensor_BRR1_mean[c(181:240)],ylim=range(2600,2750),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean[c(181:240)],ylim=range(2600,2750),col="blue")
# plot voltage max/voltage min
plot.ts(sensor_BRR1_mean[c(61:120)]/sensor_BRR1_mean[c(1:60)],ylim=range(1.075,1.115),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean[c(61:120)]/sensor_BRR0_mean[c(1:60)],ylim=range(1.075,1.115),col="blue")
# plot engine speed min/ voltage min
plot.ts(sensor_BRR1_mean[c(121:180)]/sensor_BRR1_mean[c(1:60)],,ylim=range(37,40),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean[c(121:180)]/sensor_BRR0_mean[c(1:60)],ylim=range(37,40),col="blue")
# plot engine speed max/ voltage max
plot.ts(sensor_BRR1_mean[c(181:240)]/sensor_BRR1_mean[c(61:120)],ylim=range(183,195),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean[c(181:240)]/sensor_BRR0_mean[c(61:120)],ylim=range(183,195),col="blue")
# plot engine speed max/ engine speed min
plot.ts(sensor_BRR1_mean[c(181:240)]/sensor_BRR1_mean[c(121:180)],ylim=range(5.0,5.8),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean[c(181:240)]/sensor_BRR0_mean[c(121:180)],ylim=range(5.0,5.8),col="blue")


#date to days of the week of DemandDate

DemandDate <- sensor_data[,7]
DemandDate <- as.Date(DemandDate,"%m/%d/%Y")
DemandDayOfTheWeek <- weekdays(DemandDate)

DemandDateBRR0 <- sensor_BRR0_final[,7]
DemandDateBRR0 <- as.Date(DemandDateBRR0,"%m/%d/%Y")
DemandDayWeekBRR0 <- weekdays(DemandDateBRR0)

DemandDateBRR1 <- sensor_BRR1[,7]
DemandDateBRR1 <- as.Date(DemandDateBRR1,"%m/%d/%Y")
DemandDayWeekBRR1 <- weekdays(DemandDateBRR1)

# bucket into 7 days
names(sensor_BRR1_final) <- names(sensor_BRR0_final)
sensor_final <- rbind(sensor_BRR1_final, sensor_BRR0_final)

# compute mean over every 7 days
volmin_mean <- data.frame(matrix(ncol = 8, nrow = 223016))
for(i in c(1:8)){
  volmin_mean[,i] <- rowMeans(sensor_final[,c((3+i*7):(9+i*7))],na.rm=TRUE)
}
names(volmin_mean) <-c("volmin1","volmin2","volmin3","volmin4",
                       "volmin5","volmin6","volmin7","volmin8")

volmax_mean <- data.frame(matrix(ncol = 8, nrow = 223016))
for(i in c(1:8)){
  volmax_mean[,i] <- rowMeans(sensor_final[,c((63+i*7):(69+i*7))],na.rm=TRUE)
}
names(volmax_mean) <-c("volmax1","volmax2","volmax3","volmax4",
                       "volmax5","volmax6","volmax7","volmax8")

esdmin_mean <- data.frame(matrix(ncol = 8, nrow = 223016))
for(i in c(1:8)){
  esdmin_mean[,i] <- rowMeans(sensor_final[,c((123+i*7):(129+i*7))],na.rm=TRUE)
}
names(esdmin_mean) <-c("esdmin1","esdmin2","esdmin3","esdmin4",
                       "esdmin5","esdmin6","esdmin7","esdmin8")

esdmax_mean <- data.frame(matrix(ncol = 8, nrow = 223016))
for(i in c(1:8)){
  esdmax_mean[,i] <- rowMeans(sensor_final[,c((183+i*7):(189+i*7))],na.rm=TRUE)
}
names(esdmax_mean) <-c("esdmax1","esdmax2","esdmax3","esdmax4",
                       "esdmax5","esdmax6","esdmax7","esdmax8")

# use the 1-2 buckets train as Y, use 5-6 bucket train as N
train_1 <- cbind(sensor_final[,c(1:9)],volmin_mean[,c(1:2)],volmax_mean[,c(1:2)],
                 esdmin_mean[,c(1:2)],esdmax_mean[,c(1:2)],sensor_final[,250])
train_2 <- cbind(sensor_final[,c(1:9)],volmin_mean[,c(5:6)],volmax_mean[,c(5:6)],
                 esdmin_mean[,c(5:6)],esdmax_mean[,c(5:6)],sensor_final[,250])
names(train_2)<- names(train_1)
train_2$BRR <-0
names(train_1)[18] <- "BRR"
train_2 <- train_2[,-18]
train <- rbind(train_1, train_2)
write.csv(train,"data_for_trail/data_for_model/0607/train_data_7day2bucket.csv")

# use the 3-4 buckets and 7-8 buckets to test
test_1 <- cbind(sensor_final[,c(1:9)],volmin_mean[,c(3:4)],volmax_mean[,c(3:4)],
                 esdmin_mean[,c(3:4)],esdmax_mean[,c(3:4)],sensor_final[,250])
test_2 <- cbind(sensor_final[,c(1:9)],volmin_mean[,c(7:8)],volmax_mean[,c(7:8)],
                 esdmin_mean[,c(7:8)],esdmax_mean[,c(7:8)],sensor_final[,250])

names(test_1)<- names(train_1)
names(test_2)<- names(train_1)
names(test_1)[18] <-"BRR"
names(test_2)[18] <-"BRR"
write.csv(test_1,"data_for_trail/data_for_model/0607/test_data_3&4buckets.csv")
write.csv(test_2,"data_for_trail/data_for_model/0607/test_data_7&8buckets.csv")


# June 9
plot.ts(sensor_BRR1_mean[c(182:200)],sensor_BRR1_mean[c(122:140)],col="red",xlim=range(2620,2780),ylim=range(450,550))
par(new=T)
plot.ts(sensor_BRR0_mean[c(182:200)],sensor_BRR0_mean[c(122:140)],col="blue",xlim=range(2620,2780),ylim=range(450,550))
# voltage
plot.ts(sensor_BRR1_mean[c(2:22)],sensor_BRR1_mean[c(62:82)],col="red",xlim=range(12,14),ylim=range(14,15))
par(new=T)
plot.ts(sensor_BRR0_mean[c(2:22)],sensor_BRR0_mean[c(62:82)],col="blue",xlim=range(12,14),ylim=range(14,15))


plot.ts(sensor_BRR1_mean[c(2:12)],sensor_BRR1_mean[c(62:72)],col="red")
plot.ts(sensor_BRR0_mean[c(2:12)],sensor_BRR0_mean[c(62:72)],col="blue")

sensor_BRR1_ts <- ts(sensor_BRR1_final)
sensor_BRR0_ts <- ts(sensor_BRR0_final)


# use only Battery B&R

sensor_BRR1_only <- subset(sensor_data, TaskName=="Battery R&R\t")  #13096

sensor_BRR0_only<- subset(sensor_data, TaskName!="Battery R&R\t")

sensor_BRR1_unique_only <- sensor_BRR1_only[!duplicated(sensor_BRR1_only[,c(10:21)]),]  #46834
sensor_BRR0_unique_only <- sensor_BRR0_only[!duplicated(sensor_BRR0_only[,c(10:21)]),] #184680
sensor_BRR0_subset_only <- sqldf("select * from  sensor_BRR0_unique_only where VehicleID not in 
                            (select VehicleID from sensor_BRR1_unique_only)") #38336

sensor_BRR0_final_only  <- sensor_BRR0_subset_only
sensor_BRR1_final_only <- sensor_BRR1_unique_only

sensor_BRR0_final_only$BRR <-0
sensor_BRR1_final_only$BRR <-1

# explore the mean of BRR=1 and BRR=0
sensor_BRR1_final_only[,c(10:249)] <- sapply(sensor_BRR1_final_only[,c(10:249)] , as.numeric)
sensor_BRR0_final_only[,c(10:249)] <- sapply(sensor_BRR0_final_only[,c(10:249)] , as.numeric)
sensor_BRR1_mean_only <- colMeans(sensor_BRR1_final_only[,c(10:249)],na.rm=T)
sensor_BRR0_mean_only <- colMeans(sensor_BRR0_final_only[,c(10:249)],na.rm=T)
#sensor_BRR1_mod_7 <- colMeans(sensor_BRR1_final[,(c(10:249))%%7],na.rm=T)
#sensor_BRR0_mod_7 <- colMeans(sensor_BRR0_final[,c(10:249)%%7],na.rm=T)
# plot voltage min
plot.ts(sensor_BRR1_mean_only[c(1:60)],ylim=range(12.5,13.3),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(1:60)],ylim=range(12.5,13.3),col="blue")

# plot voltage max
plot.ts(sensor_BRR1_mean_only[(c(61:120))],ylim=range(14.1,14.4),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[(c(61:120))],ylim=range(14.1,14.4),col="blue")

# plot engine speed min
plot.ts(sensor_BRR1_mean_only[c(121:180)],ylim=range(480,525),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(121:180)],ylim=range(480,525),col="blue")
# plot engine speed max
plot.ts(sensor_BRR1_mean_only[c(181:240)],ylim=range(2600,2750),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(181:240)],ylim=range(2600,2750),col="blue")
# plot voltage max/voltage min
plot.ts(sensor_BRR1_mean_only[c(61:120)]/sensor_BRR1_mean_only[c(1:60)],ylim=range(1.075,1.115),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(61:120)]/sensor_BRR0_mean_only[c(1:60)],ylim=range(1.075,1.115),col="blue")
# plot engine speed min/ voltage min
plot.ts(sensor_BRR1_mean_only[c(121:180)]/sensor_BRR1_mean_only[c(1:60)],,ylim=range(37,40),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(121:180)]/sensor_BRR0_mean_only[c(1:60)],ylim=range(37,40),col="blue")
# plot engine speed max/ voltage max
plot.ts(sensor_BRR1_mean_only[c(181:240)]/sensor_BRR1_mean_only[c(61:120)],ylim=range(183,195),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(181:240)]/sensor_BRR0_mean_only[c(61:120)],ylim=range(183,195),col="blue")
# plot engine speed max/ engine speed min
plot.ts(sensor_BRR1_mean[c(181:240)]/sensor_BRR1_mean[c(121:180)],ylim=range(5.0,5.8),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean[c(181:240)]/sensor_BRR0_mean[c(121:180)],ylim=range(5.0,5.8),col="blue")


# percentage difference between BRR0 and BRR1

plot.ts((sensor_BRR1_mean[(c(1:60))]-sensor_BRR0_mean[(c(1:60))])/sensor_BRR0_mean[(c(1:60))],
        ylim=range(-0.05,0.05),col="blue")
par(new=T)
plot.ts((sensor_BRR1_mean[(c(61:120))]-sensor_BRR0_mean[(c(61:120))])/sensor_BRR0_mean[(c(61:120))]
        ,ylim=range(-0.05,0.05),col="red")
par(new=T)
plot.ts((sensor_BRR1_mean[(c(121:180))]-sensor_BRR0_mean[(c(121:180))])/sensor_BRR0_mean[(c(121:180))]
        ,ylim=range(-0.05,0.05),col="black")
par(new=T)
plot.ts((sensor_BRR1_mean[(c(181:240))]-sensor_BRR0_mean[(c(181:240))])/sensor_BRR0_mean[(c(181:240))]
        ,ylim=range(-0.05,0.05),col="green")
# percentage difference between BRR0 and BRR1 only
plot.ts((sensor_BRR1_mean_only[(c(1:60))]-sensor_BRR0_mean_only[(c(1:60))])/sensor_BRR0_mean_only[(c(1:60))],
        ylim=range(-0.05,0.05),col="blue")
par(new=T)
plot.ts((sensor_BRR1_mean_only[(c(61:120))]-sensor_BRR0_mean_only[(c(61:120))])/sensor_BRR0_mean_only[(c(61:120))]
        ,ylim=range(-0.05,0.05),col="red")
par(new=T)
plot.ts((sensor_BRR1_mean_only[(c(121:180))]-sensor_BRR0_mean_only[(c(121:180))])/sensor_BRR0_mean_only[(c(121:180))]
        ,ylim=range(-0.05,0.05),col="black")
par(new=T)
plot.ts((sensor_BRR1_mean_only[(c(181:240))]-sensor_BRR0_mean_only[(c(181:240))])/sensor_BRR0_mean_only[(c(181:240))]
        ,ylim=range(-0.05,0.05),col="green")


plot.ts(sensor_BRR1_mean[c(2:60)],sensor_BRR1_mean[c(62:120)],col="red",xlim=range(12.5,13.5),ylim=range(14,14.4))
par(new=T)
plot.ts(sensor_BRR0_mean[c(2:60)],sensor_BRR0_mean[c(62:120)],col="blue",xlim=range(12.5,13.5),ylim=range(14,14.4))

plot.ts(sensor_BRR1_mean[c(122:180)],sensor_BRR1_mean[c(182:240)],col="red",xlim=range(492,525),ylim=range(2620,2750))
par(new=T)
plot.ts(sensor_BRR0_mean[c(122:180)],sensor_BRR0_mean[c(182:240)],col="blue",xlim=range(492,525),ylim=range(2620,2750))

plot.ts(sensor_BRR1_mean_only[c(2:60)],sensor_BRR1_mean_only[c(62:120)],col="red",xlim=range(12,14),ylim=range(14,15))
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(2:60)],sensor_BRR0_mean_only[c(62:120)],col="blue",xlim=range(12,14),ylim=range(14,15))



# June 9 train battery
# use the 1-2 buckets train as Y, use 5-6 bucket train as N
train_1 <- cbind(sensor_final[,c(1:9)],volmin_mean[,c(1:2)],volmax_mean[,c(1:2)],
                 esdmin_mean[,c(1:2)],esdmax_mean[,c(1:2)],sensor_final[,250])
train_2 <- cbind(sensor_final[,c(1:9)],volmin_mean[,c(5:6)],volmax_mean[,c(5:6)],
                 esdmin_mean[,c(5:6)],esdmax_mean[,c(5:6)],sensor_final[,250])
names(train_2)<- names(train_1)
train_2$BRR <-0
names(train_1)[18] <- "BRR"
train_2 <- train_2[,-18]
train <- rbind(train_1, train_2)
write.csv(train,"data_for_trail/data_for_model/0607/train_data_7day2bucket.csv")

# use the 3-4 buckets and 7-8 buckets to test
test_1 <- cbind(sensor_final[,c(1:9)],volmin_mean[,c(3:4)],volmax_mean[,c(3:4)],
                esdmin_mean[,c(3:4)],esdmax_mean[,c(3:4)],sensor_final[,250])
test_2 <- cbind(sensor_final[,c(1:9)],volmin_mean[,c(7:8)],volmax_mean[,c(7:8)],
                esdmin_mean[,c(7:8)],esdmax_mean[,c(7:8)],sensor_final[,250])

names(test_1)<- names(train_1)
names(test_2)<- names(train_1)
names(test_1)[18] <-"BRR"
names(test_2)[18] <-"BRR"
write.csv(test_1,"data_for_trail/data_for_model/0607/test_data_3&4buckets.csv")
write.csv(test_2,"data_for_trail/data_for_model/0607/test_data_7&8buckets.csv")




# 180days data
vol_max <- read.csv("daily_180/vol_max.csv",sep="|",header=T,stringsAsFactors=FALSE)
vol_min <- read.csv("daily_180/vol_min.csv",sep="|",header=T,stringsAsFactors=FALSE)
esd_max <- read.csv("daily_180/esd_max.csv",sep="|",header=T,stringsAsFactors=FALSE)
esd_min <- read.csv("daily_180/esd_min.csv",sep="|",header=T,stringsAsFactors=FALSE)
vol_count <-read.csv("daily_180/vol_count.csv",sep="|",header=T,stringsAsFactors=FALSE)
vol_mean <- read.csv("daily_180/vol_mean.csv",sep="|",header=T,stringsAsFactors=FALSE)
sensor_data <- cbind(vol_min,vol_max[,c(10:189)],vol_mean[,c(10:189)],esd_min[,c(10:189)],esd_max[,c(10:189)])
write.csv(sensor_data,"daily_180/sensor_vol_esd.csv")
sensor_data <-read.csv("daily_180/sensor_vol_esd.csv")
sensor_BRR1 <- subset(sensor_data, TaskName=="Alternator Belt R&R\t"|
                        TaskName=="Alternator Belt ADJ\t"|
                        TaskName=="Battery R&R\t"|
                        TaskName=="Battery ADJ\t"|
                        TaskName=="Charging/starting System DIA\t"|
                        TaskName=="Electrical System DIA\t"|
                        TaskName=="Alternator R&R\t"|
                        TaskName=="Alternator - REP\t"|
                        TaskName=="Battery/hold Down R&R\t"|
                        TaskName=="Battery Cable R&R\t"|
                        TaskName=="Battery Cable REP\t"|
                        TaskName== "Battery, Charge REP\t"|
                        TaskName=="Voltage Regulator R&R\t"|
                        TaskName=="Electrical System OTH\t"|
                        TaskName=="Battery,egr Warning Lite R&R\t"|
                        TaskName=="Battery Aux Engine Aerial R&R\t"|
                        TaskName=="Battery Mpu/forklift R&R\t"|
                        TaskName=="Battery Aux Engine Aerial R&R\t"|
                        TaskName=="Alternator Mpu R&R\t"|
                        TaskName=="Battery Vpu R&R\t"|
                        TaskName=="Jump Start Vehicle OTH\t"|
                        TaskName=="Battery,auxillary Engine R&R\t"|
                        TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                        TaskName=="Starter R&R\t"|
                        TaskName=="Spark Plugs R&R\t"|
                        TaskName=="Wiring/harness R&R\t"|
                        TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                        TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                        TaskName=="Jump Start Vehicle\t")  #13096

sensor_BRR0<- subset(sensor_data, TaskName!="Alternator Belt R&R\t"&
                       TaskName!="Alternator Belt ADJ\t"&
                       TaskName!="Battery R&R\t"&
                       TaskName!="Battery ADJ\t"&
                       TaskName!="Charging/starting System DIA\t"&
                       TaskName!="Electrical System DIA\t"&
                       TaskName!="Alternator R&R\t"&
                       TaskName!="Alternator - REP\t"&
                       TaskName!="Battery/hold Down R&R\t"&
                       TaskName!="Battery Cable R&R\t"&
                       TaskName!="Battery Cable REP\t"&
                       TaskName!= "Battery, Charge REP\t"&
                       TaskName!="Voltage Regulator R&R\t"&
                       TaskName!="Electrical System OTH\t"&
                       TaskName!="Battery,egr Warning Lite R&R\t"&
                       TaskName!="Battery Aux Engine Aerial R&R\t"&
                       TaskName!="Battery Mpu/forklift R&R\t"&
                       TaskName!="Battery Aux Engine Aerial R&R\t"&
                       TaskName!="Alternator Mpu R&R\t"&
                       TaskName!="Battery Vpu R&R\t"&
                       TaskName!="Jump Start Vehicle OTH\t"&
                       TaskName!="Battery,auxillary Engine R&R\t"&
                       TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                       TaskName!="Starter R&R\t"&
                       TaskName!="Spark Plugs R&R\t"&
                       TaskName!="Wiring/harness R&R\t"&
                       TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                       TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                       TaskName!="Jump Start Vehicle\t")  #77182

sensor_BRR1_unique <- sensor_BRR1[!duplicated(sensor_BRR1[,c(10:40)]),]  #46834
sensor_BRR1_dup <- sensor_BRR1[duplicated(sensor_BRR1[,c(10:360)]),]  #46834
sensor_BRR0_unique <- sensor_BRR0[!duplicated(sensor_BRR0[,c(10:40)]),] #184680
sensor_BRR0_subset <- sqldf("select * from  sensor_BRR0_unique where VehicleID not in 
                            (select VehicleID from sensor_BRR1_unique)") #38336
sensor_BRR0_final  <- sensor_BRR0_subset
sensor_BRR1_final <- sensor_BRR1_unique

sensor_BRR0_final$BRR <-0  # 38333,730
sensor_BRR1_final$BRR <-1  # 46766, 730
write.csv(sensor_BRR0_final, "data_for_trail/data_for_model/0612/model_2_sensor/sensor_BRR0_final.csv")
write.csv(sensor_BRR1_final, "data_for_trail/data_for_model/0612/model_2_sensor/sensor_BRR1_final.csv")
# only battery R&R and alternator R&R
sensor_data <- vol_mean
sensor_data$DemandDate <- as.Date(sensor_data$DemandDate,"%m/%d/%Y")
sensor_data$Month <- as.numeric(format(sensor_data$DemandDate,"%m"))
voltage_by_Month <- tapply(sensor_BRR1_final_only$Battery_Voltage_median_1, sensor_BRR1_final_only$Month, sd,na.rm=T)

sensor_BRR1_only <- subset(sensor_data,TaskName=="Battery R&R\t"|TaskName=="Alternator R&R\t")


sensor_BRR0_only<- subset(sensor_data,TaskName!="Battery R&R\t"&TaskName!="Alternator R&R\t")


sensor_BRR1_unique_only <- sensor_BRR1_only[!duplicated(sensor_BRR1_only[,c(10:21)]),]  #46834
sensor_BRR0_unique_only <- sensor_BRR0_only[!duplicated(sensor_BRR0_only[,c(10:21)]),] #184680
sensor_BRR0_subset_only <- sqldf("select * from  sensor_BRR0_unique_only where VehicleID not in 
                            (select VehicleID from sensor_BRR1_unique_only)") #38336

sensor_BRR0_final_only  <- sensor_BRR0_subset_only
sensor_BRR1_final_only <- sensor_BRR1_unique_only

sensor_BRR0_final_only$BRR <-0  # 38333,730
sensor_BRR1_final_only$BRR <-1  # 46766, 730

# explore the mean of BRR=1 and BRR=0
sensor_BRR1_final_only[,c(10:189)] <- sapply(sensor_BRR1_final_only[,c(10:189)] , as.numeric)
sensor_BRR0_final_only[,c(10:189)] <- sapply(sensor_BRR0_final_only[,c(10:189)] , as.numeric)
sensor_BRR1_mean_only <- colMeans(sensor_BRR1_final_only[,c(10:189)],na.rm=T)
sensor_BRR0_mean_only <- colMeans(sensor_BRR0_final_only[,c(10:189)],na.rm=T)


# plot by months
sensor_BRR0_Spring <- subset(sensor_BRR0_final_only, Month==3|Month==4|Month==5)
sensor_BRR0_Summer <- subset(sensor_BRR0_final_only, Month==6|Month==7|Month==8)
sensor_BRR0_Fall <- subset(sensor_BRR0_final_only, Month==9|Month==10|Month==11)
sensor_BRR0_Winter <- subset(sensor_BRR0_final_only, Month==12|Month==1|Month==2)
sensor_BRR1_Spring <- subset(sensor_BRR1_final_only, Month==3|Month==4|Month==5)
sensor_BRR1_Summer <- subset(sensor_BRR1_final_only, Month==6|Month==7|Month==8)
sensor_BRR1_Fall <- subset(sensor_BRR1_final_only, Month==9|Month==10|Month==11)
sensor_BRR1_Winter <- subset(sensor_BRR1_final_only, Month==12|Month==1|Month==2)
# look at in the winter, difference between south and north
sensor_BRR0_S_Winter <- subset(sensor_BRR0_final_only, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR0_N_Winter <- subset(sensor_BRR0_final_only, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|State=="MT"|State=="NY"|State=="OH"))
sensor_BRR1_S_Winter <- subset(sensor_BRR1_final_only, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Winter <- subset(sensor_BRR1_final_only, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|State=="MT"|State=="NY"|State=="OH"))
sensor_BRR0_S_Winter[,c(10:189)] <- sapply(sensor_BRR0_S_Winter[,c(10:189)] , as.numeric)
sensor_BRR0_N_Winter[,c(10:189)] <- sapply(sensor_BRR0_N_Winter[,c(10:189)] , as.numeric)
sensor_BRR1_S_Winter[,c(10:189)] <- sapply(sensor_BRR1_S_Winter[,c(10:189)] , as.numeric)
sensor_BRR1_N_Winter[,c(10:189)] <- sapply(sensor_BRR1_N_Winter[,c(10:189)] , as.numeric)
sensor_BRR0_mean_SWinter<- colMeans(sensor_BRR0_S_Winter[,c(10:189)],na.rm=T)
sensor_BRR0_mean_NWinter<- colMeans(sensor_BRR0_N_Winter[,c(10:189)],na.rm=T)
sensor_BRR1_mean_SWinter<- colMeans(sensor_BRR1_S_Winter[,c(10:189)],na.rm=T)
sensor_BRR1_mean_NWinter<- colMeans(sensor_BRR1_N_Winter[,c(10:189)],na.rm=T)
plot.ts(sensor_BRR0_mean_SWinter[c(1:180)],ylim=range(13.3,14.3),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_NWinter[c(1:180)],ylim=range(13.3,14.3),col="green")
par(new=T)
plot.ts(sensor_BRR1_mean_SWinter[c(1:180)],ylim=range(13.3,14.3),col="blue")
par(new=T)
plot.ts(sensor_BRR1_mean_NWinter[c(1:180)],ylim=range(13.3,14.3),col="black")




sensor_BRR0_S_Summer<- subset(sensor_BRR0_final_only, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR0_N_Winter <- subset(sensor_BRR0_final_only, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|State=="MT"|State=="NY"|State=="OH"))
sensor_BRR1_S_Winter <- subset(sensor_BRR1_final_only, (Month==12|Month==1|Month==52)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Winter <- subset(sensor_BRR1_final_only, (Month==12|Month==1|Month==52)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|State=="MT"|State=="NY"|State=="OH"))
sensor_BRR0_S_Winter[,c(10:189)] <- sapply(sensor_BRR0_S_Winter[,c(10:189)] , as.numeric)
sensor_BRR0_N_Winter[,c(10:189)] <- sapply(sensor_BRR0_N_Winter[,c(10:189)] , as.numeric)
sensor_BRR1_S_Winter[,c(10:189)] <- sapply(sensor_BRR1_S_Winter[,c(10:189)] , as.numeric)
sensor_BRR1_N_Winter[,c(10:189)] <- sapply(sensor_BRR1_N_Winter[,c(10:189)] , as.numeric)
sensor_BRR0_mean_SWinter<- colMeans(sensor_BRR0_S_Winter[,c(10:189)],na.rm=T)
sensor_BRR0_mean_NWinter<- colMeans(sensor_BRR0_N_Winter[,c(10:189)],na.rm=T)
sensor_BRR1_mean_SWinter<- colMeans(sensor_BRR1_S_Winter[,c(10:189)],na.rm=T)
sensor_BRR1_mean_NWinter<- colMeans(sensor_BRR1_N_Winter[,c(10:189)],na.rm=T)
plot.ts(sensor_BRR0_mean_SWinter[c(1:180)],ylim=range(13.3,14.3),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_NWinter[c(1:180)],ylim=range(13.3,14.3),col="green")
par(new=T)
plot.ts(sensor_BRR1_mean_SWinter[c(1:180)],ylim=range(13.3,14.3),col="blue")
par(new=T)
plot.ts(sensor_BRR1_mean_NWinter[c(1:180)],ylim=range(13.3,14.3),col="black")



sensor_BRR0_Spring[,c(10:189)] <- sapply(sensor_BRR0_Spring[,c(10:189)] , as.numeric)
sensor_BRR0_Summer[,c(10:189)] <- sapply(sensor_BRR0_Summer[,c(10:189)] , as.numeric)
sensor_BRR0_Fall[,c(10:189)] <- sapply(sensor_BRR0_Fall[,c(10:189)] , as.numeric)
sensor_BRR0_Winter[,c(10:189)] <- sapply(sensor_BRR0_Winter[,c(10:189)] , as.numeric)
sensor_BRR1_Spring[,c(10:189)] <- sapply(sensor_BRR1_Spring[,c(10:189)] , as.numeric)
sensor_BRR1_Summer[,c(10:189)] <- sapply(sensor_BRR1_Summer[,c(10:189)] , as.numeric)
sensor_BRR1_Fall[,c(10:189)] <- sapply(sensor_BRR1_Fall[,c(10:189)] , as.numeric)
sensor_BRR1_Winter[,c(10:189)] <- sapply(sensor_BRR1_Winter[,c(10:189)] , as.numeric)
sensor_BRR0_mean_Spring<- colMeans(sensor_BRR0_Spring[,c(10:189)],na.rm=T)
sensor_BRR0_mean_Summer<- colMeans(sensor_BRR0_Summer[,c(10:189)],na.rm=T)
sensor_BRR0_mean_Fall<- colMeans(sensor_BRR0_Fall[,c(10:189)],na.rm=T)
sensor_BRR0_mean_Winter<- colMeans(sensor_BRR0_Winter[,c(10:189)],na.rm=T)
sensor_BRR1_mean_Spring<- colMeans(sensor_BRR1_Spring[,c(10:189)],na.rm=T)
sensor_BRR1_mean_Summer<- colMeans(sensor_BRR1_Summer[,c(10:189)],na.rm=T)
sensor_BRR1_mean_Fall<- colMeans(sensor_BRR1_Fall[,c(10:189)],na.rm=T)
sensor_BRR1_mean_Winter<- colMeans(sensor_BRR1_Winter[,c(10:189)],na.rm=T)
plot.ts(sensor_BRR0_mean_Spring[c(1:180)],ylim=range(13.5,14.3),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_Summer[c(1:180)],ylim=range(13.5,14.3),col="blue")
par(new=T)
plot.ts(sensor_BRR0_mean_Fall[c(1:180)],ylim=range(13.5,14.3),col="green")
par(new=T)
plot.ts(sensor_BRR0_mean_Winter[c(1:180)],ylim=range(13.5,14.3),col="black")



sensor_BRR0_mean <- cbind(sensor_BRR0_mean_Spring,sensor_BRR0_mean_Summer,sensor_BRR0_mean_Fall,sensor_BRR0_mean_Winter)
plot.ts(sensor_BRR0_mean,ylim=range(13.5,14.3),col="red")
plot.ts(sensor_BRR1_mean_Spring[c(1:180)],ylim=range(13.5,14.3),col="red")
par(new=T)
plot.ts(sensor_BRR1_mean_Summer[c(1:180)],ylim=range(13.5,14.3),col="blue")
par(new=T)
plot.ts(sensor_BRR1_mean_Fall[c(1:180)],ylim=range(13.5,14.3),col="green")
par(new=T)
plot.ts(sensor_BRR1_mean_Winter[c(1:180)],ylim=range(13.5,14.3),col="black")
# spring+summer+fall+winter
sensor_BRR0_mean <- cbind(sensor_BRR0_mean_Spring,sensor_BRR0_mean_Summer,sensor_BRR0_mean_Fall,sensor_BRR0_mean_Winter)
sensor_BRR1_mean <- cbind(sensor_BRR1_mean_Spring,sensor_BRR1_mean_Summer,sensor_BRR1_mean_Fall,sensor_BRR1_mean_Winter)
plot.ts(sensor_BRR0_mean,ylim=range(13.5,14.3),col="red")
plot.ts(sensor_BRR1_mean,ylim=range(13.5,14.3),col="blue")
#spring
plot.ts(sensor_BRR0_mean_Spring,ylim=range(13.4,14.2),col="blue")
par(new=T)
plot.ts(sensor_BRR1_mean_Spring,ylim=range(13.4,14.2),col="red")
#summer
plot.ts(sensor_BRR0_mean_Summer,ylim=range(13.0,14.3),col="blue")
par(new=T)
plot.ts(sensor_BRR1_mean_Summer,ylim=range(13.0,14.3),col="red")
#fall
plot.ts(sensor_BRR0_mean_Fall,ylim=range(13.0,14.3),col="blue")
par(new=T)
plot.ts(sensor_BRR1_mean_Fall,ylim=range(13.0,14.3),col="red")
#winter
plot.ts(sensor_BRR0_mean_Winter,ylim=range(13.0,14.3),col="blue")
par(new=T)
plot.ts(sensor_BRR1_mean_Winter,ylim=range(13.0,14.3),col="red")

sensor_BRR1_sd_only <- apply(sensor_BRR1_final_only[,c(10:189)],2,sd, na.rm=T)
sensor_BRR0_sd_only <- apply(sensor_BRR0_final_only[,c(10:189)],2,sd,na.rm=T)
sensor_BRR1_median <- apply(sensor_BRR1_final[,c(10:729)],2,median, na.rm=T)
sensor_BRR0_median <- apply(sensor_BRR0_final[,c(10:729)],2,median,na.rm=T)
#sensor_BRR1_mod_7 <- colMeans(sensor_BRR1_final[,(c(10:249))%%7],na.rm=T)
#sensor_BRR0_mod_7 <- colMeans(sensor_BRR0_final[,c(10:249)%%7],na.rm=T)
# plot voltage min
plot.ts(subset(sensor_BRR1_mean_only[c(1:180)],),ylim=range(13.5,14.3),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(1:180)],ylim=range(13.5,14.3),col="blue")

plot.ts(sensor_BRR1_mean_only[c(181:240)],ylim=range(14,14.4),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(181:240)],ylim=range(14,14.4),col="blue")

plot.ts(sensor_BRR1_median_only[c(181:360)],ylim=range(14,14.4),col="red")
par(new=T)
plot.ts(sensor_BRR0_median_only[c(181:360)],ylim=range(14,14.4),col="blue")

plot.ts(sensor_BRR1_mean_only[c(361:540)],ylim=range(460,525),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(361:540)],ylim=range(460,525),col="blue")

plot.ts(sensor_BRR1_mean_only[c(541:720)],ylim=range(2620,2750),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(541:720)],ylim=range(2620,2750),col="blue")



plot.ts(sensor_BRR1_mean[c(181:360)]/sensor_BRR1_mean[c(1:180)],ylim=range(1,1.15),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean[c(181:360)]/sensor_BRR0_mean[c(1:180)],ylim=range(1,1.15),col="blue")


plot.ts((sensor_BRR0_mean[c(1:180)]-sensor_BRR1_mean[c(1:180)]),col="blue")
plot.ts((sensor_BRR0_mean[c(1:180)]-sensor_BRR1_mean[c(1:180)])/sensor_BRR0_mean[c(1:180)],col="blue")
par(new=T)
plot.ts((sensor_BRR0_mean[c(181:360)]-sensor_BRR1_mean[c(181:360)])/sensor_BRR0_mean[c(181:360)],col="red")
par(new=T)
plot.ts((sensor_BRR0_mean[c(361:540)]-sensor_BRR1_mean[c(361:540)])/sensor_BRR0_mean[c(361:540)],col="green")
par(new=T)
plot.ts((sensor_BRR0_mean[c(541:720)]-sensor_BRR1_mean[c(541:720)])/sensor_BRR0_mean[c(541:720)],col="black")


# only BRR

sensor_BRR1_only <- subset(sensor_data,TaskName=="Battery R&R\t")
sensor_BRR0_only<- subset(sensor_data,TaskName!="Battery R&R\t")
sensor_BRR1_unique_only <- sensor_BRR1_only[!duplicated(sensor_BRR1_only[,c(10:21)]),]  #46834
sensor_BRR0_unique_only <- sensor_BRR0_only[!duplicated(sensor_BRR0_only[,c(10:21)]),] #184680
sensor_BRR0_subset_only <- sqldf("select * from  sensor_BRR0_unique_only where VehicleID not in 
                            (select VehicleID from sensor_BRR1_unique_only)") #38336

sensor_BRR0_final_only  <- sensor_BRR0_subset_only
sensor_BRR1_final_only <- sensor_BRR1_unique_only

sensor_BRR0_final_only$BRR <-0  # 38333,730
sensor_BRR1_final_only$BRR <-1  # 46766, 730
sensor_BRR0_final_only <- sensor_BRR0_final_only[c(1:13000),]
# explore the mean of BRR=1 and BRR=0
sensor_BRR1_final_only[,c(10:729)] <- sapply(sensor_BRR1_final_only[,c(10:729)] , as.numeric)
sensor_BRR0_final_only[,c(10:729)] <- sapply(sensor_BRR0_final_only[,c(10:729)] , as.numeric)
sensor_BRR1_mean_only <- colMeans(sensor_BRR1_final_only[,c(10:729)],na.rm=T)
sensor_BRR0_mean_only <- colMeans(sensor_BRR0_final_only[,c(10:729)],na.rm=T)
sensor_BRR1_sd_only <- apply(sensor_BRR1_final_only[,c(10:729)],2,sd, na.rm=T)
sensor_BRR0_sd_only <- apply(sensor_BRR0_final_only[,c(10:729)],2,sd,na.rm=T)
sensor_BRR1_median_only <- apply(sensor_BRR1_final_only[,c(10:729)],2,median, na.rm=T)
sensor_BRR0_median_only <- apply(sensor_BRR0_final_only[,c(10:729)],2,median,na.rm=T)
#sensor_BRR1_mod_7 <- colMeans(sensor_BRR1_final[,(c(10:249))%%7],na.rm=T)
#sensor_BRR0_mod_7 <- colMeans(sensor_BRR0_final[,c(10:249)%%7],na.rm=T)
# plot voltage min
plot.ts(sensor_BRR1_mean_only[c(1:180)],ylim=range(13.5,14.0),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(1:180)],ylim=range(13.5,14.0),col="blue")

plot.ts(sensor_BRR1_mean_only[c(181:360)],ylim=range(14,14.4),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(181:360)],ylim=range(14,14.4),col="blue")

plot.ts(sensor_BRR1_median_only[c(181:360)],ylim=range(14,14.4),col="red")
par(new=T)
plot.ts(sensor_BRR0_median_only[c(181:360)],ylim=range(14,14.4),col="blue")

plot.ts(sensor_BRR1_mean_only[c(361:540)],ylim=range(460,525),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(361:540)],ylim=range(460,525),col="blue")

plot.ts(sensor_BRR1_mean_only[c(541:720)],ylim=range(2620,2750),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(541:720)],ylim=range(2620,2750),col="blue")



plot.ts(sensor_BRR1_mean_only[c(181:360)]/sensor_BRR1_mean[c(1:180)],ylim=range(1,1.15),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_only[c(181:360)]/sensor_BRR0_mean[c(1:180)],ylim=range(1,1.15),col="blue")


plot.ts((sensor_BRR0_mean_only[c(1:180)]-sensor_BRR1_mean[c(1:180)]),col="blue")
plot.ts((sensor_BRR0_mean_only[c(1:180)]-sensor_BRR1_mean[c(1:180)])/sensor_BRR0_mean[c(1:180)],col="blue")
par(new=T)
plot.ts((sensor_BRR0_mean_only[c(181:360)]-sensor_BRR1_mean[c(181:360)])/sensor_BRR0_mean[c(181:360)],col="red")
par(new=T)
plot.ts((sensor_BRR0_mean_only[c(361:540)]-sensor_BRR1_mean[c(361:540)])/sensor_BRR0_mean[c(361:540)],col="green")
par(new=T)
plot.ts((sensor_BRR0_mean_only[c(541:720)]-sensor_BRR1_mean[c(541:720)])/sensor_BRR0_mean[c(541:720)],col="black")


# June 10, data by location and month
volmin <- read.csv("daily_180/vol_min.csv",sep="|",header=T,stringsAsFactors=FALSE)
volmax <- read.csv("daily_180/vol_max.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmin <- read.csv("daily_180/esd_min.csv",sep="|",header=T,stringsAsFactors=FALSE)
esdmax <- read.csv("daily_180/esd_max.csv",sep="|",header=T,stringsAsFactors=FALSE)

sensor_data <- cbind(volmin,volmax[,c(10:189)],esdmin[,c(10:189)],esdmax[,c(10:189)])
sensor_data$DemandDate <- as.Date(sensor_data$DemandDate,"%m/%d/%Y")
sensor_data$Month <- as.numeric(format(sensor_data$DemandDate,"%m"))
sensor_BRR1 <- subset(sensor_data, TaskName=="Alternator Belt R&R\t"|
                        TaskName=="Alternator Belt ADJ\t"|
                        TaskName=="Battery R&R\t"|
                        TaskName=="Battery ADJ\t"|
                        TaskName=="Charging/starting System DIA\t"|
                        TaskName=="Electrical System DIA\t"|
                        TaskName=="Alternator R&R\t"|
                        TaskName=="Alternator - REP\t"|
                        TaskName=="Battery/hold Down R&R\t"|
                        TaskName=="Battery Cable R&R\t"|
                        TaskName=="Battery Cable REP\t"|
                        TaskName== "Battery, Charge REP\t"|
                        TaskName=="Voltage Regulator R&R\t"|
                        TaskName=="Electrical System OTH\t"|
                        TaskName=="Battery,egr Warning Lite R&R\t"|
                        TaskName=="Battery Aux Engine Aerial R&R\t"|
                        TaskName=="Battery Mpu/forklift R&R\t"|
                        TaskName=="Battery Aux Engine Aerial R&R\t"|
                        TaskName=="Alternator Mpu R&R\t"|
                        TaskName=="Battery Vpu R&R\t"|
                        TaskName=="Jump Start Vehicle OTH\t"|
                        TaskName=="Battery,auxillary Engine R&R\t"|
                        TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                        TaskName=="Starter R&R\t"|
                        TaskName=="Spark Plugs R&R\t"|
                        TaskName=="Wiring/harness R&R\t"|
                        TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                        TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                        TaskName=="Jump Start Vehicle\t")  #13096

sensor_BRR0<- subset(sensor_data, TaskName!="Alternator Belt R&R\t"&
                       TaskName!="Alternator Belt ADJ\t"&
                       TaskName!="Battery R&R\t"&
                       TaskName!="Battery ADJ\t"&
                       TaskName!="Charging/starting System DIA\t"&
                       TaskName!="Electrical System DIA\t"&
                       TaskName!="Alternator R&R\t"&
                       TaskName!="Alternator - REP\t"&
                       TaskName!="Battery/hold Down R&R\t"&
                       TaskName!="Battery Cable R&R\t"&
                       TaskName!="Battery Cable REP\t"&
                       TaskName!= "Battery, Charge REP\t"&
                       TaskName!="Voltage Regulator R&R\t"&
                       TaskName!="Electrical System OTH\t"&
                       TaskName!="Battery,egr Warning Lite R&R\t"&
                       TaskName!="Battery Aux Engine Aerial R&R\t"&
                       TaskName!="Battery Mpu/forklift R&R\t"&
                       TaskName!="Battery Aux Engine Aerial R&R\t"&
                       TaskName!="Alternator Mpu R&R\t"&
                       TaskName!="Battery Vpu R&R\t"&
                       TaskName!="Jump Start Vehicle OTH\t"&
                       TaskName!="Battery,auxillary Engine R&R\t"&
                       TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                       TaskName!="Starter R&R\t"&
                       TaskName!="Spark Plugs R&R\t"&
                       TaskName!="Wiring/harness R&R\t"&
                       TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                       TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                       TaskName!="Jump Start Vehicle\t")  #77182


sensor_BRR1_unique <- sensor_BRR1[!duplicated(sensor_BRR1[,c(10:21)]),]  #36809, unique vehicles =17202
sensor_BRR0_unique <- sensor_BRR0[!duplicated(sensor_BRR0[,c(10:21)]),] 
sensor_BRR0_subset <- sqldf("select * from  sensor_BRR0_unique where VehicleID not in 
                            (select VehicleID from sensor_BRR1_unique)") #48633,unique vehicle=8838

sensor_BRR0_final  <- sensor_BRR0_subset
sensor_BRR1_final <- sensor_BRR1_unique

sensor_BRR0_final$BRR <-0
sensor_BRR1_final$BRR <-1


# by location

sensor_BRR0_S_Winter <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR0_N_Winter <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|State=="MT"|State=="NY"|State=="OH"))
sensor_BRR1_S_Winter <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Winter <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|State=="MT"|State=="NY"|State=="OH"))


sensor_BRR0_final[,c(10:729)] <- sapply(sensor_BRR0_final[,c(10:729)] , as.numeric)
sensor_BRR1_final[,c(10:729)] <- sapply(sensor_BRR1_final[,c(10:729)] , as.numeric)
# by month
sensor_BRR0_1 <- subset(sensor_BRR0_final, Month==1)
sensor_BRR0_2 <- subset(sensor_BRR0_final, Month==2)
sensor_BRR0_3 <- subset(sensor_BRR0_final, Month==3)
sensor_BRR0_4 <- subset(sensor_BRR0_final, Month==4)
sensor_BRR0_5 <- subset(sensor_BRR0_final, Month==5)
sensor_BRR0_6 <- subset(sensor_BRR0_final, Month==6)
sensor_BRR0_7 <- subset(sensor_BRR0_final, Month==7)
sensor_BRR0_8 <- subset(sensor_BRR0_final, Month==8)
sensor_BRR0_9 <- subset(sensor_BRR0_final, Month==9)
sensor_BRR0_10 <- subset(sensor_BRR0_final, Month==10)
sensor_BRR0_11 <- subset(sensor_BRR0_final, Month==11)
sensor_BRR0_12 <- subset(sensor_BRR0_final, Month==12)
sensor_BRR1_1 <- subset(sensor_BRR1_final, Month==1)
sensor_BRR1_2 <- subset(sensor_BRR1_final, Month==2)
sensor_BRR1_3 <- subset(sensor_BRR1_final, Month==3)
sensor_BRR1_4 <- subset(sensor_BRR1_final, Month==4)
sensor_BRR1_5 <- subset(sensor_BRR1_final, Month==5)
sensor_BRR1_6 <- subset(sensor_BRR1_final, Month==6)
sensor_BRR1_7 <- subset(sensor_BRR1_final, Month==7)
sensor_BRR1_8 <- subset(sensor_BRR1_final, Month==8)
sensor_BRR1_9 <- subset(sensor_BRR1_final, Month==9)
sensor_BRR1_10 <- subset(sensor_BRR1_final, Month==10)
sensor_BRR1_11 <- subset(sensor_BRR1_final, Month==11)
sensor_BRR1_12 <- subset(sensor_BRR1_final, Month==12)

sensor_BRR0_mean_1<- colMeans(sensor_BRR0_1[,c(10:729)],na.rm=T)
sensor_BRR0_mean_2<- colMeans(sensor_BRR0_2[,c(10:729)],na.rm=T)
sensor_BRR0_mean_3<- colMeans(sensor_BRR0_3[,c(10:729)],na.rm=T)
sensor_BRR0_mean_4<- colMeans(sensor_BRR0_4[,c(10:729)],na.rm=T)
sensor_BRR0_mean_5<- colMeans(sensor_BRR0_5[,c(10:729)],na.rm=T)
sensor_BRR0_mean_6<- colMeans(sensor_BRR0_6[,c(10:729)],na.rm=T)
sensor_BRR0_mean_7<- colMeans(sensor_BRR0_7[,c(10:729)],na.rm=T)
sensor_BRR0_mean_8<- colMeans(sensor_BRR0_8[,c(10:729)],na.rm=T)
sensor_BRR0_mean_9<- colMeans(sensor_BRR0_9[,c(10:729)],na.rm=T)
sensor_BRR0_mean_10<- colMeans(sensor_BRR0_10[,c(10:729)],na.rm=T)
sensor_BRR0_mean_11<- colMeans(sensor_BRR0_11[,c(10:729)],na.rm=T)
sensor_BRR0_mean_12<- colMeans(sensor_BRR0_12[,c(10:729)],na.rm=T)
sensor_BRR1_mean_1<- colMeans(sensor_BRR1_1[,c(10:729)],na.rm=T)
sensor_BRR1_mean_2<- colMeans(sensor_BRR1_2[,c(10:729)],na.rm=T)
sensor_BRR1_mean_3<- colMeans(sensor_BRR1_3[,c(10:729)],na.rm=T)
sensor_BRR1_mean_4<- colMeans(sensor_BRR1_4[,c(10:729)],na.rm=T)
sensor_BRR1_mean_5<- colMeans(sensor_BRR1_5[,c(10:729)],na.rm=T)
sensor_BRR1_mean_6<- colMeans(sensor_BRR1_6[,c(10:729)],na.rm=T)
sensor_BRR1_mean_7<- colMeans(sensor_BRR1_7[,c(10:729)],na.rm=T)
sensor_BRR1_mean_8<- colMeans(sensor_BRR1_8[,c(10:729)],na.rm=T)
sensor_BRR1_mean_9<- colMeans(sensor_BRR1_9[,c(10:729)],na.rm=T)
sensor_BRR1_mean_10<- colMeans(sensor_BRR1_10[,c(10:729)],na.rm=T)
sensor_BRR1_mean_11<- colMeans(sensor_BRR1_11[,c(10:729)],na.rm=T)
sensor_BRR1_mean_12<- colMeans(sensor_BRR1_12[,c(10:729)],na.rm=T)

# winter
sensor_BRR0_S_Winter <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR0_N_Winter <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Winter <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Winter <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_W<- colMeans(sensor_BRR0_S_Winter[,c(10:729)],na.rm=T)
sensor_BRR0_N_W<- colMeans(sensor_BRR0_N_Winter[,c(10:729)],na.rm=T)
sensor_BRR1_S_W<- colMeans(sensor_BRR1_S_Winter[,c(10:729)],na.rm=T)
sensor_BRR1_N_W<- colMeans(sensor_BRR1_N_Winter[,c(10:729)],na.rm=T)
plot.ts(sensor_BRR0_N_W[c(1:180)],ylim=range(12.4,14),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_W[c(1:180)],ylim=range(12.4,14),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_W[c(1:180)],ylim=range(12.4,14),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_W[c(1:180)],ylim=range(12.4,14),col="black")

plot.ts(sensor_BRR0_N_W[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_W[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_W[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_W[c(181:360)],ylim=range(13.8,14.6),col="black")

# Summer for both north and south regions

sensor_BRR0_S_Summer <- subset(sensor_BRR0_final, (Month==6|Month==7|Month==8)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Summer <- subset(sensor_BRR0_final, (Month==6|Month==7|Month==8)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Summer <- subset(sensor_BRR1_final, (Month==6|Month==7|Month==8)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Summer <- subset(sensor_BRR1_final, (Month==6|Month==7|Month==8)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_S<- colMeans(sensor_BRR0_S_Summer[,c(10:729)],na.rm=T)
sensor_BRR0_N_S<- colMeans(sensor_BRR0_N_Summer[,c(10:729)],na.rm=T)
sensor_BRR1_S_S<- colMeans(sensor_BRR1_S_Summer[,c(10:729)],na.rm=T)
sensor_BRR1_N_S<- colMeans(sensor_BRR1_N_Summer[,c(10:729)],na.rm=T)
# min voltage
plot.ts(sensor_BRR0_N_S[c(1:180)],ylim=range(12.4,14.0),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_S[c(1:180)],ylim=range(12.4,14.0),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_S[c(1:180)],ylim=range(12.4,14.0),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_S[c(1:180)],ylim=range(12.4,14.0),col="black")
# max voltage
plot.ts(sensor_BRR0_N_S[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_S[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_S[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_S[c(181:360)],ylim=range(13.8,14.6),col="black")
# Spring

sensor_BRR0_S_Spring <- subset(sensor_BRR0_final, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Spring <- subset(sensor_BRR0_final, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Spring <- subset(sensor_BRR1_final, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Spring <- subset(sensor_BRR1_final, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_Sp<- colMeans(sensor_BRR0_S_Spring[,c(10:729)],na.rm=T)
sensor_BRR0_N_Sp<- colMeans(sensor_BRR0_N_Spring[,c(10:729)],na.rm=T)
sensor_BRR1_S_Sp<- colMeans(sensor_BRR1_S_Spring[,c(10:729)],na.rm=T)
sensor_BRR1_N_Sp<- colMeans(sensor_BRR1_N_Spring[,c(10:729)],na.rm=T)
# min voltage
plot.ts(sensor_BRR0_N_Sp[c(1:180)],ylim=range(12.4,14.0),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_Sp[c(1:180)],ylim=range(12.4,14.0),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_Sp[c(1:180)],ylim=range(12.4,14.0),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(1:180)],ylim=range(12.4,14.0),col="black")
# max voltage
plot.ts(sensor_BRR0_N_Sp[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_Sp[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_Sp[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(181:360)],ylim=range(13.8,14.6),col="black")

#Fall

sensor_BRR0_S_Fall <- subset(sensor_BRR0_final, (Month==9|Month==10|Month==11)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Fall <- subset(sensor_BRR0_final, (Month==9|Month==10|Month==11)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Fall <- subset(sensor_BRR1_final, (Month==9|Month==10|Month==11)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Fall <- subset(sensor_BRR1_final, (Month==9|Month==10|Month==11)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_F<- colMeans(sensor_BRR0_S_Fall[,c(10:729)],na.rm=T)
sensor_BRR0_N_F<- colMeans(sensor_BRR0_N_Fall[,c(10:729)],na.rm=T)
sensor_BRR1_S_F<- colMeans(sensor_BRR1_S_Fall[,c(10:729)],na.rm=T)
sensor_BRR1_N_F<- colMeans(sensor_BRR1_N_Fall[,c(10:729)],na.rm=T)
# min voltage
plot.ts(sensor_BRR0_N_F[c(1:180)],ylim=range(12.4,14.0),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_F[c(1:180)],ylim=range(12.4,14.0),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_F[c(1:180)],ylim=range(12.4,14.0),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_F[c(1:180)],ylim=range(12.4,14.0),col="black")
# max voltage
plot.ts(sensor_BRR0_N_F[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_F[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_F[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_F[c(181:360)],ylim=range(13.8,14.6),col="black")
#min esd
plot.ts(sensor_BRR0_N_F[c(361:540)],ylim=range(470,600),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_F[c(361:540)],ylim=range(470,600),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_F[c(361:540)],ylim=range(470,600),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_F[c(361:540)],ylim=range(470,600),col="black")
# max esd
plot.ts(sensor_BRR0_N_F[c(541:720)],ylim=range(2500,2800),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_F[c(541:720)],ylim=range(2500,2800),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_F[c(541:720)],ylim=range(2500,2800),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_F[c(541:720)],ylim=range(2500,2800),col="black")


library(TTR)   
#Fall
sensor_S_F_diff_volmin <- sensor_BRR0_S_F[c(1:180)]-sensor_BRR1_S_F[c(1:180)]
sensor_N_F_diff_volmin <- sensor_BRR0_N_F[c(1:180)]-sensor_BRR1_N_F[c(1:180)]
sensor_S_F_diff_volmin_SMA<- SMA(sensor_S_F_diff_volmin,n=10)   
sensor_N_F_diff_volmin_SMA<- SMA(sensor_N_F_diff_volmin,n=10)   
plot.ts(sensor_S_F_diff_volmin_SMA,col="red",ylim=range(0,0.5))
par(new=T)
plot.ts(sensor_S_F_diff_volmin,col="blue",ylim=range(0,0.5))
par(new=T)
plot.ts(sensor_N_F_diff_volmin_SMA,col="purple",ylim=range(0,0.5))
par(new=T)
plot.ts(sensor_N_F_diff_volmin,col="black",ylim=range(0,0.5))


sensor_S_F_diff_volmax <- sensor_BRR0_S_F[c(181:360)]-sensor_BRR1_S_F[c(181:360)]
sensor_N_F_diff_volmax <- sensor_BRR0_N_F[c(181:360)]-sensor_BRR1_N_F[c(181:360)]
sensor_S_F_diff_volmax_SMA<- SMA(sensor_S_F_diff_volmax,n=6)   
sensor_N_F_diff_volmax_SMA<- SMA(sensor_N_F_diff_volmax,n=6)   
plot.ts(sensor_S_F_diff_volmax_SMA,col="red",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_S_F_diff_volmax,col="blue",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_F_diff_volmax_SMA,col="purple",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_F_diff_volmax,col="black",ylim=range(0,0.2))

sensor_S_F_diff_esdmin <- sensor_BRR0_S_F[c(361:540)]-sensor_BRR1_S_F[c(361:540)]
sensor_N_F_diff_esdmin <- sensor_BRR0_N_F[c(361:540)]-sensor_BRR1_N_F[c(361:540)]
sensor_S_F_diff_esdmin_SMA<- SMA(sensor_S_F_diff_esdmin,n=16)   
sensor_N_F_diff_esdmin_SMA<- SMA(sensor_N_F_diff_esdmin,n=16)   
plot.ts(sensor_S_F_diff_esdmin_SMA,col="red",ylim=range(0,50))
par(new=T)
plot.ts(sensor_S_F_diff_esdmin,col="blue",ylim=range(0,50))
par(new=T)
plot.ts(sensor_N_F_diff_esdmin_SMA,col="purple",ylim=range(0,50))
par(new=T)
plot.ts(sensor_N_F_diff_esdmin,col="black",ylim=range(0,50))

sensor_S_F_diff_volmax <- sensor_BRR0_S_F[c(181:360)]-sensor_BRR1_S_F[c(181:360)]
sensor_N_F_diff_volmax <- sensor_BRR0_N_F[c(181:360)]-sensor_BRR1_N_F[c(181:360)]
sensor_S_F_diff_volmax_SMA<- SMA(sensor_S_F_diff_volmax,n=6)   
sensor_N_F_diff_volmax_SMA<- SMA(sensor_N_F_diff_volmax,n=6)   
plot.ts(sensor_S_F_diff_volmax_SMA,col="red",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_S_F_diff_volmax,col="blue",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_F_diff_volmax_SMA,col="purple",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_F_diff_volmax,col="black",ylim=range(0,0.2))

sensor_S_Sp_diff_volmax <- sensor_BRR0_S_Sp[c(181:360)]-sensor_BRR1_S_Sp[c(181:360)]
sensor_N_Sp_diff_volmax <- sensor_BRR0_N_Sp[c(181:360)]-sensor_BRR1_N_Sp[c(181:360)]
sensor_S_Sp_diff_volmax_SMA<- SMA(sensor_S_Sp_diff_volmax,n=16)   
sensor_N_Sp_diff_volmax_SMA<- SMA(sensor_N_Sp_diff_volmax,n=16)   
plot.ts(sensor_S_Sp_diff_volmax_SMA,col="red",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_S_Sp_diff_volmax,col="blue",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_Sp_diff_volmax_SMA,col="purple",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_Sp_diff_volmax,col="black",ylim=range(0,0.2))

plot.ts(sensor_S_Sp_diff_volmax_SMA,col="red",ylim=range(-0.02,0.2))
par(new=T)
plot.ts(sensor_N_Sp_diff_volmax_SMA,col="blue",ylim=range(-0.02,0.2))

#Spring
sensor_S_Sp_diff_volmin <- sensor_BRR0_S_Sp[c(1:180)]-sensor_BRR1_S_Sp[c(1:180)]
sensor_N_Sp_diff_volmin <- sensor_BRR0_N_Sp[c(1:180)]-sensor_BRR1_N_Sp[c(1:180)]
sensor_S_Sp_diff_volmin_SMA<- SMA(sensor_S_Sp_diff_volmin,n=10)   
sensor_N_Sp_diff_volmin_SMA<- SMA(sensor_N_Sp_diff_volmin,n=10)   
plot.ts(sensor_S_Sp_diff_volmin_SMA,col="red",ylim=range(0,0.5))
par(new=T)
plot.ts(sensor_S_Sp_diff_volmin,col="blue",ylim=range(0,0.5))
par(new=T)
plot.ts(sensor_N_Sp_diff_volmin_SMA,col="purple",ylim=range(0,0.5))
par(new=T)
plot.ts(sensor_N_Sp_diff_volmin,col="black",ylim=range(0,0.5))


sensor_S_Sp_diff_volmax <- sensor_BRR0_S_Sp[c(181:360)]-sensor_BRR1_S_Sp[c(181:360)]
sensor_N_Sp_diff_volmax <- sensor_BRR0_N_Sp[c(181:360)]-sensor_BRR1_N_Sp[c(181:360)]
sensor_S_Sp_diff_volmax_SMA<- SMA(sensor_S_Sp_diff_volmax,n=16)   
sensor_N_Sp_diff_volmax_SMA<- SMA(sensor_N_Sp_diff_volmax,n=16)   
plot.ts(sensor_S_Sp_diff_volmax_SMA,col="red",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_S_Sp_diff_volmax,col="blue",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_Sp_diff_volmax_SMA,col="purple",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_Sp_diff_volmax,col="black",ylim=range(0,0.2))

plot.ts(sensor_S_Sp_diff_volmax_SMA,col="red",ylim=range(-0.02,0.2))
par(new=T)
plot.ts(sensor_N_Sp_diff_volmax_SMA,col="blue",ylim=range(-0.02,0.2))


#difference between volmax and volmin
sensor_S_Sp_diff_BRR0 <- sensor_BRR0_S_Sp[c(181:360)]-sensor_BRR0_S_Sp[c(1:180)]
sensor_S_Sp_diff_BRR1 <- sensor_BRR1_S_Sp[c(181:360)]-sensor_BRR1_S_Sp[c(1:180)]
sensor_N_Sp_diff_BRR0 <- sensor_BRR0_N_Sp[c(181:360)]-sensor_BRR0_N_Sp[c(1:180)]
sensor_N_Sp_diff_BRR1 <- sensor_BRR1_N_Sp[c(181:360)]-sensor_BRR1_N_Sp[c(1:180)]
sensor_S_Sp_diff_BRR0_SMA<- SMA(sensor_S_Sp_diff_BRR0,n=6)   
sensor_S_Sp_diff_BRR1_SMA<- SMA(sensor_S_Sp_diff_BRR1,n=6)
sensor_N_Sp_diff_BRR0_SMA<- SMA(sensor_N_Sp_diff_BRR0,n=6)   
sensor_N_Sp_diff_BRR1_SMA<- SMA(sensor_N_Sp_diff_BRR1,n=6)   
plot.ts(sensor_S_Sp_diff_BRR0_SMA,col="red",ylim=range(0.8,1.5))
par(new=T)
plot.ts(sensor_S_Sp_diff_BRR1_SMA,col="blue",ylim=range(0.8,1.5))
par(new=T)
plot.ts(sensor_N_Sp_diff_BRR0_SMA,col="purple",ylim=range(0.8,1.5))
par(new=T)
plot.ts(sensor_N_Sp_diff_BRR1_SMA,col="black",ylim=range(0.8,1.5))

#Winter
sensor_S_W_diff_volmin <- sensor_BRR0_S_W[c(1:180)]-sensor_BRR1_S_W[c(1:180)]
sensor_N_W_diff_volmin <- sensor_BRR0_N_W[c(1:180)]-sensor_BRR1_N_W[c(1:180)]
sensor_S_W_diff_volmin_SMA<- SMA(sensor_S_W_diff_volmin,n=6)   
sensor_N_W_diff_volmin_SMA<- SMA(sensor_N_W_diff_volmin,n=6)   
plot.ts(sensor_S_W_diff_volmin_SMA,col="red",ylim=range(0,0.5))
par(new=T)
plot.ts(sensor_S_W_diff_volmin,col="blue",ylim=range(0,0.5))
par(new=T)
plot.ts(sensor_N_W_diff_volmin_SMA,col="purple",ylim=range(0,0.5))
par(new=T)
plot.ts(sensor_N_W_diff_volmin,col="black",ylim=range(0,0.5))


sensor_S_W_diff_volmax <- sensor_BRR0_S_W[c(181:360)]-sensor_BRR1_S_W[c(181:360)]
sensor_N_W_diff_volmax <- sensor_BRR0_N_W[c(181:360)]-sensor_BRR1_N_W[c(181:360)]
sensor_S_W_diff_volmax_SMA<- SMA(sensor_S_W_diff_volmax,n=6)   
sensor_N_W_diff_volmax_SMA<- SMA(sensor_N_W_diff_volmax,n=6)   
plot.ts(sensor_S_W_diff_volmax_SMA,col="red",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_S_W_diff_volmax,col="blue",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_W_diff_volmax_SMA,col="purple",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_W_diff_volmax,col="black",ylim=range(0,0.2))
#Summer
sensor_S_S_diff_volmin <- sensor_BRR0_S_S[c(1:180)]-sensor_BRR1_S_S[c(1:180)]
sensor_N_S_diff_volmin <- sensor_BRR0_N_S[c(1:180)]-sensor_BRR1_N_S[c(1:180)]
sensor_S_S_diff_volmin_SMA<- SMA(sensor_S_S_diff_volmin,n=6)   
sensor_N_S_diff_volmin_SMA<- SMA(sensor_N_S_diff_volmin,n=6)   
plot.ts(sensor_S_S_diff_volmin_SMA,col="red",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_S_S_diff_volmin,col="blue",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_S_diff_volmin_SMA,col="purple",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_S_diff_volmin,col="black",ylim=range(0,0.2))

sensor_S_S_diff_volmax <- sensor_BRR0_S_S[c(181:360)]-sensor_BRR1_S_S[c(181:360)]
sensor_N_S_diff_volmax <- sensor_BRR0_N_S[c(181:360)]-sensor_BRR1_N_S[c(181:360)]
sensor_S_S_diff_volmax_SMA<- SMA(sensor_S_S_diff_volmax,n=6)   
sensor_N_S_diff_volmax_SMA<- SMA(sensor_N_S_diff_volmax,n=6)   
plot.ts(sensor_S_S_diff_volmax_SMA,col="red",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_S_S_diff_volmax,col="blue",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_S_diff_volmax_SMA,col="purple",ylim=range(0,0.2))
par(new=T)
plot.ts(sensor_N_S_diff_volmax,col="black",ylim=range(0,0.2))






sensor_BRR0_mean_S_S_volmin_SMA<- SMA(sensor_BRR0_N_S[c(1:180)],n=6)   
plot.ts(sensor_BRR0_mean_S_S_volmin_SMA,col="red",ylim=range(13.0,13.8))
par(new=T)
plot.ts(sensor_BRR0_N_S[c(1:180)],col="blue",ylim=range(13.0,13.8))
plot.ts(sensor_BRR0_S_F[c(181:360)]-sensor_BRR1_S_F[c(181:360)],ylim=range(0.05,0.2),col="red")





plot.ts(sensor_BRR0_N_S[c(1:180)]/sensor_BRR0_N_S[c(181:360)],ylim=range(0.8,1.0),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_S[c(1:180)]/sensor_BRR0_N_S[c(181:360)],ylim=range(0.8,1.0),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_S[c(1:180)]/sensor_BRR0_N_S[c(181:360)],ylim=range(0.8,1.0),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_S[c(1:180)]/sensor_BRR0_N_S[c(181:360)],ylim=range(0.8,1.0),col="black")
# min esd
plot.ts(sensor_BRR0_N_S[c(361:540)],ylim=range(450,600),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_S[c(361:540)],ylim=range(450,600),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_S[c(361:540)],ylim=range(450,600),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_S[c(361:540)],ylim=range(450,600),col="black")

#max esd

plot.ts(sensor_BRR1_mean_1[c(1:180)],ylim=range(12.4,13.5),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_1[c(1:180)],ylim=range(12.4,13.5),col="green")

plot.ts(sensor_BRR1_mean_5[c(181:360)],ylim=range(13.4,14.5),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_5[c(181:360)],ylim=range(13.4,14.5),col="green")

plot.ts(sensor_BRR1_mean_1[c(361:540)],ylim=range(450,550),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_1[c(361:540)],ylim=range(450,550),col="green")

plot.ts(sensor_BRR1_mean_1[c(541:720)],col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_1[c(541:720)],col="green")

# look at in the winter, difference between south and north

sensor_BRR0_S_Winter[,c(10:189)] <- sapply(sensor_BRR0_S_Winter[,c(10:189)] , as.numeric)
sensor_BRR0_N_Winter[,c(10:189)] <- sapply(sensor_BRR0_N_Winter[,c(10:189)] , as.numeric)
sensor_BRR1_S_Winter[,c(10:189)] <- sapply(sensor_BRR1_S_Winter[,c(10:189)] , as.numeric)
sensor_BRR1_N_Winter[,c(10:189)] <- sapply(sensor_BRR1_N_Winter[,c(10:189)] , as.numeric)
sensor_BRR0_mean_SWinter<- colMeans(sensor_BRR0_S_Winter[,c(10:189)],na.rm=T)
sensor_BRR0_mean_NWinter<- colMeans(sensor_BRR0_N_Winter[,c(10:189)],na.rm=T)
sensor_BRR1_mean_SWinter<- colMeans(sensor_BRR1_S_Winter[,c(10:189)],na.rm=T)
sensor_BRR1_mean_NWinter<- colMeans(sensor_BRR1_N_Winter[,c(10:189)],na.rm=T)
plot.ts(sensor_BRR0_mean_SWinter[c(1:180)],ylim=range(13.3,14.3),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_NWinter[c(1:180)],ylim=range(13.3,14.3),col="green")
par(new=T)
plot.ts(sensor_BRR1_mean_SWinter[c(1:180)],ylim=range(13.3,14.3),col="blue")
par(new=T)
plot.ts(sensor_BRR1_mean_NWinter[c(1:180)],ylim=range(13.3,14.3),col="black")

plot.ts(sensor_BRR0_mean_SWinter[c(1:180)]-sensor_BRR1_mean_SWinter[c(1:180)],ylim=range(-0.2,0.2),col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_NWinter[c(1:180)]-sensor_BRR1_mean_NWinter[c(1:180)],ylim=range(-0.2,0.2),col="green")



# standard deviation change when split data into north/south and seasons
sensor_sd <- apply(sensor_data[,c(10:729)], 2, sd,na.rm=T)
sensor_BRR0_S_Winter_sd <- apply(sensor_BRR0_S_Winter[,c(10:729)], 2, sd,na.rm=T)
sensor_BRR0_N_Winter_sd <- apply(sensor_BRR0_N_Winter[,c(10:729)], 2, sd,na.rm=T)
sensor_BRR1_S_Winter_sd <- apply(sensor_BRR1_S_Winter[,c(10:729)], 2, sd,na.rm=T)
sensor_BRR1_N_Winter_sd <- apply(sensor_BRR1_N_Winter[,c(10:729)], 2, sd,na.rm=T)
sensor_BRR0_S_Summer_sd <- apply(sensor_BRR0_S_Summer[,c(10:729)], 2, sd,na.rm=T)

sensor_BRR0_S_1 <- subset(sensor_BRR0_final, (Month==1)&
                               (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_1 <- subset(sensor_BRR0_final, (Month==1)&
                               (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                  State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_1 <- subset(sensor_BRR1_final, (Month==1)&
                               (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_1 <- subset(sensor_BRR1_final, (Month==1&
                               (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                  State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
                          
sensor_BRR0_S_1_sd <- apply(sensor_BRR0_S_1[,c(10:729)], 2, sd,na.rm=T) 
sensor_BRR0_N_1_sd <- apply(sensor_BRR0_N_1[,c(10:729)], 2, sd,na.rm=T)
sensor_BRR1_S_1_sd <- apply(sensor_BRR1_S_1[,c(10:729)], 2, sd,na.rm=T)
sensor_BRR1_N_1_sd <- apply(sensor_BRR1_N_1[,c(10:729)], 2, sd,na.rm=T)
                          
                          
# Smooth the 180days curves                         
library(TTR)                       
sensor_BRR0_mean_SWinter_volmin_SMA<- SMA(sensor_BRR0_mean_SWinter[c(1:180)],n=6)   
plot.ts(sensor_BRR0_mean_SWinter_volmin_SMA,col="red")
par(new=T)
plot.ts(sensor_BRR0_mean_SWinter[c(1:180)],col="blue")

sensor_BRR0_mean_S_S_volmin_SMA<- SMA(sensor_BRR0_N_S[c(1:180)],n=6)   
plot.ts(sensor_BRR0_mean_S_S_volmin_SMA,col="red",ylim=range(13.0,13.8))
par(new=T)
plot.ts(sensor_BRR0_N_S[c(1:180)],col="blue",ylim=range(13.0,13.8))



# June 11
batt_data <- read.csv("data_for_trail/data_for_model/0607/battery_age_mileage_processed_with_master.csv")
battage_by_SpecID_mean <- tapply(batt_data$BatteryAge, batt_data$SpecID, mean)
battage_by_SpecID_sd <- tapply(batt_data$BatteryAge, batt_data$SpecID, sd)
batt_mean_by_SpecID <- cbind(names(battage_by_SpecID_mean),battage_by_SpecID_mean)
batt_mean_by_SpecID <- as.data.frame(batt_mean_by_SpecID)
batt_sd_by_SpecID <- cbind(names(battage_by_SpecID_sd),battage_by_SpecID_sd)
batt_sd_by_SpecID <- as.data.frame(batt_sd_by_SpecID)
names(batt_mean_by_SpecID) <- c("SpecID","battage_mean_by_SpecID")
names(batt_sd_by_SpecID) <- c("SpecID","Battage_sd_by_SpecID")
library(sqldf)
data <- merge(x = batt_data,y=batt_mean_by_SpecID, by="SpecID",x.all=T)
data2 <- merge(x = data,y=batt_sd_by_SpecID, by="SpecID",x.all=T)  #67262

jobdate <- as.Date(data2$JobDate,"%m/%d/%y")
data2$JobDate <- jobdate
data2$Month <- as.numeric(format(data2$JobDate, "%m"))
write.csv(data2,"data_for_trail/data_for_model/0607/Batt_age_processed_stand.csv")


# vol mean
vol_mean <- read.csv("daily_180/vol_mean.csv",sep="|",header=T)
vol_median <- read.csv("daily_180/vol_median.csv",sep="|",header=T)
vol_below <- read.csv("daily_180/vol_below.csv",sep="|",header=T)

vol_below[,c(10:189)] <- sapply(vol_below[,c(10:189)] , as.numeric)
vol_below_mean<- colMeans(vol_below[,c(10:189)],na.rm=T)
plot.ps(vol_below_mean)

# standard deviation with vehicle group

sensor_BRR0_S_Winter_vg20 <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&(VehicleGroup==20)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR0_N_Winter_vg20 <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&(VehicleGroup==20)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Winter_vg20 <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&(VehicleGroup==20)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Winter_vg20 <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&(VehicleGroup==20)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))


sensor_BRR0_S_Winter_vg11 <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&(VehicleGroup==11)&
                                      (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR0_N_Winter_vg11 <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&(VehicleGroup==11)&
                                      (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                         State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_Winter_vg10 <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&(VehicleGroup==10)&
                                      (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR0_N_Winter_vg10 <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&(VehicleGroup==10)&
                                      (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                         State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_Winter_vg20_sd <- apply(sensor_BRR0_S_Winter_vg20[,c(10:729)],2,sd,na.rm=T)
sensor_BRR0_N_Winter_vg20_sd <- apply(sensor_BRR0_N_Winter_vg20[,c(10:729)],2,sd,na.rm=T)
sensor_BRR0_S_Winter_vg11_sd <- apply(sensor_BRR0_S_Winter_vg11[,c(10:729)],2,sd,na.rm=T)
sensor_BRR0_N_Winter_vg11_sd <- apply(sensor_BRR0_N_Winter_vg11[,c(10:729)],2,sd,na.rm=T)
sensor_BRR0_S_Winter_vg10_sd <- apply(sensor_BRR0_S_Winter_vg10[,c(10:729)],2,sd,na.rm=T)
sensor_BRR0_N_Winter_vg10_sd <- apply(sensor_BRR0_N_Winter_vg10[,c(10:729)],2,sd,na.rm=T)



# voltage mean
head(vol_mean)
sensor_BRR0_S_Winter <- subset(vol_mean, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR0_N_Winter <- subset(vol_mean, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Winter <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Winter <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_W<- colMeans(sensor_BRR0_S_Winter[,c(10:729)],na.rm=T)
sensor_BRR0_N_W<- colMeans(sensor_BRR0_N_Winter[,c(10:729)],na.rm=T)
sensor_BRR1_S_W<- colMeans(sensor_BRR1_S_Winter[,c(10:729)],na.rm=T)
sensor_BRR1_N_W<- colMeans(sensor_BRR1_N_Winter[,c(10:729)],na.rm=T)
plot.ts(sensor_BRR0_N_W[c(1:180)],ylim=range(12.4,14),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_W[c(1:180)],ylim=range(12.4,14),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_W[c(1:180)],ylim=range(12.4,14),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_W[c(1:180)],ylim=range(12.4,14),col="black")

plot.ts(sensor_BRR0_N_W[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_W[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_W[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_W[c(181:360)],ylim=range(13.8,14.6),col="black")

# Summer for both north and south regions

sensor_BRR0_S_Summer <- subset(sensor_BRR0_final, (Month==6|Month==7|Month==8)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Summer <- subset(sensor_BRR0_final, (Month==6|Month==7|Month==8)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Summer <- subset(sensor_BRR1_final, (Month==6|Month==7|Month==8)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Summer <- subset(sensor_BRR1_final, (Month==6|Month==7|Month==8)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_S<- colMeans(sensor_BRR0_S_Summer[,c(10:729)],na.rm=T)
sensor_BRR0_N_S<- colMeans(sensor_BRR0_N_Summer[,c(10:729)],na.rm=T)
sensor_BRR1_S_S<- colMeans(sensor_BRR1_S_Summer[,c(10:729)],na.rm=T)
sensor_BRR1_N_S<- colMeans(sensor_BRR1_N_Summer[,c(10:729)],na.rm=T)
# min voltage
plot.ts(sensor_BRR0_N_S[c(1:180)],ylim=range(12.4,14.0),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_S[c(1:180)],ylim=range(12.4,14.0),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_S[c(1:180)],ylim=range(12.4,14.0),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_S[c(1:180)],ylim=range(12.4,14.0),col="black")
# max voltage
plot.ts(sensor_BRR0_N_S[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_S[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_S[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_S[c(181:360)],ylim=range(13.8,14.6),col="black")
# Spring

sensor_BRR0_S_Spring <- subset(sensor_BRR0_final, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Spring <- subset(sensor_BRR0_final, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Spring <- subset(sensor_BRR1_final, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Spring <- subset(sensor_BRR1_final, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_Sp<- colMeans(sensor_BRR0_S_Spring[,c(10:729)],na.rm=T)
sensor_BRR0_N_Sp<- colMeans(sensor_BRR0_N_Spring[,c(10:729)],na.rm=T)
sensor_BRR1_S_Sp<- colMeans(sensor_BRR1_S_Spring[,c(10:729)],na.rm=T)
sensor_BRR1_N_Sp<- colMeans(sensor_BRR1_N_Spring[,c(10:729)],na.rm=T)
# min voltage
plot.ts(sensor_BRR0_N_Sp[c(1:180)],ylim=range(12.4,14.0),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_Sp[c(1:180)],ylim=range(12.4,14.0),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_Sp[c(1:180)],ylim=range(12.4,14.0),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(1:180)],ylim=range(12.4,14.0),col="black")
# max voltage
plot.ts(sensor_BRR0_N_Sp[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_Sp[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_Sp[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(181:360)],ylim=range(13.8,14.6),col="black")

#Fall

sensor_BRR0_S_Fall <- subset(sensor_BRR0_final, (Month==9|Month==10|Month==11)&
                               (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Fall <- subset(sensor_BRR0_final, (Month==9|Month==10|Month==11)&
                               (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                  State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Fall <- subset(sensor_BRR1_final, (Month==9|Month==10|Month==11)&
                               (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Fall <- subset(sensor_BRR1_final, (Month==9|Month==10|Month==11)&
                               (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                  State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_F<- colMeans(sensor_BRR0_S_Fall[,c(10:729)],na.rm=T)
sensor_BRR0_N_F<- colMeans(sensor_BRR0_N_Fall[,c(10:729)],na.rm=T)
sensor_BRR1_S_F<- colMeans(sensor_BRR1_S_Fall[,c(10:729)],na.rm=T)
sensor_BRR1_N_F<- colMeans(sensor_BRR1_N_Fall[,c(10:729)],na.rm=T)
# min voltage
plot.ts(sensor_BRR0_N_F[c(1:180)],ylim=range(12.4,14.0),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_F[c(1:180)],ylim=range(12.4,14.0),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_F[c(1:180)],ylim=range(12.4,14.0),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_F[c(1:180)],ylim=range(12.4,14.0),col="black")
# max voltage
plot.ts(sensor_BRR0_N_F[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_F[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_F[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_F[c(181:360)],ylim=range(13.8,14.6),col="black")



# June 11 Only keep vol_min =[12.5,13.8] and vol_max=[13.8,14.5]
vol_max <- read.csv("daily_180/vol_max.csv",sep="|",header=T,stringsAsFactors=FALSE)
vol_min <- read.csv("daily_180/vol_min.csv",sep="|",header=T,stringsAsFactors=FALSE)
esd_max <- read.csv("daily_180/esd_max.csv",sep="|",header=T,stringsAsFactors=FALSE)
esd_min <- read.csv("daily_180/esd_min.csv",sep="|",header=T,stringsAsFactors=FALSE)
vol_count <-read.csv("daily_180/vol_count.csv",sep="|",header=T,stringsAsFactors=FALSE)
vol_mean <- read.csv("daily_180/vol_mean.csv",sep="|",header=T,stringsAsFactors=FALSE)
sensor_data <- cbind(vol_min,vol_max[,c(10:189)],vol_mean[,c(10:189)],esd_min[,c(10:189)],esd_max[,c(10:189)])
write.csv(sensor_data,"daily_180/sensor_vol_esd.csv")
sensor_BRR1 <- subset(sensor_data, TaskName=="Alternator Belt R&R\t"|
                        TaskName=="Alternator Belt ADJ\t"|
                        TaskName=="Battery R&R\t"|
                        TaskName=="Battery ADJ\t"|
                        TaskName=="Charging/starting System DIA\t"|
                        TaskName=="Electrical System DIA\t"|
                        TaskName=="Alternator R&R\t"|
                        TaskName=="Alternator - REP\t"|
                        TaskName=="Battery/hold Down R&R\t"|
                        TaskName=="Battery Cable R&R\t"|
                        TaskName=="Battery Cable REP\t"|
                        TaskName== "Battery, Charge REP\t"|
                        TaskName=="Voltage Regulator R&R\t"|
                        TaskName=="Electrical System OTH\t"|
                        TaskName=="Battery,egr Warning Lite R&R\t"|
                        TaskName=="Battery Aux Engine Aerial R&R\t"|
                        TaskName=="Battery Mpu/forklift R&R\t"|
                        TaskName=="Battery Aux Engine Aerial R&R\t"|
                        TaskName=="Alternator Mpu R&R\t"|
                        TaskName=="Battery Vpu R&R\t"|
                        TaskName=="Jump Start Vehicle OTH\t"|
                        TaskName=="Battery,auxillary Engine R&R\t"|
                        TaskName=="12 volt Battery, hvy duty auxiliary R&R\t"|
                        TaskName=="Starter R&R\t"|
                        TaskName=="Spark Plugs R&R\t"|
                        TaskName=="Wiring/harness R&R\t"|
                        TaskName=="Power unit All DIA (mpu,vpu,hesco..)\t"|
                        TaskName=="Power unit All OTH (mpu,vpu,hesco..)\t"|
                        TaskName=="Jump Start Vehicle\t")  #13096

sensor_BRR0<- subset(sensor_data, TaskName!="Alternator Belt R&R\t"&
                       TaskName!="Alternator Belt ADJ\t"&
                       TaskName!="Battery R&R\t"&
                       TaskName!="Battery ADJ\t"&
                       TaskName!="Charging/starting System DIA\t"&
                       TaskName!="Electrical System DIA\t"&
                       TaskName!="Alternator R&R\t"&
                       TaskName!="Alternator - REP\t"&
                       TaskName!="Battery/hold Down R&R\t"&
                       TaskName!="Battery Cable R&R\t"&
                       TaskName!="Battery Cable REP\t"&
                       TaskName!= "Battery, Charge REP\t"&
                       TaskName!="Voltage Regulator R&R\t"&
                       TaskName!="Electrical System OTH\t"&
                       TaskName!="Battery,egr Warning Lite R&R\t"&
                       TaskName!="Battery Aux Engine Aerial R&R\t"&
                       TaskName!="Battery Mpu/forklift R&R\t"&
                       TaskName!="Battery Aux Engine Aerial R&R\t"&
                       TaskName!="Alternator Mpu R&R\t"&
                       TaskName!="Battery Vpu R&R\t"&
                       TaskName!="Jump Start Vehicle OTH\t"&
                       TaskName!="Battery,auxillary Engine R&R\t"&
                       TaskName!="12 volt Battery, hvy duty auxiliary R&R\t"&
                       TaskName!="Starter R&R\t"&
                       TaskName!="Spark Plugs R&R\t"&
                       TaskName!="Wiring/harness R&R\t"&
                       TaskName!="Power unit All DIA (mpu,vpu,hesco..)\t"&
                       TaskName!="Power unit All OTH (mpu,vpu,hesco..)\t"&
                       TaskName!="Jump Start Vehicle\t")  #77182

sensor_BRR1_unique <- sensor_BRR1[!duplicated(sensor_BRR1[,c(10:21)]),]  #46834
sensor_BRR0_unique <- sensor_BRR0[!duplicated(sensor_BRR0[,c(10:21)]),] #184680
sensor_BRR0_subset <- sqldf("select * from  sensor_BRR0_unique where VehicleID not in 
                            (select VehicleID from sensor_BRR1_unique)") #38336
sensor_BRR0_final  <- sensor_BRR0_subset
sensor_BRR1_final <- sensor_BRR1_unique

sensor_BRR0_final$BRR <-0  # 38333,730
sensor_BRR1_final$BRR <-1  # 46766, 730
write.csv(sensor_BRR0_final, "daily_180/sensor_BRR0_final.csv")
write.csv(sensor_BRR1_final, "daily_180/sensor_BRR1_final.csv")
dim(sensor_BRR0_final)
dim(sensor_BRR1_final)
sensor_BRR0 <- sensor_BRR0_final
sensor_BRR1 <- sensor_BRR1_final
sensor_test<- sensor_BRR0_final[c(1:10),c(10:19)]

# remove outliers only keep vol_min=[12.3,13.8] and vol_max=[13.8,14.7]
for(i in c(1:48633)){
  for(j in c(10:189)){
    if(is.na(sensor_BRR0[i,j]) ||(sensor_BRR0[i,j]<12.3|sensor_BRR0[i,j]>13.8)){
      sensor_BRR0[i,j]<-NA
    }
  }
}

for(i in c(1:48633)){
  for(j in c(190:369)){
    if(is.na(sensor_BRR0[i,j]) ||(sensor_BRR0[i,j]<13.8|sensor_BRR0[i,j]>14.7)){
      sensor_BRR0[i,j]<-NA
    }
  }
}

for(i in c(1:36809)){
  for(j in c(10:189)){
    if(is.na(sensor_BRR1[i,j]) ||(sensor_BRR1[i,j]<12.2|sensor_BRR1[i,j]>13.9)){
      sensor_BRR1[i,j]<-NA
    }
  }
}

for(i in c(1:36809)){
  for(j in c(190:369)){
    if(is.na(sensor_BRR1[i,j]) ||(sensor_BRR1[i,j]<13.8|sensor_BRR1[i,j]>14.7)){
      sensor_BRR1[i,j]<-NA
    }
  }
}
# read in the sensor data after cleaning the outliers
sensor_BRR0_final <- read.csv("daily_180/sensor_BRR0_final_processed.csv")
sensor_BRR1_final <- read.csv("daily_180/sensor_BRR1_final_processed.csv")
sensor_BRR0_final <- sensor_BRR0_final[,-1]
sensor_BRR1_final <- sensor_BRR1_final[,-1]

sensor_BRR0_final$DemandDate <- as.Date(sensor_BRR0_final$DemandDate,"%m/%d/%y")
sensor_BRR1_final$DemandDate <- as.Date(sensor_BRR1_final$DemandDate,"%m/%d/%y")
sensor_BRR0_final$Month <- as.numeric(format(sensor_BRR0_final$DemandDate,"%m"))
sensor_BRR1_final$Month <- as.numeric(format(sensor_BRR1_final$DemandDate,"%m"))
sensor_BRR0_final$BRR <-0
sensor_BRR1_final$BRR <-1


sensor_BRR0_S_Winter <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR0_N_Winter <- subset(sensor_BRR0_final, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Winter <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Winter <- subset(sensor_BRR1_final, (Month==12|Month==1|Month==2)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_Winter[,c(10:909)] <- sapply(sensor_BRR0_S_Winter[,c(10:909)] , as.numeric)
sensor_BRR0_N_Winter[,c(10:909)] <- sapply(sensor_BRR0_N_Winter[,c(10:909)] , as.numeric)
sensor_BRR1_S_Winter[,c(10:909)] <- sapply(sensor_BRR1_S_Winter[,c(10:909)] , as.numeric)
sensor_BRR1_N_Winter[,c(10:909)] <- sapply(sensor_BRR1_N_Winter[,c(10:909)] , as.numeric)

sensor_BRR0_S_W<- colMeans(sensor_BRR0_S_Winter[,c(10:909)],na.rm=T)
sensor_BRR0_N_W<- colMeans(sensor_BRR0_N_Winter[,c(10:909)],na.rm=T)
sensor_BRR1_S_W<- colMeans(sensor_BRR1_S_Winter[,c(10:909)],na.rm=T)
sensor_BRR1_N_W<- colMeans(sensor_BRR1_N_Winter[,c(10:909)],na.rm=T)


sensor_Winter_fit_volmin <- cbind(sensor_BRR0_S_W[c(1:180)],sensor_BRR0_N_W[c(1:180)],
                                  sensor_BRR1_S_W[c(1:180)],sensor_BRR1_N_W[c(1:180)])
sensor_Winter_fit_volmax <- cbind(sensor_BRR0_S_W[c(181:360)],sensor_BRR0_N_W[c(181:360)],
                                  sensor_BRR1_S_W[c(181:360)],sensor_BRR1_N_W[c(181:360)])

write.csv(sensor_Winter_fit_volmin,"data_for_trail/data_for_model/0612/model_3_outlier/sensor_Winter_fit_volmin.csv")
write.csv(sensor_Winter_fit_volmax,"data_for_trail/data_for_model/0612/model_3_outlier/sensor_Winter_fit_volmax.csv")
sensor_BRR0_S_W_sd<- apply(sensor_BRR0_S_Winter[,c(10:909)],2,sd,na.rm=T)
sensor_BRR0_N_W_sd<- apply(sensor_BRR0_N_Winter[,c(10:909)],2,sd,na.rm=T)
write.csv()
plot.ts(sensor_BRR0_N_W[c(1:180)],ylim=range(12.4,13.8),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_W[c(1:180)],ylim=range(12.4,13.8),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_W[c(1:180)],ylim=range(12.4,13.8),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_W[c(1:180)],ylim=range(12.4,13.8),col="black")

plot.ts(sensor_BRR0_N_W[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_W[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_W[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_W[c(181:360)],ylim=range(13.8,14.6),col="black")
par(new=T)
plot.ts(sensor_BRR0_S_W_SMA,ylim=range(13.8,14.6),col="purple")
#smooth curve
sensor_BRR0_S_W_SMA<- SMA(sensor_BRR0_S_W[c(181:360)],n=3) 
sensor_BRR0_N_W_SMA<- SMA(sensor_BRR0_N_W[c(181:360)],n=3)  
sensor_BRR1_S_W_SMA<- SMA(sensor_BRR1_S_W[c(181:360)],n=3)  
sensor_BRR1_N_W_SMA<- SMA(sensor_BRR1_N_W[c(181:360)],n=3)  
#plot smooth curve
plot.ts(sensor_BRR0_N_W_SMA,ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_W_SMA,ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_W_SMA,ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_W_SMA,ylim=range(13.8,14.6),col="black")
# Summer for both north and south regions

sensor_BRR0_S_Summer <- subset(sensor_BRR0_final, (Month==6|Month==7|Month==8)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Summer <- subset(sensor_BRR0_final, (Month==6|Month==7|Month==8)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Summer <- subset(sensor_BRR1_final, (Month==6|Month==7|Month==8)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Summer <- subset(sensor_BRR1_final, (Month==6|Month==7|Month==8)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_Summer[,c(10:909)] <- sapply(sensor_BRR0_S_Summer[,c(10:909)] , as.numeric)
sensor_BRR0_N_Summer[,c(10:909)] <- sapply(sensor_BRR0_N_Summer[,c(10:909)] , as.numeric)
sensor_BRR1_S_Summer[,c(10:909)] <- sapply(sensor_BRR1_S_Summer[,c(10:909)] , as.numeric)
sensor_BRR1_N_Summer[,c(10:909)] <- sapply(sensor_BRR1_N_Summer[,c(10:909)] , as.numeric)



sensor_BRR0_S_S<- colMeans(sensor_BRR0_S_Summer[,c(10:909)],na.rm=T)
sensor_BRR0_N_S<- colMeans(sensor_BRR0_N_Summer[,c(10:909)],na.rm=T)
sensor_BRR1_S_S<- colMeans(sensor_BRR1_S_Summer[,c(10:909)],na.rm=T)
sensor_BRR1_N_S<- colMeans(sensor_BRR1_N_Summer[,c(10:909)],na.rm=T)

sensor_BRR0_S_S_sd<- apply(sensor_BRR0_S_Summer[,c(10:909)],2,sd,na.rm=T)



sensor_Summer_fit_volmin <- cbind(sensor_BRR0_S_S[c(1:180)],sensor_BRR0_N_S[c(1:180)],
                                  sensor_BRR1_S_S[c(1:180)],sensor_BRR1_N_S[c(1:180)])
sensor_Summer_fit_volmax <- cbind(sensor_BRR0_S_S[c(181:360)],sensor_BRR0_N_S[c(181:360)],
                                  sensor_BRR1_S_S[c(181:360)],sensor_BRR1_N_S[c(181:360)])

write.csv(sensor_Summer_fit_volmin,"data_for_trail/data_for_model/0612/model_3_outlier/sensor_Summer_fit_volmin.csv")
write.csv(sensor_Summer_fit_volmax,"data_for_trail/data_for_model/0612/model_3_outlier/sensor_Summer_fit_volmax.csv")
sensor_BRR0_S_W_sd<- apply(sensor_BRR0_S_Winter[,c(10:909)],2,sd,na.rm=T)
# min voltage
plot.ts(sensor_BRR0_N_S[c(1:180)],ylim=range(12.4,13.8),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_S[c(1:180)],ylim=range(12.4,13.8),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_S[c(1:180)],ylim=range(12.4,13.8),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_S[c(1:180)],ylim=range(12.4,13.8),col="black")
# max voltage
plot.ts(sensor_BRR0_N_S[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_S[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_S[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_S[c(181:360)],ylim=range(13.8,14.6),col="black")
# Spring

sensor_BRR0_S_Spring <- subset(sensor_BRR0_final, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Spring <- subset(sensor_BRR0_final, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Spring <- subset(sensor_BRR1_final, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Spring <- subset(sensor_BRR1_final, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_Spring[,c(10:909)] <- sapply(sensor_BRR0_S_Spring[,c(10:909)] , as.numeric)
sensor_BRR0_N_Spring[,c(10:909)] <- sapply(sensor_BRR0_N_Spring[,c(10:909)] , as.numeric)
sensor_BRR1_S_Spring[,c(10:909)] <- sapply(sensor_BRR1_S_Spring[,c(10:909)] , as.numeric)
sensor_BRR1_N_Spring[,c(10:909)] <- sapply(sensor_BRR1_N_Spring[,c(10:909)] , as.numeric)



sensor_BRR0_S_Sp<- colMeans(sensor_BRR0_S_Spring[,c(10:909)],na.rm=T)
sensor_BRR0_N_Sp<- colMeans(sensor_BRR0_N_Spring[,c(10:909)],na.rm=T)
sensor_BRR1_S_Sp<- colMeans(sensor_BRR1_S_Spring[,c(10:909)],na.rm=T)
sensor_BRR1_N_Sp<- colMeans(sensor_BRR1_N_Spring[,c(10:909)],na.rm=T)

sensor_Spring_fit_volmin <- cbind(sensor_BRR0_S_Sp[c(1:180)],sensor_BRR0_N_Sp[c(1:180)],
                                  sensor_BRR1_S_Sp[c(1:180)],sensor_BRR1_N_Sp[c(1:180)])
sensor_Spring_fit_volmax <- cbind(sensor_BRR0_S_Sp[c(181:360)],sensor_BRR0_N_Sp[c(181:360)],
                                  sensor_BRR1_S_Sp[c(181:360)],sensor_BRR1_N_Sp[c(181:360)])

write.csv(sensor_Spring_fit_volmin,"data_for_trail/data_for_model/0612/model_3_outlier/sensor_Spring_fit_volmin.csv")
write.csv(sensor_Spring_fit_volmax,"data_for_trail/data_for_model/0612/model_3_outlier/sensor_Spring_fit_volmax.csv")


sensor_BRR0_S_Sp_sd<- apply(sensor_BRR0_S_Spring[,c(10:909)],2,sd,na.rm=T)
# min voltage
plot.ts(sensor_BRR0_N_Sp[c(1:180)],ylim=range(12.4,13.8),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_Sp[c(1:180)],ylim=range(12.4,13.8),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_Sp[c(1:180)],ylim=range(12.4,13.8),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(1:180)],ylim=range(12.4,13.8),col="black")
# max voltage
plot.ts(sensor_BRR0_N_Sp[c(181:360)],ylim=range(13.8,14.6),col="red",ylab="max voltage")
par(new=T)
plot.ts(sensor_BRR1_N_Sp[c(181:360)],ylim=range(13.8,14.6),col="blue",ylab="max voltage")
par(new=T)
plot.ts(sensor_BRR0_S_Sp[c(181:360)],ylim=range(13.8,14.6),col="green",ylab="max voltage")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(181:360)],ylim=range(13.8,14.6),col="black",ylab="max voltage")

sensor_BRR0_S_W_SMA<- SMA(sensor_BRR0_S_W[c(181:360)],n=3) 
sensor_BRR0_N_W_SMA<- SMA(sensor_BRR0_N_W[c(181:360)],n=3)  
sensor_BRR1_S_W_SMA<- SMA(sensor_BRR1_S_W[c(181:360)],n=3)  
sensor_BRR1_N_W_SMA<- SMA(sensor_BRR1_N_W[c(181:360)],n=3)  
#plot smooth curve
plot.ts(sensor_BRR0_N_W_SMA,ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_W_SMA,ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_W_SMA,ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_W_SMA,ylim=range(13.8,14.6),col="black")


#Fall

sensor_BRR0_S_Fall <- subset(sensor_BRR0_final, (Month==9|Month==10|Month==11)&
                               (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Fall <- subset(sensor_BRR0_final, (Month==9|Month==10|Month==11)&
                               (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                  State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Fall <- subset(sensor_BRR1_final, (Month==9|Month==10|Month==11)&
                               (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Fall <- subset(sensor_BRR1_final, (Month==9|Month==10|Month==11)&
                               (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                  State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))



sensor_BRR0_S_Fall[,c(10:909)] <- sapply(sensor_BRR0_S_Fall[,c(10:909)] , as.numeric)
sensor_BRR0_N_Fall[,c(10:909)] <- sapply(sensor_BRR0_N_Fall[,c(10:909)] , as.numeric)
sensor_BRR1_S_Fall[,c(10:909)] <- sapply(sensor_BRR1_S_Fall[,c(10:909)] , as.numeric)
sensor_BRR1_N_Fall[,c(10:909)] <- sapply(sensor_BRR1_N_Fall[,c(10:909)] , as.numeric)


sensor_BRR0_S_F<- colMeans(sensor_BRR0_S_Fall[,c(10:909)],na.rm=T)
sensor_BRR0_N_F<- colMeans(sensor_BRR0_N_Fall[,c(10:909)],na.rm=T)
sensor_BRR1_S_F<- colMeans(sensor_BRR1_S_Fall[,c(10:909)],na.rm=T)
sensor_BRR1_N_F<- colMeans(sensor_BRR1_N_Fall[,c(10:909)],na.rm=T)

sensor_BRR0_S_F_sd<- apply(sensor_BRR0_S_Fall[,c(10:909)],2,sd,na.rm=T)




sensor_Fall_fit_volmin <- cbind(sensor_BRR0_S_F[c(1:180)],sensor_BRR0_N_F[c(1:180)],
                                  sensor_BRR1_S_F[c(1:180)],sensor_BRR1_N_F[c(1:180)])
sensor_Fall_fit_volmax <- cbind(sensor_BRR0_S_F[c(181:360)],sensor_BRR0_N_F[c(181:360)],
                                  sensor_BRR1_S_F[c(181:360)],sensor_BRR1_N_F[c(181:360)])

write.csv(sensor_Fall_fit_volmin,"data_for_trail/data_for_model/0612/model_3_outlier/sensor_Fall_fit_volmin.csv")
write.csv(sensor_Fall_fit_volmax,"data_for_trail/data_for_model/0612/model_3_outlier/sensor_Fall_fit_volmax.csv")
# min voltage
plot.ts(sensor_BRR0_N_F[c(1:180)],ylim=range(12.4,13.8),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_F[c(1:180)],ylim=range(12.4,13.8),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_F[c(1:180)],ylim=range(12.4,13.8),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_F[c(1:180)],ylim=range(12.4,13.8),col="black")
# max voltage
plot.ts(sensor_BRR0_N_F[c(181:360)],ylim=range(13.8,14.6),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_F[c(181:360)],ylim=range(13.8,14.6),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_F[c(181:360)],ylim=range(13.8,14.6),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_F[c(181:360)],ylim=range(13.8,14.6),col="black")


sensor_BRR0_S_F_SMA<- SMA(sensor_BRR0_S_F[c(181:360)],n=9) 
sensor_BRR0_N_F_SMA<- SMA(sensor_BRR0_N_F[c(181:360)],n=9)  
sensor_BRR1_S_F_SMA<- SMA(sensor_BRR1_S_F[c(181:360)],n=9)  
sensor_BRR1_N_F_SMA<- SMA(sensor_BRR1_N_F[c(181:360)],n=9)  
#plot smooth curve
plot.ts(sensor_BRR0_N_F_SMA,ylim=range(13.8,14.6),col="red",ylab="max voltage",xlab="number of days")
par(new=T)
plot.ts(sensor_BRR1_N_F_SMA,ylim=range(13.8,14.6),col="blue",ylab="max voltage",xlab="number of days")
par(new=T)
plot.ts(sensor_BRR0_S_F_SMA,ylim=range(13.8,14.6),col="green",ylab="max voltage",xlab="number of days")
par(new=T)
plot.ts(sensor_BRR1_S_F_SMA,ylim=range(13.8,14.6),col="black",ylab="max voltage",xlab="number of days")
# June 11 bucket into 7 days, train everything as yes
sensor_BRR0 <- read.csv("daily_180/sensor_BRR0_final_processed.csv")
sensor_BRR1 <- read.csv("daily_180/sensor_BRR1_final_processed.csv")
sensor_BRR0 <- sensor_BRR0[,-1]
sensor_BRR1 <- sensor_BRR1[,-1]
# compute mean over every 7 days
sensor_BRR0$BRR <-0
sensor_BRR1$BRR <-1
sensor_BRR0[,c(10:909)] <- sapply(sensor_BRR0[,c(10:909)] , as.numeric)
sensor_BRR1[,c(10:909)] <- sapply(sensor_BRR1[,c(10:909)] , as.numeric)



volmin_mean_0 <- data.frame(matrix(ncol = 24, nrow = 48632))
for(i in c(1:24)){
  volmin_mean_0[,i] <- rowMeans(sensor_BRR0[,c((3+i*7):(9+i*7))],na.rm=TRUE)
}
names(volmin_mean_0) <-c("volmin1","volmin2","volmin3","volmin4",
                       "volmin5","volmin6","volmin7","volmin8",
                       "volmin9","volmin10","volmin11","volmin12",
                       "volmin13","volmin14","volmin15","volmin18",
                       "volmin17","volmin18","volmin19","volmin20",
                       "volmin21","volmin22","volmin23","volmin24")

volmax_mean_0 <- data.frame(matrix(ncol = 24, nrow = 48632))
for(i in c(1:24)){
  volmax_mean_0[,i] <- rowMeans(sensor_BRR0[,c((183+i*7):(189+i*7))],na.rm=TRUE)
}
names(volmax_mean_0) <-c("volmax1","volmax2","volmax3","volmax4",
                       "volmax5","volmax6","volmax7","volmax8",
                       "volmax9","volmax10","volmax11","volmax12",
                       "volmax13","volmax14","volmax15","volmax16",
                       "volmax17","volmax18","volmax19","volmax20",
                       "volmax21","volmax22","volmax23","volmax24")

esdmin_mean_0 <- data.frame(matrix(ncol = 24, nrow = 48632))
for(i in c(1:24)){
  esdmin_mean_0[,i] <- rowMeans(sensor_BRR0[,c((543+i*7):(549+i*7))],na.rm=TRUE)
}
names(esdmin_mean_0) <-c("esdmin1","esdmin2","esdmin3","esdmin4",
                       "esdmin5","esdmin6","esdmin7","esdmin8",
                       "esdmin9","esdmin10","esdmin11","esdmin12",
                       "esdmin13","esdmin14","esdmin15","esdmin16",
                       "esdmin17","esdmin18","esdmin19","esdmin20",
                       "esdmin21","esdmin22","esdmin23","esdmin24")

esdmax_mean_0 <- data.frame(matrix(ncol = 24, nrow = 48632))
for(i in c(1:24)){
  esdmax_mean_0[,i] <- rowMeans(sensor_BRR0[,c((723+i*7):(729+i*7))],na.rm=TRUE)
}
names(esdmax_mean_0) <-c("esdmax1","esdmax2","esdmax3","esdmax4",
                       "esdmax5","esdmax6","esdmax7","esdmax8",
                       "esdmax9","esdmax10","esdmax11","esdmax12",
                       "esdmax13","esdmax14","esdmax15","esdmax16",
                       "esdmax17","esdmax18","esdmax19","esdmax20",
                       "esdmax21","esdmax22","esdmax23","esdmax24")


volmin_mean_1 <- data.frame(matrix(ncol = 24, nrow = 36809))
for(i in c(1:24)){
  volmin_mean_1[,i] <- rowMeans(sensor_BRR1[,c((3+i*7):(9+i*7))],na.rm=TRUE)
}
names(volmin_mean_1) <-c("volmin1","volmin2","volmin3","volmin4",
                       "volmin5","volmin6","volmin7","volmin8",
                       "volmin9","volmin10","volmin11","volmin12",
                       "volmin13","volmin14","volmin15","volmin16",
                       "volmin17","volmin18","volmin19","volmin20",
                       "volmin21","volmin22","volmin23","volmin24")

volmax_mean_1 <- data.frame(matrix(ncol = 24, nrow = 36809))
for(i in c(1:24)){
  volmax_mean_1[,i] <- rowMeans(sensor_BRR1[,c((183+i*7):(189+i*7))],na.rm=TRUE)
}
names(volmax_mean_1) <-c("volmax1","volmax2","volmax3","volmax4",
                         "volmax5","volmax6","volmax7","volmax8",
                         "volmax9","volmax10","volmax11","volmax12",
                         "volmax13","volmax14","volmax15","volmax16",
                         "volmax17","volmax18","volmax19","volmax20",
                         "volmax21","volmax22","volmax23","volmax24")

esdmin_mean_1 <- data.frame(matrix(ncol = 24, nrow = 36809))
for(i in c(1:24)){
  esdmin_mean_1[,i] <- rowMeans(sensor_BRR1[,c((543+i*7):(549+i*7))],na.rm=TRUE)
}
names(esdmin_mean_1) <-c("esdmin1","esdmin2","esdmin3","esdmin4",
                         "esdmin5","esdmin6","esdmin7","esdmin8",
                         "esdmin9","esdmin10","esdmin11","esdmin12",
                         "esdmin13","esdmin14","esdmin15","esdmin16",
                         "esdmin17","esdmin18","esdmin19","esdmin20",
                         "esdmin21","esdmin22","esdmin23","esdmin24")

esdmax_mean_1 <- data.frame(matrix(ncol = 24, nrow = 36809))
for(i in c(1:24)){
  esdmax_mean_1[,i] <- rowMeans(sensor_BRR1[,c((723+i*7):(729+i*7))],na.rm=TRUE)
}
names(esdmax_mean_1) <-c("esdmax1","esdmax2","esdmax3","esdmax4",
                         "esdmax5","esdmax6","esdmax7","esdmax8",
                         "esdmax9","esdmax10","esdmax11","esdmax12",
                         "esdmax13","esdmax14","esdmax15","esdmax16",
                         "esdmax17","esdmax18","esdmax19","esdmax20",
                         "esdmax21","esdmax22","esdmax23","esdmax24")

data_train_BRR0 <- cbind(sensor_BRR0[,c(1:9)],volmin_mean_0[,c(1:8)],volmax_mean_0[,c(1:8)],
                 esdmin_mean_0[,c(1:8)],esdmax_mean_0[,c(1:8)],sensor_BRR0[,910])
data_train_BRR1 <- cbind(sensor_BRR1[,c(1:9)],volmin_mean_1[,c(1:8)],volmax_mean_1[,c(1:8)],
                   esdmin_mean_1[,c(1:8)],esdmax_mean_1[,c(1:8)],sensor_BRR1[,910])

names(data_train_BRR0)[42] <-"BRR"
names(data_train_BRR1)[42] <-"BRR"

data_train <- rbind(data_train_BRR0,data_train_BRR1)

write.csv(data_train,"data_for_trail/data_for_model/0612/sensor_data_train_7daybuckets.csv")


data_test1_BRR0 <- cbind(sensor_BRR0[,c(1:9)],volmin_mean_0[,c(9:16)],volmax_mean_0[,c(9:16)],
                         esdmin_mean_0[,c(9:16)],esdmax_mean_0[,c(9:16)],sensor_BRR0[,910])
data_test1_BRR1 <- cbind(sensor_BRR1[,c(1:9)],volmin_mean_1[,c(9:16)],volmax_mean_1[,c(9:16)],
                         esdmin_mean_1[,c(9:16)],esdmax_mean_1[,c(9:16)],sensor_BRR1[,910])

names(data_test1_BRR0)[42] <-"BRR"
names(data_test1_BRR1)[42] <-"BRR"
names(data_test1_BRR1) <- names(data_test1_BRR0)
data_test1 <- rbind(data_test1_BRR0,data_test1_BRR1)
names(data_test1) <- names(data_train)
write.csv(data_test1,"data_for_trail/data_for_model/0612/sensor_data_test1_7daybuckets.csv")

data_test2_BRR0 <- cbind(sensor_BRR0[,c(1:9)],volmin_mean_0[,c(17:24)],volmax_mean_0[,c(17:24)],
                         esdmin_mean_0[,c(17:24)],esdmax_mean_0[,c(17:24)],sensor_BRR0[,910])
data_test2_BRR1 <- cbind(sensor_BRR1[,c(1:9)],volmin_mean_1[,c(17:24)],volmax_mean_1[,c(17:24)],
                         esdmin_mean_1[,c(17:24)],esdmax_mean_1[,c(17:24)],sensor_BRR1[,910])


names(data_test2_BRR0)[42] <-"BRR"
names(data_test2_BRR1)[42] <-"BRR"

data_test2 <- rbind(data_test2_BRR0,data_test2_BRR1)
names(data_test2) <- names(data_train)

write.csv(data_test2,"data_for_trail/data_for_model/0612/sensor_data_test2_7daybuckets.csv")


# June 12th find near complete cases to test early prediction detection based on the line curve outlier detection
sensor_BRR0_final <- read.csv("daily_180/sensor_BRR0_final_processed.csv")
sensor_BRR1_final <- read.csv("daily_180/sensor_BRR1_final_processed.csv")

sensor_BRR0_final$DemandDate <- as.Date(sensor_BRR0_final$DemandDate,"%m/%d/%y")
sensor_BRR1_final$DemandDate <- as.Date(sensor_BRR1_final$DemandDate,"%m/%d/%y")
sensor_BRR0_final$Month <- as.numeric(format(sensor_BRR0_final$DemandDate,"%m"))
sensor_BRR1_final$Month <- as.numeric(format(sensor_BRR1_final$DemandDate,"%m"))
sensor_BRR0_final$BRR <-0
sensor_BRR1_final$BRR <-1



sensor_BRR0_comp <-sensor_BRR0_final[complete.cases(sensor_BRR0_final[c(189:220)]),]
sensor_BRR1_comp <-sensor_BRR1_final[complete.cases(sensor_BRR1_final[c(189:210)]),]
#train
sensor_BRR0_S_Spring <- subset(sensor_BRR0_final, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|
                                    State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Spring <- subset(sensor_BRR0_final, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Spring <- subset(sensor_BRR1_final, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Spring <- subset(sensor_BRR1_final, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
#test
sensor_BRR0_comp$DemandDate <- as.Date(sensor_BRR0_comp$DemandDate,"%m/%d/%y")
sensor_BRR1_comp$DemandDate <- as.Date(sensor_BRR1_comp$DemandDate,"%m/%d/%y")
sensor_BRR0_comp$Month <- as.numeric(format(sensor_BRR0_comp$DemandDate,"%m"))
sensor_BRR1_comp$Month <- as.numeric(format(sensor_BRR1_comp$DemandDate,"%m"))
sensor_BRR0_comp$BRR <-0
sensor_BRR1_comp$BRR <-1
sensor_BRR0_S_Spring_test <- subset(sensor_BRR0_comp, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"|State=="OK"|State=="GA"))
sensor_BRR0_N_Spring_test <- subset(sensor_BRR0_comp, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))
sensor_BRR1_S_Spring_test <- subset(sensor_BRR1_comp, (Month==3|Month==4|Month==5)&
                                 (State=="CA"|State=="FL"|State=="AL"|State=="MS"|State=="TX"|State=="NM"|State=="AZ"))
sensor_BRR1_N_Spring_test <- subset(sensor_BRR1_comp, (Month==3|Month==4|Month==5)&
                                 (State=="MI"|State=="ME"|State=="WI"|State=="MN"|State=="ND"|State=="WA"|
                                    State=="MT"|State=="NY"|State=="OH"|State=="IL"|State=="MO"|State=="SD"))

sensor_BRR0_S_Spring[,c(11:909)] <- sapply(sensor_BRR0_S_Spring[,c(11:909)] , as.numeric)
sensor_BRR0_N_Spring[,c(11:909)] <- sapply(sensor_BRR0_N_Spring[,c(11:909)] , as.numeric)
sensor_BRR1_S_Spring[,c(11:909)] <- sapply(sensor_BRR1_S_Spring[,c(11:909)] , as.numeric)
sensor_BRR1_N_Spring[,c(11:909)] <- sapply(sensor_BRR1_N_Spring[,c(11:909)] , as.numeric)

sensor_BRR0_S_Spring_test[,c(11:909)] <- sapply(sensor_BRR0_S_Spring_test[,c(11:909)] , as.numeric)
sensor_BRR0_N_Spring_test[,c(11:909)] <- sapply(sensor_BRR0_N_Spring_test[,c(11:909)] , as.numeric)
sensor_BRR1_S_Spring_test[,c(11:909)] <- sapply(sensor_BRR1_S_Spring_test[,c(11:909)] , as.numeric)
sensor_BRR1_N_Spring_test[,c(11:909)] <- sapply(sensor_BRR1_N_Spring_test[,c(11:909)] , as.numeric)

sensor_BRR0_S_Sp<- colMeans(sensor_BRR0_S_Spring[,c(11:909)],na.rm=T)
sensor_BRR0_N_Sp<- colMeans(sensor_BRR0_N_Spring[,c(11:909)],na.rm=T)
sensor_BRR1_S_Sp<- colMeans(sensor_BRR1_S_Spring[,c(11:909)],na.rm=T)
sensor_BRR1_N_Sp<- colMeans(sensor_BRR1_N_Spring[,c(11:909)],na.rm=T)

sensor_BRR0_S_Sp_sd<- apply(sensor_BRR0_S_Spring[,c(11:909)],2,sd,na.rm=T)
# min voltage
plot.ts(sensor_BRR0_N_Sp[c(1:180)],ylim=range(12.4,13.8),col="red",ylab="min voltage")
par(new=T)
plot.ts(sensor_BRR1_N_Sp[c(1:180)],ylim=range(12.4,13.8),col="blue",ylab="min voltage")
par(new=T)
plot.ts(sensor_BRR0_S_Sp[c(1:180)],ylim=range(12.4,13.8),col="green",ylab="min voltage")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(1:180)],ylim=range(12.4,13.8),col="black",ylab="min voltage")
par(new=T)
plot.ts(as.list(sensor_BRR1_S_Spring_test[3,c(10:189)]),ylim=range(12.4,13.8),col="purple")
# max voltage
plot.ts(sensor_BRR0_N_Sp[c(181:360)],ylim=range(13.8,14.6),col="red",ylab="max voltage")
par(new=T)
plot.ts(sensor_BRR1_N_Sp[c(181:360)],ylim=range(13.8,14.6),col="blue",ylab="max voltage")
par(new=T)
plot.ts(sensor_BRR0_S_Sp[c(181:360)],ylim=range(13.8,14.6),col="green",ylab="max voltage")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(181:360)],ylim=range(13.8,14.6),col="black",ylab="max voltage")
par(new=T)
plot.ts(as.list(sensor_BRR1_S_Spring_test[8,c(190:369)]),ylim=range(13.8,14.4),col="purple")
# mean voltage

plot.ts(sensor_BRR0_N_Sp[c(361:540)],ylim=range(13.5,14.3),col="red")
par(new=T)
plot.ts(sensor_BRR1_N_Sp[c(361:540)],ylim=range(13.5,14.3),col="blue")
par(new=T)
plot.ts(sensor_BRR0_S_Sp[c(361:540)],ylim=range(13.5,14.3),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(361:540)],ylim=range(13.5,14.3),col="black")
par(new=T)
plot.ts(as.list(sensor_BRR0_S_Spring_test[9,c(370:549)]),ylim=range(13.5,14.3),col="purple")
library(TTR) 

sensor_BRR1_S_Spring_test_Smooth <- loess(sensor_BRR1_S_Spring_test[,c(190,369)])
sensor_BRR1_S_Spring_test_SMA<- SMA(sensor_BRR1_S_Spring_test,n=6)   
sensor_BRR1_S_Spring_ts <- ts(sensor_BRR1_S_Spring_test[,c(10:909)])




# check model 1
batt_data <- read.csv("data_for_trail/data_for_model/0607/Batt_age_processed_stand.csv")
batt_data_BRR0 <- subset(batt_data, BRR==0)
batt_data_BRR1 <- subset(batt_data, BRR==1)

test_battdata <- subset(batt_data, GarageID=="MO0006"|GarageID=="GA111"|GarageID=="TX0022"|GarageID=="TX050"|GarageID=="TX040")
train_battdata <- subset(batt_data, GarageID!="MO0006"&GarageID!="GA111"&GarageID!="TX0022"&GarageID!="TX050"&GarageID!="TX040")
write.csv(test_battdata,"data_for_trail/data_for_model/0612/model1_battdata_test.csv")
write.csv(train_battdata,"data_for_trail/data_for_model/0612/model1_battdata_train.csv")


# check model 2
sensor_data <- read.csv("data_for_trail/data_for_model/0612/sensor_data_train_7daybuckets.csv")
test_sensordata <- sqldf("select * from sensor_data where VehicleID in (select Vehicle from test_battdata )") #3497,unique1277
train_sensordata <- sqldf("select * from sensor_data where VehicleID not in (select Vehicle from test_battdata )")#unique 24763
write.csv(test_sensordata,"data_for_trail/data_for_model/0612/model_2_sensor/model2_sensor_test.csv")
write.csv(train_sensordata,"data_for_trail/data_for_model/0612/model_2_sensor/model2_sensor_train.csv")

# merge the results in two models
pred_model1 <- read.csv("data_for_trail/data_for_model/0612/model_1_batt/Prediction_model1_comp.csv",stringsAsFactors=F)
pred_model2 <- read.csv("data_for_trail/data_for_model/0612/model_2_sensor/Prediction_model2_comp.csv")

pred_model <- read.csv("data_for_trail/data_for_model/0612/model_3_outlier/model_1_2_merged_actual.csv")
pred_vehicle1$JobDate <- as.Date(pred_vehicle1$JobDate,"%m/%d/%y")
pred_vehicle1$DetailsDate <- as.Date(pred_vehicle1$DetailsDate,"%m/%d/%y")
pred_vehicle1$Month <- as.numeric(format(pred_vehicle1$JobDate,"%m"))



volmin<- read.csv("daily_180/vol_min.csv",sep="|",header=T,stringsAsFactors=F)
volmax <- read.csv("daily_180/vol_max.csv",sep="|",header=T,stringsAsFactors=F)
sensor_data <- cbind(volmin,volmax[10:189])
sensor_data$DetailsDate <- as.Date(sensor_data$DetailsDate,"%m/%d/%Y")
names(sensor_data)[7] <-"DetailsDate"
names(sensor_data)[1] <-"Vehicle"
library(sqldf)
names(pred_model)[1]="Vehicle"
pred_veh_sensor <- merge(x=pred_model,y=sensor_data,by=c("Vehicle"))
pred_veh_sensor <- pred_veh_sensor[unique(pred_veh_sensor$Vehicle),]
pred_veh_sensor_data <- sapply(pred_veh_sensor[,c(16:375)],as.numeric)
pred_vehicle_Winter <- subset(pred_veh_sensor, (Month==12|Month==1|Month==2)) #57
pred_vehicle_Spring <- subset(pred_veh_sensor, (Month==3|Month==4|Month==5)) #35
pred_vehicle_Summer <- subset(pred_veh_sensor, (Month==6|Month==7|Month==8)) #21
pred_vehicle_Fall <- subset(pred_veh_sensor, (Month==9|Month==10|Month==11)) #36
       
#volmin
sensor_Spring_volmin <- rbind(sensor_BRR0_S_Sp[c(1:180)], sensor_BRR1_S_Sp[c(1:180)],pred_vehicle_Spring[,c(16:195)])
sensor_Summer_volmin <- rbind(sensor_BRR0_S_S[c(1:180)], sensor_BRR1_S_S[c(1:180)],pred_vehicle_Summer[,c(16:195)])
sensor_Fall_volmin <- rbind(sensor_BRR0_S_F[c(1:180)], sensor_BRR1_S_F[c(1:180)],pred_vehicle_Fall[,c(16:195)])
sensor_Winter_volmin <- rbind(sensor_BRR0_S_W[c(1:180)], sensor_BRR1_S_W[c(1:180)],pred_vehicle_Winter[,c(16:195)])
#volmax
sensor_Spring_volmax <- rbind(sensor_BRR0_S_Sp[c(181:360)], sensor_BRR1_S_Sp[c(181:360)],pred_vehicle_Spring[,c(196:375)])
sensor_Summer_volmax <- rbind(sensor_BRR0_S_S[c(181:360)], sensor_BRR1_S_S[c(181:360)],pred_vehicle_Summer[,c(196:375)])
sensor_Fall_volmax <- rbind(sensor_BRR0_S_F[c(181:360)], sensor_BRR1_S_F[c(181:360)],pred_vehicle_Fall[,c(196:375)])
sensor_Winter_volmax <- rbind(sensor_BRR0_S_W[c(181:360)], sensor_BRR1_S_W[c(181:360)],pred_vehicle_Winter[,c(196:375)])

#compute distance
dist_s_volmin <-dist(sensor_Spring_volmin,method = "euclidean",diag=F)
s_volmin_m <- as.matrix(dist_s_volmin)
dist_compare_min <- s_volmin_m[2,]-s_volmin_m[1,]

dist_s_volmax <-dist(sensor_Spring_volmax,method = "euclidean",diag=F)
s_volmax_m <- as.matrix(dist_s_volmax)
dist_compare_max <- s_volmax_m[2,]-s_volmax_m[1,]




plot.ts(sensor_BRR0_S_Sp[c(1:360)],ylim=range(13.5,14.3),col="green")
par(new=T)
plot.ts(sensor_BRR1_S_Sp[c(181:360)],ylim=range(13.5,14.3),col="black")
par(new=T)
plot.ts(as.list(sensor_BRR0_S_Spring_test[9,c(370:549)]),ylim=range(13.5,14.3),col="purple")


prediction_model3 <- cbind(pred_vehicle_Spring[,c(1:17)],dist_compare_min[-c(1:2)],dist_compare_max[-c(1:2)])
pred_unique<- sqldf("select Vehicle,* from prediction_model3 group by Vehicle")
write.csv(pred_unique, "data_for_trail/data_for_model/0612/model_3_outlier/prediction_model3.csv")



# plot fall min voltage in the south
sensor_BRR0_S_F_SMA<- SMA(sensor_BRR0_S_F[c(181:360)],n=9) 
sensor_BRR0_N_F_SMA<- SMA(sensor_BRR0_N_F[c(181:360)],n=9)  
sensor_BRR1_S_F_SMA<- SMA(sensor_BRR1_S_F[c(181:360)],n=9)  
sensor_BRR1_N_F_SMA<- SMA(sensor_BRR1_N_F[c(181:360)],n=9)  
plot.ts(sensor_BRR0_S_F_SMA,ylim=range(14.05,14.25),col="green",ylab="max voltage",xlab="number of days")
par(new=T)
plot.ts(sensor_BRR1_S_F_SMA,ylim=range(14.05,14.25),col="red",ylab="max voltage",xlab="number of days")
# plot over one year
data <- cbind(t(as.matrix(sensor_BRR0_S_Sp[c(181:270)])),t(as.matrix(sensor_BRR0_S_S[c(181:270)])),
              t(as.matrix(sensor_BRR0_S_F[c(181:270)])),t(as.matrix(sensor_BRR0_S_W[c(181:270)])))

data <- cbind(t(as.matrix(sensor_BRR1_S_S[c(181:360)])),t(as.matrix(sensor_BRR1_W[c(181:360)])))


data <- cbind(t(as.matrix(sensor_BRR0_S_W[c(1:90)])),t(as.matrix(sensor_BRR0_S_Sp[c(1:90)])),
              t(as.matrix(sensor_BRR0_S_S[c(1:90)])),t(as.matrix(sensor_BRR0_S_F[c(1:90)])))
data_ts <- t(data)
data_SMA<- SMA(data_ts,n=9) 
plot.ts(data_ts,col="red",ylab="Battery Voltage")



##############################
#Sep 20th modify the battery model
batt <- read.csv("data/Battery_0919_2014.csv",stringsAsFactors=F)
batt$DaysSinceChange <- (Sys.Date()-as.Date(batt$JobDate,"%m/%d/%y"))
batt_p <- merge(x=batt, y=RR_person, by="PersonID", x.all=T)
batt_1_2010 <- subset(batt, BRR==1&TaskYr==2010&BatteryAge>15&BatteryMil>0)
batt_1_2010_unique <- batt_1_2010[unique(batt_1_2010$JobNo),]
batt_garage <- sqldf("select GarageID, avg(BatteryAge) as avg_battage_garage, stdev(BatteryAge) as std_battage_garage,
                     avg(BatteryMil) as avg_battmil_garage,stdev(BatteryMil) as std_battmil_garage, count(*) as cnt_garage
                     from batt_1_2010 group by GarageID")
batt_veh_reliable <- subset(batt_veh,Reliability==1)
write.csv(batt_veh, "data/Battery_0919_2014.csv", row.names=F)
write.csv(batt_veh_reliable, "data/Battery_0919_2014_reliable.csv", row.names=F)
batt <- subset(batt_p, Status=="A")
batt_1 <- subset(batt,BRR==1)
batt_0 <- subset(batt,BRR==0)
batt_1_2010 <- subset(batt_1,TaskYr==2010)
batt_0_2010 <- subset(batt_0,TaskYr==2010)
batt_1_2011 <- subset(batt_1,TaskYr==2011)
batt_0_2011 <- subset(batt_0,TaskYr==2011)
batt_1_2012 <- subset(batt_1,TaskYr==2012)
batt_0_2012 <- subset(batt_0,TaskYr==2012)
batt_1_2013 <- subset(batt_1,TaskYr==2013)
batt_0_2013 <- subset(batt_0,TaskYr==2013)

batt_2010 <- subset(batt,TaskYr==2010&Status=="A")
batt_2011 <- subset(batt,TaskYr==2011&Status=="A")
batt_2012 <- subset(batt,TaskYr==2012&Status=="A")
batt_2013 <- subset(batt,TaskYr==2013&Status=="A")

par(mfrow=c(4,3))
par(mar = rep(3, 4))
bins=seq(0,2500,by=25)
hist(batt_1_2010$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_0_2010$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_2010$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_1_2011$BatteryAge,  xlim=range(0,2000),breaks=bins)
hist(batt_0_2011$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_2011$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_1_2012$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_0_2012$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_2012$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_1_2013$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_0_2013$BatteryAge, xlim=range(0,2000),breaks=bins)
hist(batt_2013$BatteryAge, xlim=range(0,2000),breaks=bins)

batt_1_2010$BatteryMil <- as.numeric(batt_1_2010$BatteryMil)
batt_1_2010$WeightRatio <- as.numeric(batt_1_2010$WeightRatio)
batt_0_2010$BatteryMil <- as.numeric(batt_0_2010$BatteryMil)
batt_0_2010$WeightRatio <- as.numeric(batt_0_2010$WeightRatio)
batt_1_2011$BatteryMil <- as.numeric(batt_1_2011$BatteryMil)
batt_1_2011$WeightRatio <- as.numeric(batt_1_2011$WeightRatio)
batt_0_2011$BatteryMil <- as.numeric(batt_0_2011$BatteryMil)
batt_0_2011$WeightRatio <- as.numeric(batt_0_2011$WeightRatio)
batt_1_2012$BatteryMil <- as.numeric(batt_1_2012$BatteryMil)
batt_1_2012$WeightRatio <- as.numeric(batt_1_2012$WeightRatio)
batt_0_2012$BatteryMil <- as.numeric(batt_0_2012$BatteryMil)
batt_0_2012$WeightRatio <- as.numeric(batt_0_2012$WeightRatio)
batt_1_2013$BatteryMil <- as.numeric(batt_1_2013$BatteryMil)
batt_1_2013$WeightRatio <- as.numeric(batt_1_2013$WeightRatio)
batt_0_2013$BatteryMil <- as.numeric(batt_0_2013$BatteryMil)
batt_0_2013$WeightRatio <- as.numeric(batt_0_2013$WeightRatio)
cor(batt_1_2010[,c("BatteryAge","BatteryMil","rep_ratio","TagWeight")])
summary(batt_0_2010$BatteryMil)
summary(batt_1_2010$BatteryMil)
par(mfrow=c(4,2))
hist(batt_0_2010$BatteryMil, xlim=range(0,250000),breaks=1000)
hist(batt_1_2010$BatteryMil, xlim=range(0,250000),breaks=1000)
hist(batt_0_2011$BatteryMil, xlim=range(0,250000),breaks=500)
hist(batt_1_2011$BatteryMil, xlim=range(0,250000),breaks=50000)
hist(batt_0_2012$BatteryMil, xlim=range(0,250000),breaks=1000)
hist(batt_1_2012$BatteryMil, xlim=range(0,250000),breaks=1000000)
hist(batt_0_2013$BatteryMil, xlim=range(0,250000),breaks=1000)
hist(batt_1_2013$BatteryMil, xlim=range(0,250000),breaks=1000)

summary(batt_1_2010$BatteryMil/batt_1_2010$BatteryAge)
summary(batt_0_2010$BatteryMil/batt_0_2011$BatteryAge)

par(mfrow=c(2,2))
par(mar = rep(2, 4))
hist(batt_2010$BatteryAge, xlim=range(0,2500))
hist(batt_2011$BatteryAge, xlim=range(0,2500))
hist(batt_2012$BatteryAge, xlim=range(0,2500))
hist(batt_2013$BatteryAge, xlim=range(0,2500))

batt_1_normal <- subset(batt_1, BatteryAge > 365/2&veh_Status_new=="A")
batt_0_normal <- subset(batt_0, BatteryAge < 3*365&veh_Status_new=="A")
batt_1_normal$BatteryMil <- as.numeric(batt_1_normal$BatteryMil)
batt_0_normal$BatteryMil <- as.numeric(batt_0_normal$BatteryMil)
batt_0_nor <- subset(batt_0_normal, BatteryMil >0)
batt_1_nor <- subset(batt_1_normal, BatteryMil >0)
batt_nor <- rbind(batt_0_nor,batt_1_nor)

indexes = sample(1:nrow(batt_nor), size=0.4*nrow(batt_nor))
test = batt_nor[indexes,]
train = batt_nor[-indexes,]

write.csv(train,"data/Battery_0921_train.csv",row.names=F)
write.csv(test,"data/Battery_0921_test.csv",row.names=F)
batt_all <- rbind(train,test)
write.csv(batt_all,"data/Battery_0921.csv",row.names=F)

glm.fit <- glm(BRR~poly(Mileage,4)+poly(BatteryMil,6)+poly(TaskYr,2)+poly(BatteryAge,6)
                 ,data=train,family=binomial)
glm.fit.1 <- glm(BRR~BatteryAge
               ,data=train,family=binomial)
glm.probs = predict(glm.fit, test, type="response")
glm.pred=rep("0",50911)
glm.pred[glm.probs>0.25]="1"
table(glm.pred, test$BRR)

library(tree)

tree.fit <- tree(factor(BRR)~DaysSinceChange+TaskType+TaskYr+TaskMon+BatteryAge+BatteryMil+Mileage+Odometer+VehicleGroup+DateInservice_yy
                 +FuelType+WeightRatio+TagWeight+State+rep_ratio, train)
tree.pred <- predict(tree.fit, test, type="class")
plot(tree.fit)
text(tree.fit, pretty=0)
table(tree.pred, test$BRR)
train[is.na(train)] <-0
lm.fit <- lm(BRR~poly(Mileage,4)+poly(BatteryMil,6)+poly(TaskYr,2)+poly(BatteryAge,6)+poly(TagWeight,4) + poly(TaskMon,2)
               ,data=train,family=binomial)


############ Sep 21th
batt_1 <- subset(batt, BRR==1&BatteryAge>365/2&veh_Status_new=="A")
batt_0 <- subset(batt, BRR==0&BatteryAge<3.5*365&veh_Status_new=="A")
summary(batt_1$BatteryAge)
summary(batt_0$BatteryAge)
batt_p_good <- subset(batt_p, rep_ratio<0.3)
batt_p_g <- subset(batt_p_good, as.numeric(BatteryMil)>0)
batt_p_g_A <- subset(batt_p_g, veh_Status_new=="A")
batt_good_1 <- subset(batt_p_g_A, BRR==1&BatteryAge>365/2)
batt_good_0 <- subset(batt_p_g_A, BRR==0&BatteryAge<3.5*365)
summary(batt_good_1$BatteryAge)
summary(batt_good_0$BatteryAge)



###### Sep 22
job23 <- read.csv("data/Job23_0922.csv", stringsAsFactors=F)
job_unique <- job23[!duplicated(job23[,c("Vehicle","TaskName","JobDate")]),]
summary(factor(job_unique$TaskName))
sqldf("select count(distinct Vehicle) from job_unique")

dia <- sqldf("select distinct Vehicle from job_unique where TaskName='Charging/starting System DIA' OR TaskName='Electrical System DIA'")
brr <- sqldf("select distinct Vehicle from job_unique where TaskFam=6 AND TaskName like '%R&R%' ")
brr_dia <- sqldf("select distinct Vehicle from brr where Vehicle in (select Vehicle from dia)")

vehicle_unique <- sqldf("select distinct Vehicle from job_unique")

batt_1 <- rbind(batt_1_2010, batt_1_2011)
batt_0 <- rbind(batt_0_2010, batt_0_2011)
garage_1 <- sqldf("select GarageID, count(*) as cnt_1 from batt_1 group by GarageID")
garage_0 <- sqldf("select GarageID, count(*) as cnt_0 from batt_0 group by GarageID")
garage <- merge(garage_1,garage_0, by="GarageID", x.all=T)
garage$ratio <- garage$cnt_0/garage$cnt_1
garage_sort <- garage[order(garage$ratio, decreasing=T),] 

garage_good <- garage_sort[1:200,]
garage_bad <- garage_sort[471:570,]
batt_1_good <- sqldf("select Vehicle,BatteryAge,TaskYr,GarageID from batt_1 where GarageID in (select GarageID from garage_good) and (TaskYr=2010 or TaskYr=2011) ")
batt_0_good <- sqldf("select Vehicle,BatteryAge,TaskYr,GarageID from batt_0 where GarageID in (select GarageID from garage_good) and (TaskYr=2010 or TaskYr=2011) ")
batt_1_bad <- sqldf("select Vehicle,BatteryAge,TaskYr,GarageID from batt_1 where GarageID in (select GarageID from garage_bad) and (TaskYr=2010 or TaskYr=2011) ")
batt_0_bad <- sqldf("select Vehicle,BatteryAge,TaskYr,GarageID from batt_0 where GarageID in (select GarageID from garage_bad) and (TaskYr=2010 or TaskYr=2011) ")

par(mfrow=c(2,2))
hist(batt_1_good$BatteryAge,xlim=range(0,2500))
hist(batt_0_good$BatteryAge,xlim=range(0,2500))
hist(batt_1_bad$BatteryAge,xlim=range(0,2500))
hist(batt_0_bad$BatteryAge,xlim=range(0,2500))
summary(batt_1_good$BatteryAge)
summary(batt_0_good$BatteryAge)
summary(batt_1_bad$BatteryAge)
summary(batt_0_bad$BatteryAge)
summary(subset(batt_1, rep_ratio>0.35)$BatteryAge)
summary(subset(batt_1, rep_ratio<0.15)$BatteryAge)
summary(subset(batt_0, rep_ratio>0.35)$BatteryAge)
summary(subset(batt_0, rep_ratio<0.15)$BatteryAge)
summary(as.numeric(subset(batt_1, rep_ratio>0.25)$BatteryMil))
summary(as.numeric(subset(batt_1, rep_ratio<0.25)$BatteryMil))



good_bat <- subset(batt, BatteryAge > 2.5*365)
good_bat$BatteryStatus <- "Good"
bad_bat <- subset(batt_1, BatteryAge < 365)
bad_bat$BatteryStatus <- "Bad"
nor_bat <- subset(batt_1, BatteryAge>365&BatteryAge<2.5*365)
nor_bat$BatteryStatus <- "Normal"

bat <- rbind(good_bat, bad_bat)
bat <- rbind(bat,nor_bat)
bat_shuffle <- bat[sample(nrow(bat)),]
write.csv(bat_shuffle,"data/BatteryStatus.csv", row.names=F)


#### Sep 23 Survival Analysis
install.packages("survival")
library(survival)
mini <- head(batt,200)
attach(mini)
mini.surv <- survfit(Surv(mini$BatteryAge,mini$BRR)~1)
par(mfrow=c(1,1))
plot(mini.surv, xlab="Time", ylab="Survival Probability")
summary(mini.surv)
data(ovarian)


##Sep 24 Battery Age by GarageID
Batt_1 <- subset(batt, BRR==1&BatteryMil>0&TaskYr==2010&BatteryAge>15)
batt_age_garage <- sqldf("select GarageID, avg(BatteryAge) as avg_battage, stdev(BatteryAge) as sdt_battage, avg(BatteryMil) as avg_battmil, avg(rep_ratio) as avg_repratio,
                         count(*) as cnt from Batt_1 group by GarageID order by avg_battage desc")
batt_age_by_garage <- tapply(Batt_1$BatteryAge, Batt_1$GarageID, summary)
vehiclegroup_by_garage <- tapply(Batt_1$BatteryAge, Batt_1$VehicleGroup, summary)
batt_age_by_garage_df <- as.data.frame(batt_age_by_garage)
batt_age_garage_large <- subset(batt_age_garage, cnt>30)

batt_age_state <- sqldf("select State, avg(BatteryAge) as avg_battage, stdev(BatteryAge) as sdt_battage, avg(BatteryMil) as avg_battmil, avg(rep_ratio) as avg_repratio,
                         count(*) as cnt from Batt_1 group by State order by avg_battage desc")
batt_age_state_large <- subset(batt_age_state, cnt>30)


### Sep 26
batt <- read.csv("data/Battery_0919_2014.csv",stringsAsFactors=F)
batt$DaysSinceChange <- (Sys.Date()-as.Date(batt$JobDate,"%m/%d/%y"))
batt_1_2010_unique <- batt_1_2010[!duplicated(batt_1_2010$JobNo),]
batt_p <- merge(x=batt_1_2010_unique, y=RR_person, by="PersonID", x.all=T)
batt_garage <- sqldf("select GarageID, avg(BatteryAge) as avg_battage_garage, stdev(BatteryAge) as std_battage_garage,
                     avg(BatteryMil) as avg_battmil_garage,stdev(BatteryMil) as std_battmil_garage, count(*) as cnt_garage
                     from batt_1_2010 group by GarageID")
batt_model <- sqldf("select MakeID,Model, avg(BatteryAge) as avg_battage_model, stdev(BatteryAge) as std_battage_model,
                     avg(BatteryMil) as avg_battmil_model,stdev(BatteryMil) as std_battmil_model, count(*) as cnt_model
                     from batt_1_2010 group by Model")
batt_vehgroup <- sqldf("select VehicleGroup, avg(BatteryAge) as avg_battage_vehgroup, stdev(BatteryAge) as std_battage_vehgroup,
                     avg(BatteryMil) as avg_battmil_vehgroup,stdev(BatteryMil) as std_battmil_vehgroup, count(*) as cnt_vehgroup
                    from batt_1_2010 group by VehicleGroup")
batt <- merge(x=batt,y=batt_garage, by="GarageID", x.all=T)
batt <- merge(x=batt, y=batt_model, by="Model", x.all=T)
batt <- merge(x=batt, y=batt_vehgroup, by="VehicleGroup", x.all=T)
batt$JobDate <- as.Date(batt$JobDate,"%m/%d/%y")
batt$ReplaceDate <- as.Date(batt$ReplaceDate,"%m/%d/%y")
batt_1_2010 <- subset(batt, BRR==1&TaskYr==2010&BatteryAge>15&BatteryMil>0)
batt_1_2011 <- subset(batt, BRR==1&TaskYr==2011&BatteryAge>15&BatteryMil>0)
batt_1$VehicleGroup <- factor(batt_1$VehicleGroup)
batt_1$MilPerDay <- as.numeric(batt_1$BatteryMil)/as.numeric(batt_1$BatteryAge)
lm.fit <- lm(BatteryAge~poly(avg_battage_garage,4)+poly(avg_battmil_garage,4)+poly(rep_ratio,4)+poly(DateInservice_yy,4)
          +poly(Mileage+Odometer,4)+poly(JobType,4)+poly(TaskMon,4)+poly(avg_battmil_model,4)+poly(avg_battage_model,4)+poly(avg_battmil_vehgroup,4)
          +poly(avg_battage_vehgroup,4)+poly(cnt_vehgroup,4)+poly(cnt_model,4)+poly(cnt_garage,4)+poly(MilPerDay,4)
             ,data=batt_1)

write.csv(batt_1, "data/BatteryAgeLM_09262014.csv",row.names=F)
glm.fit.1 <- glm(BRR~BatteryAge
                 ,data=train,family=binomial)
glm.probs = predict(glm.fit, test, type="response")
glm.pred=rep("0",50911)
glm.pred[glm.probs>0.25]="1"
table(glm.pred, test$BRR)

## sep 27th
weather <- fread("data/weather_data/weather_2010_small.csv",header=T,stringsAsFactors=F)
weather$timestamp <- as.Date(weather$timestamp)
sensor <- fread("data/sensor/battery_voltage/Batt_vol_avg.txt",sep="\001",header=F,stringsAsFactors=F)
sensor <- as.data.frame(sensor)
sensor_veh <- sqldf("select Vehicle, avg(SensorValue) as avgbattvol_veh,stdev(SensorValue) as stdbattvol_veh, count(*) as cnt,
                   max(Date)-min(Date) as datediff from sensor group by Vehicle")
sensor_veh$ratioofrunning <- sensor_veh$cnt/sensor_veh$datediff
names(sensor) <- c("Vehicle","Date","SensorName","SensorValue","Key")
sensor$Date <- as.Date(sensor$Date)

batt_sensor <- sqldf("select batt.*,sensor.SensorValue as ReplaceDateVol from batt left join sensor on
                            (batt.Vehicle=sensor.Vehicle and batt.ReplaceDate=sensor.Date)")
batt_sensor <- sqldf("select batt_sensor.*, sensor_veh.* from batt_sensor left join sensor_veh on
                     batt_sensor.Vehicle=sensor_veh.Vehicle")
batt_sensor_weather <- sqldf("select batt_sensor.*, weather.tempAvg, weather.relHumAvg from")
batt_sensor <- batt_sensor[,-c(52,64)]
batt_sensor$MilPerDay <- as.numeric(batt_sensor$BatteryMil)/as.numeric(batt_sensor$BatteryAge)
batt_sensor_1 <- subset(batt_sensor,ReplaceMon>0&ReplaceDateVol>0)
write.csv(batt_sensor_1,"data/Battery_Sensor_09272014.csv",row.names=F)
batt_sensor_1 <- read.csv("data/Battery_Sensor_09272014.csv",stringsAsFactors=F)
batt_sensor_1[is.na(batt_sensor_1$WeightRatio)] <-0
batt_sensor_1[is.na(batt_sensor_1$TagRatio)] <-0
batt_sensor_1 <- batt_sensor_1[batt_sensor_1$WeightRatio>0,]
batt_sensor_1 <- batt_sensor_1[batt_sensor_1$MilPerDay>0,]
batt_sensor_1 <- batt_sensor_1[batt_sensor_1$TagWeight>0,]
batt_sensor_1_normal <- subset(batt_sensor_1, BatteryAge>365/2&BatteryAge<365*3)

indexes = sample(1:nrow(batt_sensor_1_normal), size=0.4*nrow(batt_sensor_1_normal))
test = batt_sensor_1_normal[indexes,]
train = batt_sensor_1_normal[-indexes,]

lm.fit <- lm(BatteryAge~VehicleGroup+poly(as.numeric(ReplaceMon),2)+poly(as.numeric(MilPerDay),2)
            +poly(BatteryMil,2)+poly(TaskYr,2)+poly(DateInservice_yy,2)+poly(DateInservice_mm,2)+poly(NomDaysUsed,2)
            +poly(as.numeric(WeightRatio),2)+poly(Odometer,2)+poly(avg_battage_model,2)+poly(avg_battmil_model,2)
            +poly(avg_battage_vehgroup,2)+poly(avg_battmil_vehgroup,2)+poly(ReplaceDateVol,2),
             data=batt_sensor_1)
lm.fit.2 <- lm(BatteryAge~as.numeric(ReplaceMon)+as.numeric(MilPerDay)
             +BatteryMil+TaskYr+DateInservice_yy+DateInservice_mm+NomDaysUsed
             +as.numeric(WeightRatio)+Odometer+avg_battage_model+avg_battmil_model
             +avg_battage_vehgroup+avg_battmil_vehgroup+ReplaceDateVol,
             data=batt_sensor_1_normal)
lm_fit <- train(BatteryAge~as.numeric(ReplaceMon)+as.numeric(MilPerDay)
             +BatteryMil+TaskYr+DateInservice_yy+DateInservice_mm+NomDaysUsed
             +as.numeric(WeightRatio)+Odometer+avg_battage_model+avg_battmil_model
             +avg_battage_vehgroup+avg_battmil_vehgroup+ReplaceDateVol,
             data=batt_sensor_1_normal,method="lm",preProc=c("center","scale"))
lm_fit_2 <- train(BatteryAge~VehicleGroup+poly(as.numeric(ReplaceMon),1)+poly(as.numeric(MilPerDay),2)
                  +poly(BatteryMil,2)+poly(TaskYr,2)+poly(DateInservice_yy,2)+poly(DateInservice_mm,2)+poly(NomDaysUsed,2)
                  +poly(as.numeric(WeightRatio),2)+poly(Odometer,2)+poly(avg_battage_model,2)+poly(avg_battmil_model,2)
                  +poly(avg_battage_vehgroup,2)+poly(avg_battmil_vehgroup,2)+poly(ReplaceDateVol,2),
                  data=batt_sensor_1,method="lm",preProc=c("center","scale"))
ridge_fit_6 <- train(BatteryAge~VehicleGroup+poly(as.numeric(ReplaceMon),6)+poly(as.numeric(MilPerDay),6)
                  +poly(TaskYr,1)+poly(DateInservice_yy,6)+poly(DateInservice_mm,6)+poly(NomDaysUsed,6)
                  +poly(as.numeric(WeightRatio),6)+poly(as.numeric(TagWeight),6)+poly(Odometer,6)+poly(avg_battage_model,6)+poly(avg_battmil_model,4)
                  +poly(avg_battage_vehgroup,6)+poly(avg_battmil_vehgroup,4)+poly(Mileage,6)
                    +poly(std_battmil_model,6)+poly(std_battage_vehgroup,6)+poly(std_battmil_vehgroup,6)
                  +poly(ReplaceDateVol,6)+poly(cnt_vehgroup,6)+poly(cnt_model,6)+poly(cnt_garage,6)
                  +poly(ratioofrunning,6)+poly(avgbattvol_veh,6)+poly(stdbattvol_veh,6),
                  data=train,method="ridge",preProc=c("center","scale"))

lm_fit_4 <- train(BatteryAge~VehicleGroup+poly(as.numeric(ReplaceMon),4)+poly(as.numeric(MilPerDay),4)
                     +poly(TaskYr,4)+poly(DateInservice_yy,4)+poly(DateInservice_mm,4)+poly(NomDaysUsed,4)
                     +poly(as.numeric(WeightRatio),4)+poly(as.numeric(TagWeight),4)+poly(Odometer,4)+poly(avg_battage_model,4)+poly(avg_battmil_model,4)
                     +poly(avg_battage_vehgroup,4)+poly(avg_battmil_vehgroup,4)+poly(Mileage,4)
                     +poly(std_battmil_model,4)+poly(std_battage_vehgroup,4)+poly(std_battmil_vehgroup,4)
                     +poly(ReplaceDateVol,4)+poly(cnt_vehgroup,4)+poly(cnt_model,4)+poly(cnt_garage,4)
                     +poly(ratioofrunning,4)+poly(avgbattvol_veh,4)+poly(stdbattvol_veh,4),
                     data=train,method="lm",preProc=c("center","scale"))

ridge_pred <- predict(ridge_fit_6,test)
test_pred <- cbind(test, ridge_pred)
write.csv(test_pred, "data/Ridge_batteryage_09282014.csv",row.names=F)


### sep 29th
result <- read.csv("data/result_3models_sort.csv",stringsAsFactors=F)
cor(result[,c(32,44,45,28,60)])


### Sep 30th
batt <- read.csv("data/Battery_0919_2014.csv",stringsAsFactors=F)
weather <- fread("data/weather_data/weather_2010_small.csv",header=T,stringsAsFactors=F)
weather$timestamp <- as.Date(weather$timestamp)
sensor <- fread("data/sensor/battery_voltage/Batt_vol_avg.txt",sep="\001",header=F,stringsAsFactors=F)
sensor <- as.data.frame(sensor)
sensor_veh <- sqldf("select Vehicle, avg(SensorValue) as avgbattvol_veh,stdev(SensorValue) as stdbattvol_veh, count(*) as cnt,
                    max(Date)-min(Date) as datediff from sensor group by Vehicle")
sensor_veh$ratioofrunning <- sensor_veh$cnt/sensor_veh$datediff
names(sensor) <- c("Vehicle","Date","SensorName","SensorValue","Key")
sensor$Date <- as.Date(sensor$Date)
batt_sensor <- sqldf("select batt.*,sensor.SensorValue as ReplaceDateVol from batt left join sensor on
                     (batt.Vehicle=sensor.Vehicle and batt.ReplaceDate=sensor.Date)")
batt_sensor <- sqldf("select batt_sensor.*, sensor_veh.* from batt_sensor left join sensor_veh on
                     batt_sensor.Vehicle=sensor_veh.Vehicle")
batt_sensor_weather <- sqldf("select batt_sensor.*, weather.tempAvg, weather.relHumAvg from batt_sensor left join weather on")


batt_1 <- subset(batt_sensor, BRR==1)
batt_1$MilPerDay <- as.numeric(batt_1$BatteryMil)/as.numeric(batt_1$BatteryAge)
batt_1_2010 <- subset(batt_1, BatteryAge>30&&BatteryAge<1500&BatteryMil>0&DateInservice_yy>1995)
batt_1_2010_byserviceyear <- tapply(batt_1_2010$MilPerDay,batt_1_2010$DateInservice_yy, summary)
batt_1_2010$BatteryMil <- as.numeric(batt_1_2010$BatteryMil)
cor(batt_1_2010[,c(12,15,16,20,49,47)])

batt_garage <- sqldf("select GarageID, avg(BatteryAge) as avg_battage_garage, stdev(BatteryAge) as std_battage_garage,
                     avg(BatteryMil) as avg_battmil_garage,stdev(BatteryMil) as std_battmil_garage, count(*) as cnt_garage
                     from batt_1_2010 group by GarageID")
batt_replacegarage <- sqldf("select ReplaceGarage, avg(BatteryAge) as avg_battage_rg, stdev(BatteryAge) as std_battage_rg,
                     avg(BatteryMil) as avg_battmil_rg,stdev(BatteryMil) as std_battmil_rg, count(*) as cnt_rg
                     from batt_1_2010 group by ReplaceGarage")
batt_person <- sqldf("select PersonID, avg(BatteryAge) as avg_battage_person, stdev(BatteryAge) as std_battage_person,
                     avg(BatteryMil) as avg_battmil_person,stdev(BatteryMil) as std_battmil_person, count(*) as cnt_person
                     from batt_1_2010 group by PersonID")
batt_replaceperson <- sqldf("select ReplacePerson, avg(BatteryAge) as avg_battage_rp, stdev(BatteryAge) as std_battage_rp,
                     avg(BatteryMil) as avg_battmil_rp,stdev(BatteryMil) as std_battmil_rp, count(*) as cnt_rp
                     from batt_1_2010 group by ReplacePerson")
batt_model <- sqldf("select MakeID,Model, avg(BatteryAge) as avg_battage_model, stdev(BatteryAge) as std_battage_model,
                    avg(BatteryMil) as avg_battmil_model,stdev(BatteryMil) as std_battmil_model, count(*) as cnt_model
                    from batt_1_2010 group by Model")
batt_DateInserviceYr <- sqldf("select DateInservice_yy, avg(BatteryAge) as avg_battage_DIS, stdev(BatteryAge) as std_battage_DIS,
                    avg(BatteryMil) as avg_battmil_DIS,stdev(BatteryMil) as std_battmil_DIS, count(*) as cnt_DIS
                    from batt_1_2010 group by DateInservice_yy")
batt_TaskMon <- sqldf("select TaskMon, avg(BatteryAge) as avg_battage_TaskMon, stdev(BatteryAge) as std_battage_TaskMon,
                    avg(BatteryMil) as avg_battmil_TaskMon,stdev(BatteryMil) as std_battmil_TaskMon, count(*) as cnt_TaskMon
                              from batt_1_2010 group by TaskMon")
batt_ReplaceMon <- sqldf("select ReplaceMon, avg(BatteryAge) as avg_battage_ReplaceMon, stdev(BatteryAge) as std_battage_ReplaceMon,
                    avg(BatteryMil) as avg_battmil_ReplaceMon,stdev(BatteryMil) as std_battmil_ReplaceMon, count(*) as cnt_ReplaceMon
                      from batt_1_2010 group by ReplaceMon")
batt_vehgroup <- sqldf("select VehicleGroup, avg(BatteryAge) as avg_battage_vehgroup, stdev(BatteryAge) as std_battage_vehgroup,
                       avg(BatteryMil) as avg_battmil_vehgroup,stdev(BatteryMil) as std_battmil_vehgroup, count(*) as cnt_vehgroup
                       from batt_1_2010 group by VehicleGroup")
batt_1_2010 <- merge(x=batt_1_2010, y=batt_garage, by="GarageID",x.all=T)
batt_1_2010 <- merge(x=batt_1_2010, y=batt_replacegarage, by="ReplaceGarage",x.all=T)
batt_1_2010 <- merge(x=batt_1_2010, y=batt_person, by="PersonID",x.all=T)
batt_1_2010 <- merge(x=batt_1_2010, y=batt_replaceperson, by="ReplacePerson",x.all=T)
batt_1_2010 <- merge(x=batt_1_2010, y=batt_model, by="Model",x.all=T)
batt_1_2010 <- merge(x=batt_1_2010, y=batt_DateInserviceYr, by="DateInservice_yy",x.all=T)
batt_1_2010 <- merge(x=batt_1_2010, y=batt_TaskMon, by="TaskMon",x.all=T)
batt_1_2010 <- merge(x=batt_1_2010, y=batt_ReplaceMon, by="ReplaceMon",x.all=T)
batt_1_2010 <- merge(x=batt_1_2010, y=batt_vehgroup, by="VehicleGroup",x.all=T)


write.csv(batt_1_2010,"data/Battery_1_09302014.csv",row.names=F)
batt_1_2010_comp <- subset(batt_1_2010,avgbattvol_veh >0&WeightRatio>0)
library(caret)
ridge_fit_4 <- train(BatteryAge~VehicleGroup+TaskMon+DateInservice_yy+TagWeight+poly(MilPerDay,4)
                     +poly(avg_battage_garage,4)+poly(avg_battmil_garage,4)+poly(cnt_garage,4)
                     +poly(avg_battage_person,4)+poly(avg_battmil_person,4)+poly(cnt_person,4)
                     +poly(avg_battage_rg,4)+poly(avg_battmil_rg,4)+poly(cnt_rg,4)
                     +poly(avg_battage_rp,4)+poly(avg_battmil_rp,4)+poly(cnt_rp,4)
                     +poly(avg_battage_model,4)+poly(avg_battmil_model,4)+poly(cnt_model,4)
                     +poly(avg_battage_DIS,4)+poly(avg_battmil_DIS,4)+poly(cnt_DIS,4)
                     +poly(avg_battage_TaskMon,4)+poly(avg_battmil_TaskMon,4)+poly(cnt_TaskMon,4)
                     +poly(avg_battage_vehgroup,4)+poly(avg_battmil_vehgroup,4)+poly(cnt_vehgroup,4),
                     data=batt_1_2010,method="ridge",preProc=c("center","scale"))
batt_1_2010[is.na(batt_1_2010)] <-0

+poly(avgbattvol_veh,6)+poly(stdbattvol_veh,6)
lm_fit_4 <- train(BatteryAge~VehicleGroup+poly(as.numeric(ReplaceMon),6)+poly(as.numeric(MilPerDay),6)

                     +poly(DateInservice_yy,6)+poly(DateInservice_mm,6)+poly(NomDaysUsed,6)
                     +poly(as.numeric(TagWeight),6)+poly(Odometer,6)+poly(as.numeric(WeightRatio),6)
                     +poly(avg_battage_garage,4)+poly(avg_battmil_garage,4)+poly(cnt_garage,4)
                     +poly(avg_battage_person,4)+poly(avg_battmil_person,4)+poly(cnt_person,4)
                     +poly(avg_battage_rg,4)+poly(avg_battmil_rg,4)+poly(cnt_rg,4)
                     +poly(avg_battage_rp,4)+poly(avg_battmil_rp,4)+poly(cnt_rp,4)
                     +poly(avg_battage_model,4)+poly(avg_battmil_model,4)+poly(cnt_model,4)
                     +poly(avg_battage_DIS,4)+poly(avg_battmil_DIS,4)+poly(cnt_DIS,4)
                     +poly(avg_battage_TaskMon,4)+poly(avg_battmil_TaskMon,4)+poly(cnt_TaskMon,4)
                     +poly(avg_battage_vehgroup,4)+poly(avg_battmil_vehgroup,4)+poly(cnt_vehgroup,4),
                     data=batt_1_2010_comp,method="lm",preProc=c("center","scale"))

library(randomForest)
rfModel <- randomForest(BatteryAge~VehicleGroup+TaskMon+DateInservice_yy+TagWeight+MilPerDay
                        +avg_battage_garage+avg_battmil_garage+cnt_garage
                        +avg_battage_person+avg_battmil_person+cnt_person
                        +avg_battage_rg+avg_battmil_rg+cnt_rg
                        +avg_battage_rp+avg_battmil_rp+cnt_rp
                        +avg_battage_model+avg_battmil_model+cnt_model
                        +avg_battage_DIS+avg_battmil_DIS+cnt_DIS
                        +avg_battage_TaskMon+avg_battmil_TaskMon+cnt_TaskMon
                        +avg_battage_vehgroup+avg_battmil_vehgroup+cnt_vehgroup,
                        data=batt_1_2010_comp,importance=T, ntress=30)



## oct 1
type23 <- read.csv("data/JobType23_10012014.csv",stringsAsFactors=F)
noprobfound <- subset(type23, TaskName=="Predictive Maintenance- no problem found")
noprobfound_unique <- noprobfound[!duplicated(noprobfound$JobNo),]
corrected <- subset(type23, TaskName=="Predictive Maintenance - Corrected")
corrected_unique <- corrected[!duplicated(corrected$JobNo),]
write.csv(noprobfound_unique,"data/jobtype23_noproblemfound_10012014.csv", row.names=F)
write.csv(corrected_unique,"data/jobtype23_corrected_10012014.csv", row.names=F)


noprobfound_by_GarageID <- sqldf("select GarageID, count(*) as noproblemfound_count from noprobfound_unique group by GarageID")
corrected_by_GarageID <- sqldf("select GarageID, count(*) as corrected_count from corrected_unique group by GarageID")
by_garageID <- merge(x=noprobfound_by_GarageID, y=corrected_by_GarageID, by="GarageID", all=T )
by_garageID$noproblemfound_ratio <- by_garageID$noproblemfound_count/(by_garageID$corrected_count+by_garageID$noproblemfound_count)
by_garageID_sort <- by_garageID[order(by_garageID$noproblemfound_ratio, decreasing=T),] 
write.csv(by_garageID_sort, "data/10012014_by_garage.csv", row.names=F)

noprobfound_by_PersonID <- sqldf("select PersonID, count(*) as noproblemfound_count from noprobfound_unique group by PersonID")
corrected_by_PersonID <- sqldf("select PersonID, count(*) as corrected_count from corrected_unique group by PersonID")
by_PersonID <- merge(x=noprobfound_by_PersonID, y=corrected_by_PersonID, by="PersonID", all=T )
by_PersonID$noproblemfound_ratio <- by_PersonID$noproblemfound_count/(by_PersonID$corrected_count+by_PersonID$noproblemfound_count)
by_PersonID_sort <- by_PersonID[order(by_PersonID$noproblemfound_ratio, decreasing=T),] 
write.csv(by_PersonID_sort, "data/10012014_by_person.csv", row.names=F)

noprobfound_by_SpecID <- sqldf("select SpecID, count(*) as noproblemfound_count from noprobfound_unique group by SpecID")
corrected_by_SpecID <- sqldf("select SpecID, count(*) as corrected_count from corrected_unique group by SpecID")
by_SpecID <- merge(x=noprobfound_by_SpecID, y=corrected_by_SpecID, by="SpecID", all=T )
by_SpecID$noproblemfound_ratio <- by_SpecID$noproblemfound_count/(by_SpecID$corrected_count+by_SpecID$noproblemfound_count)
by_SpecID_sort <- by_SpecID[order(by_SpecID$noproblemfound_ratio, decreasing=T),] 
write.csv(by_SpecID_sort, "data/10012014_by_specid.csv", row.names=F)

noprobfound_by_SpecID <- sqldf("select SpecID, count(*) as noproblemfound_count from noprobfound_unique group by SpecID")
corrected_by_SpecID <- sqldf("select SpecID, count(*) as corrected_count from corrected_unique group by SpecID")
by_SpecID <- merge(x=noprobfound_by_SpecID, y=corrected_by_SpecID, by="SpecID", all=T )
by_SpecID$noproblemfound_ratio <- by_SpecID$noproblemfound_count/(by_SpecID$corrected_count+by_SpecID$noproblemfound_count)
by_SpecID_sort <- by_SpecID[order(by_SpecID$noproblemfound_ratio, decreasing=T),] 
write.csv(by_SpecID_sort, "data/10012014_by_specid.csv", row.names=F)

noprobfound_by_VehicleGroup <- sqldf("select VehicleGroup, count(*) as noproblemfound_count from noprobfound_unique group by VehicleGroup")
corrected_by_VehicleGroup <- sqldf("select VehicleGroup, count(*) as corrected_count from corrected_unique group by VehicleGroup")
by_VehicleGroup <- merge(x=noprobfound_by_VehicleGroup, y=corrected_by_VehicleGroup, by="VehicleGroup", all=T )
by_VehicleGroup$noproblemfound_ratio <- by_VehicleGroup$noproblemfound_count/(by_VehicleGroup$corrected_count+by_VehicleGroup$noproblemfound_count)
by_VehicleGroup_sort <- by_VehicleGroup[order(by_VehicleGroup$noproblemfound_ratio, decreasing=T),] 
write.csv(by_VehicleGroup_sort, "data/10012014_by_vehgroup.csv", row.names=F)

noprobfound_by_Model <- sqldf("select Model, count(*) as noproblemfound_count from noprobfound_unique group by Model")
corrected_by_Model <- sqldf("select Model, count(*) as corrected_count from corrected_unique group by Model")
by_Model <- merge(x=noprobfound_by_Model, y=corrected_by_Model, by="Model", all=T )
by_Model$noproblemfound_ratio <- by_Model$noproblemfound_count/(by_Model$corrected_count+by_Model$noproblemfound_count)
by_Model_sort <- by_Model[order(by_Model$noproblemfound_ratio, decreasing=T),] 
write.csv(by_Model_sort, "data/10012014_by_model.csv", row.names=F)

noprobfound_by_State <- sqldf("select State, count(*) as noproblemfound_count from noprobfound_unique group by State")
corrected_by_State <- sqldf("select State, count(*) as corrected_count from corrected_unique group by State")
by_State <- merge(x=noprobfound_by_State, y=corrected_by_State, by="State", all=T )
by_State$noproblemfound_ratio <- by_State$noproblemfound_count/(by_State$corrected_count+by_State$noproblemfound_count)
by_State_sort <- by_State[order(by_State$noproblemfound_ratio, decreasing=T),] 
write.csv(by_State_sort, "data/10012014_by_state.csv", row.names=F)

noprobfound_by_TaskMon <- sqldf("select TaskMon, count(*) as noproblemfound_count from noprobfound_unique group by TaskMon")
corrected_by_TaskMon <- sqldf("select TaskMon, count(*) as corrected_count from corrected_unique group by TaskMon")
by_TaskMon <- merge(x=noprobfound_by_TaskMon, y=corrected_by_TaskMon, by="TaskMon", all=T )
by_TaskMon$noproblemfound_ratio <- by_TaskMon$noproblemfound_count/(by_TaskMon$corrected_count+by_TaskMon$noproblemfound_count)
by_TaskMon_sort <- by_TaskMon[order(by_TaskMon$noproblemfound_ratio, decreasing=T),] 
write.csv(by_TaskMon_sort, "data/10012014_by_taskmon.csv", row.names=F)


### Oct 2
DIS2014 <- read.csv("data/BRR_DIS2014.csv",stringsAsFactors=F)
DIS2014_unique <- DIS2014[!duplicated(DIS2014$JobNo),]
DIS <- read.csv("data/DIS_10-14_Electrical.csv",stringsAsFactors=F)
DIS_unique <- DIS[!duplicated(DIS$JobNo),]
DIS_1yr <- subset(DIS_unique,DateDiff<366)
write.csv(DIS_1yr, "data/DIS_jobwithin1yr.csv",row.names=F)
DIS_garage <-sqldf("select GarageID, count(*) as cnt from DIS_1yr group by GarageID order by cnt desc")

### Oct 3
# Battery R&R data from solr
BRR <- read.csv("data/BRR_1003_2014.csv",stringsAsFactors=F)
BRR_unique <- BRR[!duplicated(BRR$JobNo),]
# All the jobs on solr under JobType 23
type23 <- read.csv("data/JobType23_10012014.csv",stringsAsFactors=F)
noprobfound <- subset(type23, TaskName=="Predictive Maintenance- no problem found")
noprobfound_unique <- noprobfound[!duplicated(noprobfound$JobNo),]
BRR_noprobfound <- sqldf("select * from BRR_unique where Vehicle in (select Vehicle from noprobfound_unique)")
BRR_noprobfound_join <- merge(x=noprobfound_unique,y=BRR_noprobfound, by="Vehicle",y.all=T)
# if use JobDate
BRR_noprobfound_join$JobDate.x <- as.Date(BRR_noprobfound_join$JobDate.x,"%m/%d/%y")
BRR_noprobfound_join$JobDate.y <- as.Date(BRR_noprobfound_join$JobDate.y,"%m/%d/%y")
BRR_noprobfound_join$BRRsinceNoprob_jobdate <- BRR_noprobfound_join$JobDate.y-BRR_noprobfound_join$JobDate.x
BRR_noprobfound_join_1 <- subset(BRR_noprobfound_join,as.numeric(BRRsinceNoprob_jobdate)>0&as.numeric(BRRsinceNoprob_jobdate)<70)
BRR_noprobfound_result <- BRR_noprobfound_join_1[,c("Vehicle","JobDate.x","TaskName.x","JobDate.y","TaskName.y")]
write.csv(BRR_noprobfound_result,"data/BRR_noprobfound_result.csv", row.names=F)

BRR_PdM <- subset(BRR_unique, JobCategory=="PdM")


BRR_corrected <- sqldf("select * from BRR_unique where Vehicle in (select Vehicle from corrected_unique)")
BRR_corrected_join <- merge(x=corrected_unique,y=BRR_corrected, by="Vehicle",y.all=T)
BRR_corrected_join$DetailDate.x <- as.Date(BRR_corrected_join$DetailDate.x,"%m/%d/%y")
BRR_corrected_join$DetailDate.y <- as.Date(BRR_corrected_join$DetailDate.y,"%m/%d/%y")
BRR_corrected_join$JobDate.x <- as.Date(BRR_corrected_join$JobDate.x,"%m/%d/%y")
BRR_corrected_join$JobDate.y <- as.Date(BRR_corrected_join$JobDate.y,"%m/%d/%y")
BRR_corrected_join$BRRsinceCorrected_jobdate <- BRR_corrected_join$JobDate.y-BRR_corrected_join$JobDate.x



job23 <- read.csv("data/JobType23_10032014.csv",stringsAsFactors=F)
job23_unique <- job23[!duplicated(job23[,c("JobNo","TaskName")]),]
job23<- subset(job23_unique, TaskName=="Predictive Maintenance - Corrected"|TaskName=="Predictive Maintenance- no problem found"|TaskFam==6)
job23$count <- 1
job23_wide <- dcast(job23, Vehicle+JobNo+JobDate~TaskName,value.var=c("count"))
job23_wide[is.na(job23_wide)] <- 0
write.csv(job23_wide,"data/Job23_wide_10032014.csv",row.names=F)

corrected_in_noprob <- sqldf("select * from noprobfound_unique where Vehicle in (select Vehicle from corrected_unique)")



### Oct 16th
job23 <- read.csv("data/10162014_Jobtype23.csv",stringsAsFactors=F)
job23_unique <- job23[!duplicated(job23$JobNo),]
pred <- read.csv("data/10162014_prediction.csv",stringsAsFactors=F)
job23_pm <- subset(job23, TaskName=="Predictive Maintenance - Corrected"|TaskName=="Predictive Maintenance- no problem found")
job23_pm_unique <- job23_pm[!duplicated(job23_pm$JobNo),]

job23_pred <- merge(x=pred, y=job23_pm_unique, by="Vehicle",x.all=T)
not_pred <- sqldf("select Vehicle, JobNo from job23_pm_unique where Vehicle not in (select Vehicle from pred)")
write.csv(job23_pred,"data/10162014_job23_pred.csv",row.names=F)
write.csv(not_pred,"data/10162014_notpred.csv",row.names=F)


## Oct 21
vehicle <- read.csv("data/vehicle.csv",stringsAsFactors=F)
batteryRnR <- read.csv("data/10212014_BatteryRnR.csv",stringsAsFactors=F)
BattRnR_unique <- batteryRnR[!duplicated(batteryRnR$JobNo),]
vehicle_A <- subset(vehicle, Status == "A")
vehicle_no_brr <-sqldf("select * from vehicle_A where Vehicle not in (select Vehicle from BattRnR_unique)")
vehicle_no_brr$DateInservice <- as.Date(vehicle_no_brr$DateInservice,"%m-%d-%Y")
vehicle_no_brr$BRR <- 0
vehicle_no_brr$BatteryAge_1 <- as.numeric(Sys.Date()-vehicle_no_brr$DateInservice)
vehicle_no_brr$BatteryAge_2 <- as.numeric(Sys.Date()-as.Date("2010-01-01","%Y-%m-%d"))
vehicle_no_brr$BatteryAge <- pmin(vehicle_no_brr$BatteryAge_1, vehicle_no_brr$BatteryAge_2)
hist(vehicle_no_brr_normal$BatteryAge,breaks=500,xlim=range(0,1800),ylim=range(0,3000))
write.csv(vehicle_no_brr,"data/10212014_vehicle_no_brr.csv",row.names=F)
vehicle_no_brr_normal <- sqldf("select * from vehicle_no_brr where Vehicle not like '5%'")
write.csv(vehicle_no_brr_normal,"data/10212014_vehicle_no_brr_normal.csv",row.names=F)
BattRnR <- BattRnR_unique[,c("Vehicle","FuelType","JobDate","GarageID","Status","Model","SpecID","VehicleGroup","BatteryReplaceDate","BRR","BatteryAge",
                                           "TagWeight","CombinedWeight","State","MakeID","Mileage","TaskMon")]

ifelse(vehicle_no_brr_normal$BatteryAge == vehicle_no_brr_normal$BatteryAge_1,
       vehicle_no_brr_normal$TaskMon <-  as.numeric(format(vehicle_no_brr_normal$DateInservice,"%m")),
       vehicle_no_brr_normal$TaskMon <- "NA")
names(vehicle_no_brr_normal)[16] <- "Mileage"
vehicle_no_brr_normal$BatteryReplaceDate <- "NA"
names(vehicle_no_brr_normal)[3] <-"VehicleGroup"
vehicle_no_brr_normal$JobDate <- "NA"
vehicle_no_BRR <- vehicle_no_brr_normal[,c("Vehicle","FuelType","JobDate","GarageID","Status","Model","SpecID","VehicleGroup","BatteryReplaceDate","BRR","BatteryAge",
                                           "TagWeight","CombinedWeight","State","MakeID","Mileage","TaskMon")]
Total_batteryRnR <- rbind(vehicle_no_BRR,BattRnR) 
write.csv(Total_batteryRnR, "data/10222014_BatteryRnR_total.csv",row.names=F)



indexes = sample(1:nrow(Total_batteryRnR), size=0.5*nrow(Total_batteryRnR))
test = Total_batteryRnR[indexes,]
train = Total_batteryRnR[-indexes,]
write.csv(train, "data/10222014_BatteryRnR_total_train.csv",row.names=F)
write.csv(test, "data/10222014_BatteryRnR_total_test.csv",row.names=F)

### Oct 23
data <- read.csv("data/10222014_BatteryRnR_total.csv",stringsAsFactors=F)
data$ReplaceDate <- as.Date(data$ReplaceDate,"%m/%d/%y")
data$ReplaceMon <- as.numeric(format(data$ReplaceDate,"%m"))
data$BatteryAge <- as.numeric(data$BatteryAge)
data$BatteryAgeModify <- data$BatteryAge**0.35
train_lr <- subset(data, BRR==1&BatteryAge>15)
test_lr <- subset(data, BRR==0)
#### BatteryAgeModify = BatteryAge**0.35
train_lr$BatteryAgeModify <- train_lr$BatteryAge**0.35
test_lr$BatteryAgeModify <- test_lr$BatteryAge**0.35

sensor <- fread("data/mh_10202014_vehicle_sensor.csv",sep="\001")
names(sensor) <- c("Vehicle","Sensor_Name","Mean_Sensor_Value","Sd_Sensor_Value","Min_Sensor_Value","Max_Sensor_Value","Count")
sensor_data <- merge(x=data, y=sensor_wide_avg, by="Vehicle", x.all=T)
write.csv(sensor_data,"data/10252014_Battery_sensor.csv",row.names=F)
train_lr <- subset(sensor_data, BRR==1&BatteryAge>15)
test_lr <- subset(sensor_data, BRR==0)
write.csv(train_lr,"data/10252014_BatteryRnR_total_BRR1.csv",row.names=F)
write.csv(test_lr,"data/10252014_BatteryRnR_total_BRR0.csv",row.names=F)
## Build random regression forest on BatteryAge
install.packages("/Users/bdcoe/Downloads/h2o-2.9.0.1560/R/h2o_2.9.0.1560.tar.gz",
                 repos = NULL, type = "source")
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
data <- h2o.importFile(localH2O, path="data/10222014_BatteryRnR_total.csv")
features <- c("FuelType","GarageID","Model","SpecID","VehicleGroup","TagWeight","CombinedWeight","State","MakeID","Mileage","ReplaceMon")
gbm.fit <- h2o.gbm(BatteryAgeModify ~., distribution = "gaussian", data=train_1, key = "", n.trees = 200,
                   interaction.depth = 5,  shrinkage = 0.1, n.bins = 100,
                   importance = T, nfolds = 0,
                   max.after.balance.size = 5)
gbm.pred = h2o.predict(object = gbm.fit, newdata = test_1)
gbm.combine <- cbind(test_1,gbm.pred)
head(gbm.combine,30)[c(11,22)]


gbm.pred <- as.data.frame(gbm.pred)
test_pred <- cbind(test_lr,gbm.pred)
test_pred <- as.data.frame(test_pred)

indexes = sample(1:nrow(train), size=0.5*nrow(train))
train_1 <- train[indexes,]
test_1 <- train[-indexes,]

write.csv(test_pred,"data/10222014_Prediction_gbm.csv",row.names=F)

glm.fit <- h2o.glm(x=features, y="BatteryAgeModify", data=train_1, family="gaussian", alpha = 0.5, nlambda = -1, lambda.min.ratio = -1,
                   lambda = 1e-5, epsilon = 1e-4, standardize = TRUE, variable_importances = T, use_all_factor_levels = FALSE)
glm.pred = h2o.predict(object = glm.fit, newdata = test)
glm.pred <- as.data.frame(glm.pred)
test_pred_glm <- cbind(test_lr,glm.pred)
test_pred_glm <- as.data.frame(test_pred_glm)
write.csv(test_pred_glm,"data/10222014_Prediction_glm.csv")
h2o.shutdown(localH2O, prompt = TRUE)


#### Oct 26
data_small <- data[sample(1:nrow(data), size=0.001*nrow(data)),]

features <- c("FuelType","GarageID","Model","VehicleGroup","Mileage")
target <- c("BatteryAge","BRR")
coxph.fit <- h2o.coxph(x=features, y=target, data=data_small)


#### Nov 11
PM <- read.csv("data/11112014_PMInspection.csv",stringsAsFactors=F)
hist(as.numeric(PM$DateDiff),xlim=range(0,15),breaks=100)
hist(as.numeric(PM$MileDiff),xlim=range(0,20),breaks=20000)
summary(as.numeric(PM$MileDiff))
summary(as.numeric(PM$DateDiff))



alternator <- read.csv("data/11112014_alternator.csv",stringsAsFactors=F)
indexes = sample(1:nrow(alternator), size=0.4*nrow(alternator))
test = alternator[indexes,]
train = alternator[-indexes,]
write.csv(train,"data/11112014_alternator_train.csv",row.names=F)
write.csv(test,"data/11112014_alternator_test.csv",row.names=F)


#### Nov 18th
drum <- read.csv("data/11182014_DrumsBrake.csv",stringsAsFactors=F)
pad  <- read.csv("data/11182014_DiscBrakesFrontRnR.csv",stringsAsFactors=F)
rotor <- read.csv("data/11182014_BrakeRotorRnR.csv",stringsAsFactors=F)