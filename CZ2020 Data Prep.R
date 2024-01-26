#Copyright (c) 2024, Christopher S. Fowler
#  All rights reserved.
#
#This source code is licensed under the BSD-style license found in the
#LICENSE file in the root directory of this source tree. 
#
#########################################
#CZ2020 Data Prep
#
#This file captures the downloading of data and the creation of the files needed
#for delineating Commuting Zones. This includes county to county flows and populations
#as well as wage data from BLS for use in generating fit metrics.
#########################################
#Libraries
require(tidyverse)
require(tidycensus)
require(readxl)
require(sf)
########################################
source("./CZ2020_Functions.R")
########################################
if(!file.exists("./Input Data")){dir.create("./Input Data")}
if(!file.exists("./Input Data/Wages")){dir.create("./Input Data/Wages")}
if(!file.exists("./Intermediate Data")){dir.create("./Intermediate Data")}
if(!file.exists("./Output Data")){dir.create("./Output Data")}


#1. County boundaries with population
# Download 2022 county boundaries and total population to account for change to planning regions in 2022
# that has been retroactively applied to the flow data
# Repeat for 2010 for comparison only
if(!file.exists("./Intermediate Data/counties20.Rdata")){ #if the processed file already exists, skip these steps
  county20 <- get_acs(
    geography = "county", 
    year = 2022, #get 2022 data to account for retroactive change to planning regions
    variables = c(TotPop20 ="B01003_001"),
    output="wide",
    geometry = TRUE
  )
  county20$FIPS<-as.numeric(county20$GEOID)

  county10 <- get_decennial(
    geography = "county", 
    year = 2010,
    variables = c(TotPop10 ="P001001"),
    output="wide",
    geometry = TRUE
  )
  county10$FIPS<-as.numeric(county10$GEOID)
  
#2.Download and format Wage data from Quarterly Census of Employment and Wages
#We will need 2016-2020
  years<-2016:2020
  for(i in years){
    wage<-qcewGetCountyWageData(i)
    chg_CT<-convertCT(popData=wage[wage$FIPS<10000 & wage$FIPS>9000,],from='county',to='planning',transform='mean')
    wage<-wage[wage$FIPS>10000 | wage$FIPS<9000,] #remove CT
    colnames(chg_CT)<-colnames(wage) #rename columns to match
    wage<-rbind(wage,chg_CT) #add in the converted CT
    county20<-left_join(county20,wage,by='FIPS',)
  }

  #for comparison purposes only
  years<-2006:2010
  for(i in years){
    county10<-left_join(county10,qcewGetCountyWageData(i),by='FIPS',)
  }
#consistent with prior methodology drop Puerto Rico and outlying territories
  county10<-county10[county10$FIPS<57000,] #remove Puerto Rico
  
#for 2010 load the official CZ delineation
  cz10<-read.csv("https://sites.psu.edu/psucz/files/2018/09/counties10-zqvz0r.csv")
  cz10<-cz10[,c("FIPS","OUT10")]
#merge with county10
  county10<-left_join(county10,cz10,by='FIPS')
  
  #USGS Contiguous US Albers Equal Area
  county10<-st_transform(county10,5070)
  county20<-st_transform(county20,5070)
  
  #summary(county20)
  #county20[is.na(county20$Wage2016),]
#Copper River (2066) and Chugach (2063) Alaska are new after 2019--they were created out of Valdez-Cordova (code 2261) they are left as NA for now. They could be combined if the wage data were of particular importance.
#Kalawao County (15005) Hawaii is reported with Maui and does not appear in any of the bls wage data.

#summary(county10)
#county10[is.na(county10$Wage2006),]
#5 Alaska Counties are missing in 2006 (02105,02275,02195,02198,02230),drops to three and then zero in subsequent years
#Kalawao 15005 is missing in all five years.

#Add CBSA info to county files
#download CBSA definitions
  url<-"https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls"
  download.file(url,destfile = "./Input Data/list1_2020.xls",mode="wb")
  cbsa23<- read_excel(path="./Input Data/list1_2020.xls",sheet = "List 1",skip=2)
  cbsa23<-cbsa23[,c("CBSA Code","CBSA Title","FIPS State Code","FIPS County Code","Central/Outlying County")]
  colnames(cbsa23)<-c("CBSA","CBSA_Name","StateFP","CountyFP","Central_Outlying")
  cbsa23<-cbsa23[complete.cases(cbsa23),]
  cbsa23$FIPS<-as.numeric(paste0(cbsa23$StateFP,cbsa23$CountyFP))
#merge with county20
  county20<-left_join(county20,cbsa23,by="FIPS")
  

  url<-"https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2013/delineation-files/list1.xls"
  download.file(url,destfile = "./Input Data/list1_2013.xls",mode="wb")
  cbsa13<- read_excel(path="./Input Data/list1_2013.xls",sheet = "List 1",skip=2)
  cbsa13<-cbsa13[,c("CBSA Code","CBSA Title","FIPS State Code","FIPS County Code","Central/Outlying County")]
  colnames(cbsa13)<-c("CBSA","CBSA_Name","StateFP","CountyFP","Central_Outlying")
  cbsa13<-cbsa13[complete.cases(cbsa13),]
  cbsa13$FIPS<-as.numeric(paste0(cbsa13$StateFP,cbsa13$CountyFP))
  #merge with county10
  county10<-left_join(county10,cbsa13,by="FIPS")
  
#OUT10 to county20 using spatial join
  out10<-county10%>% group_by(OUT10) %>% summarize()
  cty20_pts<-st_point_on_surface(county20)
  cty20_pts<-st_join(cty20_pts,out10,join=st_intersects)
  cty20_pts<-st_drop_geometry(cty20_pts[,c("OUT10","GEOID")])
  county20<-left_join(county20,cty20_pts,by="GEOID")
#Drop MOE column
  county20<-county20 %>% select(-TotPop20M)
#Change TotPop colname
  colnames(county20)[colnames(county20)=="TotPop20E"]<-"TotPop20"
  

  save(county10,file="./Intermediate Data/counties10.Rdata")
  save(county20,file="./Intermediate Data/counties20.Rdata")
  rm(cz10,wage,chg_CT,cbsa13,cbsa23,i,years,url,cty20_pts,out10)
}else{
  load("./Intermediate Data/counties10.Rdata")
  load("./Intermediate Data/counties20.Rdata")
}


### 3. Commuter Flow Data for 2016-2020

#Note: initially used LODES due to delayed release of Census data. Now using correct data since LODES doesn't produce results consistent with older versions instead
#abandoned code appears after functional code using Census, Retained for posterity but not used
# Get state names
if(!file.exists("./Intermediate Data/flows.Rdata")){
  download.file("https://www2.census.gov/programs-surveys/demo/tables/metro-micro/2020/commuting-flows-2020/table1.xlsx", destfile = "./Input Data/table1.xlsx", mode = "wb")
  #get full pathname to table1.xlsx
  path<-file.path(getwd(),"Input Data/table1.xlsx")
  data <- read_excel(path,sheet = "Table 1",skip = 7)
  data<-data[,c(1,2,5,6,9)] #reduce raw data to just resident state and county, work state and county, and flow between
  colnames(data)<-c("RES_ST","RES_CTY","WK_ST","WK_CTY","Flow")
  data$Res_FIPS<-paste0(data$RES_ST,data$RES_CTY) #Generate residence county FIPS
  data$WK_ST<-substring(data$WK_ST,2) 
  data$Wk_FIPS<-paste0(data$WK_ST,data$WK_CTY) #Generate work county FIPS
  data<-data[,c("Res_FIPS","Wk_FIPS","Flow")] #Reduce to just FIPS and flow
  colnames(data)<-c("Cnty_Res","Cnty_Work","FlowCount")
  flows<-prepData(data = data[,c("Cnty_Res","Cnty_Work","FlowCount")])#reshaping data to a square matrix with all valid county pairs
  
  #making a slight change to the data here. Yakutat, Alaska is a county with no one working outside of it.
  #This places a 1 in the flows.prop matrix, which creates a huge outlier effect and draws in weird connections.
  #For now we join it back to the Skagway-Hoonah-Angoon Census Area, which is where it was located until 2019
  flows[flows$Cnty_Res %in% c("Cnty_Res02282","Cnty_Res02063","Cnty_Res02105")==FALSE,"Cnty_Wk02282"]<-NA
  flows.prop<-propFlow(data = flows) # Convert to Proportional Flow measure from 1980 methodology
  
  save(flows,file = "./Intermediate Data/flows.Rdata")
  save(flows.prop,file = "./Intermediate Data/flows.prop.Rdata")
  
# Unused code to build the above matrices from LEHD LODES data.
# if(!file.exists("./Input Data/lodes20.Rdata")){
# for(i in state.abb){
#   if(i=="AK"){ #Alaska has no data after 2016
#     m=2016
#   }else if(i=="AR"){ #Arkansas has no data after 2019
#     m=2019
#   }else if(i=="MS"){#MS has data only up to 2018
#     m=2018
#   }else{
#     m=2020
#   }
#   for(j in c("main","aux")){
#     lodes_od <- my_grab_lodes(
#       state = i,
#       year = m,
#       state_part = j,
#       use_cache = TRUE
#     )%>%
#     select(w_county,h_county,S000)
#     if(i=="AL" & j=="main"){
#       lodes<-lodes_od
#     }else{
#       lodes<-rbind(lodes,lodes_od)
#     }
#   }
# }
#  save(lodes,file="./Input Data/lodes20.Rdata")
# }else{load("./Input Data/lodes20.Rdata")}
#lodes$Cnty_Res<-as.numeric(lodes$h_county)
#lodes$Cnty_Work<-as.numeric(lodes$w_county)
#lodes$FlowCount<-lodes$S000
#lodes<-lodes%>%select(c(Cnty_Res,Cnty_Work,FlowCount))

#flows<-prepData(data = lodes[,c("Cnty_Res","Cnty_Work","FlowCount")])
#flows.prop<-propFlow(data = flows) # Convert to Proportional Flow measure from 1980 methodology
rm(path)
}else {
  load("./Intermediate Data/flows.Rdata")
  load("./Intermediate Data/flows.prop.Rdata")
}

### 4. Commuter Flow Data for 2006-2010
if(!file.exists("./Intermediate Data/flows10.Rdata")){
  download.file("https://www2.census.gov/programs-surveys/demo/tables/metro-micro/2010/commuting-employment-2010/table1.xlsx", 
                destfile = "./Input Data/table10.xlsx", mode = "wb")
  data <- read_excel(path = "./Input Data/table10.xlsx",sheet = "Table1",skip = 4)
  data<-data[,c(1:5)] #reduce raw data to just resident state and county, work state and county, and flow between
  
  # Issues in testing based on file being stored in OneDrive. I had to save the downloaded file as a csv and then load
  #data <-read.csv("./Input Data/table10.csv",skip = 4,as.is = TRUE)
  #data<-data[,c(1:5)] #reduce raw data to just resident state and county, work state and county, and flow between
  
  colnames(data)<-c("RES_ST","RES_CTY","WK_ST","WK_CTY","Flow")
  #another issue from saving as a csv --Excel does not retain leading zeroes except in first column
  #data$Res_FIPS<-data$RES_ST*1000+data$RES_CTY #Generate residence county FIPS
  #data$Wk_FIPS<-data$WK_ST*1000+data$WK_CTY #Generate work county FIPS
  
  data$Res_FIPS<-paste0(data$RES_ST,data$RES_CTY) #Generate residence county FIPS
  data$WK_ST<-substring(data$WK_ST,2) 
  data$Wk_FIPS<-paste0(data$WK_ST,data$WK_CTY) #Generate work county FIPS
  
  data<-data[,c("Res_FIPS","Wk_FIPS","Flow")] #Reduce to just FIPS and flow
  colnames(data)<-c("Cnty_Res","Cnty_Work","FlowCount")
  
  #Eliminate flows to and from Puerto Rico
  data<-data%>%filter(Cnty_Res<72000 & Cnty_Work<72000)
  flows<-prepData(data = data[,c("Cnty_Res","Cnty_Work","FlowCount")])#reshaping data to a square matrix with all valid county pairs
  flows.prop<-propFlow(data = flows) # Convert to Proportional Flow measure from 1980 methodology
  flows10<-flows
  save(flows10,file = "./Intermediate Data/flows10.Rdata")
  save(flows.prop,file = "./Intermediate Data/flows10.prop.Rdata")
}else {
  load("./Intermediate Data/flows10.Rdata")
}
rm(my_grab_lodes,prepData,propFlow,qcewGetCountyWageData) #clean up environment