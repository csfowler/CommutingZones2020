#Copyright (c) 2024, Christopher S. Fowler
#  All rights reserved.
#
#This source code is licensed under the BSD-style license found in the
#LICENSE file in the root directory of this source tree. 
#
#####################################################
#Functions in support of LMA2 Paper.Rmd
#begun by csf 12/2/15 with functions pulled initially from
#RERS_Functions.R from the LMA Project file.
######################################################
#Libraries
require(spdep)
require(vroom)
require(gt)

#Function to access quarterly census of employment and wages data from BLS web site.
qcewGetCountyWageData <- function(year) {
   out_file<-paste0("./Input Data/Wages/wages_",year,".Rdata")
   if(!file.exists(out_file)){
     url <- "https://data.bls.gov/cew/data/files/YEAR/csv/YEAR_annual_singlefile.zip"
     url <- sub("YEAR", year, url, ignore.case=FALSE)
     url <- sub("YEAR", year, url, ignore.case=FALSE)
     gz_filename<-paste0(getwd(),"/Input Data/Wages/",year,"_annual_singlefile.zip")
     download.file(url = url,destfile = gz_filename)
     data<-vroom(paste0("./Input Data/Wages/",year,"_annual_singlefile.zip"))
     data<-data[data$agglvl_code==70,c("area_fips","year","avg_annual_pay")]
     
     save(data,file = out_file)
     unlink(data)
     file.remove(gz_filename)
   }
   load(paste0("./Input Data/Wages/wages_",year,".Rdata"))
   data$FIPS<-as.numeric(data$area_fips)
   #eliminate FIPS ending in 996-999 as non-county records
   tmp<-floor(data$FIPS/1000)*1000
   tmp<-data$FIPS-tmp
   data<-data[tmp%in%c(996:999)==FALSE,]
   #Remove  outlying territories
   data<-data[data$FIPS<78000,]
   data<-data[,c("FIPS","avg_annual_pay")]
   colnames(data)[2]<-paste0("Wage",year)
   return(data)
}

#update of function from package lehdr to accommodate a simple directory changes to LODES 8
my_grab_lodes<-function (state, year, 
                         lodes_type = "od", 
                         job_type = "JT00",
                         agg_geo_to= "county",
                         state_part = c("main", "aux"),
                         download_dir = file.path(rappdirs::user_cache_dir(appname = "lehdr")), 
                         use_cache = FALSE) {
  state <- tolower(state)
  cols_types <- cols()
  url <- as.character("")
  url <- glue::glue("https://lehd.ces.census.gov/data/lodes/LODES8/{state}/{lodes_type}/{state}_{lodes_type}_{state_part}_{job_type}_{year}.csv.gz")
  col_types <- cols(w_geocode = col_character(), h_geocode = col_character(), 
                      createdate = col_character())
  httr::stop_for_status(httr::HEAD(url), paste("retrieve data for this combination of state and year on LODES.", 
    "Please see the most recent LEHD Technical Document for a list of available state/year.", 
    "https://lehd.ces.census.gov/data/lodes/LODES8/"))
  download_dir <- path.expand(download_dir)
  if (!dir.exists(download_dir)) 
    dir.create(download_dir, recursive = TRUE)
  fil <- file.path(download_dir, basename(url))
  if (use_cache) {
    if (file.exists(fil)) {
      message(glue::glue("Cached version of file found in {fil}\n Reading now..."))
    }
    else {
      message(glue::glue("Downloading {url} to {fil} now..."))
      res <- httr::GET(url, httr::write_disk(fil))
    }
  }
  else {
    if (file.exists(fil)) {
      message(glue::glue("Cached version of file found in {fil}."))
      message(glue::glue("Consider setting use_cache=TRUE to use previously downloaded files."))
      message(glue::glue("Overwriting {url} to {fil} now..."))
    }
    else {
      message(glue::glue("Downloading {url} to {fil} now..."))
    }
    res <- httr::GET(url, httr::write_disk(fil, overwrite = TRUE))
  }
  lehdr_df <- suppressMessages(readr::read_csv(fil, col_types = col_types))
  if (!use_cache) {
    if (unlink(fil)) {
      message(glue::glue("Could not remove temporary file {fil}."))
    }
    else {
      message(glue::glue("Removed {fil}."))
      if (length(list.files(download_dir)) == 0) {
        unlink(download_dir, recursive = TRUE)
      }
    }
  }
  lehdr_df <- lehdr_df %>% mutate(year = year, state = toupper(state))
  if (agg_geo_to != "block" && !is.null(agg_geo_to)) {
    id_len <- switch(agg_geo_to, bg = 12, tract = 11, county = 5, 
                     state = 2)
    lehdr_df <- lehdr:::aggregate_lodes_df(lehdr_df, id_len, agg_geo_to)
  }
  return(lehdr_df)
}


#convert data to wide format in preparation for creating dissimilarity matrix
prepData<-function(data){
  cat("Original number of rows ",length(data$Cnty_Res),"\n")
  # Get rid of incomplete records
  data<-data[complete.cases(data),]
  cat("Removed records with missing information. Now  ",length(data$Cnty_Res), " records\n")
  cat("Original number of counties ",length(unique(c(data$Cnty_Work,data$Cnty_Res))),"\n")
  cat("Original number of unique work counties",length(unique(data$Cnty_Work)),"\n")
  common_values<-intersect(data$Cnty_Res,data$Cnty_Work)
  data<-data[which(data$Cnty_Work %in% common_values),]
  data<-data[which(data$Cnty_Res %in% common_values),]
  cat("Removed records to make square matrix",length(common_values),"\n")
  # Convert to a wide matrix
  data.wide <- data %>%
    pivot_wider(names_from = Cnty_Work, values_from = FlowCount)
  #retain numeric version of work and residence counties
  wk<-colnames(data.wide)
  wk<-order(as.numeric(wk[-1]))
  res<-order(as.numeric(data.wide$Cnty_Res))
  #Fix column names
  data.wide$Cnty_Res<-paste0("Cnty_Res",data.wide$Cnty_Res)
  colnames(data.wide)[-1]<-paste0("Cnty_Wk",colnames(data.wide)[-1])
  # Now get the ordering correct to make it a square matrix
  cat("Reordering columns and rows to make symetric")
  tmp<-data.wide$Cnty_Res
  data.wide<-data.wide[,-1]
  data.wide<-data.wide[res,wk]
  data.wide<-cbind(tmp,data.wide)
  colnames(data.wide)[1]<-"Cnty_Res"
  return(data.wide)
}

#convert flow data to proportional flows configuration.
#takes in a square matrix with county of residence in rows and county of work
#in columns and uses (fij + fji)/min(total lf i, total lf i)
propFlow<-function(data){
  lf<-rowSums(data[,-1],na.rm=TRUE) #get resident labor force
  #convert NA to Zero
  data[is.na(data)]<-0
  dat<-data[,-1] #remove the county of residence fips code
  
  #Apply proportional flow calculation to every cell of matrix
  #This was the original slow method but it is easier to interpret than the 
  #vectorized version below.
#  dat2<-dat #create a version that goes unchanged during calculation
#  for(i in 1:nrow(dat)){
#    for(j in 1:ncol(dat)){
#      dat[i,j]<-(dat2[i,j]+dat2[j,i])/min(lf[i],lf[j])
#    }
#  }
  dat_t<-t(dat)
  dat_sum<-dat+dat_t
  # create a matrix of lf[i] values and a matrix of lf[j] values
  lf_i <- matrix(rep(lf, ncol(dat)), nrow=nrow(dat), byrow=TRUE)
  lf_j <- matrix(rep(lf, ncol(dat)), nrow=nrow(dat),byrow=FALSE)
  lf_min<-pmin(lf_i,lf_j)
  # divide df by the sum of lf[i] and lf[j]
  dat<-dat_sum/lf_min
  
  #Per original methodology, when connection is 1 or more convert to .999
  dat[dat>=.999]<-.999
  dat<-1-dat
  diag(dat)<-0
  dat<-as.dist(m=as.matrix(dat),diag = TRUE) #convert to distance matrix object
  return(dat)
}

#Build a crosswalk to give population proportions from sub-county units to allow data collected at county level
#to be output at planning region level and vice versa.
#Takes in data with either counties or planning units and outputs it to the opposite geography based on population proportions
convertCT<-function(popData,from='county',to='planning',transform=c('sum','mean')){
  #Build crosswalk file
  if(!file.exists("./Input Data/ct_cou_to_cosub_crosswalk.Rdata")){
    #download geographic crosswalk file
    download.file(url = "https://www2.census.gov/geo/docs/reference/ct_change/ct_cou_to_cousub_crosswalk.txt",
                  destfile="./Input Data/ct_cou_to_cosub_crosswalk.txt")
    cswlk<-read.delim("./Input Data/ct_cou_to_cosub_crosswalk.txt",sep="|")
    cswlk<-cswlk[1:174,c(1,2,3,4,5,6,7,8)]
    colnames(cswlk)<-c("STATEFP","OLD_COUNTYFP","OLD_COUNTY_NM","NEW_CNTYFP","NEW_CNTY_NM","COUSUBFP","OLD_COUSUB_GEO","NEW_COUSUB_GEO")
    #get Connecticut county subdivision population
    ct_sub<-get_acs(geography = "county subdivision",
                variables = "B01003_001",
                state = "CT",
                year=2022)
    ct_sub<-ct_sub[,c("GEOID","estimate")]
    ct_sub$GEOID<-as.numeric(ct_sub$GEOID)
    #join with crosswalk based on GEOID
    cswlk<-merge(x=cswlk,y=ct_sub,by.y="GEOID",by.x="NEW_COUSUB_GEO")   
    #aggregate to county | planning region pairs
    cswlk<-aggregate(cswlk$estimate,by=list(cswlk$OLD_COUNTYFP,cswlk$NEW_CNTYFP),FUN=sum)
    colnames(cswlk)<-c("OLD_COUNTYFP","NEW_CNTYFP","Population")
    cswlk$OLD_COUNTYFP<-cswlk$OLD_COUNTYFP+9000 #convert to more usual ST+CTY FIPS
    cswlk$NEW_CNTYFP<-cswlk$NEW_CNTYFP+9000
    #for each row in cswlk, divide population by sum of population for all OLD_COUNTYFP
    cswlk<-cswlk%>%
      group_by(OLD_COUNTYFP) %>%
      mutate(CTY_to_PR = Population/sum(Population)) 
    #now reverse it for conversions from planning region to county
    cswlk<-cswlk%>%
      group_by(NEW_CNTYFP) %>%
      mutate(PR_to_CTY = Population/sum(Population))
    #save crosswalk file
    save(cswlk,file="./Input Data/ct_cou_to_cosub_crosswalk.RData")
  }else{
    load("./Input Data/ct_cou_to_cosub_crosswalk.RData")
  }
  colnames(popData)<-c("FIPS","Value")
  if(from=='county' & to=='planning'){ #convert from county to planning
    #merge with crosswalk
    popData<-merge(popData,cswlk[,c("OLD_COUNTYFP","NEW_CNTYFP","CTY_to_PR")],by.x="FIPS",by.y="OLD_COUNTYFP")
    #planning region share
    popData$NewValue<-popData$Value*popData$CTY_to_PR
    if(transform=='sum'){
      retData<-aggregate(popData$NewValue,by=list(popData$NEW_CNTYFP),FUN=sum)
    }else if(transform=='mean'){
      retData<-aggregate(popData$NewValue,by=list(popData$NEW_CNTYFP),FUN=mean)
    }
  }else if(from=='planning' & to=='county'){ #convert from planning to county  
    #merge with crosswalk
    popData<-merge(popData,cswlk[,c("OLD_COUNTYFP","NEW_CNTYFP","{PR_to_CTY")],by.x="FIPS",by.y="NEW_CNTYFP")
    #county share
    popData$NewValue<-popData*popData$PR_to_CTY
    if(transform=='sum'){
      retData<-aggregate(popData$NewValue,by=list(popData$OLD_COUNTYFP),FUN=sum)
    }else if(transform=='mean'){
      retData<-aggregate(popData$NewValue,by=list(popData$OLD_COUNTYFP),FUN=mean)
    }
  }else{print("Invalid conversion requested")}
  colnames(retData)<-c("FIPS","Value")
  return(retData)
}

#
##
### Fit measures
##
#
#Takes in an sf object with a column of cluster IDs and returns the compactness of each cluster
checkCompactness<-function(df,czName){
    #union on czName 
    cz<-df %>% 
      group_by_at(czName) %>% 
      summarize(geometry = st_union(geometry))
    #calculate perimeter of each cluster
    perimeters<-st_length(st_cast(cz,"MULTILINESTRING"))
    #calculate area of each cluster
    areas<-st_area(cz)
    Ratio<-(4*pi*areas)/((perimeters)^2)
    return(mean(Ratio))
}
#Counts number of CBSAs that are split into multiple CZs
splitCBSAs<-function(counties.df,CZ_title,CBSA_title){
  counties.unique<-unique(counties.df[,c(CZ_title,CBSA_title)])
  counties.unique<-counties.unique[complete.cases(counties.unique),]
  split_list<-aggregate(counties.unique,by = list(counties.unique[,CBSA_title]),FUN = length)
  split_list<-split_list[split_list[,2]>1,"Group.1"]
  result<-unique(counties.unique[counties.unique[,CBSA_title%in%split_list,CBSA_title],])
  cat(length(result[,1])," CBSA's are split into multiple commute zones\n")
  return(result)
}
#Takes in an sf object with a column of cluster IDs and a column of FIPS codes or observation IDs
#returns if any non-island counties are in a cluster to which they are not contiguous
checkContiguity<-function(df,czName,fips){
  #Create list of queens first order contiguity for df
  outfile<-paste0("./Intermediate Data/",czName,"_nb.RData")
  if(!file.exists(outfile)){
    #bulky way to remove 'tibble' class from some sf objects that then can't have row.names
    df_geom<-st_geometry(df)
    df<-as.data.frame(df)
    st_geometry(df)<-df_geom
    region.ids<-as.vector(unlist(st_drop_geometry(df[,fips])))
    row.names(df)<-region.ids
    nb<-poly2nb(df,queen=TRUE)
    #add row.names to list objects
    for(i in 1:length(nb)){
      if(nb[i][[1]][1]!=0){
        nb[i][[1]]<-region.ids[nb[i][[1]]]
      }
    }
    #fix island counties
    if(length(row.names(df))%in% c(3143,3222)){
      print("assuming this is a 2010 or 2020 county file with 3143 or 3222 counties, adjusting neighbor weights accordingly. See function checkContiguity for details")
      #Make Alaska Islands neighbors
      nb[which(row.names(df) == "02016")][[1]]<-"02013"
      nb[which(row.names(df) == "02220")][[1]]<-c("02198","02105","02195")
      nb[which(row.names(df) == "02195")][[1]]<-c("02275", "02198", "02105","02220")
      #Make Hawaiian Islands neighbors
      nb[which(row.names(df) == "15001")][[1]]<-c(15003,15005,15007,15009)
      nb[which(row.names(df) == "15003")][[1]]<-c(15001,15005,15007,15009)
      nb[which(row.names(df) == "15005")][[1]]<-c(15001,15003,15007,15009)
      nb[which(row.names(df) == "15007")][[1]]<-c(15001,15003,15005,15009)
      nb[which(row.names(df) == "15009")][[1]]<-c(15001,15003,15005,15007)
      #Make Massachusettes Islands neighbors
      nb[which(row.names(df) == "25007")][[1]]<-c(25019,25001)
      nb[which(row.names(df) == "25019")][[1]]<-c(25007,25001)
      #Make New York Islands neighbors
      nb[which(row.names(df) == "36085")][[1]]<-c(36047,34025,34023,34039,34017,36061)
      #Connect Oregon and Washington Counties across a bridge
      nb[which(row.names(df) == "41007")][[1]]<-c(41067,53069,41057,41009,53049)
      #Make Washington Islands neighbors
      nb[which(row.names(df) == "53055")][[1]]<-c(53009,53029,53031,53057,53073)
      nb[which(row.names(df) == "53049")][[1]]<-c(53027,53041,53069,41007)
      #Make Puerto Rico Islands neighbors
      nb[which(row.names(df) == "72147")][[1]]<-c(72049,72053,72037,72103,72069)
      nb[which(row.names(df) == "72049")][[1]]<-c(72147,72053,72037,72103,72069)
    }
    #adjust structure of object for more accurate (name based rather than position based) access
    nbs<-nb[1:length(nb)]
    names(nbs)<-attributes(nb)$region.id
    nb<-nbs
    save(nb,file=outfile)
  }else{
    load(outfile)
  }
  #test if any counties are not in a cluster to which they are contiguous
  cty_cz_xwalk<-st_drop_geometry(df[,c(fips,czName)])
  cty_cz_xwalk$Contiguous<-NA
  cty_cz_freq<-as.data.frame(table(cty_cz_xwalk[,czName]))
  colnames(cty_cz_freq)<-c(czName,"Freq")
  cty_cz_xwalk<-merge(cty_cz_xwalk,cty_cz_freq,by=czName)
  for(i in 1:length(nb)){
    cty<-names(nb)[i] #county fips
    nbs<-nb[[cty]] #neighbor fips
    cty_cz<-cty_cz_xwalk[cty_cz_xwalk[,fips]==cty,czName] #county cz
    nbs_cz<-unique(cty_cz_xwalk[cty_cz_xwalk[,fips]%in%nbs,czName]) #neighbor czs
    if(
      cty_cz_xwalk[cty_cz_xwalk[,fips]==cty,"Freq"]==1){cty_cz_xwalk[cty_cz_xwalk[,fips]==cty,"Contiguous"]<-TRUE #singleton counties are always contiguous
    }else if(cty_cz %in% nbs_cz){
      cty_cz_xwalk[cty_cz_xwalk[,fips]==cty,"Contiguous"]<-TRUE #contiguous if a neighbor is in the same cz 
    }else{
      cty_cz_xwalk[cty_cz_xwalk[,fips]==cty,"Contiguous"]<-FALSE #not contiguous if no neighbors are in the same cz
    }
  }
  return(cty_cz_xwalk)
}
#return all of the descriptive variables we calculate for a given delineation
#Takes in an sf object with a column of cluster IDs and a column of Population, 
# and returns a vector of descriptive variables
descriptive.suite<-function(df,czName,popCol){
   df$area<-st_area(df)/1000000 #area in square km
   df2<-st_drop_geometry(df) #duplicate without geometry for simpler calls
   df2<-as_tibble(df2)
   result<-data.frame(Variable="Name",
                      Value=czName) #initialize result
   result<-rbind(result,c("No. of Counties in Delineation",
                           nrow(df2))) #All counties 
   result<-rbind(result,c("Number of Commuting Zones",
                          nrow(unique(df2[,czName]))))#No. of clusters
   result<-rbind(result,c("No. of Single County CZs",
                          sum(table(df2[,czName])==1)))#Number of single county clusters
   result<-rbind(result,c("No. of Counties in Largest CZ",
                          max(table(df2[,czName]))))#Number of counties in largest cluster
   non_contig<-checkContiguity(df,czName,fips = "GEOID")
   non_contig_czs<-length(unique(non_contig[non_contig$Contiguous==FALSE,czName]))
   result<-rbind(result,c("No. of Non-Contiguous CZs",non_contig_czs))#Number of unique non-contiguous clusters)))
   #Population in clusters
   suppressWarnings( czPop<-df2[,c(popCol,czName)]%>%group_by(pick(czName))%>%summarize(pop=sum(pick(popCol))))
   result<-rbind(result,c("Min Population in CZ",
                          min(czPop$pop)))#Min population in cluster
   result<-rbind(result,c("Average Population in CZ",
                          round(mean(czPop$pop),0)))#Average population in cluster
   result<-rbind(result,c("Max Population in CZ",
                          max(czPop$pop)))#Max population in cluster
   #Area of clusters
   suppressWarnings(area<-df2[,c(czName,"area")]%>%group_by(pick(czName))%>%summarize(area=sum(area)))
   result<-rbind(result,c("Min Area of CZ (sq.km)",
                          round(min(area$area),0)))#Min area of clusters
   result<-rbind(result,c("Average Area of CZ (sq.km)",
                          round(mean(area$area))))#Average area of clusters
   result<-rbind(result,c("Max Area of CZ (sq.km)",
                          round(max(area$area),0)))#Max area of clusters
   #Compactness of labor-sheds
   result<-rbind(result,c("Compactness of CZ's",
                          round(checkCompactness(df,czName),2)))#Compactness of labor-sheds
   return(result)
}

fit.suite<-function(df,flo,czName,czYear,cbsaName,popCol){
  #Fit suite, the workhorse of this analysis prepares all the fit statistics used in the paper
    #Output Structure:
    #When run this function takes in a delineation with wage data and cbsa definition, 
    #and decade appropriate flow matrix
    #Outputs 3 dataframes for counties, for labor markets, and for national totals/averages
    # Values output in each data frame:
    #                 County | Labor Market | Total     --Notes
    #Delineation        x           x           x
    #Year               x           x           x
    #FIPS               x
    #LM Code            x           x
    #No. Metros Split               x           x       --if LM contains split metro then 1 else 0
    #Is/Has Core        x           x                   --Core county or LM contains Core County
    #Share w/ Core                              x       --Calculate from share of above for LM
    #Share Work Core   x                               --Share of residents in a county working in a core county in LM *M1
    #Min. Core                      x           x       --Calculate from above
    #Share who work in core         x           x       --Calculate from above
    #Core I                                     x       --Calculate from above
    #Share From Core  X                                 --Share of workforce in a county who reside in a core county in LM
    #Min. From Core               X            X       --Calculate from above 
    #Share who live in core       X            X       --Calculate from above
    #From Core I                               X       --Calculate from above
    #Pair Wage Corr.    x                               --Average for a county across other counties in LM
    #Min. Wage Corr.                x           x       --Min. for LM or Min. Mean Min. for all LM
    #Mean Wage Corr.                x           x       --Mean for LM or Mean of Means
    #Mwan Wage Corr. no singles                 x       --Mean of Means for LM's excluding single county LM's
    #Wage Corr. I                               x       --Moran's I of LM mean
    #Contained          x                               --Pct. of Residents who work in LM
    #Min. Contain                   x           x       --Min of above
    #Mean Contain                   x           x       --Mean of Contained
    #Contain I                                  x       --Moran's I of Contained
    #Share of Pop. Contained                    x       --Share of total population that lives and works in same LM
    #Work-Contained     x                               --Pct. of Jobs filled by residents of LM
    #Mean Work Contain              x           x       --Mean of Work-Contained
##Setup###################################
 #Delineation        x           x           x
 #Year               x           x           x
 #FIPS               x
 # #LM Code            x           x
 # #########################################
    fl=flo #make a working copy
    dat<-as.data.frame(df)
    dat2<-st_drop_geometry(dat)
    fit.cols<-c("Delineation","Year","No. Metros Split","Share w/ Core",
                #"Min. Core",
                "Share who work in Core",#"Core Moran's I",
                #"Min. From Core",
                "Share who live in Core",#"From Core Moran's I",
                "Min. Wage Correlation","Mean Wage Correlation",
                "Mean Wage Corr no singles",#"Wage Correlation Moran's I",
                "Min. Contained","Mean Contained",#"Contained Moran's I",
                #"Mean Workforce Contained",
                "Share of Pop Contained")
    #National Table for Output
    result<-data.frame(Variables=fit.cols,Value=rep(NA,length(fit.cols)))
    result[result$Variables=="Delineation","Value"]<-czName #Delineation name
    result[result$Variables=="Year","Value"]<-czYear #Delineation year
    #LM Dataframe
    LM.result<-data.frame(Delineation=czName,Year=czYear,LM.Code=unique(dat2[,czName]),
                          No.Metros.Split=0,Has.Core=0,Min.Core=NA,Mean.Core=NA,Min.From.Core=NA,
                          Mean.From.Core=NA,Min.Wage.Corr=NA,Mean.Wage.Corr=NA,Min.Contain=NA,Mean.Contain=NA)
    LM.result<-LM.result[order(LM.result$LM.Code),]
    LM.result<-LM.result[!is.na(LM.result$LM.Code),] #NA gets created as a LM code for delineations that don't cover all counties
    #County Dataframe
    DAT.result<-data.frame(Delineation=czName,Year=czYear,FIPS=dat$FIPS,LM.Code=dat[,czName],Is.Core=0,Share.Work.Core=NA,
                           Share.From.Core=NA,Pair.Wage.Corr=NA,Contained=NA) #Note that DAT.result may have counties not in the current delineation if the delineation doesn't cover all counties
 #####################################################  
 #No. Metros Split               x           x       --if LM contains split metro then 1 else 0
 #####################################################
    splits<-unique(dat[,c(czName,cbsaName)])
    splits2<-as.data.frame(table(splits[,cbsaName]),stringsAsFactors = FALSE)
    splits2<-splits2[splits2$Freq>1,]
    splits<-merge(splits,splits2,by.x=cbsaName,by.y="Var1")
    result[result$Variables=="No. Metros Split","Value"]<-length(splits2[,1])
    LM.result[LM.result$LM.Code%in%splits[,czName],"No.Metros.Split"]<-1
 #####################################################    
 #Is/Has Core        x           x                   --Core county or LM contains Core County
 #Share w/ Core                              x       --Calculate from share of above for LM
 #####################################################
   DAT.result[DAT.result$FIPS%in%dat2[!is.na(dat2$Central) & dat2$Central_Outlying=="Central","FIPS"],"Is.Core"]<-1
   LM.result[LM.result$LM.Code%in%unique(DAT.result[DAT.result$Is.Core==1,"LM.Code"]),"Has.Core"]<-1
   result[result$Variables=="Share w/ Core","Value"]<-paste0((round(length(unique(DAT.result[DAT.result$Is.Core==1,"LM.Code"]))/length(LM.result[,1]),3)*100),"%")
 #####################################################  
   #Share Work Core   x                               --Share of residents in a county working in a core county in LM *M1
   #Min. Core                      x           x       --Calculate from above
   #Mean Core                      x           x       --Calculate from above
 #####################################################  
   #replace first column that contains residence FIPS with rownames and drop column for transformations
   rownames(fl)<-fl$Cnty_Res
   fl<-fl[,-1]
   
   DAT.result<-DAT.result[order(DAT.result$FIPS),]
   DAT.hold<-DAT.result[,c("FIPS","LM.Code","Is.Core")]  
   DAT.hold$Residents<-fl%>%rowSums(na.rm = TRUE)  
   
   flow.commute<-fl  
   diag(flow.commute)<-0  #remove those who work in county of residence to generate commuters
   DAT.hold$Commuters<-rowSums(x=flow.commute,na.rm = TRUE)
   rm(flow.commute)
   
   #For counties we need count of residents who work in a county that is both core and in LM
   #DAT.hold already has residents, so attach labor market to resident, transform, then subset to core counties, then replace county with LM
   #We will be transforming flow a lot so leave it alone and work with 'res'
   res<-fl
   res<-as.data.frame(t(res)) #transform so rows are work and columns are live.
   res[is.na(res)]<-0 #NA's not relevant. Signal zero flows between places
   res$FIPS<-DAT.hold$FIPS #attach the FIPS code to the work counties
   res<-res[res$FIPS%in%DAT.hold[DAT.hold$Is.Core==1,"FIPS"],] #subset to just work counties that are core counties
   res<-merge(res,DAT.hold[,c("FIPS","LM.Code")],by="FIPS",sort=FALSE) #attach the labor-shed value to the core counties
   res<-res[!is.na(res$LM.Code),] #destinations that aren't in a LM can't count positively and so are dropped to avoid problems
   res<-res[,colnames(res)!="FIPS"] #remove the FIPS column
   res<-aggregate(x = res,by = list(res[,"LM.Code"]),FUN = sum) #aggregate work counties to labor markets
   rownames(res)<-res$Group.1 #assign rownames based on arbitrary num or labor market code
   res<-res[,which(colnames(res)%in%c("Group.1","LM.Code")==FALSE)] #drop the non-flow columns
   res<-t(as.matrix(res)) #transform to live in rows, work in columns columns are now core counties only labeled by LM
   DAT.hold$Work.In.Core<-0
   for(i in 1:length(DAT.hold$FIPS)){
     work<-DAT.hold[i,"LM.Code"]
     tmp<-res[i,colnames(res)==work]
     DAT.hold[i,"Work.In.Core"]<-ifelse(length(tmp)==0,0,tmp)
   }
   DAT.hold$Share.Work.Core<-round(DAT.hold$Work.In.Core/DAT.hold$Residents,2)
   #Share Work Core
   DAT.result$Share.Work.Core<-DAT.hold$Share.Work.Core
   #Min. Work Core
   LM.result$Min.Core<-round(sapply(X = LM.result$LM.Code,FUN = function(x) min(DAT.hold[DAT.hold$LM.Code==x & !is.na(DAT.hold$LM.Code),"Share.Work.Core"],na.rm=TRUE)),2)
   #Mean Work Core
   LM.result$Mean.Core<-round(sapply(X = LM.result$LM.Code,FUN = function(x) mean(DAT.hold[DAT.hold$LM.Code==x & !is.na(DAT.hold$LM.Code),"Share.Work.Core"],na.rm=TRUE)),2)
   result[result$Variables=="Share who work in Core","Value"]<-paste0(round(mean(LM.result$Mean.Core),2)*100,"%")
   #result[result$Variables=="Min. Core","Value"]<-paste0(min(LM.result$Mean.Core)*100,"%")
   #Core I --Moran's I of Mean Core
   #       --hold for later so only need to join data to sf once
 #####################################################  
   #Share From Core   x                               --Share of workforce in a county living in a core county in LM *M1
   #Min. From Core                      x           x       --Calculate from above
   #Share who live in core              x           x       --Calculate from above
   #####################################################  
     #For all counties we need count of workforce who reside in a core county that in LM
   #We will be transforming flow a lot so leave it alone and work with 'res'
   res<-fl
   res[is.na(res)]<-0 #NA's not relevant. Signal zero flows between places
   
   res<-as.data.frame(t(res)) #transform so rows are work and columns are live.
   DAT.hold$Workforce<-rowSums(res) #Generate denominator for our measure--workforce in each county
   res<-fl #Start over as we will be working with where people live
   res[is.na(res)]<-0 #NA's not relevant. Signal zero flows between places
   res$FIPS<-DAT.hold$FIPS #attach FIPS
   res<-res[res$FIPS%in%DAT.hold[DAT.hold$Is.Core==1,"FIPS"],] #subset to just workers in core counties
   res<-merge(res,DAT.hold[,c("FIPS","LM.Code")],by="FIPS",sort=FALSE) #attach the labor-shed value to the core counties
   res<-res[!is.na(res$LM.Code),] #origins that aren't in a LM can't count positively and so are dropped to avoid problems
   res<-res[,colnames(res)!="FIPS"] #remove the FIPS column
   res<-aggregate(x = res,by = list(res[,"LM.Code"]),FUN = sum) #aggregate resident counties to labor markets
   rownames(res)<-res$Group.1 #assign rownames based on arbitrary num or labor market code
   res<-res[,which(colnames(res)%in%c("Group.1","LM.Code")==FALSE)] #drop the non-flow columns
   res<-t(as.matrix(res)) #transform to work in rows, live in columns columns are now core counties only labeled by LM
   DAT.hold$Live.In.Core<-0
   for(i in 1:length(DAT.hold$FIPS)){
     live<-DAT.hold[i,"LM.Code"]
     tmp<-res[i,colnames(res)==live]
     DAT.hold[i,"Live.In.Core"]<-ifelse(length(tmp)==0,0,tmp)
   }
   DAT.hold$Share.From.Core<-round(DAT.hold$Live.In.Core/DAT.hold$Workforce,2)
   #Share From Core
   DAT.result$Share.From.Core<-DAT.hold$Share.From.Core
   #Min. From Core
   LM.result$Min.From.Core<-round(sapply(X = LM.result$LM.Code,FUN = function(x) min(DAT.hold[DAT.hold$LM.Code==x & !is.na(DAT.hold$LM.Code),"Share.From.Core"],na.rm=TRUE)),2)
   #result[result$Variables=="Min. From Core","Value"]<-paste0(min(LM.result$Min.From.Core)*100,"%")
   #Mean From Core
   LM.result$Mean.From.Core<-round(sapply(X = LM.result$LM.Code,FUN = function(x) mean(DAT.hold[DAT.hold$LM.Code==x & !is.na(DAT.hold$LM.Code),"Share.From.Core"],na.rm=TRUE)),2)
   result[result$Variables=="Share who live in Core","Value"]<-paste0(round(mean(LM.result$Mean.From.Core),2)*100,"%")
   #From Core I --Moran's I of Mean From Core
   #       --hold for later so only need to join data to sf once
   
   #Core connect tells us what the sum of residents who work in core plus share of workforce coming from core is. This is the number Census uses to determine if a county is connected to a CBSA
   DAT.result$Core.Connect<-0
   DAT.result$Core.Connect<-apply(DAT.result[,c("Share.Work.Core","Share.From.Core")],MARGIN = 1,FUN = sum)
   #Min. Core Connect
   LM.result$Min.Core.Connect<-round(sapply(X = LM.result$LM.Code,FUN = function(x) min(DAT.result[DAT.result$LM.Code==x & !is.na(DAT.result$LM.Code),"Core.Connect"],na.rm=TRUE)),2)
   #Mean Core Connect
   LM.result$Mean.Core.Connect<-round(sapply(X = LM.result$LM.Code,FUN = function(x) mean(DAT.result[DAT.result$LM.Code==x & !is.na(DAT.result$LM.Code),"Core.Connect"],na.rm=TRUE)),2)
   #result[result$Variables=="Mean Core Connect","Value"]<-paste0(round(mean(LM.result$Mean.Core.Connect),2)*100,"%")
   #result[result$Variables=="Min. Core Connect","Value"]<-paste0(min(LM.result$Mean.Core.Connect)*100,"%")
   #Core Connect I --Moran's I of Mean Core Connect
   #       --hold for later so only need to join data to sf once
 ################################################  
 #Pair Wage Corr.    x                               --Average for a county across other counties in LM
 #Min. Wage Corr.                x           x       --Min. for LM or Min. Mean Min. for all LM
 #Mean Wage Corr.                x           x       --Mean for LM or Mean of Means
 ###############################################
 #Calculate average pairwise correlation across 6 years wage data for all counties
 #in the same LM
 #NOTE: Foote et al. paper uses population weighting for the averages. 
 #Correlation for a LM is 1/2N * SUM i in C SUM j in C  wij * pij
 
 # where wij = (reslfi = reslfj)   <<--equals sign may be a typo, + ?
 #             --------------
 #             2* SUM k in C reslfk
 #By my interpretation weighting is half of the contribution of counties i and j to the total residential labor force of C
 #pij in first term is the pairwise correlation across six years of data for two counties i and j
 #So we add all of the correlations for i with its peers j then divide the whole thing by 2N where N is number of counties in C--Divide by 2N is right because sum sum  over i and j is weighted average, so each i in N sums to 1 but aall of them get reported twice.  Mystery is how to deal with i cor i alway being 1. This just gets included by assumption, otherwise the weighted average component doesn't work.
  #a=c(1,2,3)
  #b=matrix(rep(a,length(a)),nrow=length(a))
  #b+t(b)
   for(i in 1:length(LM.result$LM.Code)){
     lm.code<-LM.result[i,"LM.Code"]
     dat.no.na<-dat2[!is.na(dat2[,czName]),]
     rownames(dat.no.na)<-dat.no.na[,"FIPS"]
     wages<-dat.no.na[dat.no.na[,czName]==lm.code,grepl( "Wage" , names(dat.no.na) )]
     out<-cor(t(wages),use="pairwise.complete.obs")
   
     #Generate population weighting matrix
     pop<-dat.no.na[dat.no.na[,czName]==lm.code,grepl("Pop",names(dat.no.na))]
     pop.mat<-matrix(rep(pop,length(pop)),nrow=length(pop))
     total.pop<-sum(pop,na.rm=TRUE)
     pop.mat<-pop.mat+t(pop.mat)
     pop.mat<-pop.mat/(2*total.pop)
     out.mat<-out*pop.mat
     answer<-apply(out,MAR=1,mean,na.rm=TRUE)
     if(length(answer)==1){ #perfect correlation if LM only has one county in it
       DAT.result[DAT.result$FIPS%in%names(answer),"Pair.Wage.Corr"]<-1
     }else{
       DAT.result[DAT.result$FIPS%in%names(answer),"Pair.Wage.Corr"]<-answer
     }
     #Add in code for LM.result here as well
     LM.result[i,"Mean.Wage.Corr"]<- sum(out.mat,na.rm=TRUE)/(2*length(pop))
   }  
   DAT.result[is.nan(DAT.result$Pair.Wage.Corr),"Pair.Wage.Corr"]<-NA
   #Min. Wage Corr.
   LM.result$Min.Wage.Corr<-round(sapply(X = LM.result$LM.Code,FUN = function(x) min(DAT.result[DAT.result$LM.Code==x & !is.na(DAT.result$LM.Code),"Pair.Wage.Corr"],na.rm=TRUE)),2)
   LM.result[is.infinite(LM.result$Min.Wage.Corr),"Min.Wage.Corr"]<-NA #Assign NA when missing values make correlation impossible to assess
   #Mean Wage Corr.
   LM.result$Mean.Wage.Corr<-round(sapply(X = LM.result$LM.Code,FUN = function(x) mean(DAT.result[DAT.result$LM.Code==x & !is.na(DAT.result$LM.Code),"Pair.Wage.Corr"],na.rm=TRUE)),2)
   result[result$Variables=="Mean Wage Correlation","Value"]<-round(mean(LM.result$Mean.Wage.Corr),2)
   result[result$Variables=="Mean Wage Corr no singles","Value"]<-round(mean(LM.result[LM.result$Mean.Wage.Corr !=1,"Mean.Wage.Corr"]),2)
   result[result$Variables=="Min. Wage Correlation","Value"]<-min(LM.result$Mean.Wage.Corr,na.rm=TRUE)
   
   #Wage Corr. I 
   #       --hold for later so only need to join data to sf once
 ########################################################
 #Contained          x                               --Pct. of Residents who work in LM
 #Min. Contain                   x           x       --Min of above
 #Mean Contain                   x           x       --Mean of Contained
 #Share of Pop. Contained                    x       --Share of total population that lives and works in same LM
 #Work-Contained     x                               --Pct. of Jobs filled by residents of LM
 #Mean Work Contain              x           x       --Mean of Work-Contained
 ########################################################  
   #For counties we need count of residents who work in their labor market
   #DAT.hold already has residents, so transform, attach labor market to work, transform back, then subset to only workers in same LM
   res<-fl
   res[is.na(res)]<-0 #NA's not relevant. Signal zero flows between places
   res<-as.data.frame(t(res)) #transform so rows are work and columns are live.
   res$FIPS<-DAT.hold$FIPS #attach FIPS
   res<-merge(res,DAT.hold[,c("FIPS","LM.Code")],by="FIPS",sort=FALSE) #attach the labor-shed value
   res<-res[,colnames(res)!="FIPS"] #remove the FIPS column
   res<-aggregate(x = res,by = list(res[,"LM.Code"]),FUN = sum) #aggregate work counties to labor markets
   rownames(res)<-res$Group.1 #assign rownames based on arbitrary num or labor market code
   res<-res[,which(colnames(res)%in%c("Group.1","LM.Code")==FALSE)] #drop the non-flow columns
   res<-t(as.matrix(res)) #transform to live in rows, work in columns columns are now core counties only labeled by LM
   DAT.hold$Work.In.LM<-0
   for(i in 1:length(DAT.hold$FIPS)){
     work<-DAT.hold[i,"LM.Code"]
     tmp<-res[i,colnames(res)==work]
     DAT.hold[i,"Work.In.LM"]<-ifelse(length(tmp)==0,0,tmp)
   }
   DAT.hold$Share.Work.LM<-round(DAT.hold$Work.In.LM/DAT.hold$Residents,2)
   #Share Contained 
   DAT.result$Contained<-DAT.hold$Share.Work.LM
   #Min. Contained
   LM.result$Min.Contain<-round(sapply(X = LM.result$LM.Code,FUN = function(x) min(DAT.result[DAT.result$LM.Code==x & !is.na(DAT.result$LM.Code),"Contained"],na.rm=TRUE)),2)
   
   #Mean Contained
   LM.result$Mean.Contain<-round(sapply(X = LM.result$LM.Code,FUN = function(x) mean(DAT.result[DAT.result$LM.Code==x & !is.na(DAT.result$LM.Code),"Contained"])),2)
   result[result$Variables=="Mean Contained","Value"]<-paste0(round(mean(LM.result$Mean.Contain),2)*100,"%")
   result[result$Variables=="Min. Contained","Value"]<-paste0(min(LM.result$Mean.Contain)*100,"%")
   #Core I --Moran's I of Mean Core
   #       --hold for later so only need to join data to sf once
   result[result$Variables=="Share of Pop Contained","Value"]<-paste0(round(sum(DAT.hold$Work.In.LM,na.rm=TRUE)/sum(DAT.hold$Residents),2)*100,"%")
   
   #Work-Contained -count of jobs filled by residents of labor market
   res<-fl
   res[is.na(res)]<-0 #NA's not relevant. Signal zero flows between places
   employment<-colSums(res)
   res$FIPS<-DAT.hold$FIPS #attach FIPS
   res<-merge(res,DAT.hold[,c("FIPS","LM.Code")],by="FIPS",sort=FALSE) #attach the labor-shed value
   res<-res[,colnames(res)!="FIPS"] #remove the FIPS column
   res<-aggregate(x = res,by = list(res[,"LM.Code"]),FUN = sum) #aggregate counties of residence to labor markets
   rownames(res)<-res$Group.1 #assign rownames based on arbitrary num or labor market code
   res<-res[,which(colnames(res)%in%c("Group.1","LM.Code")==FALSE)] #drop the non-flow columns
   res<-t(as.matrix(res)) #transform to live in columns, work in rows columns are now labeled by LM
   rownames(res)<-DAT.hold$FIPS
   DAT.hold$Live.In.LM<-0
   DAT.hold$Share.Live.LM<-0
   for(i in 1:length(DAT.hold$FIPS)){
     reside<-DAT.hold[i,"LM.Code"]
     tmp<-res[i,colnames(res)==reside]
     DAT.hold[i,"Live.In.LM"]<-ifelse(length(tmp)==0,0,tmp)
     DAT.hold[i,"Share.Live.LM"]<-DAT.hold[i,"Live.In.LM"]/employment[i]
   }
   #Share Work-Contained 
   DAT.result$Work.Contained<-DAT.hold$Share.Live.LM
   
   #Mean Work Contained
   LM.result$Mean.Work.Contain<-round(sapply(X = LM.result$LM.Code,FUN = function(x) mean(DAT.result[DAT.result$LM.Code==x & !is.na(DAT.result$LM.Code),"Work.Contained"])),2)
   #result[result$Variables=="Mean Workforce Contained","Value"]<-paste0(round(mean(LM.result$Mean.Work.Contain),2)*100,"%")
   ################################################
   #Core I                                     x       --Moran's I of LM Mean
   #Core Connect I                             x       --Moran's I of LM Mean Connect
   #Wage Corr. I                               x       --Moran's I of LM Mean
   #Contain I                                  x       --Moran's I of LM Mean
   
   #Create sf for LM for use in Moran's I
   #LM.sf<- df %>% group_by(pick(czName)) %>% summarize()
   #Add labor-shed fit statistic to labor-shed polygon
   #LM.sf<-merge(LM.sf,LM.result,by.x=czName, by.y="LM.Code",sort=FALSE)
   #Create k=5 contiguity matrix, suppresses warning about st_centroid assuming constant attributes
   #suppressWarnings(listW<-nb2listw(knn2nb(knearneigh(st_centroid(LM.sf), k=5)), style="W"))
   #Moran's I Core
   #m.I<-moran.mc(x = LM.sf$Mean.Core,listw = listW,nsim=999,na.action=na.omit)
   #result[result$Variables=="Core Moran's I","Value"]<-round(m.I$statistic,2)
   #Moran's I Core Connect
   #m.I<-moran.mc(x = LM.sf$Mean.Core.Connect,listw = listW,nsim=999,na.action=na.omit)
   #result[result$Variables=="From Core Moran's I","Value"]<-round(m.I$statistic,2)
   #Moran's I Wage Corr. I
   #m.I<-moran.mc(x = LM.sf$Mean.Wage.Corr,listw = listW,nsim=999,na.action =na.omit)
   #result[result$Variables=="Wage Correlation Moran's I","Value"]<-round(m.I$statistic,2)
   #Moran's I Contain I
   #m.I<-moran.mc(x = LM.sf$Mean.Contain,listw = listW,nsim=999,na.action=na.omit)
   #result[result$Variables=="Contained Moran's I","Value"]<-round(m.I$statistic,2)
   return(list(County=DAT.result,LM=LM.result,National=result))
}

#Function to calculate Jacand similarity for an arbitrary cut point and return the mean and standard deviation of the scores
cutTest<-function(h){
  output<-cutree(hdata20,h =  h) #cut point
  outTest<-data.frame(
    FIPS=as.numeric(str_sub(names(output),start=8)), #Need to extract FIPS from names of output
    CZ=output,stringsAsFactors = FALSE) #create dataframe with county names and cluster assignments
  ctyTest<-cty%>%left_join(outTest,by="FIPS") #join cluster assignments to county data
  #calculate jacand similarity
  ctyTest$Score<-NA
  for(i in 1:nrow(ctyTest)){
    ten<-ctyTest[which(ctyTest$OUT10==ctyTest[i,"OUT10"]),"FIPS"]
    twenty<-ctyTest[which(ctyTest$CZ==ctyTest[i,"CZ"]),"FIPS"]
    both<-length(intersect(ten,twenty))
    all<-length(unique(c(ten,twenty)))
    ctyTest[i,"Score"]<-both/all
  }
  return(list(mean=round(mean(ctyTest$Score),3),sd=round(sd(ctyTest$Score),3)))
}

#Function to calculate distance between non-contiguous counties and their CZs
getDistanceOfNonContiguous<-function(df,czName,fips){
  contig<-checkContiguity(df,czName,fips)
  df_no_geom<-st_drop_geometry(df) #speed up subsetting by dropping geometry
  #create sf object of contiguous counties only
  cc<-df[df_no_geom[,fips] %in% contig[contig$Contiguous==TRUE,fips],]
  #create sf object of non-contiguous counties only
  nc<-df[df_no_geom[,fips] %in% contig[contig$Contiguous==FALSE,fips],]
  #Group by commuting zone but excluding the non-contiguous counties
  cc<-cc %>% group_by_at(czName) %>% summarize()
  #get distance between polygons in non-contiguous group and their nearest co-commuting zone border
  nc$dist<-NA
  nc_no_sf<-st_drop_geometry(nc) #get rid of geometry for ability to subset without dragging geometry around
  cc_no_sf<-st_drop_geometry(cc) #get rid of geometry for ability to subset without dragging geometry around
  for(i in 1:nrow(nc)){
    #first test to see if there is a match in cc at all, if not we will look in nc for the possibility of a multi- county CZ that isn't contiguous
    if(nrow(cc_no_sf[cc_no_sf[,czName]==nc_no_sf[i,czName],])==0){
    #look for match in nc20
      in_nc<-nc_no_sf[nc_no_sf[,czName]==nc_no_sf[i,czName],]
    #drop self from in_nc
      in_nc<-in_nc[in_nc[,fips]!=nc_no_sf[i,fips],]
    #calculate distance to remaining county
      nc[i,"dist"]<-st_distance(nc[i,],nc[nc_no_sf[,fips]==in_nc[1,fips],])
      nc_lines<-st_nearest_points(nc[i,],nc[nc_no_sf[,fips]==in_nc[1,fips],])
    }else {
    #expected case where there is a group of counties in a CZ and we want the distance to the nearest point in the CZ
      nc[i,"dist"]<-st_distance(nc[i,],cc[cc_no_sf[,czName]==nc_no_sf[i,czName],])
      nc_lines<-st_nearest_points(nc[i,],cc[cc_no_sf[,czName]==nc_no_sf[i,czName],])
    }
    if(i==1){
      out_lines<-nc_lines
    }else{
      out_lines<-c(out_lines,nc_lines)
    }
  }
  return(list(nc,out_lines))
}

#examine connections for individual problem counties
examine_cty<-function(fips,czName="CZ20",df=county20){
  df2<-st_drop_geometry(df)
  nc2<-st_drop_geometry(nc_data)
  rec<-nc2[nc2$FIPS==as.numeric(fips),]
  print(rec) #this is the problem record
  cz=st_drop_geometry(rec[,czName])
  cz_members<-df2[df2[,czName]==cz,] #These are the other members of the problem cz
  #get the flows for all members of the problem cz
  flows2<-data.frame(flows)
  fl<-data.frame(t(flows2[flows2$Cnty_Res %in% c(paste0("Cnty_Res",fips),paste0("Cnty_Res",cz_members$GEOID)),]))
  colnames(fl)<-fl[1,]
  fl<-fl[-1,]
  fl<-as.data.frame(apply(fl,MARGIN = 2,function(x) as.numeric(x)))
  nm_col<-length(colnames(fl))
  fl$Cnty_Wk<-colnames(flows2)[-1]
  fl<-fl[rowSums(fl[,1:nm_col],na.rm = TRUE)>0,]
  nm_col2<-which(colnames(fl)==paste0("Cnty_Res",fips))
  fl<-fl[order(fl[,nm_col2],decreasing = TRUE),]
  print(fl)
}
#Reusable table formatting options
default.table.format<-function(df){
  gt(df)%>%
    fmt_number(
    decimals = 1,
    use_seps = TRUE,
    sep_mark = ",",
    drop_trailing_dec_mark = TRUE,
    drop_trailing_zeros = TRUE
  )%>%
  cols_label(
    Name = md("Measure"),
    CZ20 = md("2020"),
    OUT10 = md("2010")
  ) %>%
  #bold header
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_labels()
  )%>%
  #right justify
  tab_style(
    style = cell_text(align = "right"),
    locations = cells_body(
      columns = c(CZ20,OUT10)
    )
  )%>%
  #add lines
  tab_options(
    table.font.size = 12,
    table.border.top.width = px(1),
    table.border.bottom.width = px(1),
    table.border.left.width = px(1),
    table.border.right.width = px(1),
    table.border.top.color = "black",
    table.border.bottom.color = "black",
    table.border.left.color = "black",
    table.border.right.color = "black",
    row_group.as_column=TRUE
  )
}
#Reusable table formatting options
default.table.format2<-function(df,grp=FALSE){
    gt(df,groupname_col = grp)%>%
    fmt_number(
      decimals = 1,
      use_seps = TRUE,
      sep_mark = ",",
      drop_trailing_dec_mark = TRUE,
      drop_trailing_zeros = TRUE
    )%>%
    cols_label(
      Name = md("Measure"),
      CZ20 = md("2020"),
      OUT10 = md("2010")
    ) %>%
    #bold header
    tab_style(
      style = cell_text(weight = "bold"),
      locations = cells_column_labels()
    )%>%
    #right justify
    tab_style(
      style = cell_text(align = "right"),
      locations = cells_body(
        columns = c(CZ20,OUT10)
      )
    )%>%
    #add lines
    tab_options(
      table.font.size = 12,
      table.border.top.width = px(1),
      table.border.bottom.width = px(1),
      table.border.left.width = px(1),
      table.border.right.width = px(1),
      table.border.top.color = "black",
      table.border.bottom.color = "black",
      table.border.left.color = "black",
      table.border.right.color = "black",
      row_group.as_column=TRUE
    )
}
default.table.format3<-function(df){
  df<-df[order(df$GEOID),]
  gt(df)%>%
    fmt_number(
      decimals = 1,
      use_seps = TRUE,
      sep_mark = ",",
      drop_trailing_dec_mark = TRUE,
      drop_trailing_zeros = TRUE
    )%>%
    cols_label(
      GEOID = md("FIPS"),
      County.Name = md("County Name"),
      State.Name = md("State Name"),
      CZ20 = md("CZ ID"),
      New.CZ20 = md("New CZ ID"),
      Notes=md("Notes")
    ) %>%
    #bold header
    tab_style(
      style = cell_text(weight = "bold"),
      locations = cells_column_labels()
    )%>%
        #add lines
    tab_options(
      table.font.size = 12,
      table.border.top.width = px(1),
      table.border.bottom.width = px(1),
      table.border.left.width = px(1),
      table.border.right.width = px(1),
      table.border.top.color = "black",
      table.border.bottom.color = "black",
      table.border.left.color = "black",
      table.border.right.color = "black",
      row_group.as_column=TRUE
    )
}