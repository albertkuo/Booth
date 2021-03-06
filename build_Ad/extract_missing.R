# extract_missing.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: July 5, 2017
#
# This is an R script that finds and extracts
# non matching Network TV occurrences.

library(foreach)
library(doParallel)
library(data.table)
library(lubridate)
library(chron)

registerDoParallel(cores = NULL)

# Time Zones
time_zones = fread("./build_Ad/data/DMA_and_recording_method.csv")
time_intervals = fread("./build_Ad/data/Time_Interval_spot.csv")

# Path names for traversal through year and month directories ----------
source_dir = "/nielsen_raw/Ad_Intel"
output_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel/missing_network"

dirs = list.dirs(full.names=F,recursive=F,path=source_dir)
years = as.character(c(2010:2014))
months = c("01","02","03","04","05","06","07","08","09","10","11","12")
monthpath.strings = vector("list",length(years)*12)
for(i in 1:length(years)){
  yeardir = dirs[grep(paste0(years[i],".*?extracted"),dirs)]
  monthdirs = paste0(years[i],months)
  for(k in 1:length(monthdirs)){
    monthpath.strings[(i-1)*12+k] = paste(source_dir,yeardir,monthdirs[k],sep="/")
  }
}

# Helper functions ----------
media_id_to_desc = function(DT){
  DT[MediaTypeID==1, MediaTypeDesc:="Network TV"]
  DT[MediaTypeID==4, MediaTypeDesc:="Network TV"] # Spanish Language
  DT[MediaTypeID==13, MediaTypeDesc:="Network Clearance Spot TV"]
  DT[MediaTypeID==5, MediaTypeDesc:="Spot TV"]
  DT[MediaTypeID==14, MediaTypeDesc:="Syndicated Clearance"]
  DT[MediaTypeID==3, MediaTypeDesc:="Syndicated TV"]
}

convert_timezone = data.table(time_zones_1=c("ETZ","CTZ","MTZ","PTZ","YTZ","HTZ"),
                              time_zones_2=c("US/Eastern","US/Central",
                                             "US/Mountain","US/Pacific",
                                             "US/Alaska","US/Hawaii"))
get_time_interval = function(AdTime){
  sapply(AdTime, function(x){time_intervals[(times(start_time) <= times(x)) & 
                                            (times(end_time) >= times(x))]$time_interval_number[1]})
}

# Variables and Parameters  ----------
counter = 1
provider_vec = c("ABC", "NBC", "ION", "FOX", "CW", "CBS", "MT3", "TEL", "UNI", "UMA", "AZA") 
provider_code_vec = c("A","N","X","F","Y","C", "E", "T", "U", "V", "Z") 
time_window = 6 # 6 seconds

# Main section  ----------
foreach(i = 1:length(monthpath.strings)) %dopar% { 
#for(i in 1:length(monthpath.strings)){
  monthpath.string = monthpath.strings[[i]]
  print(basename(monthpath.string))
  missing_DT_list = list()
  
  # Read files
  dis_filename = list.files(pattern="RDC.*?DIS",path=monthpath.string)
  dis <- fread(paste(monthpath.string,dis_filename,dis_filename,sep="/"),showProgress=F)
  nat_filename = list.files(pattern="OCC.*?BN",path=monthpath.string)
  national <- fread(paste(monthpath.string,nat_filename,nat_filename,sep="/"),showProgress=F)
  nat_filename = list.files(pattern="OCC.*?EN",path=monthpath.string)
  national_2 <- fread(paste(monthpath.string,nat_filename,nat_filename,sep="/"),showProgress=F)
  setnames(national_2, "PrimeBrandCode", "PrimBrandCode")
  national = rbind(national, national_2, fill=T)
  clear_filename = list.files(pattern="OCC.*?NC",path=monthpath.string)
  clear <- fread(paste(monthpath.string,clear_filename,clear_filename,sep="/"),showProgress=F)
  spot_filename = list.files(pattern="OCC.*?SP",path=monthpath.string)
  spot <- fread(paste(monthpath.string,spot_filename,spot_filename,sep="/"),showProgress=F)

  # Subset Provider (e.g. ABC)
  for(j in 1:length(provider_vec)){
    provider = provider_vec[j]
    provider_code = provider_code_vec[j]
    marketcodes = dis[Affiliation==provider]$MarketCode
    
    # Subset Market (e.g. New York)
    for(marketcode in marketcodes){
      tz = time_zones[MarketCode==marketcode]$time_zone
      tz = convert_timezone[time_zones_1==tz]$time_zones_2
      dc = dis[MarketCode==marketcode & Affiliation==provider]$DistributorCode
      network = national[DistributorCode==provider_code] 
      network[, time:=mdy_hms(paste(AdDate, AdTime), tz="US/Eastern")]
      network[, time:=with_tz(time, tz=tz)]
      
      # Account for unpredictable time delays in west coast and other time zones
      # by replicating and aggregating by ID later
      if(tz!="US/Central" & tz!="US/Eastern"){
        network[, id:=.I]
        network = network[rep(1:nrow(network), each=7)]
        network[, delay:=rep(-3:3, nrow(network)/7)]
        network[, time:=time+hours(delay)]
      }
      
      # Create data table of Network, Network Clearance, Spot, and Syndicated Clearance
      ny_clear = clear[DistributorCode %in% dc] #5015 = NY ABC, 5012 = NY CBS, 5060 = Chicago ABC, 5050=Chicago CBS
      ny_spot = spot[DistributorCode %in% dc]
      ny = rbind(ny_clear, ny_spot, fill=T)
      ny[, time:=mdy_hms(paste(AdDate, AdTime), tz=tz)]
      ny = rbind(network, ny, fill=T)
      media_id_to_desc(ny)
      ny[, AdDate:=as.Date(AdDate, format="%m/%d/%Y")]
      ny = ny[order(time)]
      
      # Calculate time delays and matches -> non_missing Network TV occurrences
      ny[, difftime_after:=diff(time)]
      ny[, difftime_before:=shift(difftime_after,1)]
      ny[, match_clearance:=(difftime_after<=time_window & 
                               shift(MediaTypeDesc, 1, type="lead")=="Network Clearance Spot TV" &
                               PrimBrandCode==shift(PrimBrandCode, 1, type="lead")) 
         | (difftime_before<=time_window & 
              shift(MediaTypeDesc, 1, type="lag")=="Network Clearance Spot TV" &
              PrimBrandCode==shift(PrimBrandCode, 1, type="lag"))]
      ny[, replaced_by_spot:=(difftime_after<=time_window & 
                                shift(MediaTypeDesc, 1, type="lead")=="Spot TV")
         | (difftime_before<=time_window &
              shift(MediaTypeDesc, 1, type="lag")=="Spot TV")]
      # explained_missing: Spot TV overran, start/end of commercial break, between 2am-5am
      ny[, explained_missing:=(difftime_before<=shift(Duration, 1, type="lag") &
                                 shift(MediaTypeDesc, 1, type="lag")=="Spot TV")
         | (difftime_before>90) | (difftime_after>90) 
         | (2<=hour(time) & hour(time)<5)]
      ny[, non_missing:=(match_clearance | replaced_by_spot | explained_missing)]
      ny[MediaTypeDesc!="Network TV", `:=`(match_clearance=NA, replaced_by_spot=NA, 
                                           explained_missing=NA, non_missing=NA)]
      
      # Calculate block_missing Network TV occurrences
      ny[, match_network:=(difftime_after<=time_window & 
                             shift(MediaTypeDesc, 1, type="lead")=="Network TV" &
                             PrimBrandCode==shift(PrimBrandCode, 1, type="lead")) 
         | (difftime_before<=time_window & 
              shift(MediaTypeDesc, 1, type="lag")=="Network TV" &
              PrimBrandCode==shift(PrimBrandCode, 1, type="lag"))]
      ny[MediaTypeDesc!="Network Clearance Spot TV", match_network:=NA]
      # This is not perfect (less conservative)
      unmatched_clearance_brands = ny[match_network==FALSE, PrimBrandCode]
      ny[, match_network:=NULL]
      
      # Borrow values from Network Clearance/Spot for later use
      di = unlist(ny[MediaTypeDesc=="Network Clearance Spot TV" |
                       MediaTypeDesc=="Spot TV"]$DistributorID)[[1]]
      pym = unlist(ny[MediaTypeDesc=="Network Clearance Spot TV" |
                        MediaTypeDesc=="Spot TV"]$PeriodYearMonth)[[1]]
      ny = ny[MediaTypeDesc=="Network TV"]
      
      # Collapse by id for west coast and other time zones
      if(tz!="US/Central" & tz!="US/Eastern"){
        ny = ny[, .(non_missing=any(non_missing)),
                by=.(id, AdDate, AdTime, MediaTypeID, MediaTypeDesc,
                     PrimBrandCode, ScndBrandCode, TerBrandCode,
                     Duration, Spend)]
        ny[, id:=NULL]
      }
      ny[, block_missing:=(!shift(non_missing, 1, type="lag") & 
                             !shift(non_missing, 1, type="lead") &
                             !non_missing)]
      ny[, block_missing:=(((shift(block_missing, 1, type="lag") | 
                               shift(block_missing, 1, type="lead")) &
                              !non_missing) | block_missing)]
      ny[, block_missing:=(block_missing & !(PrimBrandCode %in% 
                                               unmatched_clearance_brands))]
      ny[is.na(block_missing), block_missing:=F]
      ny = ny[non_missing==F]
      ny = ny[, .(AdDate, AdTime, MediaTypeID, MediaTypeDesc, 
                  PrimBrandCode, ScndBrandCode, TerBrandCode,
                  Duration, Spend, block_missing)]
      ny[, `:=`(MarketCode = marketcode,
                DistributorID = di,
                PeriodYearMonth = pym,
                DayOfWeek = wday(AdDate),
                TimeIntervalNumber = as.integer(get_time_interval(AdTime)))]
      missing_DT_list[[counter]] = copy(ny)
      counter = counter + 1
    }
  }
  missing_DT_all = rbindlist(missing_DT_list, fill=T)
  print(paste("Saving",basename(monthpath.string)))
  saveRDS(missing_DT_all, paste0(output_dir,"/",basename(monthpath.string),".rds"))
}