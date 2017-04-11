# Checking all TV occurrences across time series for time gaps
# and matching between Network TV and Network Clearance
# Run on grid

library(data.table)
library(lubridate)

# Time Zones
time_zones = fread("~/DMA_and_recording_method.csv")

# Path names for traversal through year and month directories ----------
source_dir = "/nielsen_raw/Ad_Intel"
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
  DT[MediaTypeID==13, MediaTypeDesc:="Network Clearance Spot TV"]
  DT[MediaTypeID==5, MediaTypeDesc:="Spot TV"]
  DT[MediaTypeID==14, MediaTypeDesc:="Syndicated Clearance"]
}

convert_timezone = data.table(time_zones_1=c("ETZ","CTZ","MTZ","PTZ","YTZ","HTZ"),
                              time_zones_2=c("US/Eastern","US/Central",
                                             "US/Mountain","US/Pacific",
                                             "US/Alaska","US/Hawaii"))

# Variables and Parameters  ----------
missing_DT_list = list()
counter = 1
provider = "ABC"
provider_code = "A" #A = ABC, C=CBS

for(i in 1:length(monthpath.strings)){
#for(i in 1:2){
  print(i)
  monthpath.string = monthpath.strings[[i]]
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
  synd_clear_filename = list.files(pattern="OCC.*?SC",path=monthpath.string)
  synd_clear <- fread(paste(monthpath.string,synd_clear_filename,synd_clear_filename,sep="/"),showProgress=F)
  
  marketcodes = dis[Affiliation==provider]$MarketCode
  for(marketcode in marketcodes){
    print(marketcode)
    tz = time_zones[MarketCode==marketcode]$time_zone
    tz = convert_timezone[time_zones_1==tz]$time_zones_2
    dc = dis[MarketCode==marketcode & Affiliation==provider]$DistributorCode
    network = national[DistributorCode==provider_code] 
    network[, time:=mdy_hms(paste(AdDate, AdTime), tz="US/Eastern")]
    network[, time:=with_tz(time, tz=tz)]
    
    # Account for unpredictable time delays in west coast and other time zones
    # Replicate and aggregate by ID later
    if(tz!="US/Central" & tz!="US/Eastern"){
      network[, id:=.I]
      network = network[rep(1:nrow(network), each=7)]
      network[, delay:=rep(-3:3, nrow(network)/7)]
      network[, time:=time+hours(delay)]
    }
    
    # Create data table of Network, Network Clearance, Spot, and Syndicated Clearance
    #print("Creating data table...")
    ny_clear = clear[DistributorCode %in% dc] #5015 = NY ABC, 5012 = NY CBS, 5060 = Chicago ABC, 5050=Chicago CBS
    ny_spot = spot[DistributorCode %in% dc]
    ny_synd_clear = synd_clear[DistributorCode %in% dc]
    ny = rbind(ny_clear, ny_spot, ny_synd_clear, fill=T)
    ny[, time:=mdy_hms(paste(AdDate, AdTime), tz=tz)]
    ny = rbind(network, ny, fill=T)
    media_id_to_desc(ny)
    ny[, AdDate:=as.Date(AdDate, format="%m/%d/%Y")]
    ny = ny[order(time)]
    
    # Calculate time delays and matches -> non_missing Network TV occurrences
    #print("Calculating delays and matches...")
    ny[, difftime_after:=diff(time)]
    ny[, difftime_before:=shift(difftime_after,1)]
    ny[, time_gap:=difftime_after>15*60]
    time_window = 6
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
    ny[, has_match:=(match_clearance | replaced_by_spot)]
    # Possible explanations: Spot TV overran, start/end of commercial break, between 2am-5am
    ny[, explained_missing:=(difftime_before<=shift(Duration, 1, type="lag") &
                               shift(MediaTypeDesc, 1, type="lag")=="Spot TV")
       | (difftime_before>90) | (difftime_after>90) 
       | (2<=hour(time) & hour(time)<=5)]
    ny[, non_missing:=(has_match | explained_missing)]
    ny[MediaTypeDesc!="Network TV", `:=`(match_clearance=NA, replaced_by_spot=NA, 
                                         has_match=NA, explained_missing=NA,
                                         non_missing=NA)]
    
    # Calculate block_missing Network TV occurrences
    #print("Calculating block_missing...")
    ny[, match_network:=(difftime_after<=time_window & 
                           shift(MediaTypeDesc, 1, type="lead")=="Network TV" &
                           PrimBrandCode==shift(PrimBrandCode, 1, type="lead")) 
       | (difftime_before<=time_window & 
            shift(MediaTypeDesc, 1, type="lag")=="Network TV" &
            PrimBrandCode==shift(PrimBrandCode, 1, type="lag"))]
    ny[MediaTypeDesc!="Network Clearance Spot TV", match_network:=NA]
    # This is not perfect (less conservative)
    unmatched_clearance_brands = ny[match_network==FALSE, PrimBrandCode] 
    
    ny = ny[MediaTypeDesc=="Network TV"]
    if(tz!="US/Central" & tz!="US/Eastern"){
      ny = ny[, .(non_missing=any(non_missing)),
              by=.(id, AdDate, PrimBrandCode)]
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
    
    # Aggregate over week variable
    #print("Aggregating over week...")
    ny[, Week:=ceiling_date(AdDate, "week")]
    missing_DT = ny[, .(non_missing_true = length(non_missing[non_missing==T]),
                        non_missing_false = length(non_missing[non_missing==F]),
                        block_missing_true = length(block_missing[block_missing==T]),
                        block_missing_false = length(block_missing[block_missing==F])),
                    by=Week]
    missing_DT[, MarketCode:=marketcode]
    missing_DT_list[[counter]] = copy(missing_DT)
    counter = counter + 1
  }
}
missing_DT_all = rbindlist(missing_DT_list)
missing_DT_all = missing_DT_all[, .(non_missing_true = sum(non_missing_true, na.rm=T),
                                     non_missing_false = sum(non_missing_false, na.rm=T),
                                     block_missing_true = sum(block_missing_true, na.rm=T),
                                     block_missing_false = sum(block_missing_false, na.rm=T)),
                                by=.(Week, MarketCode)]
save(missing_DT_all, file="check_clearance_results.RData")