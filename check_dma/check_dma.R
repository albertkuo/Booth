library(data.table)
library(foreach)
library(doParallel)

registerDoParallel(cores = NULL)

#setwd('/Volumes/Nielsen_Raw/Ad_Intel/')
source_dir = "/nielsen_raw/Ad_Intel"
source_dir = "/Volumes/Nielsen_Raw/Ad_Intel"
#monthpath.string = "2010_BackData_20160212_extracted/201001"
dirs = list.dirs(full.names=F,recursive=F,path=source_dir)
years = as.character(c(2010:2010))
months = c("01","02","03","04","05","06","07","08","09","10","11","12")
monthpath.strings = vector("list",length(years)*12)
for(i in 1:length(years)){
  yeardir = dirs[grep(paste0(years[i],".*?extracted"),dirs)]
  monthdirs = paste0(years[i],months)
  for(k in 1:length(monthdirs)){
    monthpath.strings[(i-1)*12+k] = paste(source_dir,yeardir,monthdirs[k],sep="/")
  }
}

# # Create a DistributorID to MarketCode dictionary
# for(monthpath.string in monthpath.strings){
#   print(monthpath.string)
#   spot_occs = "(SP|SC|NC)"      
#   occfilenames = list.files(pattern = paste0("OCC.*?",spot_occs),path = monthpath.string)
#   
#   for(occfilename in occfilenames){
#     occ = fread(paste(monthpath.string, occfilename, occfilename, sep='/'),showProgress=F)
#     #print(names(occ))
#     occ = occ[,.(MarketCode,DistributorID)]
#     #print(head(occ))
#     setkey(occ, "DistributorID")
#     if(exists("occdict")){
#       occdict = rbind(occdict,unique(occ))
#       setkey(occdict)
#       occdict = unique(occdict)
#     } else {
#       occdict = unique(occ)
#     }
#   }
# }
# saveRDS(occdict, file="occdict.rds")

# occdict = readRDS(file="occdict.rds")
# marketcodes = vector('list',length(monthpath.strings))
# for(i in 1:length(monthpath.strings)){
#   monthpath.string = monthpath.strings[[i]]
#   print(monthpath.string)
#   impfilename = list.files(pattern = "IMPC.*?SP", path = monthpath.string)
#   imp = fread(paste(monthpath.string,impfilename,impfilename,sep='/'))
#   occimp = merge(occdict, imp, by=c("DistributorID"), all.y=T)
#   
#   #View(occimp)
#   #na_tvhh = occimp[is.na(TV_HH),]
#   #print(unique(na_tvhh$MarketCode))
#   marketcodes[[i]] = unique(occimp$MarketCode)
#   print(marketcodes[[i]])
# }
# marketcodes is a list of markets that appear in each month's impression file
# saveRDS(marketcodes, file="marketcodes.rds")

#####################################################################
## Check if every occurrence in the 25 DMAs match to an impression
#####################################################################
# marketcodes = readRDS('marketcodes.rds')
# lpm = Reduce(intersect, marketcodes)
# lpm = lpm[!is.na(lpm)]
# 
# no_matches = vector('list',length(monthpath.strings)*3)
# foreach(i = 1:length(monthpath.strings)) %dopar% { 
# #for(i in 1:length(monthpath.strings)){
#   monthpath.string = monthpath.strings[[i]]
#   print(monthpath.string)
#   spot_occs = "(SP|SC|NC)"
#   occfilenames = list.files(pattern = paste0("OCC.*?",spot_occs),path = monthpath.string)
#   impfilename = list.files(pattern = "IMPC.*?SP", path = monthpath.string)
#   imp = fread(paste(monthpath.string,impfilename,impfilename,sep='/'))
#     
#   for(j in 1:length(occfilenames)){
#     occfilename = occfilenames[j]
#     occ = fread(paste(monthpath.string, occfilename, occfilename, sep='/'), showProgress=F)
#     occ = occ[MarketCode %in% lpm]
#     keys_imp = c("PeriodYearMonth","DistributorID","DayOfWeek","TimeIntervalNumber")
#     occimp1 = merge(occ, imp, by=keys_imp, allow.cartesian = T)
#     occimp2 = merge(occ, imp, by=keys_imp, all.x=T, allow.cartesian = T) 
#     print(nrow(occimp1))
#     print(nrow(occimp2))
#     no_matches[[(i-1)*3+j]] = nrow(occimp2)-nrow(occimp1)
#     print(nrow(occimp2)-nrow(occimp1))
#   }
# }
# write.csv(no_matches, file='num_no_matches.csv')

#####################################################################
# Create a file of 2010 impressions for only set meter market=Buffalo
# and show that only 4 months show up
#####################################################################
occdict = readRDS(file="occdict.rds")
#Buffalo 
occdict = occdict[MarketCode==114]
dist_in_buffalo = occdict$DistributorID
imps = vector('list',length(monthpath.strings))
for(i in 1:length(monthpath.strings)){
  monthpath.string = monthpath.strings[[i]]
  print(monthpath.string)
  impfilename = list.files(pattern = "IMPC.*?SP", path = monthpath.string)
  imp = fread(paste(monthpath.string,impfilename,impfilename,sep='/'))
  imp = imp[DistributorID %in% dist_in_buffalo]
  imp[,Month:=i]
  imps[[i]] = imp
}
imps_2010 = Reduce(rbind, imps)
saveRDS(imps_2010,file="buffalo_imps_2010.rds")
write.csv(imps_2010,file="buffalo_imps_2010.csv",row.names=FALSE)
  impfilename = t.files(pattern = "IMPC.*?SP", path = monthpath.string)