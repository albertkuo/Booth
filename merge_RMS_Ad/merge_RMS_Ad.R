# merge_RMS_Ad.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: December 15, 2016
#
# This is an R script that merges RMS and Ad Intel based on brand, market, week

## ================
## Set-up =========
## ================
library(data.table)
library(bit64)

run_grid = T
if(!run_grid){
  dir_name = 1484
  brand_code = 531429
  RMS_example = readRDS("Booth/merge_RMS_Ad/531429.rds")
  Ad_example = readRDS("Booth/merge_RMS_Ad/201206_aggregated.rds")
  load("Booth/merge_RMS_Ad/Stores.RData")        #stores
  load("Booth/merge_RMS_Ad/Store-Address.RData") #store_info
  zipborders = fread("Booth/county/zipborders.csv")
  string_matches = fread("Booth/merge_RMS_Ad/string_matches.csv")
} 

# Read files
RMS_input_dir = "/grpshares/hitsch_shapiro_ads/data/RMS/Brand-Aggregates"
ad_output_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel"
output_dir = "/grpshares/hitsch_shapiro_ads/data/RMS_Ad"

dir_name = 1484
#brand_code = 518917
RMS_filenames = list.files(file.path(RMS_input_dir,toString(dir_name)), full.names=T)
brand_codes = list()
for(i in 1:length(RMS_filenames)){
  RMS_filename = RMS_filenames[[i]]
  brand_codes[[i]] = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(RMS_filename))
}
load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Stores.RData')
load('~/merge_metadata/Store-Address.RData')
zipborders = fread('~/merge_metadata/zipborders.csv')
string_matches = fread('~/merge_metadata/string_matches.csv')

# Clean metadata
stores = stores[, c("store_code_uc","year","store_zip3","fips_state_code","fips_county_code","dma_code"), with=F]
store_info[, Year:=strtoi(substring(DATE, 0, 4), base=10)]
store_info[, store_code_uc:=`Store ID UC`]
store_info[, c("Store ID UC", "NAME", "PARENT", "BANNER"):=NULL]
store_info[, rollyear:=Year]
setkey(store_info, store_code_uc, rollyear)
store_info = unique(store_info)
#setkey(zipborders, zip3, on_border, bordername)
#zipborders = unique(zipborders)
#zipborders[, zip:=NULL]

# Set parameters
weight = 0.5 # Weight for ad stock
window = 52 # Window of T weeks for ad stock
datastream = 2 # (need to implement) Datastream to use for National TV, note that Local TV only has Datastream = 3

## ================
## RMS ============
## ================
print("Reading RMS file...")

for(i in 1:length(RMS_filenames)){
  RMS_filename = RMS_filenames[[i]]
  RMS_brand_code = brand_codes[[i]]
  RMS_example = readRDS(RMS_filename)
  # Clean some data stuff on RMS side
  RMS_example[, year:=year(week_end)]
  RMS_example[, Week:=format(week_end,format="%W/%Y")]
  RMS_example[, product_module_code:=dir_name]
  
  # Join to zip/DMA matching
  RMS_example[, rollyear:=year]
  setkey(RMS_example, store_code_uc, rollyear)
  RMS_example = store_info[RMS_example, roll=T, rollends=c(T,T)] 
  RMS_example[, c("rollyear", "Year", "DATE"):=NULL]
  RMS_example = merge(RMS_example, stores, by=c("store_code_uc", "year"))
  RMS_example = merge(RMS_example, zipborders, by.x=c("ZIP"), by.y=c("zip"), allow.cartesian=T)
  RMS_example[, dma_code:=dma_code-400] # match with Ad Intel code format
  
  # Join to brand matching
  # Note that if there are no string matches, we get an empty data frame
  RMS_example = merge(RMS_example, string_matches, by.x=c("brand_code_uc_corrected", "product_module_code"), 
                            by.y=c("brand_code_uc", "product_module_code"), allow.cartesian=T)

  # Read in ad files
  print("Reading in ad files...")
  RMS_ad_filenames = list.files(file.path(ad_output_dir, "aggregated_extracts", toString(dir_name)), 
                                pattern=RMS_brand_code, full.names=T)
  ad_datalist = lapply(RMS_ad_filenames, fread)
  Ad_example = rbindlist(ad_datalist) 
  
  if(nrow(Ad_example)>0){
    # Re-accumulate by week to fix month cross-over values
    Ad_example = Ad_example[,.(National_GRP = as.numeric(sum(National_GRP, na.rm=T)), 
                               Local_GRP = as.numeric(sum(Local_GRP, na.rm=T)),
                               National_occ = as.numeric(sum(National_occ, na.rm=T)),
                               Local_occ = as.numeric(sum(Local_occ, na.rm=T))),
                               by=c("BrandCode", "Week", "MarketCode")]
    
    # Fill in missing rows
    print("Filling in missing rows...")
    #print(tail(Ad_example))
    print(nrow(Ad_example))
    Ad_example[, WeekDate:=as.Date(paste0("1/",Week),format="%w/%W/%Y")]
    setkey(Ad_example, BrandCode, MarketCode, WeekDate)
    Ad_example = Ad_example[CJ(unique(BrandCode), unique(MarketCode), seq(min(WeekDate), max(WeekDate), by=7))]
    #print(tail(Ad_example))
    print(nrow(Ad_example))
    
    # Calculate Ad stock and Total values
    # Assumption that all weeks need to be filled for correct weight
    # Check that order is by weeks
    print("Calculating ad stock...")
    setorder(Ad_example, WeekDate)
    # Under option (b), first create estimated data occurring before time interval using average
    b=T
    if(b){
      simulated = Ad_example[, .(National_GRP = sum(National_GRP*(seq(.N)<=window))/window,
                                 Local_GRP = sum(Local_GRP*(seq(.N)<=window))/window,
                                 National_occ = sum(National_occ*(seq(.N)<=window))/window,
                                 Local_occ = sum(Local_occ*(seq(.N)<=window))/window),
                             by=c("BrandCode", "MarketCode")]
      simulated = simulated[rep(seq(.N), each=window)]
      simulated_weekdate = as.Date("01/01/1900",format="%w/%W/%Y")
      simulated[, `:=`(WeekDate=simulated_weekdate, Week=NA)] #Note that simulated data does not have real Week values
      # Assign all markets' codes
      setcolorder(simulated, c("BrandCode", "MarketCode", "Week", "WeekDate",
                               "National_GRP", "Local_GRP", "National_occ", "Local_occ"))
      setcolorder(Ad_example, c("BrandCode", "MarketCode", "Week", "WeekDate",
                                "National_GRP", "Local_GRP", "National_occ", "Local_occ"))
      Ad_example = rbind(simulated, Ad_example)
    }
    
    # Calculate Ad stock using cumsum
    Ad_example[, `:=`(National_GRP = cumsum(ifelse(is.na(National_GRP), 0, National_GRP)*weight^(rev(seq(.N))-1))/
                                     (weight^(rev(seq(.N))-1)), 
                      Local_GRP = cumsum(ifelse(is.na(Local_GRP), 0, Local_GRP)*weight^(rev(seq(.N))-1))/
                                  (weight^(rev(seq(.N))-1)), 
                      National_occ = cumsum(ifelse(is.na(National_occ), 0, National_occ)*weight^(rev(seq(.N))-1))/
                                     (weight^(rev(seq(.N))-1)),
                      Local_occ = cumsum(ifelse(is.na(Local_occ), 0, Local_occ)*weight^(rev(seq(.N))-1))/
                                  (weight^(rev(seq(.N))-1))),
                  by=c("BrandCode", "MarketCode")]
    print(tail(Ad_example))

    # Implement window=T by subtracting previous values 
    # Under option (a), this will leave the first 52 rows as NA
    # Under option (b), this will leave simulated data as NA
    Ad_example[, `:=`(National_GRP = National_GRP - shift(National_GRP, window, type="lag")*weight^window,
                      Local_GRP = Local_GRP - shift(Local_GRP, window, type="lag")*weight^window,
                      National_occ = National_occ - shift(National_occ, window, type="lag")*weight^window,
                      Local_occ = Local_occ - shift(Local_occ, window, type="lag")*weight^window),
                      by=c("BrandCode", "MarketCode")]
    print(tail(Ad_example))
    Ad_example[, Total_GRP:=rowSums(.SD, na.rm=T), .SDcols=c("National_GRP", "Local_GRP")]
    Ad_example[, Total_occ:=rowSums(.SD, na.rm=T), .SDcols=c("National_occ", "Local_occ")]
    Ad_example[, Week:=format(WeekDate,format="%W/%Y")]
    Ad_example[, WeekDate:=NULL]
      
    # Join RMS to Ad Intel months
    print("Joining RMS to Ad Intel...")
    print(nrow(RMS_example))
    output = merge(RMS_example, Ad_example, by.x=c("BrandCode", "dma_code", "Week"),
                   by.y=c("BrandCode", "MarketCode", "Week"))
    print(nrow(output))
    
    # Sum up by matchtier/MarketCode/Week
    print("Aggregating...")
    keep_cols = c("brand_code_uc_corrected", "product_module_code", "store_code_uc", "STR NUM",
                "STREET", "CITY", "ST", "ZIP", "fips_state_code", "fips_county_code", "fips", 
                "dma_code", "on_border", "bordername", "week_end", "Week", "price", "units", "base_price", "promo_percentage",
                "promo_dummy")
    output = output[,.(National_GRP = sum(National_GRP, na.rm=T), 
                       Local_GRP = sum(Local_GRP, na.rm=T),
                       Total_GRP = sum(Total_GRP, na.rm=T),
                       National_occ = sum(National_occ, na.rm=T),
                       Local_occ = sum(Local_occ, na.rm=T),
                       Total_occ = sum(Total_occ, na.rm=T)), by=c(keep_cols)] # match tier? without it, it's 1-4
    print(head(output))
    print(nrow(output))
    
    if(nrow(output)>0){
      print("Saving...")
      dir.create(file.path(output_dir, toString(dir_name)), showWarnings = FALSE)
      saveRDS(output, file=paste0(file.path(output_dir, toString(dir_name)), "/",
                                  RMS_brand_code, ".rds"))
    }
  } else {
    print("Ad_example empty")
  }
}

# Also need to add competitor values once data is built the first time

        
        