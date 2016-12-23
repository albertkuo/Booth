# merge_RMS_Ad.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: December 19, 2016
#
# This is an R script that merges RMS and Ad Intel based on brand, market, week
# Run aggregate_RMS.R and ad_extract.R prior to this

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

load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Stores.RData')
load('~/merge_metadata/Store-Address.RData')
zipborders = fread('~/merge_metadata/zipborders.csv')
string_matches = fread('~/merge_metadata/string_matches.csv')

# Set parameters
weight = 0.9 # Weight for ad stock
window = 52 # Window of T weeks for ad stock
associated_brands = F # Indicator for including level 4 matches

# Clean metadata
stores = stores[, c("store_code_uc","year","store_zip3","fips_state_code","fips_county_code","dma_code"), with=F]
store_dma_mapping = stores[, c("store_code_uc", "year", "dma_code"), with=F]
setkey(store_dma_mapping)
store_dma_mapping = unique(store_dma_mapping)
store_info[, Year:=strtoi(substring(DATE, 0, 4), base=10)]
store_info[, store_code_uc:=`Store ID UC`]
store_info[, c("Store ID UC", "NAME", "PARENT", "BANNER"):=NULL]
store_info[, rollyear:=Year]
#setkey(zipborders, zip3, on_border, bordername)
#zipborders = unique(zipborders)
#zipborders[, zip:=NULL]

dir_names = list.dirs(RMS_input_dir, full.names=F, recursive=F)

#for(k in 1:length(dir_names)){ # Soft drinks = 1484
for(k in 1:1){
  print(k)
  #dir_name = dir_names[[k]]
  dir_name = "1484"
  print(dir_name)
  RMS_filenames = list.files(file.path(RMS_input_dir, dir_name), full.names=T)
  brand_codes = list()
  for(i in 1:length(RMS_filenames)){
    RMS_filename = RMS_filenames[[i]]
    brand_codes[[i]] = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(RMS_filename))
  }
  
  print("Reading RMS file...")
  
  for(i in 1:length(RMS_filenames)){
    RMS_filename = RMS_filenames[[i]]
    print(RMS_filename)
    RMS_brand_code = brand_codes[[i]]
    RMS_example = readRDS(RMS_filename)
    
    # Clean some data stuff on RMS side
    RMS_example[, year:=year(week_end)]
    RMS_example[, Week:=format(week_end,format="%W/%Y")]
    RMS_example[, product_module_code:=strtoi(dir_name, base=10)]
  
    # Join to zip/DMA matching
    RMS_example = merge(RMS_example, store_dma_mapping, by=c("store_code_uc", "year"))
  
    # Read in ad files
    print("Reading in ad files...")
    RMS_ad_filenames = list.files(file.path(ad_output_dir, "aggregated_extracts", dir_name),
                                  pattern=RMS_brand_code, full.names=T)
    ad_datalist = lapply(RMS_ad_filenames, fread)
    Ad_example = rbindlist(ad_datalist)
    # Select whether to include match_tier = 4 or not
    if(!associated_brands & nrow(Ad_example)>0){
      filter_brands = string_matches[brand_code_uc==RMS_brand_code & product_module_code==strtoi(dir_name) & match_tier!=4]$BrandCode
      Ad_example = Ad_example[BrandCode %in% filter_brands]
    }
  
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
      #print(nrow(Ad_example))
      Ad_example[, WeekDate:=as.Date(paste0("1/",Week),format="%w/%W/%Y")]
      setkey(Ad_example, BrandCode, MarketCode, WeekDate)
      Ad_example = Ad_example[CJ(unique(BrandCode), unique(MarketCode), seq(min(WeekDate), max(WeekDate), by=7))]
      #print(tail(Ad_example))
      #print(nrow(Ad_example))
  
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
  
      # Implement window=T by subtracting previous values
      # Under option (a), this will leave the first 52 rows as NA
      # Under option (b), this will leave simulated data as NA
      Ad_example[, `:=`(National_GRP = National_GRP - shift(National_GRP, window, type="lag")*weight^window,
                        Local_GRP = Local_GRP - shift(Local_GRP, window, type="lag")*weight^window,
                        National_occ = National_occ - shift(National_occ, window, type="lag")*weight^window,
                        Local_occ = Local_occ - shift(Local_occ, window, type="lag")*weight^window),
                        by=c("BrandCode", "MarketCode")]
      Ad_example[, Total_GRP:=rowSums(.SD, na.rm=T), .SDcols=c("National_GRP", "Local_GRP")]
      Ad_example[, Total_occ:=rowSums(.SD, na.rm=T), .SDcols=c("National_occ", "Local_occ")]
      Ad_example[, Week:=format(WeekDate,format="%W/%Y")]
      Ad_example[, WeekDate:=NULL]
      # Aggregate over brand match_tiers for RMS_brand
      Ad_example = Ad_example[,.(National_GRP = sum(National_GRP, na.rm=T),
                                 Local_GRP = sum(Local_GRP, na.rm=T),
                                 Total_GRP = sum(Total_GRP, na.rm=T),
                                 National_occ = sum(National_occ, na.rm=T),
                                 Local_occ = sum(Local_occ, na.rm=T),
                                 Total_occ = sum(Total_occ, na.rm=T)), by=c("MarketCode","Week")]
      Ad_example[, MarketCode:=MarketCode+400] # match with dma_code numbers
  
      # Join RMS to Ad Intel
      print("Joining RMS to Ad Intel...")
      #print(nrow(RMS_example))
      RMS_example = merge(RMS_example, Ad_example, by.x=c("dma_code", "Week"),
                     by.y=c("MarketCode", "Week"))
      #print(nrow(output))
      
      # Add address metadata
      RMS_example[, rollyear:=year]
      setkey(RMS_example, store_code_uc, rollyear)
      setkey(store_info, store_code_uc, rollyear)
      store_info = unique(store_info)
      RMS_example = store_info[RMS_example, roll=T, rollends=c(T,T)]
      RMS_example[, c("rollyear", "Year", "DATE"):=NULL]
      stores[, dma_code:=NULL]
      RMS_example = merge(RMS_example, stores, by=c("store_code_uc", "year"))
      RMS_example = merge(RMS_example, zipborders, by.x=c("ZIP"), by.y=c("zip"), allow.cartesian=T)
      
      # Drop columns
      keep_cols = c("brand_code_uc_corrected", "product_module_code", "store_code_uc",
                    "dma_code", "week_end", "Week", "year", "price", "units", "base_price", "promo_percentage",
                    "promo_dummy", "National_GRP", "Local_GRP", "Total_GRP", "National_occ",
                    "Local_occ", "Total_occ", "ZIP", "STR NUM", "STREET", "CITY", "ST", 
                    "fips_state_code", "fips_county_code", "fips", "on_border", "bordername")
      output = RMS_example[, keep_cols, with=F]
  
      if(nrow(output)>0){
        print("Saving...")
        dir.create(file.path(output_dir, dir_name), showWarnings = FALSE)
        saveRDS(output, file=paste0(file.path(output_dir, toString(dir_name)), "/",
                                    RMS_brand_code, ".rds"))
      }
    } else {
      print("Ad_example empty")
    }
  }
}
          
          