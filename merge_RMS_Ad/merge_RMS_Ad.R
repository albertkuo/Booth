# merge_RMS_Ad.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: March 27, 2016
#
# This is an R script that merges RMS and Ad Intel based on brand, market, week
# Run aggregate_RMS.R and ad_extract.R and ad_extract_save.R prior to this

## ================
## Set-up =========
## ================
library(data.table)
library(bit64)
library(RcppRoll)

run_grid = T
if(!run_grid){
  dir_name = 1484
  brand_code = 531429
  RMS_data = readRDS("Booth/merge_RMS_Ad/531429.rds")
  Ad_data = readRDS("Booth/merge_RMS_Ad/201206_aggregated.rds")
  load("Booth/merge_RMS_Ad/Stores.RData")        #stores
  load("Booth/merge_RMS_Ad/Store-Address.RData") #store_info
  zipborders = fread("Booth/county/zipborders.csv")
  string_matches = fread("Booth/merge_RMS_Ad/string_matches.csv")
} 

# Read files
RMS_input_dir = "/grpshares/hitsch_shapiro_ads/data/RMS/Brand-Aggregates"
ad_output_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel"
output_dir = "/grpshares/hitsch_shapiro_ads/data/RMS_Ad"
dir_names = list.dirs(RMS_input_dir, full.names=F, recursive=F)

load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Stores.RData')
store_dma_mapping = stores[, c("store_code_uc", "year", "dma_code"), with=F]
setkey(store_dma_mapping)
store_dma_mapping = unique(store_dma_mapping)
string_matches = fread('~/merge_metadata/string_matches.csv')

# Set parameters
weight = 0.9 # Weight for ad stock
window = 52 # Window of T weeks for ad stock
associated_brands = F # Indicator for including level 4 matches
simulation_bool = T # Indicator for creating estimated data

for(k in 1:length(dir_names)){ 
#for(k in 1:1){
  print(k)
  dir_name = dir_names[[k]]
  #dir_name = "1484" # Soft drinks = 1484, Paper towels = 7734, Yogurt = 3603, Diapers = 8444
  print(dir_name)
  RMS_filenames = list.files(file.path(RMS_input_dir, dir_name), full.names=T)
  
  print("Reading RMS file...")
  for(i in 1:length(RMS_filenames)){
    RMS_filename = RMS_filenames[[i]]
    print(RMS_filename)
    RMS_brand_code = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(RMS_filename))
    RMS_data = readRDS(RMS_filename)
    
    # Clean some data stuff on RMS side
    RMS_data[, year:=year(week_end)]
    RMS_data[, product_module_code:=as.integer(dir_name)]

    # Read ad file
    print("Reading in ad file...")
    ad_filename = paste0(file.path(ad_output_dir, "aggregated_extracts", dir_name),
                         "/", RMS_brand_code, ".rds")
    if(file.exists(ad_filename)){
      Ad_data = readRDS(ad_filename)
    
      # Select whether to include match_tier = 4 or not
      if(!associated_brands & nrow(Ad_data)>0){
        filter_brands = string_matches[brand_code_uc==RMS_brand_code & 
                                       product_module_code==as.integer(dir_name) & 
                                       match_tier!=4]$BrandCode
        Ad_data = Ad_data[BrandCode %in% filter_brands]
      }

      if(nrow(Ad_data)>0){
        # Fill in missing rows with cross-join
        print("Filling in missing rows...")
        setkey(Ad_data, BrandCode, MarketCode, Week)
        Ad_data = Ad_data[CJ(unique(BrandCode), unique(MarketCode), 
                             seq(min(Week), max(Week), by=7))]
        Ad_data[is.na(Ad_data)] = 0
    
        # Calculate Ad stock and Total values
        print("Calculating ad stock...")
        setorder(Ad_data, Week)
        
        # Simulation option
        if(simulation_bool){
          simulated = Ad_data[, .(National_GRP = sum(National_GRP*(seq(.N)<=window))/window,
                                  Local_GRP = sum(Local_GRP*(seq(.N)<=window))/window,
                                  National_occ = sum(National_occ*(seq(.N)<=window))/window,
                                  Local_occ = sum(Local_occ*(seq(.N)<=window))/window),
                                 by=c("BrandCode", "MarketCode")]
          simulated = simulated[rep(seq(.N), each=window)]
          # Placeholder week values
          simulated_weekdate = as.Date("01/01/1900",format="%w/%W/%Y")
          simulated[, `:=`(Week=simulated_weekdate, 
                           National_spend=NA, Local_spend=NA)] 
          order_cols = c("BrandCode", "MarketCode", "Week", 
                        "National_GRP", "Local_GRP", "National_occ", "Local_occ",
                        "National_spend", "Local_spend")
          setcolorder(simulated, order_cols)
          setcolorder(Ad_data, order_cols)
          Ad_data = rbind(simulated, Ad_data)
        }
    
        # Calculate Ad stock using roll_sum
        geom_weights = cumprod(c(1.0, rep(weight, times = window)))
        geom_weights = sort(geom_weights)
        setkey(Ad_data, BrandCode, MarketCode, Week)
        Ad_data[,`:=`(National_GRP = roll_sum(National_GRP, n = window+1, weights = geom_weights,
                                              normalize = F, align = "right", fill = NA),
                      Local_GRP = roll_sum(Local_GRP, n = window+1, weights = geom_weights,
                                           normalize = F, align = "right", fill = NA),
                      National_occ = roll_sum(National_occ, n = window+1, weights = geom_weights,
                                              normalize = F, align = "right", fill = NA),
                      Local_occ = roll_sum(Local_occ, n = window+1, weights = geom_weights,
                                           normalize = F, align = "right", fill = NA)),
                      by=c("BrandCode", "MarketCode")]
  
        # Aggregate over brand match_tiers for RMS_brand
        Ad_data = Ad_data[,.(National_GRP = sum(National_GRP, na.rm=T),
                             Local_GRP = sum(Local_GRP, na.rm=T),
                             National_occ = sum(National_occ, na.rm=T),
                             Local_occ = sum(Local_occ, na.rm=T),
                             National_spend = sum(National_spend, na.rm=T),
                             Local_spend = sum(Local_spend, na.rm=T)), by=c("MarketCode","Week")]
        Ad_data[, Total_GRP:=National_GRP+Local_GRP]
        Ad_data[, Total_occ:=National_occ+Local_occ]
        Ad_data[, MarketCode:=MarketCode+400] 
  
        # Join to zip/DMA matching
        RMS_data = merge(RMS_data, store_dma_mapping, by=c("store_code_uc", "year"))
        
        # Join RMS to Ad Intel
        print("Joining RMS to Ad Intel...")
        output = merge(RMS_data, Ad_data, by.x=c("dma_code", "week_end"),
                       by.y=c("MarketCode", "Week"), all.x=T) 
        
        # Drop columns
        keep_cols = c("brand_code_uc_corrected", "product_module_code", "store_code_uc",
                      "dma_code", "week_end", "year", "price", "units", "promo_percentage",
                      "promo_dummy", "Total_GRP", "Total_occ")
        output = output[, keep_cols, with=F]
  
        if(nrow(output)>0){
          print("Saving...")
          dir.create(file.path(output_dir, dir_name), showWarnings = FALSE)
          saveRDS(output, file=paste0(file.path(output_dir, dir_name), "/",
                                      RMS_brand_code, ".rds"))
        }
      }
    }
  }
}
          
          