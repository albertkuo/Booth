# ad_extract_save.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: January 16, 2016
#
# Take ad extracts created by ad_extract.R 
# and collapse the csv files into one rds file per brand
# and remove the csv files.

library(data.table)

# Read files
RMS_input_dir = "/grpshares/hitsch_shapiro_ads/data/RMS/Brand-Aggregates"
ad_output_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel"
dir_names = list.dirs(RMS_input_dir, full.names=F, recursive=F)

for(k in 1:length(dir_names)){ 
#for(k in 1:1){
  print(k)
  dir_name = dir_names[[k]]
  #dir_name = "7734" # Soft drinks = 1484, Paper towels = 7734, Yogurt = 3603, Diapers = 8444
  print(dir_name)
  RMS_filenames = list.files(file.path(RMS_input_dir, dir_name), pattern = "\\.rds$", full.names=T)
  
  for(i in 1:length(RMS_filenames)){
    RMS_filename = RMS_filenames[[i]]
    RMS_brand_code = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(RMS_filename))

    # Read in ad csv files
    ad_filenames = list.files(file.path(ad_output_dir, "aggregated_extracts", dir_name),
                                  pattern=paste0("^", RMS_brand_code, ".*csv$"), full.names=T)
    ad_datalist = lapply(ad_filenames, fread)
    Ad_data = rbindlist(ad_datalist)
    
    if(nrow(Ad_data)>0){
      # Re-accumulate by week to fix month cross-over values
      Ad_data = Ad_data[,.(National_GRP = as.numeric(sum(National_GRP, na.rm=T)),
                           Local_GRP = as.numeric(sum(Local_GRP, na.rm=T)),
                           National_occ = as.numeric(sum(National_occ, na.rm=T)),
                           Local_occ = as.numeric(sum(Local_occ, na.rm=T)),
                           National_spend = as.numeric(sum(National_spend, na.rm=T)),
                           Local_spend = as.numeric(sum(Local_spend, na.rm=T))),
                           by=c("BrandCode", "Week", "MarketCode")]
      saveRDS(Ad_data, file=paste0(file.path(ad_output_dir, "aggregated_extracts", dir_name),
                                   "/", RMS_brand_code, ".rds"))
    }
    file.remove(ad_filenames)
  }
}

