# add_competitors.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: December 22, 2016
#
# This is an R script that adds competitor values
# after data is built the first time with merge_RMS_Ad.R

## ================
## Set-up =========
## ================
library(data.table)
library(bit64)

RMS_input_dir = "/grpshares/hitsch_shapiro_ads/data/RMS/Brand-Aggregates"
output_dir = "/grpshares/hitsch_shapiro_ads/data/RMS_Ad"
dir_names = list.dirs(RMS_input_dir, full.names=F, recursive=F)

# Traverse through directories
#for(k in 1:length(dir_names)){ # Soft drinks = 1484
for(k in 1:1){
  print(k)
  #dir_name = dir_names[[k]]
  dir_name = "1484"
  print(dir_name)
  print("Adding competitor columns...")
  top_competitors = c("531429","616141","602144","543721")
  competitors_data = list()
  merged_filenames = list.files(file.path(output_dir,dir_name), full.names=T)
  if(length(merged_filenames)>1){
    for(i in 1:length(merged_filenames)){
      merged_filename = merged_filenames[[i]]
      RMS_brand_code = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(merged_filename))
      brand_data = readRDS(merged_filename)
      if(RMS_brand_code %in% top_competitors){
        competitor_data = brand_data[, c("Week","store_code_uc","price","promo_dummy"), with=F]
        setkeyv(competitor_data, c("Week", "store_code_uc"))
        competitor_data = unique(competitor_data)
        setnames(competitor_data, c("Week", "store_code_uc", paste(c("price", "promo_dummy"), RMS_brand_code, sep="_")))
        competitors_data[[RMS_brand_code]] = competitor_data
      }
      brand_data = brand_data[, c("Week", "dma_code", "National_GRP", "Local_GRP", "Total_GRP",
                                  "National_occ", "Local_occ", "Total_occ"), with=F]
      if(i==1){
        sum_data = copy(brand_data)
      } else {
        sum_data = rbind(sum_data, brand_data)
        sum_data = sum_data[,.(National_GRP = sum(National_GRP, na.rm=T), 
                               Local_GRP = sum(Local_GRP, na.rm=T),
                               Total_GRP = sum(Total_GRP, na.rm=T),
                               National_occ = sum(National_occ, na.rm=T),
                               Local_occ = sum(Local_occ, na.rm=T),
                               Total_occ = sum(Total_occ, na.rm=T)), by=c("Week", "dma_code")]
        print(summary(sum_data$Total_GRP))
      }
    }
    setnames(sum_data, c("Week", "dma_code", "National_GRP_sum", "Local_GRP_sum",
                         "Total_GRP_sum", "National_occ_sum", "Local_occ_sum", "Total_occ_sum"))
    #print(head(sum_data))
    
    # Add competitor columns
    for(i in 1:length(merged_filenames)){
      print(i)
      n_competitors = 0
      merged_filename = merged_filenames[[i]]
      RMS_brand_code = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(merged_filename))
      brand_data = readRDS(merged_filename)
      brand_data = merge(brand_data, sum_data, by=c("Week","dma_code"))
      brand_data[, `:=`(National_GRP_rival = National_GRP_sum-National_GRP,
                        Local_GRP_rival = Local_GRP_sum-Local_GRP,
                        Total_GRP_rival = Total_GRP_sum-Total_GRP,
                        National_occ_rival = National_occ_sum-National_occ,
                        Local_occ_rival = Local_occ_sum-Local_occ,
                        Total_occ_rival = Total_occ_sum-Total_occ)]
      brand_data[, (c("National_GRP_sum", "Local_GRP_sum", "Total_GRP_sum", 
                      "National_occ_sum", "Local_occ_sum", "Total_occ_sum")):=NULL, with=F]
      for(brand in names(competitors_data)){
        if(brand!=RMS_brand_code & n_competitors<3){
          print(RMS_brand_code)
          print(names(competitors_data[[brand]]))
          brand_data[, grep("price|promo_dummy", names(competitors_data[[brand]]), value=T):=NULL, with=F]
          brand_data = merge(brand_data, competitors_data[[brand]], by=c("Week","store_code_uc"), all.x=T)
          n_competitors = n_competitors+1
        }
      }
      print("Saving with competitors...")
      saveRDS(brand_data, file=paste0(file.path(output_dir, dir_name), "/",
                                      RMS_brand_code, ".rds"))
    }
  } else {
    print("No competitors")
  }
}