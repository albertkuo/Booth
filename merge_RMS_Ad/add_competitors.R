# add_competitors.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: February 1, 2016
#
# This is an R script that adds competitor values
# after data is built the first time with merge_RMS_Ad.R

## ================
## Set-up =========
## ================
library(data.table)
library(bit64)

output_dir = "/grpshares/hitsch_shapiro_ads/data/RMS_Ad"
dir_names = list.dirs(output_dir, full.names=F, recursive=F)

# Metadata for finding competitors
load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Products-Corrected.RData')
load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Meta-Data-Corrected.RData')
meta_data[, product_module_code:=NULL]
prod_meta = merge(products, meta_data)
brands_RMS = prod_meta[, .(rev_sum = sum(revenue_RMS)), 
                       by = c("brand_code_uc","brand_descr",
                              "product_module_code", "product_module_descr")]
brands_RMS = brands_RMS[order(-rev_sum)]
top100_brandcodes = brands_RMS[1:100]$brand_code_uc

num_competitors = 3 # Number of competitors whose prices/promotions we want to control for

# Traverse through directories
for(k in 1:length(dir_names)){ 
#for(k in 1:1){
  print(k)
  dir_name = dir_names[[k]]
  #dir_name = "1484" # Soft drinks = 1484, Paper towels = 7734, Yogurt = 3603, Diapers = 8444
  print(dir_name)
  merged_filenames = list.files(file.path(output_dir, dir_name), full.names=T)
  built_brands = sapply(merged_filenames, function(x){sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(x))})
  built_brands = sapply(built_brands, as.integer)
  competitors = brands_RMS[product_module_code == as.integer(dir_name) &
                           brand_code_uc %in% built_brands][order(-rev_sum)]$brand_code_uc
  competitors = sapply(competitors, as.character)
  top_competitors = sapply(competitors[1:min(num_competitors+1, length(competitors))], as.character)
  print(top_competitors)
  #top_competitors = c("531429","616141","602144","543721")
  competitors_datalist = list()
  
  # Read competitors data
  for(i in 1:length(top_competitors)){
    competitor_brand = top_competitors[[i]]
    competitor_DT = readRDS(paste0(file.path(output_dir, dir_name),"/",competitor_brand,".rds"))
    competitors_datalist[[competitor_brand]] = copy(competitor_DT)
  }
  competitors_DT = rbindlist(competitors_datalist)

  # Read brands in top 100 and add competitors for them
  if(length(merged_filenames)>1){
    for(i in 1:length(merged_filenames)){
      merged_filename = merged_filenames[[i]]
      RMS_brand = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(merged_filename))
      if(RMS_brand %in% top100_brandcodes){
        if(RMS_brand %in% top_competitors){
          brand_data = copy(competitors_DT)
        } else {
          brand_data = readRDS(merged_filename)
          brand_data = rbind(brand_data, competitors_DT[brand_code_uc_corrected %in% top_competitors[-length(top_competitors)]])
        }
        print(unique(brand_data$brand_code_uc_corrected))
        brand_data[, brand_type:="comp"]
        brand_data[brand_code_uc_corrected == RMS_brand, brand_type:="own"]
        
        # dcast
        brand_data = brand_data[, .(units = sum(units), price = mean(price),
                                    promo_percentage = mean(promo_percentage),
                                    Total_GRP = sum(Total_GRP), Total_occ = sum(Total_occ)),
                                by = .(store_code_uc, dma_code, week_end, year, brand_type)] 
        brand_data = dcast(brand_data, store_code_uc + dma_code + week_end + year ~ brand_type,
                           value.var = list("units", "price", "promo_percentage","Total_GRP","Total_occ"))
        brand_data[, units_comp:=NULL]
        setnames(brand_data, c("units_own"), c("units"))
        print("Saving with competitors...")
        saveRDS(brand_data, file=paste0(file.path(output_dir, dir_name), "/",
                                        RMS_brand, ".rds"))
      }
    }
  } else {
    print("No competitors")
  }
}