# build_data_subcategory.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: September 19, 2016
#
# This is an R script that will build R Formatted Ad Intel Files
# for a subcategory using existing R Formatted Ad Intel Files for all categories.
# This script is designed to be run after build_data.R has already built the data.

library(data.table)
library(foreach)
library(doParallel)

registerDoParallel(cores = NULL)

# Source directory and files 
source_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel/aggregated"
filenames = list.files(pattern="\\.rds$", path=source_dir, full.names=FALSE)

# Output directory for given product category
output_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel/aggregated_political"
product_code = "B181" # Political = B181

foreach(i = 1:length(filenames)) %dopar% { 
#for(i in 1:length(filenames)){
  all <- readRDS(paste(source_dir,filenames[i],sep="/"))
  # Extract entries in given category
  subset = all[PCCSubCode==product_code]
  saveRDS(subset, paste(output_dir,filenames[i],sep="/"))
}

# Rbind month files into one csv file
print("Combining into one file")
rdsfiles = list.files(path = output_dir, pattern = ".*?rds", full.names=T)
rdslist = lapply(rdsfiles, readRDS)
aggregated_all = rbindlist(rdslist, use.names=T, fill=T)
write.csv(aggregated_all, paste0(output_dir,"/all_aggregated.csv"), row.names=F)
