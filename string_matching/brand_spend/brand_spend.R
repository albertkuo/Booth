# brand_spend.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: May 22, 2017
#
# This is an R script that computes the spend by brand in Ad Intel data

library(data.table)
prod_cols = c("BrandCode","BrandDesc","BrandVariant",
              "AdvParentCode","AdvParentDesc","AdvSubsidCode","AdvSubsidDesc",
              "ProductID","ProductDesc","PCCSubCode","PCCSubDesc","PCCMajCode",
              "PCCMajDesc","PCCIndusCode","PCCIndusDesc")

source_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel/aggregated"
filenames = list.files(pattern="\\.rds$", path=source_dir, full.names=TRUE)

for(filename in filenames){
  all <- readRDS(filename)
  # Sum across columns (media types)
  all[, media_sum := rowSums(.SD, na.rm=T), .SDcols = grep("Spend", names(all))]
  # Sum across rows (weeks and markets)
  temp = all[,.(spend_sum = sum(media_sum)), by=prod_cols]
  
  if(exists("spend")){
    spend = merge(temp,spend,by=prod_cols, all=TRUE)
    spend[is.na(spend)] = 0
    spend[, spend_sum := spend_sum.x+spend_sum.y]
    spend[, c('spend_sum.x','spend_sum.y') := NULL]
  } else {
    spend = temp
  }
}
saveRDS(spend, "brand_spend.rds")

# Get top ten spend brands in each PCCIndusCode
spend[, spend_sum:=-spend_sum] # Just to get right order
setkey(spend, spend_sum)
topten = spend[, head(.SD, 10), by=c('PCCIndusCode','PCCIndusDesc')]
spend[, spend_sum:=-spend_sum]
topten[, spend_sum:=-spend_sum]
setkey(spend, spend_sum)
write.csv(topten, file = "brand_spend_topten.csv", row.names=F)
