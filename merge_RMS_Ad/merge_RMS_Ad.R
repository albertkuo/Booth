# Merges RMS and Ad Intel data based on zip, brand, and week
library(data.table)
library(bit64)

run_grid = F
if(!run_grid){
  RMS_example = readRDS("Booth/merge_RMS_Ad/536746.rds")
  load("Booth/merge_RMS_Ad/Stores.RData")
  zipborders = fread("Booth/county/zipborders.csv")
  string_matches = fread("Booth/string_matching/matches_files/top100/top100_matches.csv")
  # Remember to process matches to get product_module_code
}

# Clean some data stuff on RMS side
RMS_example[, year:=year(week_end)]
RMS_example[, Week:=format(week_end,format="%W/%Y")]
RMS_example[, product_module_code:=dir_name]
stores = stores[, c("store_code_uc","year","store_zip3","fips_state_code","fips_county_code","dma_code"), with=F]
setkey(zipborders, zip3, on_border, bordername)
zipborders = unique(zipborders)
zipborders[, zip:=NULL]
string_matches = string_matches[, c("brand_code", "product_module_code", "match_tier", "BrandCode")]

# Join to zip/DMA matching
RMS_example = merge(RMS_example, stores, by=c("store_code_uc", "year"))
RMS_example = merge(RMS_example, zipborders, by.x=c("store_zip3"), by.y=c("zip3"), allow.cartesian=T)
RMS_example[, dma_code:=dma_code-400] # match with Ad Intel code format

# Join to brand matching
RMS_example = merge(RMS_example, string_matches, by=c("brand_code_uc_corrected", "product_module_code"), allow.cartesian=T)

# Collapse some columns on Ad side?

# Join RMS to Ad Intel
output = merge(RMS_example, Ad_example, by.x=c("BrandCode", "dma_code", "Week"),
               by.y=c("BrandCode", "MarketCode", "Week"))

# Sum up by matchtier/MarketCode/Week/
# Remember to sum up over weeks that cross over months
        
        