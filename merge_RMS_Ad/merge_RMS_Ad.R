# Merges RMS and Ad Intel data based on zip, brand, and week
library(data.table)
library(bit64)

run_grid = F
if(!run_grid){
  RMS_example = readRDS("Booth/merge_RMS_Ad/536746.rds")
  load("Booth/merge_RMS_Ad/Stores.RData")        #stores
  load("Booth/merge_RMS_Ad/Store-Address.RData") #store_info
  zipborders = fread("Booth/county/zipborders.csv")
  string_matches = fread("Booth/string_matching/matches_files/top100_matches.csv")
  
  # Remember to process matches to get product_module_code (only do this once and then save string_matches file)
  load('Booth/price_aggregation/Products-Corrected.RData')
  products = products[, c("product_module_code", "product_module_descr"), with=F]
  setkey(products, product_module_code)
  products = unique(products)
  string_matches = string_matches[, c("brand_code_uc", "product_module_descr", "match_tier", "BrandCode"), with=F]
  string_matches = merge(string_matches, products, by="product_module_descr")
  string_matches[, product_module_descr:=NULL]
}

# Clean some data stuff on RMS side
RMS_example[, year:=year(week_end)]
RMS_example[, Week:=format(week_end,format="%W/%Y")]
RMS_example[, product_module_code:=dir_name]
stores = stores[, c("store_code_uc","year","store_zip3","fips_state_code","fips_county_code","dma_code"), with=F]
store_info[, Year:=strtoi(substring(DATE, 0, 4), base=10)]
store_info[, store_code_uc:=`Store ID UC`]
store_info[, c("Store ID UC", "NAME", "PARENT", "BANNER"):=NULL, with=F]
#setkey(zipborders, zip3, on_border, bordername)
#zipborders = unique(zipborders)
#zipborders[, zip:=NULL]

# Join to zip/DMA matching
RMS_example[, rollyear:=year]
store_info[, rollyear:=Year]
setkey(RMS_example, store_code_uc, rollyear)
setkey(store_info, store_code_uc, rollyear)
store_info = unique(store_info)
RMS_example = store_info[RMS_example, roll=T, rollends=c(T,F)] 
RMS_example[, c("rollyear", "Year", "DATE"):=NULL, with=F]
RMS_example = merge(RMS_example, stores, by=c("store_code_uc", "year"))
RMS_example = merge(RMS_example, zipborders, by.x=c("ZIP"), by.y=c("zip"), allow.cartesian=T)
RMS_example[, dma_code:=dma_code-400] # match with Ad Intel code format

# Join to brand matching
RMS_example = merge(RMS_example, string_matches, by.x=c("brand_code_uc_corrected", "product_module_code"), 
                    by.y=c("brand_code_uc", "product_module_code"))

# Collapse some columns on Ad side?

# Join RMS to Ad Intel
output = merge(RMS_example, Ad_example, by.x=c("BrandCode", "dma_code", "Week"),
               by.y=c("BrandCode", "MarketCode", "Week"))

# Sum up by matchtier/MarketCode/Week/
# Remember to sum up over weeks that cross over months
        
        