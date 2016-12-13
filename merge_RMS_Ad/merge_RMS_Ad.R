# Merges RMS and Ad Intel data based on zip, brand, and week
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
  
  # Process matches to get product_module_code (only do this once and then save string_matches file)
  # string_matches = fread("Booth/string_matching/matches_files/top100_matches.csv")
  # load('Booth/price_aggregation/Products-Corrected.RData')
  # products = products[, c("product_module_code", "product_module_descr"), with=F]
  # setkey(products, product_module_code)
  # products = unique(products)
  # string_matches = string_matches[, c("brand_code_uc", "product_module_descr", "match_tier", "BrandCode"), with=F]
  # string_matches = merge(string_matches, products, by="product_module_descr")
  # string_matches[, product_module_descr:=NULL]
  string_matches = fread("Booth/merge_RMS_Ad/string_matches.csv")
} else {
  dir_name = 7734
  #brand_code = 518917
  RMS_filenames = list.files(path = paste0("/grpshares/hitsch_shapiro_ads/data/RMS/Brand-Aggregates/",
                        toString(dir_name)), full.names=T)
  RMS_datalist = list()
  brand_codes = list()
  for(i in 1:length(RMS_filenames)){
    RMS_filename = RMS_filenames[[i]]
    RMS_datalist[[i]] = readRDS(RMS_filename)
    brand_codes[[i]] = basename(RMS_filename)
  }
  load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Stores.RData')
  load('~/merge_metadata/Store-Address.RData')
  zipborders = fread('~/merge_metadata/zipborders.csv')
  string_matches = fread('~/merge_metadata/string_matches.csv')
}

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

for(i in 1:length(RMS_datalist)){
  RMS_example = RMS_datalist[[i]]
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
  RMS_datalist[[i]] = merge(RMS_example, string_matches, by.x=c("brand_code_uc_corrected", "product_module_code"), 
                      by.y=c("brand_code_uc", "product_module_code"), allow.cartesian=T)
}

# Open all Ad Intel files and extract brand, re-accumulate, and rbind
print("Reading Ad files...")
prod_cols = c("BrandDesc","BrandVariant",
              "AdvParentCode","AdvParentDesc","AdvSubsidCode","AdvSubsidDesc",
              "ProductID","ProductDesc","PCCSubCode","PCCSubDesc","PCCMajCode",
              "PCCMajDesc","PCCIndusCode","PCCIndusDesc")
ad_filenames = list.files(path = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel/aggregated",
                          full.names=T)
ad_datalist = list()
for(i in 1:length(ad_filenames)){
  print(i)
  filename = ad_filenames[[i]]
  ad_data_full = readRDS(filename)
  print(nrow(ad_data_full))
  ad_data_full[, prod_cols:=NULL] #ignore warning here, R think prod_cols is a new column name
  
  for(j in 1:length(RMS_datalist)){
    print(j)
    if(i==1){
      ad_datalist[[j]]=list()
    }
    RMS_example = RMS_datalist[[j]]
    brand_matches = unique(RMS_example$BrandCode)
    print(brand_matches)
    if(length(brand_matches)>0){
      ad_data = ad_data_full[BrandCode %in% brand_matches]
      
      if(nrow(ad_data)>0){
        ad_data[, National_GRP:=rowSums(.SD, na.rm=T), 
                .SDcols = c("Network TV GRP 2","Syndicated TV GRP 2","Cable TV GRP 2",
                            "Spanish Language Cable TV GRP 2", "Spanish Language Network TV GRP 2")]
        ad_data[, Local_GRP:=rowSums(.SD, na.rm=T), 
                .SDcols = c("Network Clearance Spot TV GRP 3", "Syndicated Clearance Spot TV GRP 3",
                            "Spot TV GRP 3")]
        
        ad_data[, National_occ:=rowSums(.SD, na.rm=T), 
                .SDcols = c("Network TV Duration","Syndicated TV Duration","Cable TV Duration",
                           "Spanish Language Cable TV Duration", "Spanish Language Network TV Duration")]
        ad_data[, National_occ:=National_occ/30]
        ad_data[, Local_occ:=rowSums(.SD, na.rm=T),
                .SDcols = c("Network Clearance Spot TV Duration", "Syndicated Clearance Spot TV Duration",
                            "Spot TV Duration")]
        ad_data[, Local_occ:=Local_occ/30]
        
        ad_data = ad_data[, c("BrandCode", "Week", "MarketCode", 
                              "National_GRP", "Local_GRP",
                              "National_occ", "Local_occ"), with=F]
        
        ad_datalist[[j]][[i]] = copy(ad_data)
        print(nrow(ad_data))
      }
    }
  }
}
ad_datalist = lapply(ad_datalist, rbindlist)
print("Done reading Ad files!")

for(i in 1:length(RMS_datalist)){
  RMS_example = RMS_datalist[[i]]
  Ad_example = ad_datalist[[i]]
  
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
    Ad_example[CJ(unique(BrandCode), unique(MarketCode), seq(min(WeekDate), max(WeekDate), by=7))]
    #print(tail(Ad_example))
    print(nrow(Ad_example))
    # Option (a), impute 52 rows before data
    
    # Calculate Ad stock and Total values
    # Assumption that all weeks need to be filled for correct weight
    # Check that order is by weeks
    print("Calculating ad stock...")
    Ad_example[order(WeekDate)]
    
    Ad_example[, `:=`(National_GRP = cumsum(ifelse(is.na(National_GRP), 0, National_GRP)*weight^(rev(seq(.N))-1))/
                                     (weight^(rev(seq(.N))-1)), 
                      Local_GRP = cumsum(ifelse(is.na(Local_GRP), 0, Local_GRP)*weight^(rev(seq(.N))-1))/
                                  (weight^(rev(seq(.N))-1)), 
                      National_occ = cumsum(ifelse(is.na(National_occ), 0, National_occ)*weight^(rev(seq(.N))-1))/
                                     (weight^(rev(seq(.N))-1)),
                      Local_occ = cumsum(ifelse(is.na(Local_occ), 0, Local_occ)*weight^(rev(seq(.N))-1))/
                                  (weight^(rev(seq(.N))-1))),
                  by=c("BrandCode", "MarketCode")]
    
    # Under option (b), first create estimated data occuring before time interval using average
    simulated = Ad_example[, .(National_GRP = sum(National_GRP*(seq(.N)<=window))/window,
                               Local_GRP = sum(Local_GRP*(seq(.N)<=window))/window,
                               National_occ = sum(National_occ*(seq(.N)<=window))/window,
                               Local_occ = sum(Local_occ*(seq(.N)<=window))/window),
                           by=c("BrandCode", "MarketCode")]
    simulated = simulated[rep(seq(.N), each=window]
    simulated[, WeekDate:=NA, Week:=NA] #Note that simulated data does not have Week values
    # Assign all markets' codes
    Ad_example = rbind(simulated, Ad_example)
    # Implement window=T by subtracting previous values 
    # Under option (a), this will leave the first 52 rows as NA
    # Under option (b), this will leave simulated data as NA
    Ad_example[, `:=`(National_GRP = National_GRP - shift(National_GRP, window, type="lag"),
                      Local_GRP = Local_GRP - shift(Local_GRP, window, type="lag"),
                      National_occ = National_occ - shift(National_occ, window, type="lag"),
                      Local_occ = Local_occ - shift(Local_occ, window, type="lag")),
                      by=c("BrandCode", "MarketCode")]
    Ad_example[, `:=`(Total_GRP=sum(National_GRP, Local_GRP),
                      Total_occ=sum(National_occ, Local_occ)), 
               by=1:NROW(Ad_example)]
    Ad_example[, WeekDate:=NULL]
      
    # Join RMS to Ad Intel months
    print("Joining RMS to Ad Intel...")
    output = merge(RMS_example, Ad_example, by.x=c("BrandCode", "dma_code", "Week"),
                   by.y=c("BrandCode", "MarketCode", "Week"))
    
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
      saveRDS(output, file=brand_codes[[i]])
    }
  } else {
    print("Ad_example empty")
  }
}

# Also need to add competitor values once data is built the first time

        
        