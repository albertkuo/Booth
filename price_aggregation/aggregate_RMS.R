# merge_RMS_Ad.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: June 16, 2017
#
# This R script handles price and quantity aggregation in RMS and Homescan Data
# It only aggregates the RMS brands that have been matched with string_matching_app

## Set-up ---------------------
library(bit64)
library(data.table)
library(foreach)
library(doParallel)

registerDoParallel(cores = NULL)

run_grid = T

if(!run_grid){
  load('./string_matching/data/Products-Corrected.RData')
  # Sample file
  load('./price_aggregation/1356240001.RData')
} else {
  load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Products-Corrected.RData')
  # Get list of brands to aggregate from string_matches
  string_matches = fread("./string_matching/string_matches.csv")
  string_matches = unique(string_matches, by="brand_code_uc_corrected")
  topbrandcodes = string_matches$brand_code_uc_corrected
  topmodulecodes = string_matches$product_module_code
  # Only keep necessary products columns
  products = products[, c("upc", "upc_ver_uc_corrected", "product_module_code",
                          "multi", "size1_amount", "brand_code_uc_corrected", 
                          "dataset_found_uc"), with=F]
  products = unique(products, by=c("upc", "upc_ver_uc_corrected"))
}

source_dir = '/grpshares/ghitsch/data/RMS-Build-2016/RMS-Processed/Modules'
output_dir = '/grpshares/hitsch_shapiro_ads/data/RMS/Brand-Aggregates'

## Helper functions -----------
# Stack upc files for a specified brand
brand_upcs = function(brand_code, module_code){
  upc_list = products[brand_code_uc_corrected==brand_code &
                        product_module_code==module_code &
                        (dataset_found_uc=='ALL' | dataset_found_uc=='RMS')]$upc
  if(length(upc_list)>0){
    upc_list = unique(upc_list)
  }
  upc_list = paste0(upc_list,'.RData')
  return(upc_list)
}

# Function to fill in NA prices
Fill.NA.Prices <- function(DT){
  vars = c("upc", "upc_ver_uc_corrected", "store_code_uc", "week_end", "base_price")
  setkeyv(DT, vars[-5])
  inds = match(vars, colnames(DT))
  bps  = DT[, inds, with=FALSE]
  str  = bps[, -5, with=FALSE]
  bps  = bps[!is.na(base_price)]
  bps  = bps[str, roll=TRUE, rollends=c(TRUE, TRUE)]
  DT[, base_price:=bps$base_price]
  DT[is.na(imputed_price), imputed_price:=base_price]
  # At this point, if base price still has NA, it must be all NA
  # These stores are effectively un-processed
  DT[is.na(base_price), processed := FALSE]
}

# Function that does brand aggregation
brandAggregator = function(DT, weight_type, promotion_threshold, processed_only = TRUE){
  if(processed_only){
    DT = DT[processed==T]
  }
  brand_codes = unique(DT$brand_code_uc_corrected)
  print(brand_codes)
  if(length(brand_codes)>0){
    DT_brands = list()
    for(i in 1:length(brand_codes)){
      if(length(brand_codes)>1){
        brand_code = brand_codes[[i]]
        DT_brand = DT[brand_code_uc_corrected==brand_code]
      } else {
        DT_brand = DT
      }
      
      # Quantity Aggregation
      # Assumption that every row in movement is unique by product-store-week
      DT_brand[, volume:=size1_amount*multi]
      DT_brand[, quantity:=units*volume]
      DT_brand[, (c("size1_amount","multi")):=NULL, with=F]
      
      # Price Aggregation
      #print("price aggregation")
      DT_brand[, revenue:=imputed_price*units]
      if(weight_type=="store-revenue/week"){
        DT_brand[, week_range:=as.integer(difftime(max(week_end), min(week_end), units="weeks")),
                 by=c("upc", "upc_ver_uc_corrected", "store_code_uc")]
        DT_brand[, weight:=sum(revenue, na.rm=T)/week_range, by=c("upc", "upc_ver_uc_corrected", "store_code_uc")]
        DT_brand[, price_weight:=weight*!is.na(imputed_price)] 
        DT_brand[, equiv_price_weighted:=price_weight*imputed_price/volume]
      } else if(weight_type=="total revenue"){
        DT_brand[, weight:=sum(revenue, na.rm=T), by=c("upc", "upc_ver_uc_corrected")]
        DT_brand[, price_weight:=weight*!is.na(imputed_price)] 
        DT_brand[, equiv_price_weighted:=price_weight*imputed_price/volume]
      }
      
      # Promotion Aggregation
      #print("promo aggregation")
      threshold = promotion_threshold
      DT_brand[, promo_weight:=weight*(!is.na(base_price)&!is.na(imputed_price))] 
      DT_brand[, promotion:=(base_price-imputed_price)/base_price > threshold]
      DT_brand[, promotion_weighted:=promo_weight*promotion]
      
      # Base Price Aggregation
      DT_brand[, base_weight:=weight*!is.na(base_price)] 
      DT_brand[, base_price_weighted:=base_weight*base_price/volume]
      
      # Aggregation Process
      #print("aggregation")
      DT_brand = DT_brand[, .(units=sum(quantity, na.rm=T), 
                              equiv_price_weighted=sum(equiv_price_weighted, na.rm=T),
                              promotion_weighted=sum(promotion_weighted, na.rm=T),
                              base_price_weighted=sum(base_price_weighted, na.rm=T),
                              price_weight_sum=sum(price_weight),
                              promo_weight_sum=sum(promo_weight),
                              base_weight_sum=sum(base_weight)),
                          by=c("brand_code_uc_corrected", "product_module_code", "store_code_uc", "week_end")]
      DT_brand[, `:=`(price=equiv_price_weighted/price_weight_sum,
                      promo_percentage=promotion_weighted/promo_weight_sum,
                      base_price=base_price_weighted/base_weight_sum)]
      DT_brand[, promo_dummy:=(base_price-price)/base_price > threshold]
      DT_brand = DT_brand[, .(brand_code_uc_corrected, product_module_code, store_code_uc, week_end,
                              price, units, base_price, promo_percentage, promo_dummy)]
      #print("Copy to list")
      DT_brands[[i]] = copy(DT_brand)
    }
    output = rbindlist(DT_brands)
    return(output)
  }
  return(DT) # empty data table
}

## Main section -------------------------------
foreach(k = 1:length(topbrandcodes)) %dopar% { 
#for(k in 1:length(topbrandcodes)){ 
#for(k in 1:1){
  print(k)
  brand_code = topbrandcodes[[k]] # Bud light = 520795, Coca-Cola R = 531429
  module_code = topmodulecodes[[k]] # Bud light = 5010, Coca-Cola = 1484
  #print(brand_code)
  #print(module_code)
  
  #print("Reading UPC files")
  upc_filenames = paste(source_dir, module_code, brand_upcs(brand_code, module_code), sep='/')
  upc_files = list()
  for(i in 1:length(upc_filenames)){
    filename = upc_filenames[[i]]
    # File might not exist because RMS-Processed doesn't contain all brands
    if(file.exists(filename)){
      load(filename)
      move = move[, c("upc", "upc_ver_uc_corrected", "store_code_uc",
                      "week_end", "base_price", "imputed_price", "units", "processed"),
                  with=F]
      upc_files[[i]] = copy(move)
    }
  }
  
  #print("Bind product files...")
  DT = rbindlist(upc_files)
  rm(upc_files)
  if(nrow(DT)>0){
    #print("Fill NA...")
    Fill.NA.Prices(DT)
    #print("Merge to products...")
    DT = merge(DT, products, by=c("upc","upc_ver_uc_corrected")) # removed allow.cartesian=T
    DT[, dataset_found_uc:=NULL]
    DT = DT[brand_code_uc_corrected==brand_code]
    #print("Begin aggregation...")
    aggregated = brandAggregator(DT, "store-revenue/week", 0.05)
    #print(head(aggregated))
    if(nrow(aggregated)>0){
      print("Saving...")
      dir.create(file.path(output_dir, toString(module_code)), showWarnings = FALSE)
      saveRDS(aggregated, file.path(output_dir, toString(module_code), paste0(toString(brand_code),'.rds')))
    } else {
      print("Skipping saving -- empty data table because processed=F for all values...")
    }
  } else {
    print("Skipping aggregating -- no non-other upc files for brand")
  }
}

