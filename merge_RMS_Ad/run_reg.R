# # run_reg.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: May 22, 2017
#
# This is an R script that runs regression for a brand
# using data built from merge_RMS_Ad.R and add_competitors.R
#
# Things to check:
# 1) Store sensitivity

library(lfe)
library(data.table)
library(xtable)
library(stargazer)

# Metadata
load('~/merge_RMS_Ad/merge_metadata/Store-Table-Processed.RData')
store_sample = move_store_table[top_90_store==T]$store_code_uc

load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Products-Corrected.RData')
load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Meta-Data-Corrected.RData')
meta_data[, product_module_code:=NULL]
prod_meta = merge(products, meta_data)
brands_RMS = prod_meta[,.(rev_sum = sum(revenue_RMS)), 
                       by=c("brand_code_uc","brand_descr",
                            "product_module_code", "product_module_descr")]
brands_RMS = brands_RMS[order(-rev_sum)]
n = 100
top100_brandcodes = brands_RMS[1:n]$brand_code_uc

load('/grpshares/hitsch_shapiro_ads/data/RMS/Meta-Data/Stores.RData')
load('~/merge_RMS_Ad/merge_metadata/Store-Address.RData')
zipborders = fread('~/merge_RMS_Ad/merge_metadata/zipborders.csv')

stores = stores[, c("store_code_uc","year","store_zip3","fips_state_code","fips_county_code"), with=F]
store_info[, Year:=strtoi(substring(DATE, 0, 4), base=10)]
store_info[, store_code_uc:=`Store ID UC`]
store_info[, c("Store ID UC", "NAME", "PARENT", "BANNER"):=NULL]
store_info[, rollyear:=Year]
setkey(store_info, store_code_uc, rollyear)
store_info = unique(store_info)

# Read in files
input_dir = "/grpshares/hitsch_shapiro_ads/data/RMS_Ad"
dir_names = list.dirs(input_dir, full.names=F, recursive=F)

# Traverse through directories
coeffs_own_price = c()
coeffs_own_GRP = c()
for(k in 1:length(dir_names)){ 
#for(k in 1:1){
  print(k)
  dir_name = dir_names[[k]]
  #dir_name = "1484" # Soft drinks = 1484, Paper towels = 7734, Yogurt = 3603, Diapers = 8444
  # Light beer = 5010, Detergents = 7012, Toilet tissue = 7260, Potato Chips = 1323, Orange juice = 1040
  build_filenames = list.files(file.path(input_dir, dir_name), full.names=T)
  
  for(i in 1:length(build_filenames)){
    print(i)
    filename = build_filenames[[i]]
    brand_code = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(filename))
    
    # Only run regression for top 100 brands
    if(brand_code %in% top100_brandcodes){
      results = list()
      print(filename)
      # Select sample
      brand_data = readRDS(filename)
      brand_data = brand_data[store_code_uc %in% store_sample]
      brand_data = brand_data[year>=2010]
      # Process variables
      brand_data[, store_code_uc_factor:=as.factor(store_code_uc)]
      brand_data[, Week:=as.factor(week_end)]
      
      # Columns
      price_cols = c("price_own", "price_comp")
      log_price_cols = paste0("log(", price_cols, ")")
      promo_cols = c("promo_percentage_own", "promo_percentage_comp")
      own_GRP_col = c("log(1+Total_GRP_own)")
      competitor_GRP_col = c("log(1+Total_GRP_comp)")
      
      # Model 1
      results[[1]] = felm(log(1+units) ~ log(price_own)+promo_percentage_own+log(1+Total_GRP_own)+log(1+Total_GRP_comp)|0|0|dma_code, brand_data)
      
      # Model 2
      results[[2]] = felm(log(1+units) ~ log(price_own)+promo_percentage_own+log(1+Total_GRP_own)+log(1+Total_GRP_comp)|store_code_uc_factor|0|dma_code, brand_data)
      
      # Model 3
      brand_data[, week2:=(year(week_end)-2010)*52+(week(week_end)-1)]
      results[[3]] = felm(log(1+units) ~ log(price_own)+promo_percentage_own+week2+log(1+Total_GRP_own)+log(1+Total_GRP_comp)|store_code_uc_factor|0|dma_code, brand_data)
      
      # Model 4
      results[[4]] = felm(log(1+units) ~ log(price_own)+promo_percentage_own+log(1+Total_GRP_own)+log(1+Total_GRP_comp)|store_code_uc_factor+Week|0|dma_code, brand_data)
      
      # Model 5
      variables = paste(c(log_price_cols, "promo_percentage_own", own_GRP_col, competitor_GRP_col), collapse="+")
      results[[5]] = felm(as.formula(paste0("log(1+units) ~ ", variables,"|store_code_uc_factor+Week|0|dma_code")), brand_data)
      
      # Model 6
      print("Running Model 6")
      variables = paste(c(log_price_cols, promo_cols, own_GRP_col, competitor_GRP_col), collapse="+")
      results[[6]] = felm(as.formula(paste0("log(1+units) ~ ", variables,"|store_code_uc_factor+Week|0|dma_code")), brand_data)
      
      # Model 7
      print("Running Model 7")
      # Add address metadata
      print("Adding address...")
      brand_data[, rollyear:=year]
      setkey(brand_data, store_code_uc, rollyear)
      print(nrow(brand_data))
      brand_data = store_info[brand_data, roll=T, rollends=c(T,T)]
      print(nrow(brand_data))
      brand_data[, c("rollyear", "Year", "DATE"):=NULL]
      brand_data = merge(brand_data, stores, by=c("store_code_uc", "year"))
      # allow.cartesian because a zip might have more than one border
      brand_data = merge(brand_data, zipborders, by.x=c("ZIP"), by.y=c("zip"), allow.cartesian=T)
      brand_data[, bordername:=as.factor(bordername)]
      
      brand_data = brand_data[on_border==1]
      variables = paste(c(log_price_cols, promo_cols, own_GRP_col, competitor_GRP_col), collapse="+")
      results[[7]] = felm(as.formula(paste0("log(1+units) ~ ", variables,"|store_code_uc+bordername:Week|0|dma_code")), brand_data)
      
      #print(results[[7]]$coefficients)
      coeffs_own_price = c(coeffs_own_price, results[[7]]$coefficients[1])
      coeffs_own_GRP = c(coeffs_own_GRP, results[[7]]$coefficients[length(results[[7]]$coefficients)-1])
      #stargazer(results, type="text", title=brand_code, keep.stat="n")
    }
  }
}
print(coeffs_own_price)
print(coeffs_own_GRP)

# The formula specification is a response variable followed by a four part formula. The first part
# consists of ordinary covariates, the second part consists of factors to be projected out. The third
# part is an IV-specification. The fourth part is a cluster specification for the standard errors. I.e.
# something like y ~ x1 + x2 | f1 + f2 | (Q|W ~ x3+x4) | clu1 + clu2 where y is the
# response, x1,x2 are ordinary covariates, f1,f2 are factors to be projected out, Q and W are covariates
# which are instrumented by x3 and x4, and clu1,clu2 are factors to be used for computing cluster
# robust standard errors. Parts that are not used should be specified as 0, except if it’s at the end of the
# formula, where they can be omitted. The parentheses are needed in the third part since | has higher
# precedence than ~. Multiple left hand sides like y|w|x ~ x1 + x2 |f1+f2|... are allowed.
# Interactions between a covariate x and a factor f can be projected out with the syntax x:f. The terms
# in the second and fourth parts are not treated as ordinary formulas, in particular it is not possible
# with things like y ~ x1 | x*f, rather one would specify y ~ x1 + x | x:f + f. Note that f:x
# also works, since R’s parser does not keep the order. This means that in interactions, the factor must
# be a factor, whereas a non-interacted factor will be coerced to a factor. I.e. in y ~ x1 | x:f1 + f2,
# the f1 must be a factor, whereas it will work as expected if f2 is an integer vector.


