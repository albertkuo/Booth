# string_matching.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: May 22, 2016
#
# This is an R script that creates data files for the Shiny string_matching app

## ================
## Set-up =========
## ================
library(data.table)
library(dplyr)
library(bit64)

source_dir = "~/Booth/string_matching"

# Additional data to help with matching
pccs = fread(paste0(source_dir, '/highlighted_categories.csv'))
pccsubs = unique(pccs$PCCSubDesc)
productgroupdict = fread(paste0(source_dir, '/productgroupdict.csv'))
pccindusdict = fread(paste0(source_dir, '/pccindusdict.csv'))

## ================
## RMS data =======
## ================
load(paste0(source_dir, '/Products-Corrected.RData'))
load(paste0(source_dir, '/Meta-Data-Corrected.RData'))
prod_meta = merge(products, meta_data)
prod_meta = prod_meta[department_descr!="GENERAL MERCHANDISE"]

## Aggregate revenue sums by brands
brands_RMS = prod_meta[,.(rev_sum = sum(revenue_RMS)), 
                       by=c("brand_code_uc","brand_descr",
                            "product_module_descr","product_group_descr",
                            "department_descr")]

## Add broad category labels from productgroupdict
brands_RMS = merge(brands_RMS, productgroupdict, 
                   by="product_group_descr", all.x=T)
brands_RMS = brands_RMS[order(rev_sum,decreasing=T)]
brands_RMS = brands_RMS[!grepl("CTL BR", brands_RMS$brand_descr)]

## Save brand data
setcolorder(brands_RMS, c("brand_code_uc","brand_descr",
                          "product_module_descr","product_group_descr",
                          "department_descr","category","rev_sum"))
saveRDS(brands_RMS, paste0(source_dir, '/string_matching_app/data/brands_RMS.rds'))

## Save union of top brands per module per year (top competitors)
top_brands = prod_meta[,.(rev_sum_year = sum(revenue_RMS)),
                       by=c("brand_code_uc","brand_descr","product_module_descr",
                            "product_group_descr","department_descr","year")]
module_sums = prod_meta[,.(module_sum = sum(revenue_RMS)),
                        by = c("product_module_descr","year")]
top_brands = merge(top_brands, module_sums, by=c("product_module_descr","year"))
top_brands[,prop_revenue:=rev_sum_year/module_sum]
top_brands_names = top_brands %>% 
  group_by(product_module_descr, year) %>% 
  arrange(desc(prop_revenue)) %>% 
  mutate(cum_sum_prop = cumsum(prop_revenue)) %>%
  filter(cum_sum_prop < .8 | (cum_sum_prop > .8 & (cum_sum_prop-prop_revenue) < .8)) 
top_brands_names = unique(top_brands_names$brand_descr)
top_brands = top_brands[brand_descr %in% top_brands_names] %>%
  group_by(brand_descr, add=TRUE) %>% mutate(prop_revenue_avg = mean(prop_revenue)) %>%
  group_by(brand_descr, product_module_descr) %>% mutate(rev_sum = sum(rev_sum_year)) %>%
  group_by(product_module_descr) %>% distinct(brand_descr, .keep_all = TRUE) 
top_brands = merge(top_brands, productgroupdict, by="product_group_descr", all.x=T)
top_brands = top_brands[c("brand_code_uc","brand_descr","product_module_descr",
                          "product_group_descr","department_descr","category",
                          "rev_sum", "prop_revenue_avg")]
saveRDS(top_brands, paste0(source_dir, '/string_matching_app/data/top_brands.rds'))

## Save prod data
prod_meta = prod_meta[,.(rev_sum = sum(revenue_RMS)),
                      by=c("upc_descr","brand_code_uc","brand_descr",
                           "product_module_descr","product_group_descr",
                           "department_descr")]
prod_meta = prod_meta[,.(upc_descr, 
                         brand_code_uc, brand_descr,
                         product_module_descr, product_group_descr,
                         department_descr, rev_sum)]
setkey(prod_meta)
prod_meta = unique(prod_meta)
saveRDS(prod_meta, paste0(source_dir, '/string_matching_app/data/prod_meta.rds'))

## ================
## Ad Intel data ==
## ================
brands_ad = readRDS(paste0(source_dir, '/brand_spend/brand_spend.rds'))
brands_ad = brands_ad[PCCSubDesc %in% pccsubs] # highlighted pccs only
brands_ad = brands_ad[order(spend_sum,decreasing=T)]
setkey(brands_ad, BrandDesc, BrandVariant)
brands_unique_ad = unique(brands_ad)
brands_unique_ad = merge(brands_unique_ad, pccindusdict, 
                         by="PCCIndusDesc", all.x=T)
brands_unique_ad = brands_unique_ad[,.(BrandCode,BrandVariant,BrandDesc,ProductDesc,
                                       PCCSubDesc,PCCMajDesc,PCCIndusDesc,
                                       category,spend_sum)]
PCC_sums = brands_unique_ad[,.(product_sum = sum(spend_sum)),
                            by=c("ProductDesc")]
brands_unique_ad = merge(brands_unique_ad, PCC_sums, by=c("ProductDesc"))
brands_unique_ad[,prop_revenue:=spend_sum/product_sum]
brands_unique_ad = brands_unique_ad[,c("BrandCode","BrandVariant","BrandDesc",
                                       "ProductDesc","PCCSubDesc","PCCMajDesc",
                                       "PCCIndusDesc","category","spend_sum","prop_revenue"),
                                    with=F]
brands_unique_ad[,prop_revenue:=round(prop_revenue,3)]
# Have to convert to ASCII for searching to work in the app
brands_unique_ad = as.data.frame(brands_unique_ad)
brands_unique_ad[,sapply(brands_unique_ad,is.character)] <- sapply(
  brands_unique_ad[,sapply(brands_unique_ad,is.character)],
  iconv,"UTF-8","ASCII",sub="")
saveRDS(brands_unique_ad, paste0(source_dir, '/string_matching_app/data/brands_ad.rds'))
