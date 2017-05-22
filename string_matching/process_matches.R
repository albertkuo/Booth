# process_matches.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: May 22, 2017
#
# This is an R script that processes the manual matches
# into a simplified and more usable format.
# Remember to run the bash script cat_matches first to get "all.csv"
# from the individual brand matches files.

library(data.table)
library(bit64)

source_dir = "~/Booth/string_matching"

# Process matches to get product_module_code (only do this once and then save string_matches file)
string_matches = fread(paste0(source_dir, "/matches_files/all.csv"))
load(paste0(source_dir, "/Products-Corrected.RData"))
products = products[, c("product_module_code", "product_module_descr"), with=F]
setkey(products, product_module_code)
products = unique(products)
string_matches = string_matches[, c("brand_code_uc", "product_module_descr", "match_tier", "BrandCode"), with=F]
string_matches = merge(string_matches, products, by="product_module_descr")
string_matches[, product_module_descr:=NULL]
write.csv(string_matches, paste0(source_dir, "/string_matches.csv")
