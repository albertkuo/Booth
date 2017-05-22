# county_zips.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: May 22, 2017
#
# This is an R script that translates zips to borders

library(data.table)
borders = fread('~/Booth/zip_borders/borders.csv')
zipcrosscounty = fread('~/Booth/zip_borders/zipcrosscounty.csv')

# Merge
zipborders = merge(zipcrosscounty, borders, by="fips", all.x=T, allow.cartesian=T)
# Add indicator for border
zipborders[, on_border:=sapply(bordername, function(x){if(is.na(x)){return(0)}else{return(1)}})]
zipborders[, zip3:=floor(zip/100)]
zipborders = zipborders[,.(zip, zip3, fips, countyname, state, on_border, bordername)]

write.csv(zipborders, file = '~/Booth/merge_RMS_Ad/merge_metadata/zipborders.csv', row.names=F)
