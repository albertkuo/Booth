# ad_extract.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: May 22, 2017
#
# Read Ad Intel files and extract data corresponding to brands found in Brand Aggregates
# Save brand extracts in aggregated_extracts to be used later in merge_RMS_Ad.R

library(data.table)

RMS_input_dir = "/grpshares/hitsch_shapiro_ads/data/RMS/Brand-Aggregates"
ad_output_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel"

RMS_filenames = list.files(RMS_input_dir, full.names=T, recursive=T)
brand_codes = list()
dir_names = list()
for(i in 1:length(RMS_filenames)){
  RMS_filename = RMS_filenames[[i]]
  brand_codes[[i]] = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(RMS_filename))
  dir_names[[i]] = sub('.*/(.*)','\\1', dirname(RMS_filename))
}
string_matches = fread('~/string_matching/string_matches.csv')

# Parameters
datastream = 2 # datastream to use for National TV, note that Local TV only has Datastream = 3

prod_cols = c("BrandDesc","BrandVariant",
              "AdvParentCode","AdvParentDesc","AdvSubsidCode","AdvSubsidDesc",
              "ProductID","ProductDesc","PCCSubCode","PCCSubDesc","PCCMajCode",
              "PCCMajDesc","PCCIndusCode","PCCIndusDesc")
ad_filenames = list.files(path = file.path(ad_output_dir, "aggregated"), full.names=T)
for(i in 1:length(ad_filenames)){
  print(i)
  filename = ad_filenames[[i]]
  ad_data_full = readRDS(filename)
  ad_data_full[, prod_cols:=NULL] #ignore warning here, R think prod_cols is a new column name

  for(j in 1:length(brand_codes)){
    RMS_brand_code = brand_codes[[j]]
    dir_name = dir_names[[j]]
    #print(RMS_brand_code)
    #print(dir_name)
    brand_matches = unique(string_matches[brand_code_uc==RMS_brand_code &
                                            product_module_code==dir_name]$BrandCode)
    if(length(brand_matches)>0){
      ad_data = ad_data_full[BrandCode %in% brand_matches]

      if(nrow(ad_data)>0){
        ad_data[, National_GRP:=rowSums(.SD, na.rm=T),
                .SDcols = paste(c("Cable TV GRP","Spanish Language Cable TV GRP"), datastream)]
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
        
        ad_data[, National_spend:=rowSums(.SD, na.rm=T),
                .SDcols = c("Network TV Spend","Syndicated TV Spend","Cable TV Spend",
                            "Spanish Language Cable TV Spend", "Spanish Language Network TV Spend")]
        ad_data[, Local_spend:=rowSums(.SD, na.rm=T),
                .SDcols = c("Network Clearance Spot TV Spend", "Syndicated Clearance Spot TV Spend",
                            "Spot TV Spend")]

        ad_data = ad_data[, c("BrandCode", "Week", "MarketCode",
                              "National_GRP", "Local_GRP",
                              "National_occ", "Local_occ",
                              "National_spend", "Local_spend"), with=F]

        dir.create(file.path(ad_output_dir, "aggregated_extracts", toString(dir_name)), showWarnings = FALSE)
        #print(nrow(ad_data))
        write.csv(ad_data, paste0(file.path(ad_output_dir, "aggregated_extracts", toString(dir_name)), "/",
                                  RMS_brand_code, "_", toString(i), ".csv"), row.names=F)
      }
    } else {
      #print("No string matches")
    }
  }
}
