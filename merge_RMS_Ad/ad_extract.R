## ================
## Ad Intel =======
## ================
# Read Ad Intel files and extract data corresponding to brands found in Brand Aggregates
# Save brand extracts in aggregated_extracts to be used later in merge_RMS_Ad.R

RMS_input_dir = "/grpshares/hitsch_shapiro_ads/data/RMS/Brand-Aggregates"
ad_output_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel"
output_dir = "/grpshares/hitsch_shapiro_ads/data/RMS_Ad/"

#brand_code = 518917
RMS_filenames = list.files(RMS_input_dir, full.names=T, recursive=T)
brand_codes = list()
for(i in 1:length(RMS_filenames)){
  RMS_filename = RMS_filenames[[i]]
  brand_codes[[i]] = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(RMS_filename))
  dir_names[[i]] = sub('.*/(.*)','\\1', dirname(RMS_input_dir))
}
string_matches = fread('~/merge_metadata/string_matches.csv')

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
    dir_name = dir_names[[j]]
    RMS_brand_code = brand_codes[[j]]
    print(RMS_brand_code)
    brand_matches = unique(string_matches[brand_code_uc==RMS_brand_code &
                                            product_module_code==dir_name]$BrandCode)
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

        dir.create(file.path(ad_output_dir, "aggregated_extracts", toString(dir_name)), showWarnings = FALSE)
        print(nrow(ad_data))
        write.csv(ad_data, paste0(file.path(ad_output_dir, "aggregated_extracts", toString(dir_name)), "/",
                                  RMS_brand_code, "_", toString(i), ".csv"), row.names=F)
      }
    }
  }
}
