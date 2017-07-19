# ad_extract.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: July 11, 2017
#
# Read Ad Intel files and extract data corresponding to brands found in Brand-Aggregates
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
string_matches = fread('./string_matching/string_matches.csv')

# Parameters
datastream = 2 # datastream to use for National TV, note that Local TV only has Datastream = 3
national_network_option = F # whether to use Network TV (national) or National Clearance (local)
network_missing_option = 1 # whether we use missing, block_missing, or none to fill in Network TV
missing_colnames_list = list(c("Network TV Local missing"),
                                c("Network TV Local block_missing"),
                                c())
missing_colnames = missing_colnames_list[[network_missing_option]]

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
    brand_matches = unique(string_matches[brand_code_uc_corrected==RMS_brand_code &
                                            product_module_code==dir_name]$BrandCode)
    if(length(brand_matches)>0){
      ad_data = ad_data_full[BrandCode %in% brand_matches]

      if(nrow(ad_data)>0){
        if(national_network_option){
          national_colnames = c("Cable TV", "Spanish Language Cable TV", "Network TV", "Spanish Language Network TV")
        } else{
          national_colnames = c("Cable TV", "Spanish Language Cable TV")
        }
        if(national_network_option){
          local_colnames = c("Syndicated Clearance Spot TV", "Spot TV")
        } else{
          local_colnames = c("Network Clearance Spot TV", "Syndicated Clearance Spot TV", "Spot TV",
                             missing_colnames)
        }
        
        ad_data[, National_GRP:=rowSums(.SD, na.rm=T),
                .SDcols = paste(national_colnames, "GRP", datastream)]
        ad_data[, Local_GRP:=rowSums(.SD, na.rm=T),
                .SDcols = paste(local_colnames, "GRP 3")]

        ad_data[, National_occ:=rowSums(.SD, na.rm=T),
                .SDcols = paste(national_colnames, "Duration")]
        ad_data[, National_occ:=National_occ/30]
        ad_data[, Local_occ:=rowSums(.SD, na.rm=T),
                .SDcols = paste(local_colnames, "Duration")]
        ad_data[, Local_occ:=Local_occ/30]
        
        ad_data[, National_spend:=rowSums(.SD, na.rm=T),
                .SDcols = paste(national_colnames, "Spend")]
        ad_data[, Local_spend:=rowSums(.SD, na.rm=T),
                .SDcols = paste(local_colnames, "Spend")]

        ad_data = ad_data[, c("BrandCode", "Week", "MarketCode",
                              "National_GRP", "Local_GRP",
                              "National_occ", "Local_occ",
                              "National_spend", "Local_spend"), with=F]

        dir.create(file.path(ad_output_dir, "aggregated_extracts", toString(dir_name)), showWarnings = FALSE)
        write.csv(ad_data, paste0(file.path(ad_output_dir, "aggregated_extracts", toString(dir_name)), "/",
                                  RMS_brand_code, "_", toString(i), ".csv"), row.names=F)
      }
    } else {
      print("No string matches")
    }
  }
}
