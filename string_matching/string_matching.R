# Additional data to help with matching
pccs = fread('~/Booth/string_matching/highlighted_categories.csv')
pccsubs = unique(pccs$PCCSubDesc)
productgroupdict = fread('~/Booth/string_matching/productgroupdict.csv')
pccindusdict = fread('~/Booth/string_matching/pccindusdict.csv')

# RMS data
load('~/Booth/string_matching/Products.RData')
load('~/Booth/string_matching/Meta-Data.RData')
prod_meta = merge(products, meta_data)
prod_meta = prod_meta[department_descr!="GENERAL MERCHANDISE"]
## Aggregate revenue sums by brands
brands_RMS = prod_meta[,.(rev_sum = sum(revenue_RMS)), 
                        by=c("brand_code_uc","brand_descr",
                             "product_module_descr","product_group_descr",
                             "department_descr")]
## Add own category label
brands_RMS = merge(brands_RMS, productgroupdict, 
                   by="product_group_descr", all.x=T)
brands_RMS = brands_RMS[order(rev_sum,decreasing=T)]
brands_RMS = brands_RMS[!grepl("CTL BR", brands_RMS$brand_descr)]
## Save brand data
setcolorder(brands_RMS, c("brand_code_uc","brand_descr",
                          "product_module_descr","product_group_descr",
                          "department_descr","category","rev_sum"))
saveRDS(brands_RMS, '~/Booth/string_matching/string_matching_app/data/brands_RMS.rds')
## Save prod data
prod_meta = prod_meta[,.(rev_sum = sum(revenue_RMS)),
                      by=c("upc_descr","brand_code_uc","brand_descr",
                           "product_module_descr","product_group_descr",
                           "department_descr")]
prod_meta = prod_meta[,.(upc_descr, 
                         brand_code_uc, brand_descr,
                         product_module_descr, product_group_descr,
                         department_descr,rev_sum)]
setkey(prod_meta)
prod_meta = unique(prod_meta)
saveRDS(prod_meta, '~/Booth/string_matching/string_matching_app/data/prod_meta.rds')
## Select top 100 brands
brands_top_RMS = brands_RMS[1:100]
brandnames_RMS = brands_top_RMS$brand_descr

# Ad Intel Data
brands_ad = readRDS('~/Booth/brand_spend/brand_spend.rds')
brands_ad = brands_ad[PCCSubDesc %in% pccsubs] # highlighted pccs only
brands_ad = brands_ad[order(spend_sum,decreasing=T)]
setkey(brands_ad, BrandDesc, BrandVariant)
brands_unique_ad = unique(brands_ad)
brands_unique_ad = merge(brands_unique_ad, pccindusdict, 
                         by="PCCIndusDesc", all.x=T)
brands_unique_ad = brands_unique_ad[,.(BrandCode,BrandVariant,BrandDesc,ProductDesc,
                                       PCCSubDesc,PCCMajDesc,PCCIndusDesc,
                                       category,spend_sum)]
# Have to convert to ASCII for searching to work in the app
brands_unique_ad = as.data.frame(brands_unique_ad)
brands_unique_ad[,sapply(brands_unique_ad,is.character)] <- sapply(
  brands_unique_ad[,sapply(brands_unique_ad,is.character)],
  iconv,"UTF-8","ASCII",sub="")
saveRDS(brands_unique_ad, '~/Booth/string_matching/string_matching_app/data/brands_ad.rds')
brandnames_ad = brands_unique_ad$BrandDesc # or use BrandVariant?

# Fuzzy string matching with agrep
candidates_indices = sapply(brandnames_RMS, function(x){agrep(x, brandnames_ad, fixed=T)})
candidates_names = sapply(candidates_indices, function(i){brandnames_ad[i]})

# Option 1: sort by string distance 
# candidates_distances = sapply(1:length(brandnames_RMS), function(i){stringdist(brandnames_RMS[i], candidates_names[[i]])})
# candidates_indices = sapply(1:length(candidates_indices), function(i){candidates_indices[[i]][order(candidates_distances[[i]])]})
# Option 2: sort by spend
candidates_spends = sapply(1:length(brandnames_RMS), function(i){sapply(candidates_indices[[i]],function(j){brands_unique_ad[j,spend_sum]})})
candidates_indices = sapply(1:length(candidates_indices), function(i){candidates_indices[[i]][order(candidates_spends[[i]], decreasing = T)]})
# Option 3: sort by mix of both
# candidates_distances = sapply(1:length(brandnames_RMS), function(i){stringdist(brandnames_RMS[i], candidates_names[[i]])})
# candidates_spends = sapply(1:length(brandnames_RMS), function(i){sapply(candidates_indices[[i]],function(j){brands_unique_ad[j,spend_sum]})})
# candidates_indices = sapply(1:length(candidates_indices), function(i){candidates_indices[[i]][
#   order(rank(candidates_distances[[i]])-rank(candidates_spends[[i]]))]})

# Cap at 20 candidates
#candidates_indices = sapply(candidates_indices, function(x){if(length(x)>20){x[1:20]}else{x}})
candidates_names = sapply(candidates_indices, function(i){brandnames_ad[i]})
names(candidates_names) = brandnames_RMS

#dist.distances = sapply(1:length(brandnames_RMS), function(i){stringdist(brandnames_RMS[i], dist.names[[i]])})
#dist.names_ordered = sapply(1:length(dist.names), function(i){dist.names[[i]][order(dist.distances[[i]])]})
#names(dist.names_ordered) = brandnames_RMS

# Create table of candidates
empty_candidates_table <- function(i){
  candidates_table = data.table(BrandDesc=NA,ProductDesc=NA,
                                PCCSubDesc=NA,PCCMajDesc=NA,
                                PCCIndusDesc=NA,category=NA,spend_sum=NA)
  candidates_table = cbind(brands_top_RMS[i,], candidates_table)
  return(candidates_table)
}
create_candidates_table <- function(i){
  # No candidates
  if(length(candidates_indices[[i]])==0){
    return(empty_candidates_table(i))
  }
  # No. of candidates > 0
  RMS_brand_category = brands_top_RMS[i,category]
  candidates_rows = lapply(candidates_indices[[i]], function(j)
    {brands_unique_ad[j,list(BrandDesc,ProductDesc,PCCSubDesc,
                             PCCMajDesc,PCCIndusDesc,category,spend_sum)]})
  candidates_table = Reduce(rbind, candidates_rows)
  candidates_table = candidates_table[category==RMS_brand_category] # Match category (broad category)
  if(nrow(candidates_table)==0){
    return(empty_candidates_table(i))
  } else{
    candidates_table = cbind(brands_top_RMS[rep(i,nrow(candidates_table)),], candidates_table)
    return(candidates_table)
  }
}

candidates_tables = lapply(1:length(candidates_indices),create_candidates_table)
all_candidates = Reduce(rbind, candidates_tables)
write.csv(all_candidates, "all_candidates.csv", row.names=F)


# Match groups
# groups_RMS = unique(prod_meta$product_group_descr)
# groups_RMS = groups_RMS[groups_RMS!=""]
# modules_RMS = unique(prod_meta$product_module_descr)
# modules_RMS = modules_RMS[modules_RMS!=""]
# groups_ad = unique(brands_ad$PCCSubDesc)
# groups.names = sapply(groups_RMS, function(x){agrep(x, groups_ad, value=T, ignore.case=T)})
# modules.names = sapply(modules_RMS, function(x){agrep(x, groups_ad, value=T, ignore.case=T)})
# Method 2: using the native R adist, only works for min.name
# dist.name = adist(brandnames_RMS, brandnames_ad)
# dist.name=t(apply(dist.name, 1, sort))
# 
# #min.name = apply(dist.name, 1, min)
#
# match.s1.s2<-NULL  
# for(i in 1:nrow(dist.name))
# {
#     s2.ij<-match(min.name[i],dist.name[i,])
#     s1.i<-i
#     match.s1.s2 <- rbind(data.frame(ad.ij=s2.ij,rms.i=s1.i,
#                                   ad_name=brandnames_ad[s2.ij], 
#                                   rms_name=brandnames_RMS[s1.i], 
#                                   adist=top.names[i,j]),match.s1.s2)
# }
# # and we then can have a look at the results
# 
# View(match.s1.s2)



