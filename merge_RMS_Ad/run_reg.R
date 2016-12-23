# # run_reg.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: December 23, 2016
#
# This is an R script that runs regression for a brand
# using data built from merge_RMS_Ad.R 

library(lfe)
library(data.table)
library(xtable)

input_dir = "/grpshares/hitsch_shapiro_ads/data/RMS_Ad"
dir_name = "1484"
build_filenames = list.files(file.path(input_dir, dir_name), full.names=T)

run_grid=T

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

if(!run_grid){
  brand_data = readRDS('Booth/500858.rds')
  brand_data[, revenue:=price*units]
  brand_data[, store_code_uc:=as.factor(store_code_uc)]
  brand_data[, Week:=as.factor(Week)]
  result = felm(log(1+revenue) ~ log(price)+promo_dummy|store_code_uc, brand_data)
  result = felm(log(1+revenue) ~ log(price)+promo_dummy|store_code_uc+Week, brand_data)
  coeffs = result$coefficients
  se = result$se
}

coeffs_list = list()

for(i in 1:length(build_filenames)){
  filename = build_filenames[[i]]
  coeffs = list()
  print(i)
  print(filename)
  brand_data = readRDS(filename)
  brand_code = sub(pattern = "(.*?)\\..*$", replacement = "\\1", basename(filename))
  brand_data[, revenue:=price*units]
  brand_data[, store_code_uc:=as.factor(store_code_uc)]
  print(summary(brand_data$Total_GRP_rival))
  print(nrow(brand_data))
  
  # Columns
  price_cols = grep("price", names(brand_data), value=T)
  price_cols = price_cols[!grepl("base_price", price_cols)]
  log_price_cols = paste0("log(", price_cols, ")")
  promo_cols = grep("promo_dummy", names(brand_data), value=T) 
  own_GRP_col = c("log(1+Total_GRP)")
  competitor_GRP_col = c("log(1+Total_GRP_rival)")
  #print(names(brand_data))
  
  # Model 1
  result = felm(log(1+revenue) ~ log(price)+promo_dummy, brand_data)
  coeffs[[1]] = cbind(as.data.table(t(result$coefficients)), as.data.table(t(result$se)))
  setnames(coeffs[[1]], paste0(names(coeffs[[1]]), "_1"))
  
  # Model 2
  result = felm(log(1+revenue) ~ log(price)+promo_dummy|store_code_uc, brand_data)
  coeffs[[2]] = cbind(as.data.table(t(result$coefficients)), as.data.table(t(result$se)))
  setnames(coeffs[[2]], paste0(names(coeffs[[2]]), "_2"))
  
  # Model 3
  brand_data[, week2:=(year(week_end)-2010)*52+(week(week_end)-1)]
  result = felm(log(1+revenue) ~ log(price)+promo_dummy+week2|store_code_uc, brand_data)
  coeffs[[3]] = cbind(as.data.table(t(result$coefficients)), as.data.table(t(result$se)))
  setnames(coeffs[[3]], paste0(names(coeffs[[3]]), "_3"))
  
  # Model 4
  brand_data[, Week:=as.factor(Week)]
  variables = paste(c(log_price_cols, "promo_dummy"), collapse="+")
  result = felm(as.formula(paste0("log(1+revenue) ~ ", variables,"|store_code_uc+Week")), brand_data)
  coeffs[[4]] = cbind(as.data.table(t(result$coefficients)), as.data.table(t(result$se)))
  setnames(coeffs[[4]], paste0(names(coeffs[[4]]), "_4"))
  
  # Model 5
  variables = paste(c(log_price_cols, promo_cols), collapse="+")
  result = felm(as.formula(paste0("log(1+revenue) ~ ", variables,"|store_code_uc+Week")), brand_data)
  coeffs[[5]] = cbind(as.data.table(t(result$coefficients)), as.data.table(t(result$se)))
  setnames(coeffs[[5]], paste0(names(coeffs[[5]]), "_5"))
  
  # Model 6
  variables = paste(c(log_price_cols, promo_cols, own_GRP_col), collapse="+")
  result = felm(as.formula(paste0("log(1+revenue) ~ ", variables,"|store_code_uc+Week")), brand_data)
  coeffs[[6]] = cbind(as.data.table(t(result$coefficients)), as.data.table(t(result$se)))
  setnames(coeffs[[6]], paste0(names(coeffs[[6]]), "_6"))
  
  # Model 7
  variables = paste(c(log_price_cols, promo_cols, own_GRP_col, competitor_GRP_col), collapse="+")
  result = felm(as.formula(paste0("log(1+revenue) ~ ", variables,"|store_code_uc+Week")), brand_data)
  coeffs[[7]] = cbind(as.data.table(t(result$coefficients)), as.data.table(t(result$se)))
  setnames(coeffs[[7]], paste0(names(coeffs[[7]]), "_7"))

  all_coeffs = do.call(cbind, coeffs)
  all_coeffs[, brand:=brand_code]
  coeffs_list[[i]] = all_coeffs
}
coeffs_summary = rbindlist(coeffs_list)
write.csv(coeffs_summary, "coeffs_table.csv", row.names = F)
