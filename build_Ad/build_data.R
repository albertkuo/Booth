# build_data.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: June 13, 2017
#
# This is an R script that will build R Formatted Ad Intel Files
# using Nielsen_Raw tsv formatted files.

## ================
## Set-up =========
## ================
library(data.table)
library(bit64)
library(foreach)
library(doParallel)
library(lubridate)

registerDoParallel(cores = NULL)

source_dir = "/nielsen_raw/Ad_Intel"
output_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel/aggregated"
missing_network_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel/missing_network"

prod_cols = c("BrandCode","BrandDesc","BrandVariant",
              "AdvParentCode","AdvParentDesc","AdvSubsidCode","AdvSubsidDesc",
              "ProductID","ProductDesc","PCCSubCode","PCCSubDesc","PCCMajCode",
              "PCCMajDesc","PCCIndusCode","PCCIndusDesc")
id_cols = c(prod_cols,"Week","MarketCode")

# Path names for traversal through year and month directories ----------
dirs = list.dirs(full.names=F,recursive=F,path=source_dir)
years = as.character(c(2010:2014))
months = c("01","02","03","04","05","06","07","08","09","10","11","12")
monthpath.strings = vector("list",length(years)*12)
for(i in 1:length(years)){
  yeardir = dirs[grep(paste0(years[i],".*?extracted"),dirs)]
  monthdirs = paste0(years[i],months)
  for(k in 1:length(monthdirs)){
    monthpath.strings[(i-1)*12+k] = paste(source_dir,yeardir,monthdirs[k],sep="/")
  }
}

# Static Reference files ----------
static_ref_dir = "2010_BackData_20160212_extracted/Static_Reference"
mediatype <- fread(paste(source_dir,static_ref_dir,"RDMP_RDC_Y_2016_02_08-2016_02_08_MET/RDMP_RDC_Y_2016_02_08-2016_02_08_MET",sep="/"))
mkt <- fread(paste(source_dir,static_ref_dir,"RDMP_RDC_Y_2016_02_08-2016_02_08_MKT/RDMP_RDC_Y_2016_02_08-2016_02_08_MKT",sep="/"))
mktcodes = mkt[3:nrow(mkt),MarketCode]

## ==========================
## Helper functions =========
## ==========================
# Read brand, product, and advertiser and merge them ----------
read_prod <- function(monthpath.string){
  pccfilename = list.files(pattern="RDC.*?PCC",path=monthpath.string)
  pcc <- fread(paste(monthpath.string,pccfilename,pccfilename,sep="/"),showProgress=F)
  brdfilename = list.files(pattern="RDC.*?BRD",path=monthpath.string)
  brd <- fread(paste(monthpath.string,brdfilename,brdfilename,sep="/"),showProgress=F)
  advfilename = list.files(pattern="RDC.*?ADV",path=monthpath.string)
  adv <- fread(paste(monthpath.string,advfilename,advfilename,sep="/"),showProgress=F)
  
  pccbrd = merge(pcc, brd, by="ProductID")
  pccbrdadv = merge(pccbrd, adv, by=c("AdvParentCode","AdvSubsidCode"))
  pccbrdadv[, PCCSubCode.y:=NULL]
  setnames(pccbrdadv, "PCCSubCode.x", "PCCSubCode")
  setcolorder(pccbrdadv, prod_cols)
  return(pccbrdadv)
}

# Read impressions and collapse columns ----------
read_imp <- function(monthpath.string, impfilename){
  imp <- fread(paste(monthpath.string,impfilename,impfilename,sep="/"),showProgress=F)
  if(grepl("SR",impfilename)){
    # Use sum of 'Female/female' columns and 'Male' columns as imp_total for spot radio
    dem.cols = grep("male|Male",names(imp)) 
    imp[, imp_total:=rowSums(.SD), .SDcols = dem.cols]
    # 'ww' is working women, a subset of the female demographic
    remove.cols = append(dem.cols, grep("ww",names(imp)))
    imp[, (remove.cols):=NULL]
  } else {
    # Use TV_HH as imp_total for television media
    imp[, imp_total:=TV_HH]
    remove.cols = grep("male|Male|ww|children",names(imp))
    imp[, (remove.cols):=NULL]
  }
  return(imp)
}

# Read UE and collapse columns ----------
read_ue <- function(uepath.string, uefilename){
  ue <- fread(paste(uepath.string,uefilename,uefilename,sep="/"),showProgress=F)
  ue[, `:=`(StartDate=as.Date(StartDate, format="%m/%d/%y"),
            EndDate=as.Date(EndDate, format="%m/%d/%y"))]
  if(grepl("SR",uefilename)){
    # Use sum of 'Female/female' columns and 'Male' columns as ue_total for spot radio
    dem.cols = grep("male|Male",names(ue)) 
    ue[, ue_total:=rowSums(.SD), .SDcols = dem.cols]
    # 'ww' is working women, a subset of the female demographic
    remove.cols = append(dem.cols,grep("ww",names(ue)))
    ue[, (remove.cols):=NULL]
  } else {
    # Use TV_HH as ue_total for television media
    ue[, ue_total:=TV_HH]
    remove.cols = grep("male|Male|ww|children",names(ue))
    ue[, (remove.cols):=NULL]
  }
  return(ue)
}

# Read occurrence and merge with product ----------
join_occ_prod <- function(monthpath.string, occfilename, product){
  occ <- fread(paste(monthpath.string,occfilename,occfilename,sep="/"),showProgress=F)
  if(grepl("EN",occfilename)){
    setnames(occ, "PrimeBrandCode", "PrimBrandCode")
  }
  if("Imp2Plus" %in% names(occ)){
    occ[,Imp2Plus:=as.numeric(Imp2Plus)]
  }
  occ[,`:=`(AdDate=as.Date(AdDate, format="%m/%d/%Y"),
            PrimBrandCode=as.integer(PrimBrandCode),
            ScndBrandCode=as.integer(ScndBrandCode),
            TerBrandCode=as.integer(TerBrandCode))]
  prim = merge(product,occ,by.x="BrandCode",by.y="PrimBrandCode", allow.cartesian=T) 
  scnd = merge(product,occ,by.x="BrandCode",by.y="ScndBrandCode", allow.cartesian=T)
  ter = merge(product,occ,by.x="BrandCode",by.y="TerBrandCode", allow.cartesian=T)
  prim$PrimBrandCode = prim$BrandCode
  scnd$ScndBrandCode = scnd$BrandCode
  ter$TerBrandCode = ter$BrandCode
  prodocc = rbind(prim,scnd,ter,fill=T)
  return(prodocc)
}

# Clean GRP column names ---------- 
change_name <- function(str){
  str = gsub("_1"," GRP 1", str)
  str = gsub("_2"," GRP 2", str)
  str = gsub("_3"," GRP 3", str)
  str = gsub("_4"," GRP 4", str)
  str = gsub("_NA", " GRP", str)
  return(str)
}

# Impute missing impressions for Spot TV ---------- 
impute_table = data.table(month = c(1,3,4,6,8,9,10,12),
                          month1 = c("02","02","02","05","07","07","07","11"),
                          month2 = c("","05","05","07","11","11","11",""),
                          weight1 = c(1,2/3,1/3,1/2,3/4,1/2,1/4,1),
                          weight2 = c(0,1/3,2/3,1/2,1/4,1/2,3/4,0))

impute_imp <- function(prodocc, monthpath.string){
  # Exclude PeriodYearMonth from keys because that forces us to use Nielsen's imputation method
  keys_imp = c("DistributorID","DayOfWeek","TimeIntervalNumber")
  identifier_cols = names(prodocc)
  month_int = strtoi(substring(basename(monthpath.string), 5), base=10)
  params = impute_table[month==month_int]
  substring(monthpath.string, nchar(monthpath.string)-1) = params$month1
  impfilename = list.files(pattern = "IMPC.*?SP", path = monthpath.string)
  imp = read_imp(monthpath.string, impfilename)
  imp = imp[HispanicFlag=='N']
  imp[, c("MediaTypeID","PeriodYearMonth"):=NULL]
  prodoccimp = merge(prodocc, imp, by=keys_imp, all.x=T) # removed all.cartesian=T
  if(params$weight2!=0){
    substring(monthpath.string, nchar(monthpath.string)-1) = params$month2
    impfilename = list.files(pattern = "IMPC.*?SP", path = monthpath.string)
    imp = read_imp(monthpath.string, impfilename)
    imp = imp[HispanicFlag=='N']
    imp = imp[, append(keys_imp, "TV_HH"), with=F]
    names(imp) = append(keys_imp, "TV_HH_2")
    prodoccimp2 = merge(prodocc, imp, by=keys_imp, all.x=T)
    prodoccimp = merge(prodoccimp, prodoccimp2, by=identifier_cols)
    prodoccimp[, imp_total:=params$weight1*TV_HH+params$weight2*TV_HH_2]
    prodoccimp[, TV_HH_2:=NULL]
  }
  return(prodoccimp)
}

## =================================================================
## Traverse through year and month directories in parallel =========
## =================================================================
# Parallelization stops running before all months are done, need to fix; works when not in parallel
#foreach(i = 1:length(monthpath.strings)) %dopar% { 
#for(i in 1:length(monthpath.strings)){
for(i in 1:1){
  monthpath.string = monthpath.strings[[i]]
  uepath.string = paste(dirname(monthpath.string), "UE", sep="/")
  print(basename(monthpath.string))
  
  product = read_prod(monthpath.string)
  
  # GRP Media ----------
  NATIONAL_TV = 1
  LOCAL_TV = 2
  SPOT_RADIO = 3
  occ_group1 = "(BN|CN|BS|EC|EN)" # National TV, HispanicFlag = "N" or "Y"
  occ_group2 = "(SP|SC|NC)"       # Local TV, HispanicFlag = match
  occ_group3 = "(SR)"             # Spot Radio
  occ_groups = c(occ_group1,occ_group2,occ_group3)
  imp_groups = c("NT","SP","SR")
  
  for(j in 1:length(occ_groups)){
    impfilename = list.files(pattern = paste0("IMPC.*?",imp_groups[j]), path = monthpath.string)
    imp = read_imp(monthpath.string, impfilename)
    
    uefilename = list.files(pattern = paste0("UE.*?",imp_groups[j]), path = uepath.string)
    ue = read_ue(uepath.string, uefilename)
    
    occfilenames = list.files(pattern = paste0("OCC.*?",occ_groups[j]), path = monthpath.string)
    for(occfilename in occfilenames){
      prodocc = join_occ_prod(monthpath.string, occfilename, product)
      
      if(j==NATIONAL_TV){
        # Join to impressions
        # Check HispanicFlag
        if(grepl("(EC|EN)",occfilename)){
          imphisp = imp[HispanicFlag=="Y"]
          uehisp = ue[HispanicFlag=="Y"]
        } else {
          imphisp = imp[HispanicFlag=="N"]
          uehisp = ue[HispanicFlag=="N"]
        }
        # Check ImpressionType
        p = merge(prodocc[ImpressionType=="P"], imphisp,
                  by=c("NielsenProgramCode", "TelecastNumber"), allow.cartesian=T)
        t = prodocc[ImpressionType=="T"]
        if(nrow(t)>0){
          imphisp[, ImpressionDate:=as.Date(ImpressionDate, format="%b %d %Y")]
          t = merge(t, imphisp,
                    by.x=c("AdDate","DistributorID","TimeIntervalNumber"),
                    by.y=c("ImpressionDate","DistributorID","TimeIntervalNumber"),
                    allow.cartesian=T)
          prodoccimp = rbind(p,t,fill=T)
        } else {prodoccimp = p}
        
        # Join to UE
        prodoccimp[,rollDate:=AdDate]
        uehisp[,rollDate:=StartDate]
        setkey(prodoccimp, rollDate)
        setkey(uehisp, rollDate)
        prodoccimpue = uehisp[prodoccimp, roll=TRUE]
      } else if(j==LOCAL_TV){
        imp = imp[HispanicFlag=='N'] # 'Y' is a subset of 'N'
        keys_imp = c("PeriodYearMonth","DistributorID","DayOfWeek","TimeIntervalNumber")
        prodoccimp = merge(prodocc, imp, by=keys_imp)
        
        # Impute by sending orphan occurrences to a function
        impute_prodoccimp = prodocc[PeriodYearMonth!=basename(monthpath.string)]
        if(nrow(impute_prodoccimp)>0){
          prodoccimp2 = impute_imp(impute_prodoccimp, monthpath.string)
          print(nrow(prodoccimp2))
          prodoccimp = rbind(prodoccimp, prodoccimp2, fill=T)
        }
        
        prodoccimp[,rollDate:=AdDate]
        ue[,rollDate:=StartDate]
        setkey(prodoccimp, MarketCode, HispanicFlag, rollDate)
        setkey(ue, MarketCode, HispanicFlag, rollDate)
        prodoccimpue = ue[prodoccimp, roll=TRUE]
      } else if(j==SPOT_RADIO){
        imp[,AdDate:=as.Date(AdDate, format="%m/%d/%Y")]
        prodoccimp = merge(prodocc, imp,
                           by.x=c("AdDate","AdTime","MarketCode","DistributorID","RadioDaypartID","AdCode"),
                           by.y=c("AdDate","AdTime","MarketCode","DistributorID","Daypart_id","AdCode"),
                           allow.cartesian=T)
        prodoccimp[,`:=`(DataStreamID=NA, Duration=NA)]
        
        prodoccimp[,rollDate:=AdDate]
        ue[,rollDate:=StartDate]
        setkey(prodoccimp, MarketCode, rollDate)
        setkey(ue, MarketCode, rollDate)
        prodoccimpue = ue[prodoccimp, roll=TRUE]
      }
      
      # Note: only adds columns for which there are corresponding impressions
      if(nrow(prodoccimpue)>0){
        prodoccimpue = merge(prodoccimpue,mediatype,by.x="MediaTypeID.x",by.y="MediaTypeID")
        mediatypestr = prodoccimpue$MediaTypeDesc[1]
        if(!"Spend" %in% names(prodoccimpue)){prodoccimpue[, Spend:=NA]}
        
        # Aggregate sums by id_cols and cast (opposite of melt) data
        prodoccimpue[, GRP:=(imp_total/ue_total)*100]
        prodoccimpue$GRP[is.na(prodoccimpue$GRP)] = 0
        prodoccimpue[, Week:=ceiling_date(AdDate, "week")]
        prodoccimpue[, Count:=1]
        prodoccimpue = prodoccimpue[,.(GRP_sum = sum(GRP, na.rm=T), Spend_sum = sum(Spend, na.rm=T),
                                       Duration_sum = sum(Duration, na.rm=T), Number = sum(Count, na.rm=T)),
                                    by=c(id_cols,"MediaTypeDesc","DataStreamID")]
        # Use fun=mean because all the values are the same
        prodoccimpue = dcast(prodoccimpue, as.formula(paste0(paste(c(id_cols,"Spend_sum","Duration_sum","Number"), collapse="+"),
                                                             " ~ MediaTypeDesc+DataStreamID")),
                             fun=mean, value.var=c("GRP_sum")) 
        
        # Merge to build data
        setnames(prodoccimpue, c("Spend_sum", "Duration_sum", "Number"),
                 paste(mediatypestr,c("Spend", "Duration", "Number")))
        names(prodoccimpue) = sapply(names(prodoccimpue), change_name)
        if(exists("aggregated")){
          aggregated = merge(aggregated,prodoccimpue,by=id_cols, all = TRUE)
        } else {aggregated = prodoccimpue}
      }
    }
  }
  
  # Using National TV as Local TV ----------
  print("New section...")
  occ <- readRDS(paste0(missing_network_dir,"/",basename(monthpath.string),".rds"))
  occ[,`:=`(AdDate=as.Date(AdDate, format="%m/%d/%Y"),
            PrimBrandCode=as.integer(PrimBrandCode),
            ScndBrandCode=as.integer(ScndBrandCode),
            TerBrandCode=as.integer(TerBrandCode))]
  prim = merge(product,occ,by.x="BrandCode",by.y="PrimBrandCode", allow.cartesian=T) 
  scnd = merge(product,occ,by.x="BrandCode",by.y="ScndBrandCode", allow.cartesian=T)
  ter = merge(product,occ,by.x="BrandCode",by.y="TerBrandCode", allow.cartesian=T)
  prim$PrimBrandCode = prim$BrandCode
  scnd$ScndBrandCode = scnd$BrandCode
  ter$TerBrandCode = ter$BrandCode
  prodocc = rbind(prim,scnd,ter,fill=T)
  
  impfilename = list.files(pattern = paste0("IMPC.*?",imp_groups[LOCAL_TV]), path = monthpath.string)
  imp = read_imp(monthpath.string, impfilename)
  imp = imp[HispanicFlag=='N']
  print(names(imp))
  print(table(imp$DataStreamID, useNA="always"))
  keys_imp = c("PeriodYearMonth","DistributorID","DayOfWeek","TimeIntervalNumber")
  prodoccimp = merge(prodocc, imp, by=keys_imp)
  impute_prodoccimp = prodocc[PeriodYearMonth!=basename(monthpath.string)]
  if(nrow(impute_prodoccimp)>0){
    prodoccimp2 = impute_imp(impute_prodoccimp, monthpath.string)
    print(nrow(prodoccimp2))
    prodoccimp = rbind(prodoccimp, prodoccimp2, fill=T)
  }
  print(table(prodoccimp$DataStreamID, useNA="always"))
  
  uefilename = list.files(pattern = paste0("UE.*?",imp_groups[LOCAL_TV]), path = uepath.string)
  ue = read_ue(uepath.string, uefilename)
  prodoccimp[,rollDate:=AdDate]
  ue[,rollDate:=StartDate]
  setkey(prodoccimp, MarketCode, HispanicFlag, rollDate)
  setkey(ue, MarketCode, HispanicFlag, rollDate)
  prodoccimpue = ue[prodoccimp, roll=TRUE]
  
  if(nrow(prodoccimpue)>0){
    mediatypestr = "Network TV Local"
    
    prodoccimpue[, GRP:=(imp_total/ue_total)*100]
    prodoccimpue$GRP[is.na(prodoccimpue$GRP)] = 0
    prodoccimpue[, Week:=ceiling_date(AdDate, "week")]
    prodoccimpue[, Count:=1]
    print(table(prodoccimpue$DataStreamID, useNA="always"))
    prodoccimpue = prodoccimpue[,.(GRP_sum = sum(GRP, na.rm=T), Spend_sum = sum(Spend, na.rm=T),
                                   Duration_sum = sum(Duration, na.rm=T), Number = sum(Count, na.rm=T)),
                                by=c(id_cols,"MediaTypeDesc","DataStreamID","block_missing")]
    print(table(prodoccimpue$DataStreamID, useNA="always"))
    prodoccimpue = dcast(prodoccimpue, as.formula(paste0(paste(c(id_cols,"Spend_sum","Duration_sum","Number"), collapse="+"),
                                                         " ~ MediaTypeDesc+DataStreamID+block_missing")),
                         fun=mean, value.var=c("GRP_sum")) 
    setnames(prodoccimpue, c("Spend_sum", "Duration_sum", "Number"),
             paste(mediatypestr,c("Spend", "Duration", "Number")))
    print(names(prodoccimpue))
    names(prodoccimpue) = sapply(names(prodoccimpue), change_name)
    
    # Add block_missing columns?
    # prodoccimpue[, Network Clearance Missing:=
    
    if(exists("aggregated")){
      aggregated = merge(aggregated,prodoccimpue,by=id_cols, all = TRUE)
    } else {aggregated = prodoccimpue}
  }
  
  # Non-GRP Media (no impressions or UE file) ----------
  imp2plus = 1
  occ_group4 = "(NI|LI|TN|TR)"             # Has imp2plus column
  occ_group5 = "(FS|LM|LN|LS|NM|NN|NS|OU)" # Does not have imp2plus column
  #occ_group6 = "(LC)"                      # Local/Regional Cable TV doesn't have imp
  occ_groups = c(occ_group4, occ_group5)
  
  for(j in 1:length(occ_groups)){
    occfilenames = list.files(pattern = paste0("OCC.*?",occ_groups[j]), path = monthpath.string)
    for(occfilename in occfilenames){
      prodocc = join_occ_prod(monthpath.string,occfilename,product)
      
      if(nrow(prodocc)>0){
        prodocc[, Week:=ceiling_date(AdDate, "week")]
        prodocc = merge(prodocc,mediatype,by="MediaTypeID")
        mediatypestr = prodocc$MediaTypeDesc[1]
        
        if(j==imp2plus){
          prodocc = prodocc[,.(imp_sum = sum(Imp2Plus, na.rm=T), Spend_sum = sum(Spend, na.rm=T)),
                            by=c(id_cols, "MediaTypeDesc")]
          prodocc = dcast(prodocc, as.formula(paste0(paste(c(id_cols,"Spend_sum"),collapse="+"),"~ MediaTypeDesc")),
                          fun=mean, value.var=c("imp_sum"))
          setnames(prodocc, c("Spend_sum"), paste(mediatypestr, c("Spend")))
          last_col = length(names(prodocc))
          setnames(prodocc, c(last_col), paste(names(prodocc)[last_col], "Imp"))
        } else {
          prodocc = prodocc[,.(Spend_sum = sum(Spend)), by=c(id_cols, "MediaTypeDesc")]
          prodocc = dcast(prodocc, as.formula(paste0(paste(id_cols,collapse="+"),"~ MediaTypeDesc")),
                          fun=mean, value.var=c("Spend_sum"))
          last_col = length(names(prodocc))
          setnames(prodocc, c(last_col), paste(names(prodocc)[last_col], "Spend"))
        }
        
        # Merge to build data
        if(exists("aggregated")){
          aggregated = merge(aggregated,prodocc,by=id_cols,all=TRUE)
        } else {aggregated = prodocc}
      }
    }
  }
  
  if(exists("aggregated")){
    # Remove dummy column "Spot Radio Duration"
    if("Spot Radio Duration" %in% names(aggregated)){
      aggregated[, "Spot Radio Duration":=NULL]
    }
    
    # Duplicate national measures into all markets
    aggregated2 = aggregated[rep(aggregated[,.I[MarketCode==0]],each=209)]
    # Assign all markets' codes
    aggregated2[, MarketCode:=rep(mktcodes,nrow(aggregated2)/209)] 
    # Replace national market code (0) with first non-national market code (100)
    aggregated[MarketCode==0, MarketCode:=100] 
    aggregated = rbind(aggregated,aggregated2)
    aggregated[, MarketCode:=as.integer(MarketCode)]
    rm(aggregated2)
    
    # Save RDS file for the month to output_dir
    print(paste("Saving",basename(monthpath.string)))
    #saveRDS(aggregated, paste0(output_dir,"/",basename(monthpath.string),"_aggregated.rds"))
    saveRDS(aggregated, "test_aggregated.rds")
    rm(aggregated)
  } else{
    print("LIKELY ERROR: aggregated is empty")
  }
}

