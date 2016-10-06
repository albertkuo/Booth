# build_data_health.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: September 26, 2016
#
# This is an R script that will build R Formatted Ad Intel files 
# for the health insurance category using Nielsen_Raw tsv formatted files.

## ================
## Set-up =========
## ================
library(data.table)
library(foreach)
library(doParallel)

registerDoParallel(cores = NULL)

source_dir = "/nielsen_raw/Ad_Intel"
output_dir = "/grpshares/hitsch_shapiro_ads/data/Ad_Intel/aggregated_health"
product_code = "B212" # Health = B212, not dental = 2917, 2927

prod_cols = c("BrandCode","BrandDesc","BrandVariant",
              "AdvParentCode","AdvParentDesc","AdvSubsidCode","AdvSubsidDesc",
              "ProductID","ProductDesc","PCCSubCode","PCCSubDesc","PCCMajCode",
              "PCCMajDesc","PCCIndusCode","PCCIndusDesc")
id_cols = c(prod_cols,"Week","MarketCode")

# Path names for traversal through year and month ----------
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
pgt <- fread(paste(source_dir,static_ref_dir,"RDMP_RDC_Y_2016_02_08-2016_02_08_PGT/RDMP_RDC_Y_2016_02_08-2016_02_08_PGT",sep="/"))

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
  # Sum 65+ columns
  eld.cols = grep("65",names(imp)) # over 65
  imp[, eld_total:=rowSums(.SD), .SDcols = eld.cols]
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
  prim = merge(product,occ,by.x="BrandCode",by.y="PrimBrandCode") 
  scnd = merge(product,occ,by.x="BrandCode",by.y="ScndBrandCode")
  ter = merge(product,occ,by.x="BrandCode",by.y="TerBrandCode")
  prim$PrimBrandCode = prim$BrandCode
  scnd$ScndBrandCode = scnd$BrandCode
  ter$TerBrandCode = ter$BrandCode
  productocc = rbind(prim,scnd,ter,fill=T)
  return(productocc)
}

# Read TV Program and merge with TV occurrences ----------
join_tv_program <- function(monthpath.string, productocc){
  pgmfilename = list.files(pattern="RDC.*?PGM",path=monthpath.string)
  pgm <- fread(paste(monthpath.string,pgmfilename,pgmfilename,sep="/"),showProgress=F)
  tvp = merge(pgm,pgt,by="TVProgTypeCode")
  productocc = merge(productocc,tvp,by=c("NielsenProgramCode","MonitorPlusProgramCode"))
  return(productocc)
}

# Clean GRP column names ---------- 
change_name <- function(str){
  str = gsub("_sum_mean_", " ", str)
  str = gsub("_NA", "", str)
  str = gsub("_", " ", str)
  return(str)
}

## =================================================================
## Traverse through year and month directories in parallel =========
## =================================================================
foreach(i = 1:length(monthpath.strings)) %dopar% {
  monthpath.string = monthpath.strings[[i]]
  uepath.string = paste(dirname(monthpath.string),"UE",sep="/")
  
  pccbrdadv = read_prod(monthpath.string)
  product = pccbrdadv[PCCSubCode==product_code]
  product = product[!(ProductID %in% c(2917,2927))] # not dental
  
  # GRP Media ----------
  print("Merging with GRP media")
  NATIONAL_TV = 1
  LOCAL_TV = 2
  SPOT_RADIO = 3
  group1 = "(BN|CN|BS|EC|EN)" # National TV, HispanicFlag = "N" or "Y"
  group2 = "(SP|SC|NC)"       # Local TV, HispanicFlag = match
  group3 = "(SR)"             # Spot Radio
  groups = c(group1,group2,group3)
  imp_groups = c("NT","SP","SR")
  
  for(j in 1:length(groups)){
    impfilename = list.files(pattern = paste0("IMPC.*?",imp_groups[j]), path = monthpath.string)
    imp = read_imp(monthpath.string, impfilename)
    
    uefilename = list.files(pattern = paste0("UE.*?",imp_groups[j]), path = uepath.string)
    ue = read_ue(uepath.string, uefilename)
    
    occfilenames = list.files(pattern = paste0("OCC.*?",groups[j]),path = monthpath.string)
    for(occfilename in occfilenames){
      productocc = join_occ_prod(monthpath.string, occfilename, product)
      
      if(j==NATIONAL_TV){
        productocc = join_tv_program(monthpath.string, productocc)
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
        p = merge(productocc[ImpressionType=="P"], imphisp,
                  by=c("NielsenProgramCode", "TelecastNumber"), allow.cartesian=T)
        t = productocc[ImpressionType=="T"]
        if(nrow(t)>0){
          imphisp[,ImpressionDate:=as.Date(ImpressionDate, format="%b %d %Y")]
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
        keys_imp = c("PeriodYearMonth","DistributorID","DayOfWeek","TimeIntervalNumber")
        prodoccimp = merge(productocc, imp, by=keys_imp)
        
        prodoccimp[,rollDate:=AdDate]
        ue[,rollDate:=StartDate]
        setkey(prodoccimp, MarketCode, HispanicFlag, rollDate)
        setkey(ue, MarketCode, HispanicFlag, rollDate)
        prodoccimpue = ue[prodoccimp, roll=TRUE]
      } else if(j==SPOT_RADIO){
        imp[,AdDate:=as.Date(AdDate, format="%m/%d/%Y")]
        prodoccimp = merge(productocc, imp,
                     by.x=c("AdDate","AdTime","MarketCode","DistributorID","RadioDaypartID","AdCode"),
                     by.y=c("AdDate","AdTime","MarketCode","DistributorID","Daypart_id","AdCode")) #one column's name is different
        prodoccimp[,`:=`(DataStreamID=NA, Duration=NA)]
        
        prodoccimp[,rollDate:=AdDate]
        ue[,rollDate:=StartDate]
        setkey(prodoccimp, MarketCode, rollDate)
        setkey(ue, MarketCode, rollDate)
        prodoccimpue = ue[prodoccimp, roll=TRUE]
      }
      
      # Note: only adds columns for which there are impressions
      if(nrow(prodoccimpue)>0){
        prodoccimpue = merge(prodoccimpue,mediatype,by.x="MediaTypeID.x",by.y="MediaTypeID")
        mediatypestr = prodoccimpue$MediaTypeDesc[1]
        if(!"Spend" %in% names(prodoccimpue)){prodoccimpue[, Spend:=NA]}
        
        # Aggregate sums and cast (opposite of melt) data
        prodoccimpue[, `:=`(GRP=(imp_total/ue_total)*100, GRP_eld=(eld_total/ue_total)*100)]
        prodoccimpue[, Week:=format(AdDate,format="%W/%Y")]
        prodoccimpue[, Count:=1]
        prodoccimpue2 = prodoccimpue[,.(GRP_sum = sum(GRP), GRP_eld_sum = sum(GRP_eld), Spend_sum = sum(Spend),
                       Duration_sum = sum(Duration), Number = sum(Count)),
                    by=c(id_cols,"MediaTypeDesc","DataStreamID")]
        cast_values = c("GRP_sum","GRP_eld_sum")
        if("TVDaypartCode" %in% names(prodoccimpue)){
          prodoccimpue[, is_daytime:=as.numeric(TVDaypartCode=='DT')]
          prodoccimpue[, GRP_daytime:=GRP*is_daytime]
          prodoccimpue_day = prodoccimpue[,.(GRP_daytime_sum = sum(GRP_daytime)), 
                          by=c(id_cols,"MediaTypeDesc","DataStreamID")]
          prodoccimpue2 = merge(prodoccimpue_day,prodoccimpue2,by=c(id_cols,"MediaTypeDesc","DataStreamID"))
          cast_values = c(cast_values,"GRP_daytime_sum")
        }
        if("TVProgTypeCode" %in% names(prodoccimpue)){
          prodoccimpue[, is_news:=as.numeric(TVProgTypeCode=='N')]
          prodoccimpue[, GRP_news:=GRP*is_news]
          prodoccimpue_news = prodoccimpue[,.(GRP_news_sum = sum(GRP_news)),
                           by=c(id_cols,"MediaTypeDesc","DataStreamID")]
          prodoccimpue2 = merge(prodoccimpue_news,prodoccimpue2,by=c(id_cols,"MediaTypeDesc","DataStreamID"))
          cast_values = c(cast_values,"GRP_news_sum")
        }
        prodoccimpue = dcast(prodoccimpue2, as.formula(paste0(paste(c(id_cols,"Spend_sum","Duration_sum","Number"),collapse="+"),
                                             " ~ MediaTypeDesc+DataStreamID")),
                     fun=mean, value.var=cast_values) # use mean because all the values are the same
        
        # Merge to build data
        setnames(prodoccimpue, c("Spend_sum", "Duration_sum", "Number"),
                 paste(mediatypestr,c("Spend", "Duration", "Number")))
        names(prodoccimpue) = sapply(names(prodoccimpue), change_name)
        if(exists("aggregated")){
          aggregated = merge(aggregated,prodoccimpue,by=id_cols,all = TRUE)
        } else {aggregated = prodoccimpue}
      }
    }
  }
  
  # Non-GRP Media ----------
  print("Merging with Non-GRP media")
  imp2plus = 1
  group4 = "(NI|LI|TN|TR)"             # Has imp2plus column
  group5 = "(FS|LM|LN|LS|NM|NN|NS|OU)" # Does not have imp2plus column
  groups = c(group4, group5)
  
  for(j in 1:length(groups)){
    occfilenames = list.files(pattern = paste0("OCC.*?",groups[j]), path = monthpath.string)
    for(occfilename in occfilenames){
      prodocc = join_occ_prod(monthpath.string,occfilename,product)
      
      if(nrow(prodocc)>0){
        prodocc[, Week:=format(AdDate,format="%W/%Y")]
        prodocc = merge(prodocc,mediatype,by="MediaTypeID")
        mediatypestr = prodocc$MediaTypeDesc[1]
        
        if(j==imp2plus){
          prodocc = prodocc[,.(imp_sum = sum(Imp2Plus), Spend_sum = sum(Spend)),
                      by=c(id_cols, "MediaTypeDesc")]
          prodocc = dcast(prodocc, as.formula(paste0(paste(c(id_cols,"Spend_sum"),collapse="+"),"~ MediaTypeDesc")),
                       fun=mean, value.var=c("imp_sum"))
          setnames(prodocc, c("Spend_sum"),paste(mediatypestr,c("Spend")))
          last_col = length(names(prodocc))
          setnames(prodocc, c(last_col), paste(names(prodocc)[last_col], "Imp"))
        } else {
          prodocc = prodocc[,.(Spend_sum = sum(Spend)),by=c(id_cols, "MediaTypeDesc")]
          prodocc = dcast(prodocc, as.formula(paste0(paste(id_cols,collapse="+"),"~ MediaTypeDesc")),
                       fun=mean, value.var=c("Spend_sum"))
          last_col = length(names(prodocc))
          setnames(prodocc, c(last_col), paste(names(prodocc)[last_col], "Spend"))
        }
        
        # Merge to build data
        if(exists("aggregated")){
          aggregated = merge(aggregated,prodocc,by=id_cols,all = TRUE)
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
    
    print(paste("Saving",basename(monthpath.string)))
    saveRDS(aggregated, paste0(output_dir,"/",basename(monthpath.string),"_aggregated.rds"))
    rm(aggregated)
  }
}

## =============================================
## Rbind month files into one csv file =========
## =============================================
print("Combining into one file")
rdsfiles = list.files(path = output_dir, pattern = ".*?rds", full.names=T)
rdslist = lapply(rdsfiles, readRDS)
aggregated_all = rbindlist(rdslist, use.names=T, fill=T)
write.csv(aggregated_all, paste0(output_dir,"/all_aggregated.csv"), row.names=F)
