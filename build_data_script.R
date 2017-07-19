# build_data_script.R
# -----------------------------------------------------------------------------
# Author:             Albert Kuo
# Date last modified: July 19, 2017
#
# Runs all the data building R scripts


## build_Ad
start.time = Sys.time()
source("./build_Ad/extract_missing.R")
end.time = Sys.time()
print(end.time - start.time)

start.time = Sys.time()
source("./build_Ad/build_data.R")
end.time = Sys.time()
print(end.time - start.time)

## price_aggregation
start.time = Sys.time()
source("./price_aggregation/aggregate_RMS.R")
end.time = Sys.time()
print(end.time - start.time)


## merge_RMS_Ad
start.time = Sys.time()
source("./merge_RMS_Ad/ad_extract.R")
source("./merge_RMS_Ad/ad_extract_save.R")
end.time = Sys.time()
print(end.time - start.time)

source("./merge_RMS_Ad/merge_RMS_Ad.R")
source("./merge_RMS_Ad/add_competitors.R")