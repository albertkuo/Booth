# Advertising Project
This repository only contains scripts that are essential for building the RMS_Ad data. It **does not** contain any data or other auxiliary scripts used in data exploration.

- `build_data_script.R`: master script that sources and runs key R scripts for building data

## build_Ad
Builds aggregated Ad Intel data from raw data.

- `extract_missing.R`: finds network TV ads missing from clearance and saves them in intermediary data files in missing_network
- `build_data.R`: merges and aggregates raw Ad Intel data and saves the built data files in aggregated

## string_matching
Matches RMS brands with Ad Intel brands using a Shiny app.

- `brand_spend/brand_spend.R`: creates brand_spend data that gives total Ad Intel spend for each brand, which is used in string_matching.R
- `string_matching.R`: creates multiple data files with revenue sums, etc. that are used in the string matching app
- `string_matching_app/app.R`: Shiny app deployed [here](http://albertkuo.shinyapps.io/string_matching_app)
- `matches_files/cat_matches`: bash script that concatenates the individual brand matches files created from the Shiny app into one csv file
- `process_matches.R`: processes matches into condensed format for use in merge_RMS_Ad 

## price_aggregation
Builds aggregated RMS data.

- `aggregate_RMS.R`: aggregates price, promotion and quantity by brand for top n brands in RMS (contains `brandAggregator` function) and creates intermediary data files in Brand-Aggregates

## zip_borders

- `county_zips.R`: creates zipborders data, which tells us whether a zip is on a DMA border or not

## merge_RMS_Ad
Merges aggregated RMS and aggregated Ad Intel data.

- `ad_extract.R`: selects the matched Ad Intel brands from Ad Intel data and creates intermediary data files in aggregated_extracts
- `ad_extract_save.R`: binds the monthly csv files created by `ad_extract.R` into an rds file
- `merge_RMS_Ad.R`: merges RMS and Ad Intel data by brand/week/market
- `add_competitors.R`: add competitors' prices and GRP to merged data
- `run_reg.R`: runs regression on brands in merged data

