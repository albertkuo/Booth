## string_matching
Matches RMS brands with Ad Intel brands

- `brand_spend/brand_spend.R`: creates brand_spend data that gives total Ad Intel spend for each brand, which is used in string_matching.R
- `string_matching.R`: creates multiple data files with revenue sums, etc. that are used in the string matching app
- `string_matching_app/app.R`: Shiny app deployed to albertkuo.shinyapps.io/string_matching_app
- `matches_files/cat_matches`: bash script that concatenates the individual brand matches files created from the Shiny app into one csv file
- `process_matches.R`: processes matches into condensed format for use in merge_RMS_Ad 


