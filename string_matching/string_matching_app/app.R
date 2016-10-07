#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(bit64)
library(DT)
library(shinysky)
library(shiny)

# Read in data
# Data is built from string_matching.R
brands_RMS = readRDS('./data/brands_RMS.rds')
brands_RMS$rev_sum = sapply(brands_RMS$rev_sum, round)
brandnames_RMS = unique(brands_RMS$brand_descr)
brands_ad = readRDS('./data/brands_ad.rds')
brandnames_ad = brands_ad$BrandVariant
top_prod_meta = readRDS('./data/prod_meta.rds')

########
## UI ##
########
ui <- shinyUI(
  fluidPage(
    shinyjs::useShinyjs(),
    titlePanel("String Matching App"),
    fluidRow(
      column(6, align="left", h3("[optional] Step 1: Search for RMS brand.", style="color:slategray"))
    ),
    fluidRow(
      column(6, align="left",
      # Query box
        textInput.typeahead(id="brand_query",
                            placeholder="Enter brand description",
                            local=data.frame(name=c(brandnames_RMS)),
                            valueKey = "name",
                            tokens=c(1:length(brandnames_RMS)),
                            template = HTML("<p class='repo-language'>{{info}}</p> <p class='repo-name'>{{name}}</p>")
        )
      )
    ),
    
    hr(),
    fluidRow(
      column(12, align="left", h3("Step 2: Click on the RMS brand you wish to match.", style="color:slategray"))
    ),
    fluidRow(
      DT::dataTableOutput("querytable")
    ),
    
    br(),
    fluidRow(
      column(12, align="center", h3(em("You have selected the following RMS brand.", style="color:dodgerblue")))
    
      ),
    fluidRow(
      DT::dataTableOutput("selected_row")
    ),
    
    br(),
    fluidRow(
      column(12, align="center",
             actionButton("toggleAdditional", "Show/hide products and similar brands",
                 class="btn-default"))
    ),
    shinyjs::hidden(
      div(id = "additional",
        tabsetPanel(
          tabPanel('Products',
                   DT::dataTableOutput("productstable")),
          tabPanel('Similar Brands',
                   DT::dataTableOutput("RMS_candidates"))
        )
      )
    ),

    hr(),
    fluidRow(
      column(12, align="left", h3("Step 3: Select Tier 1 and Tier 2 matches from the candidate brands below.", style="color:slategray"))
    ),
    tabsetPanel(
      tabPanel('Tier 1 Candidates',
               DT::dataTableOutput("ad_candidates_1")),
      tabPanel('Tier 2 Candidates',
               DT::dataTableOutput("ad_candidates_2"))
    ),
    
    hr(),
    fluidRow(
      column(12, align="left", h3("Step 4: Review selected matches and save.", style="color:slategray"))
    ),
    fluidRow(
      DT::dataTableOutput("selected_candidates")
    ),
    
    fluidRow(
      column(12, align="center",
             downloadButton('downloadData', 'Save Matches', class="btn-primary"))
    ),
    
    br()
  )
)

############
## Server ##
############
server <- shinyServer(function(input, output, session) {
  querydata <- reactive({
    data <- brands_RMS[grep(input$brand_query,brands_RMS$brand_descr),]
    data <- data[order(data$rev_sum,decreasing=T),]
    data
  })
  
  queryrow <- reactive({
    s = input$querytable_rows_selected
    querydata <- querydata()
    queryrow <- querydata[s,]
    queryrow
  })
  
  ad_candidates <- reactive({
    queryrow <- queryrow()
    if(queryrow$brand_descr!=""){
      candidates_indices = agrep(queryrow$brand_descr, brandnames_ad, fixed=T)
      data <- brands_ad[candidates_indices,]
    } else {
      data <- brands_ad[brands_ad$BrandVariant=="",]
    }
    data <- data[data$category==queryrow$category,]
    # If no matches, return entire dataset in the same category
    if(nrow(data)==0){data <- brands_ad[brands_ad$category==queryrow$category,]}
    data <- data[order(data$spend_sum,decreasing=T),]
    data
  })
  
  ad_matches <- reactive({
    s1 = input$ad_candidates_1_rows_selected
    ad_candidates_1 <- ad_candidates()
    ad_matches_1 <- ad_candidates_1[s1,]
    if(nrow(ad_matches_1)>0){
      ad_matches_1$match_tier = c(rep(1,nrow(ad_matches_1)))
    }
    s2 = input$ad_candidates_2_rows_selected
    ad_candidates_2 <- ad_candidates()
    ad_matches_2 <- ad_candidates_2[s2,]
    if(nrow(ad_matches_2)>0){
      ad_matches_2$match_tier = c(rep(2,nrow(ad_matches_2)))
    }
    ad_matches = rbind(ad_matches_1, ad_matches_2)
    ad_matches
  })
  
  save_ad_matches <- reactive({
    ad_matches = ad_matches()
    queryrow = queryrow()
    queryrow = queryrow[rep(1,nrow(ad_matches)),]
    save_ad_matches = cbind(queryrow,ad_matches)
    save_ad_matches
  })
  
  # RMS brand query
  output$querytable <- DT::renderDataTable(
    querydata(), rownames=F, selection='single',
    caption = 'The following brands are sorted by revenue by default.',
  options = list(sDom  = '<"top">rt<"bottom">ip'))
  
  # Selected RMS brand
  output$selected_row <- DT::renderDataTable(
    queryrow(), rownames=F, selection='none',
  options = list(sDom  = '<"top">rt<"bottom">'))
  
  # Hide/show section
  shinyjs::onclick("toggleAdditional",
                   shinyjs::toggle(id = "additional", anim = TRUE))
  
  # Products for selected RMS brand
  output$productstable <- DT::renderDataTable(DT::datatable({
    queryrow <- queryrow()
    data <- top_prod_meta[top_prod_meta$brand_descr==queryrow$brand_descr
                          & top_prod_meta$product_module_descr==queryrow$product_module_descr,]
    data
  }, rownames=F, selection='none', caption = 'Displaying all RMS products in the selected brand.',
  options = list(sDom  = '<"top">rt<"bottom">ip')))
  
  # Candidate brands from RMS
  output$RMS_candidates <- DT::renderDataTable(DT::datatable({
    queryrow <- queryrow()
    if(queryrow$brand_descr!=""){
      candidates_indices = agrep(queryrow$brand_descr, brands_RMS$brand_descr, fixed=T)
      data <- brands_RMS[candidates_indices,]
    } else {
      data <- brands_RMS[brands_RMS$brand_descr=="",]
    }
    data <- data[data$category==queryrow$category,]
    data <- data[order(data$rev_sum,decreasing=T),]
    data
  }, rownames=F, selection='none', 
  caption = 'Displaying similar RMS brands, sorted by revenue by default.',
  options = list(sDom  = '<"top">rt<"bottom">ip')))
  
  # Candidate brands from Ad Intel
  output$ad_candidates_1 <- DT::renderDataTable(
    ad_candidates(), rownames=F, caption = 'Displaying candidate brands from 
    Ad Intel, sorted by spend by default. If no similar candidates are found, all
    brands in the same category are displayed for you to search manually.',
  options = list(sDom  = '<"top">frt<"bottom">ip'))
  
  output$ad_candidates_2 <- DT::renderDataTable(
    ad_candidates(), rownames=F, caption = 'Displaying candidate brands from 
    Ad Intel, sorted by spend by default. If no similar candidates are found, all
    brands in the same category are displayed for you to search manually.',
    options = list(sDom  = '<"top">frt<"bottom">ip'))
  
  # Selected candidates
  output$selected_candidates <- DT::renderDataTable(DT::datatable({
    ad_matches()
  }, rownames=F, selection='none', 
  options = list(sDom  = '<"top">rt<"bottom">ip')))
  
  # Save Data
  output$downloadData <- downloadHandler(
    filename = function() {paste(queryrow()$brand_descr, '.csv', sep='') },
    content = function(file) {
      write.csv(save_ad_matches(), file)
    }
  )
}
)

# Run the application 
shinyApp(ui = ui, server = server)

