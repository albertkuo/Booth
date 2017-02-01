#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#
# To deploy application to shinyapps, run the following:
# library(rsconnect)
# rsconnect::deployApp('~/Booth/string_matching/string_matching_app/')

library(bit64)
library(DT)
library(D3TableFilter)
library(shinysky)
library(shiny)

# Read in data
# Data is built from string_matching.R
brands_RMS = readRDS('./data/brands_RMS.rds')
brands_RMS$rev_sum = sapply(brands_RMS$rev_sum, round)
brandnames_RMS = unique(brands_RMS$brand_descr)
brands_ad = readRDS('./data/brands_ad.rds')
brandnames_ad = brands_ad$BrandVariant
brandnames_adquery = unique(brandnames_ad)
top_prod_meta = readRDS('./data/prod_meta.rds')
top_prod_meta$rev_sum = sapply(top_prod_meta$rev_sum, round)
competitors_RMS = readRDS('./data/top_brands.rds')
competitors_RMS$rev_sum = sapply(competitors_RMS$rev_sum,round)
competitors_RMS$prop_revenue_avg = sapply(competitors_RMS$prop_revenue_avg, round, 3)

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
    
    fluidRow(
      column(12, align="center",
             actionButton("toggleCompetitors", "Show/hide competitor brands",
                          class="btn-default"))
    ),
    shinyjs::hidden(
      column(12, align="left", h3("or click on a competitor brand you wish to match.", style="color:slategray")),
      div(id = "additional_competitors",
          DT::dataTableOutput("competitors")
      )
    ),
    
    br(),
    fluidRow(
      column(12, align="center", h3(em("You have selected the following RMS brand.", style="color:steelblue")))
      ),
    fluidRow(
      DT::dataTableOutput("selected_row")
    ),
    
    br(),
    fluidRow(
      column(12, align="center",
             actionButton("toggleAdditional", "Show/hide products and similar RMS brands",
                 class="btn-default"))
    ),
    shinyjs::hidden(
      div(id = "additional",
        tabsetPanel(
          tabPanel('Similar Brands',
                   DT::dataTableOutput("RMS_candidates")),
          tabPanel('Products',
                   DT::dataTableOutput("productstable"))
        )
      )
    ),

    hr(),
    fluidRow(
      column(12, align="left", h3("Step 3: Label matches from the candidate brands below and save.", style="color:slategray"))
    ),
    
    fluidRow(
      column(12, align="center",
             actionButton("toggleAdditionalQuery", "Manually add Ad Intel brands to candidate brands table",
                          class="btn-default"))
    ),
    shinyjs::hidden(
      div(id = "additional_query",
          DT::dataTableOutput("ad_querytable")
      )
    ),
    
    br(),
    fluidRow(
      column(12, align="center",
             downloadButton('downloadData', 'Save Matches', class="btn-primary"))
    ),
    br(),
    
    p("Displaying candidate brands from 
    Ad Intel, sorted by spend by default. If no similar candidates are found, all
      brands in the same category are displayed for you to search manually.",
      style="color:gray"),
    
    fluidRow(
      column(width = 12, d3tfOutput('hot'))
    )
  )
)

############
## Server ##
############
server <- shinyServer(function(input, output, session) {
  ##======= Data Structures =======##
  # A list to store reactive values, i.e. the editable ad candidates table
  revals <- reactiveValues()
  
  # Table of all RMS brands if no search term entered
  # Otherwise, table of RMS brands that match query
  querydata <- reactive({
    data <- brands_RMS[grep(input$brand_query,brands_RMS$brand_descr),]
    data <- data[order(data$rev_sum,decreasing=T),]
    data
  })
  
  # Table of competitors
  competitorsdata <- reactive({
    s = input$querytable_rows_selected
    querydata <- querydata()
    queryrow <- querydata[s,]

    # Select competitors from the same module
    data <- competitors_RMS[competitors_RMS$product_module_descr==queryrow$product_module_descr,]
    data <- data[order(data$rev_sum,decreasing=T),]
    data
  })
  
  # Selected row from querydata
  queryrow <- reactive({
    # Query brand selection
    s = input$querytable_rows_selected
    querydata <- querydata()
    queryrow <- querydata[s,]
    
    
    # Competitor brand selection
    c = input$competitors_rows_selected
    if(!is.null(c)){
      competitors = competitorsdata()
      queryrow <- competitors[c,]
    }
    queryrow
  })
  
  # Table of candidate brands from Ad Intel,
  # consisting of algorithm's matches and manually selected brands from ad_querydata
  ad_candidates <- reactive({
    queryrow <- queryrow()
    data = queryrow
    if(nrow(queryrow)>0){
      if(queryrow$brand_descr!=""){
        candidates_indices = agrep(queryrow$brand_descr, brandnames_ad, fixed=T)
        data <- brands_ad[candidates_indices,]
      } else {
        data <- brands_ad[brands_ad$BrandVariant=="",]
      }
      data <- data[data$category==queryrow$category,]
      if(nrow(data)>0){
        match_tier = factor(rep(4,nrow(data)), levels = c(0:4), ordered = TRUE)
        if(nrow(data)>30){
          match_tier = factor(rep(0,nrow(data)), levels = c(0:4), ordered = TRUE)
        }
        data = cbind(match_tier, data)
      }
      # If no matches, return entire dataset in the same category
      else {
      data <- brands_ad[brands_ad$category==queryrow$category,]
      match_tier = factor(rep(0,nrow(data)), levels = c(0:4), ordered = TRUE)
      data = cbind(match_tier, data)
      }
      data <- data[order(data$spend_sum,decreasing=T),]
      # Add query rows from Ad Intel
      ad_querydata <- ad_querydata()
      ad_queryrows <- ad_querydata[input$ad_querytable_rows_selected,]
      if(nrow(ad_queryrows)>0){
        match_tier = factor(rep(4,nrow(ad_queryrows)), levels = c(0:4), ordered = TRUE)
        ad_queryrows = cbind(match_tier, ad_queryrows)
        data <- rbind(data, ad_queryrows)
      }
    }
    revals[["hot"]] = data
    data
  })
  
  # Table consisting of all brands from Ad Intel
  ad_querydata <- reactive({
    data <- queryrow() # Just a hack to make sure data refreshes
    data <- brands_ad[order(brands_ad$spend_sum,decreasing=T),]
    data
  })
  
  # Table consisting of labeled matches
  save_ad_matches <- reactive({
    ad_matches = revals$hot
    ad_matches = ad_matches[ad_matches$match_tier!=0,]
    queryrow = queryrow()
    queryrow = queryrow[rep(1,nrow(ad_matches)),]
    save_ad_matches = cbind(queryrow,ad_matches)
    save_ad_matches
  })
  
  ##======= Functions =======##
  # RMS brand query
  output$querytable <- DT::renderDataTable(
    querydata(), rownames=F, selection='single',
    caption = 'The following brands are sorted by revenue by default.',
  options = list(sDom  = '<"top">rt<"bottom">ip'))
  
  # Hide/show section of competitor brands
  shinyjs::onclick("toggleCompetitors",
                   shinyjs::toggle(id = "additional_competitors", anim = TRUE))
  
  # Competitors
  output$competitors <- DT::renderDataTable(
    competitorsdata(), rownames=F, selection='single',
    caption = 'The following brands are sorted by revenue by default.',
    options = list(sDom  = '<"top">frt<"bottom">ip'))
  
  # Selected RMS brand
  output$selected_row <- DT::renderDataTable(
    queryrow(), rownames=F, selection='none',
  options = list(sDom  = '<"top">rt<"bottom">'))
  
  # Hide/show section of products and similar brands
  shinyjs::onclick("toggleAdditional",
                   shinyjs::toggle(id = "additional", anim = TRUE))
  
  # Products for selected RMS brand
  output$productstable <- DT::renderDataTable(DT::datatable({
    queryrow <- queryrow()
    data <- top_prod_meta[top_prod_meta$brand_descr==queryrow$brand_descr
                          & top_prod_meta$product_module_descr==queryrow$product_module_descr,]
    data <- data[order(data$rev_sum,decreasing=T),]
    data
  }, rownames=F, selection='none', caption = 'Displaying all RMS products in the selected brand.',
  options = list(sDom  = '<"top">frt<"bottom">ip')))
  
  # Similar brands from RMS
  output$RMS_candidates <- DT::renderDataTable(DT::datatable({
    queryrow <- queryrow()
    if(queryrow$brand_descr!=""){
      candidates_indices = union(agrep(queryrow$brand_descr, brands_RMS$brand_descr, fixed=T),
                                 grep(queryrow$brand_descr, brands_RMS$brand_descr))
      data <- brands_RMS[candidates_indices,]
    } else {
      data <- brands_RMS[brands_RMS$brand_descr=="",]
    }
    data <- data[data$category==queryrow$category,]
    data <- data[order(data$rev_sum,decreasing=T),]
    data
  }, rownames=F, selection='none', 
  caption = 'Displaying similar RMS brands, sorted by revenue by default.',
  options = list(sDom  = '<"top">frt<"bottom">ip')))
  
  # Hide/show section of Ad Intel brand query
  shinyjs::onclick("toggleAdditionalQuery",
                   shinyjs::toggle(id = "additional_query", anim = TRUE))
  
  # Ad Intel brand query
  output$ad_querytable <- DT::renderDataTable(
    ad_querydata(), rownames=F,
    caption = 'The following brands are sorted by spend by default.',
    options = list(sDom  = '<"top">frt<"bottom">ip'))
  
  # Candidate brands from Ad Intel
  output$hot <- renderD3tf({
    tableProps <- list(
      #behavior
      loader = TRUE,  
      loader_text = "Finding candidates...",
      # paging
      paging = TRUE,  # paging is incompatible with col_operations, at least here
      paging_length = 10,  
      results_per_page = JS("['Rows per page',[10, 25, 50]]")  
    );
    d3tf(ad_candidates(), tableProps = tableProps, edit = c("col_0"),
         tableStyle = "table table-hover");
  })
  
  # Observe edits in ad candidates
  observe({
    if(is.null(input$hot_edit)) return(NULL);
    edit <- input$hot_edit;
    
    isolate({
      # need isolate, otherwise this observer would run twice for each edit
      id <- edit$id;
      row <- as.integer(edit$row);
      col <- as.integer(edit$col);
      val <- edit$val;
      
      # validate input 
      if (col == 1){
        # input value must be between 0 and 4
        if(!(as.numeric(val) %in% c(0:4))) {
          oldval <- revals$hot[row, col];
          # reset to the old value
          # input will turn red briefly, than fade to previous color while
          # text returns to previous value
          rejectEdit(session, tbl = "hot", row = row, col = col, id = id, value = oldval);
          return(NULL);
        } 
      }
      
      # accept edits
      revals$hot[row, col] <- val;
      # confirm edits
      confirmEdit(session, tbl = "hot", row = row, col = col, id = id, value = val);
    })
  })
  
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

