#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(bit64)
library(shinysky)
library(shiny)

# Read in data
brands_RMS <- readRDS('./data/brands_RMS.rds')
brands_RMS$rev_sum = sapply(brands_RMS$rev_sum, round)
brandnames_RMS = unique(brands_RMS$brand_descr)
brands_ad <- readRDS('./data/brands_ad.rds')
brandnames_ad = brands_ad$BrandDesc

########
## UI ##
########
ui <- shinyUI(
  fluidPage(
    titlePanel("String Matching App"),
    
    # Query box
    fluidRow(
      column(12, align="center", 
        textInput.typeahead(id="brand_query",
                            placeholder="Enter RMS brand name",
                            local=data.frame(name=c(brandnames_RMS)),
                            valueKey = "name",
                            tokens=c(1:length(brandnames_RMS)),
                            template = HTML("<p class='repo-language'>{{info}}</p> <p class='repo-name'>{{name}}</p>")
        )
      ),
      br(),br()
    ),
    
    hr(),
    
    fluidRow(
      column(12, align="center", h3(em("Click on the RMS brand you wish to match.")))
    ),
    fluidRow(
      DT::dataTableOutput("querytable")
    ),
    
    fluidRow(
      column(12, align="center", h3(em("You have selected the following RMS brand.")))
    ),
    fluidRow(
      DT::dataTableOutput("selected_row")
    ),
    
    hr(),
    
    fluidRow(
      column(12, align="center", h1("Candidate Brands"))
    ),
    tabsetPanel(
      tabPanel('Ad Intel',
               DT::dataTableOutput("ad_candidates")),
      tabPanel('RMS',
               DT::dataTableOutput("RMS_candidates"))
    )
  )
)

############
## Server ##
############
server <- shinyServer(function(input, output) {
  
  output$querytable <- DT::renderDataTable(DT::datatable({
    data <- brands_RMS[brands_RMS$brand_descr==input$brand_query,]
    data <- data[order(data$rev_sum,decreasing=T),]
    data
  }, rownames=F, selection='single',
  options = list(sDom  = '<"top">rt<"bottom">ip')))
  
  output$selected_row <- DT::renderDataTable(DT::datatable({
    s = input$querytable_rows_selected
    querydata <- brands_RMS[brands_RMS$brand_descr==input$brand_query,]
    querydata <- querydata[s,]
    querydata
  }, rownames=F,
  options = list(sDom  = '<"top">rt<"bottom">')))
  
  output$ad_candidates <- DT::renderDataTable(DT::datatable({
    s = input$querytable_rows_selected
    querydata <- brands_RMS[brands_RMS$brand_descr==input$brand_query,]
    querydata <- querydata[s,]
    RMS_label <- querydata$Label
    
    if(input$brand_query!=""){
      candidates_indices = agrep(input$brand_query, brandnames_ad, fixed=T)
      data <- brands_ad[candidates_indices,]
    } else {
      data <- brands_ad[brands_ad$BrandDesc=="",]
    }
    data <- data[data$Label==RMS_label,]
    data <- data[order(data$spend_sum,decreasing=T),]
    data
  }, rownames=F, caption = 'All brands are sorted by spend.',
  options = list(sDom  = '<"top">flrt<"bottom">ip')))
  
  output$RMS_candidates <- DT::renderDataTable(DT::datatable({
    s = input$querytable_rows_selected
    querydata <- brands_RMS[brands_RMS$brand_descr==input$brand_query,]
    querydata <- querydata[s,]
    RMS_label <- querydata$Label
    
    if(input$brand_query!=""){
      candidates_indices = agrep(input$brand_query, brands_RMS$brand_descr, fixed=T)
      data <- brands_RMS[candidates_indices,]
    } else {
      data <- brands_RMS[brands_RMS$brand_descr=="",]
    }
    data <- data[data$Label==RMS_label,]
    data <- data[order(data$rev_sum,decreasing=T),]
    data
  }, rownames=F, caption = 'All brands are sorted by revenue.',
  options = list(sDom  = '<"top">flrt<"bottom">ip')))
}
)

# Run the application 
shinyApp(ui = ui, server = server)

