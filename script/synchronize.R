#!/usr/bin/env Rscript

library(here)

source(here("config.R"))

source(here("src", "forbes-lists.R"))

initialize_forbes_lists(c('rtb',
                          'billionaires'))

#synchronize_forbes_lists()
#save_forbes_lists()

