#!/usr/bin/env Rscript

library(here)

source(here("R", "forbes-lists.R"))

init_forbes_lists(c('rtb',
                    'billionaires',
                    'forbes-400',
                    'midas',
                    'athletes'),
                  from_year = 2021L)

#fetch_forbes_lists()
#save_forbes_lists()

