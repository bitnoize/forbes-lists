#!/usr/bin/env Rscript

library(here)

source(here("src", "forbes-lists.R"))

init_forbes_lists(c('rtb'))

sync_forbes_lists()
save_forbes_lists()

