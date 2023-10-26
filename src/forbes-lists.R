suppressPackageStartupMessages({
  library(rlang)
  library(magrittr)
  library(assertthat)
  library(lubridate)
  library(jsonlite)
  library(httr)
  library(rvest)
  library(readr)
  library(tibble)
  library(dplyr)
})

.state <- new.env()

initialize <-
  function(active_lists, to_year = as.integer(year(now()))) {
    assert_that(is.environment(.config) &&
                is.string(.config$base_url) &&
                !is.na(.config$base_url) &&
                is.scalar(.config$verbose) &&
                is.integer(.config$verbose) &&
                !is.na(.config$verbose) &&
                is.string(.config$useragent) &&
                !is.na(.config$useragent) &&
                is.number(.config$timeout) &&
                !is.na(.config$timeout) &&
                is.character(active_lists) &&
                length(active_lists) > 0L &&
                !any(is.na(active_lists)) &&
                !any(duplicated(active_lists)) &&
                is.integer(to_year) &&
                to_year >= 1990L)

    .state$last_time <- as.Date(NA)

    forbes_lists <- 
      tibble(name = c("rtb",
                      "billionaires",
                      "forbes-400",
                      "global2000"),
             description = c("Real-time Billionaires",
                             "Billionaires",
                             "Forbes 400",
                             "Global 2000"),
             type = c("person",
                      "person",
                      "organization",
                      "organization"),
             from_year = c(NA,
                           1997L,
                           1997L,
                           1997L))

    assert_that(!any(duplicated(forbes_lists$name)) &&
                all(na.omit(forbes_lists$from_year) >= 1990L) &&
                all(active_lists %in% forbes_lists$name))

    .state$forbes_lists <-
      forbes_lists %>%
        mutate(active = if_else(name %in% active_lists, TRUE, FALSE))

    .state$to_year <- to_year

    .state$session <-
      session(.config$base_url,
              httr::config(verbose = .config$verbose),
              httr::user_agent(.config$useragent),
              httr::timeout(.config$timeout),
              httr::config(followlocation = 0L),
              httr::set_cookies(notice_gdpr_prefs = ""))

    inform("Forbes-Lists successfully initialized")
  }

scrape_all_lists <-
  function() {
    assert_that(is_tibble(.state$forbes_lists))

    for (i in seq_len(nrow(.state$forbes_lists))) {
      forbes_list <- as.list(.state$forbes_lists[i, ])

      if (forbes_list$type == 'person') {
        scrape_person_list(forbes_list)
      } else if (forbes_list$type == 'organization') {
        scrape_organization_list(forbes_list)
      } else {
        abort(sprintf("Unknown forbes_list type '%s' at row #%d",
                      forbes_list$type, i))
      }
    }
  }

scrape_person_list <-
  function(forbes_list) {
    assert_that(is.scalar(.state$to_year) &&
                is.integer(.state$to_year) &&
                !is.na(.state$to_year) &&
                is.list(forbes_list) &&
                is.string(forbes_list$name) &&
                is.scalar(forbes_list$from_year) &&
                is.integer(forbes_list$from_year))

    fetch_raw <-
      function(name, year) {
        graceful_request(function() {
          path <- sprintf("/forbesapi/person/%s/%d/position/true.json",
                          forbes_list$name, year)
          url <- modify_url(.config$base_url, path = path)
          query <- list(limit = 5000L)

          .state$session <- session_jump_to(.state$session, url, query = query)
          stop_for_status(.state$session)

          text <- content(.state$session$response, 'text')
          assert_that(is.string(text) && !is.na(text) && nchar(text) > 1L)

          json <- fromJSON(text, simplifyVector = FALSE)
          assert_that(is.list(json) &&
                      is.list(json$personList) &&
                      is.number(json$personList$count) &&
                      json$personList$count >= 0 &&
                      is.list(json$personList$personsLists))

          json$personList$personsLists
        })
      }

    if (is.na(forbes_list$from_year)) {
      raw <- fetch_raw(forbes_list$name, 0L)
      print(raw)

    } else {
      assert_that(.state$to_year >= forbes_list$from_year)

      for (year in seq(forbes_list$from_year, .state$to_year)) {
        raw <- fetch_person_list_raw(forbes_list$name, year)
        print(raw)
      }
    }

  }

dispatch <-
  function() {
    list(rtb = parse_rtb,
         billionaires = parse_billionaires)
  }

parse_rtb <-
  function() {

  }

parse_billionaires <-
  function() {

  }

graceful_request <-
  function(fun, ...) {
    assert_that((is.na(.state$last_time) || is.POSIXct(.state$last_time)) &&
                is.number(.config$interval) &&
                !is.na(.config$interval))

    if (!is.na(.state$last_time)) {
      duration <- difftime(now(), .state$last_time, units = 'sec')
      sleep_time <- .config$interval - duration

      if (sleep_time > 0) {
        inform(sprintf("Sleeping %g seconds before doing request", sleep_time))
        Sys.sleep(sleep_time)
      } else {
        warn(sprintf("Negative sleep time: %g", sleep_time))
        Sys.sleep(1)
      }
    }

    res <- fun(...)

    .state$last_time <- now()

    res
  }

blank_persons <-
  function() {
    tibble(id = character(),
           full_name = character(),
           gender = character(),
           birth_day = Date(),
           citizenship = character(),
           country = character(),
           state = character(),
           city = character(),
           source = character(),
           bio = character(),
           about = character(),
           image = character())
  }

blank_organizations <-
  function() {
    tibble(id = character(),
           name = character(),
           industry = character(),
           country = character(),
           city = character(),
           description = character(),
           ceo_title = character(),
           ceo_name = character(),
           founded = integer(),
           image = character(),
           site = character())
  }

to_bool <-
  function(x)
    as.logical(x %||% NA)

to_integer <-
  function(x)
    suppressWarnings(as.integer(x %||% NA))

to_float <-
  function(x)
    suppressWarnings(as.numeric(x %||% NA))

to_string <-
  function(x)
    as.character(x %||% NA)

to_timestamp <-
  function(x)
    if_else(is.number(x),
            as.POSIXct(x / 1000, origin = "1970-01-01"),
            as.POSIXct(NA))

to_date <-
  function(x)
    if_else(is.number(x),
            as.Date(as.POSIXct(x / 1000, origin = "1970-01-01")),
            as.Date(NA))

to_gender <-
  function(x)
    case_when(!is.string(x) ~ as.character(NA),
              x == "M" ~ "Male",
              x == "F" ~ "Female",
              .default = "Other")


