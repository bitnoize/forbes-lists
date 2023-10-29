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
  function(active_names, to_year = as.integer(year(now()))) {
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
                is.number(.config$interval) &&
                !is.na(.config$interval) &&
                is.character(active_names) &&
                length(active_names) > 0L &&
                is.scalar(to_year) &&
                is.integer(to_year) &&
                !is.na(to_year) &&
                to_year >= 1990L)

    .state$last_time <- as.POSIXct(NA)

    .state$forbes_lists <- .make_forbes_lists(active_names)

    .state$to_year <- to_year

    .state$session <-
      session(.config$base_url,
              httr::config(verbose = .config$verbose),
              httr::user_agent(.config$useragent),
              httr::timeout(.config$timeout),
              httr::config(followlocation = 0L),
              httr::set_cookies(notice_gdpr_prefs = ""))

    .state$tables <- list()

    print(.state$forbes_lists)
  }

.make_forbes_lists <-
  function(active_names) {
    assert_that(is.character(active_names) &&
                length(active_names) > 0L &&
                !any(is.na(active_names)) &&
                !any(duplicated(active_names)))

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

    #assert_that(!any(duplicated(forbes_lists$name)) &&
    #            all(forbes_lists$from_year >= 0L) &&
    #            all(active_names %in% forbes_lists$name))

    forbes_lists %>%
      mutate(active = if_else(name %in% active_names, TRUE, FALSE))
  }

scrape_active_lists <-
  function() {
    assert_that(is_tibble(.state$forbes_lists))

    for (i in seq_len(nrow(.state$forbes_lists))) {
      forbes_list <- as.list(.state$forbes_lists[i, ])

      if (forbes_list$active) {
        .scrape_list(forbes_list$name,
                     forbes_list$type,
                     forbes_list$from_year)

        assert_that(is_tibble(.state$tables[[forbes_list$name]]))
      }
    }
  }

.scrape_list <-
  function(name, type, from_year) {
    assert_that(is.list(.state$tables) &&
                is.scalar(.state$to_year) &&
                is.integer(.state$to_year) &&
                !is.na(.state$to_year) &&
                is.string(name) &&
                !is.na(name) &&
                is.string(type) &&
                !is.na(type) &&
                # allow from_year NA
                is.scalar(from_year) &&
                is.integer(from_year))

    .state$tables[[name]] <- .get_blank_fun(name)()

    if (is.na(from_year)) {
      .scrape_list_year(name, type, 0L)
    } else {
      assert_that(from_year <= .state$to_year)

      for (year in seq(from_year, .state$to_year)) {
        .scrape_list_year(name, type, year)
      }
    }
  }

.scrape_list_year <-
  function(name, type, year) {
    assert_that(is.list(.state$tables) &&
                is.string(name) &&
                !is.na(name) &&
                is.string(type) &&
                !is.na(type) &&
                is.scalar(year) &&
                is.integer(year) &&
                !is.na(year))

    parse_fun <- .get_parse_fun(name)

    dispatch <-
      list(person = .request_person_list,
           organization = .request_organization_list)

    request_fun <- dispatch[[type]]
    assert_that(is.function(request_fun))

    request_fun(name, year)

    assert_that(is.list(.state$json))

    rows <- lapply(.state$json, parse_fun)
    rows <- as_tibble(do.call(rbind, rows))

    .state$tables[[name]] <- rbind(.state$tables[[name]], rows)

    .state$json <- NULL
  }

.request_person_list <-
  function(name, year) {
    assert_that(is.string(.config$base_url) &&
                !is.na(.config$base_url) &&
                is.string(name) &&
                !is.na(name) &&
                is.scalar(year) &&
                is.integer(year) &&
                !is.na(year))

    path <- sprintf("/forbesapi/person/%s/%d/position/true.json", name, year)
    url <- modify_url(.config$base_url, path = path)
    query <- list(limit = 5000L)

    .graceful_request(url, query)

    assert_that(is.list(.state$json) &&
                is.list(.state$json$personList) &&
                #is.number(.state$json$personList$count) &&
                #!is.na(.state$json$personList$count) &&
                #.state$json$personList$count >= 0 &&
                is.list(.state$json$personList$personsLists))

    .state$json <- .state$json$personList$personsLists
  }

.request_organization_list <-
  function(name, year) {
    assert_that(is.string(.config$base_url) &&
                !is.na(.config$base_url) &&
                is.string(name) &&
                !is.na(name) &&
                is.scalar(year) &&
                is.integer(year) &&
                !is.na(year))

    path <- sprintf("/forbesapi/organization/%s/%d/position/true.json", name, year)
    url <- modify_url(.config$base_url, path = path)
    query <- list(limit = 5000L)

    .graceful_request(url, query)

    assert_that(is.list(.state$json) &&
                is.list(.state$json$organizationList) &&
                #is.number(.state$json$organizationList$count) &&
                #!is.na(.state$json$organizationList$count) &&
                #.state$json$organizationList$count >= 0 &&
                is.list(.state$json$organizationList$organizationsLists))

    .state$json <- json$organizationList$organizationsLists
  }

.graceful_request <-
  function(url, query) {
    assert_that(is.number(.config$interval) &&
                !is.na(.config$interval) &&
                is.scalar(.state$last_time) &&
                is.POSIXct(.state$last_time) &&
                # allow .state$last_time NA
                is.session(.state$session) &&
                is.string(url) &&
                !is.na(url) &&
                is.list(query))

    if (!is.na(.state$last_time)) {
      duration <- difftime(now(), .state$last_time, units = 'sec')
      sleep_time <- .config$interval - duration
      if (sleep_time < 1) sleep_time <- 1

      inform(sprintf("Sleeping %g seconds before doing request", sleep_time))
      Sys.sleep(sleep_time)
    }

    .state$session <- session_jump_to(.state$session, url, query = query)
    stop_for_status(.state$session)

    text <- content(.state$session$response, 'text')
    assert_that(is.string(text) && !is.na(text) && nchar(text) > 1L)

    .state$json <- fromJSON(text, simplifyVector = FALSE)

    .state$last_time <- now()
  }

.get_blank_fun <- function(name) .dispatch_params(name, 1L)
.get_parse_fun <- function(name) .dispatch_params(name, 2L)

.dispatch_params <-
  function(name, idx) {
    assert_that(is.string(name) &&
                !is.na(name) &&
                is.scalar(idx) &&
                is.integer(idx) &&
                !is.na(idx))

    dispatch <-
      list("rtb" = list(.blank_rtb, .parse_rtb),
           "billionaires" = list(.blank_billionaires, .parse_billionaires),
           "forbes-400" = list(.blank_forbes400, .parse_forbes400),
           "global2000" = list(.blank_global2000, .parse_global2000))

    param_fun <- dispatch[[name]][[idx]]
    assert_that(is.function(param_fun))

    param_fun
  }

.blank_common_person <-
  function()
    tibble(natural_id = character(),
           year = integer(),
           uri = character(),
           timestamp = POSIXct(),
           rank = integer(),
           position = integer(),
           full_name = character(),
           last_name = character(),
           gender = character(),
           birth_date = Date(),
           citizenship = character(),
           state = character(),
           city = character(),
           source = character(),
           bio = character(),
           about = character(),
           image = character())

.blank_common_organizations <-
  function()
    tibble(natural_id = character(),
           year = integer(),
           uri = character(),
           timestamp = POSIXct(),
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

.blank_rtb <-
  function() {
    rtb <- .blank_common_person()

    fields <-
      c(
        'worth_final',
        'worth_prev',
        'worth_assets',
        'worth_archived')

    rtb[fields] <-
      tibble(worth_final = numeric(),
             worth_prev = numeric(),
             worth_assets = numeric(),
             worth_archived = numeric())

    rtb
  }

.blank_billionaires <-
  function() {
    billionaires <- .blank_common_person()

    billionaires
  }

.blank_forbes400 <-
  function() {
    forbes400 <- .blank_common_organization()

    forbes400
  }

.blank_global2000 <-
  function() {
    global2000 <- .blank_common_organization()

    global2000
  }

.parse_common_person <-
  function(json) {
    assert_that(is.list(json))

    person <-
      list(natural_id = to_string(json$naturalId),
           year = to_integer(json$year),
           uri = to_string(json$uri),
           timestamp = to_timestamp(json$timestamp),
           rank = to_integer(json$rank),
           position = to_integer(json$position),
           full_name = to_string(json$personName),
           last_name = to_string(json$personName),
           gender = to_gender(json$gender),
           birth_date = to_date(json$birthDate),
           citizenship = to_string(json$countryOfCitizenship),
           state = to_string(json$state),
           city = to_string(json$city),
           source = to_string(json$source),
           bio = to_text(json$bios),
           about = to_text(json$abouts),
           image = to_string(json$squareImage))

    person <- as_tibble(person)
    person
  }

.parse_common_organization <-
  function(json) {
    assert_that(is.list(json))

    organization <-
      list(natural_id = to_string(json$naturalId),
           uri = to_string(json$uri),
           name = to_string(json$organizationName),
           industry = to_string(json$industry),
           country = to_string(json$country),
           city = to_string(json$city),
           description = to_string(json$description),
           ceo_title = to_string(json$ceoTitle),
           ceo_name = to_string(json$ceoName),
           founded = to_integer(json$yearFounded),
           image = to_string(json$image),
           site = to_string(json$webSite))

    organization <- as_tibble(organization)
    organization
  }

.parse_rtb <-
  function(json) {
    assert_that(is.list(json))

    rtb <- .parse_common_person(json)

    fields <-
      c('worth_final',
        'worth_prev',
        'worth_assets',
        'worth_archived')

    rtb[fields] <-
      tibble(worth_final = to_number(json$finalWorth),
             worth_prev = to_number(json$estWorthPrev),
             worth_assets = to_number(json$privateAssetsWorth),
             worth_archived = to_number(json$archivedWorth))

    rtb
}

.parse_billionaires <-
  function(json) {
    assert_that(is.list(json))

    billionaires <- parse_common_person(json)

    fields <-
      c('worth_final',
        'worth_prev',
        'worth_assets',
        'worth_archived')

    billionaires[fields] <-
      tibble(worth_final = to_float(json$finalWorth),
             worth_prev = to_float(json$estWorthPrev),
             worth_assets = to_float(json$privateAssetsWorth),
             worth_archived = to_float(json$archivedWorth))

    billionaires
  }

.parse_forbes400 <-
  function(json) {
    assert_that(is.list(json))

    forbes400 <- .parse_common_organization(json)
  }

.parse_global2000 <-
  function(json) {
    assert_that(is.list(json))

    global2000 <- .parse_common_organization(json)
  }

to_bool <-
  function(x) {
    if (is.null(x)) x <- as.logical(NA)

    if_else(is.flag(x) && !is.na(x),
            x,
            as.logical(NA))
  }

to_integer <-
  function(x) {
    if (is.null(x)) x <- as.numeric(NA)

    if_else(is.number(x) && !is.na(x),
            as.integer(x),
            as.integer(NA))
  }

to_number <-
  function(x) {
    if (is.null(x)) x <- as.numeric(NA)

    if_else(is.number(x) && !is.na(x),
            x,
            as.numeric(NA))
  }

to_string <-
  function(x) {
    if (is.null(x)) x <- as.character(NA)

    if_else(is.string(x) && !is.na(x),
            x,
            as.character(NA))
  }

to_text <-
  function(x) {
    if (is.null(x)) x <- as.character(NA)

    if_else(is.character(x) && length(x) > 0L,
            paste(x, collapse = "\n"),
            as.character(NA))
  }

to_timestamp <-
  function(x) {
    if (is.null(x)) x <- as.numeric(NA)

    if_else(is.number(x) && !is.na(x),
            as.POSIXct(x / 1000, origin = "1970-01-01"),
            as.POSIXct(NA))
  }

to_date <-
  function(x) {
    if (is.null(x)) x <- as.numeric(NA)

    if_else(is.number(x) && !is.na(x),
            as.Date(as.POSIXct(x / 1000, origin = "1970-01-01")),
            as.Date(NA))
  }

to_gender <-
  function(x) {
    if (is.null(x)) x <- as.character(NA)

    case_when(is.string(x) && x == "M" ~ "Male",
              is.string(x) && x == "F" ~ "Female",
              is.string(x) ~ "Other",
              .default = as.character(NA))
  }
