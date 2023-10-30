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

.share <- new.env()

initialize_forbes_lists <-
  function(active_names,
           to_year = as.integer(year(now())),
           base_url = "https://www.forbes.com",
           useragent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
           verbose = 0L,
           timeout = 10,
           interval = 30,
           retries = 10L) {
    assert_that(is.character(active_names) &&
                length(active_names) > 0L &&
                is.scalar(to_year) &&
                is.integer(to_year) &&
                !is.na(to_year) &&
                to_year >= 1990L &&
                is.string(base_url) &&
                !is.na(base_url) &&
                is.string(useragent) &&
                !is.na(useragent) &&
                is.scalar(verbose) &&
                is.integer(verbose) &&
                !is.na(verbose) &&
                is.number(timeout) &&
                !is.na(timeout) &&
                is.number(interval) &&
                !is.na(interval) &&
                is.scalar(retries) &&
                is.integer(retries) &&
                !is.na(retries))

    .share$last_time <- as.POSIXct(NA)
    .share$contents <- .make_contents(active_names)
    .share$to_year <- to_year
    .share$db <- list()
    .share$base_url <- base_url
    .share$useragent <- useragent
    .share$verbose <- verbose
    .share$timeout <- timeout
    .share$interval <- interval
    .share$retries <- retries
    .share$session <- NULL
    .share$response <- NULL

    print(.share$contents)

    invisible()
  }

.make_contents <-
  function(active_names) {
    assert_that(is.character(active_names) &&
                length(active_names) > 0L &&
                !any(is.na(active_names)) &&
                !any(duplicated(active_names)))

    contents <-
      tibble(name = c("rtb",
                      "billionaires",
                      "forbes-400",
                      "global2000"),
             description = c("Real-time Billionaires",
                             "Billionaires",
                             "The Richest People in America",
                             "Global 2000"),
             type = c("person",
                      "person",
                      "person",
                      "org"),
             from_year = c(NA,
                           1997L,
                           1996L,
                           2010L))

    #assert_that(!any(duplicated(forbes_lists$name)) &&
    #            all(forbes_lists$from_year >= 0L) &&
    #            all(active_names %in% forbes_lists$name))

    contents %>%
      mutate(active = if_else(name %in% active_names, TRUE, FALSE))
  }

load_forbes_lists <-
  function() {
    assert_that(is_tibble(.share$contents) &&
                is.list(.share$db))

    for (i in seq_len(nrow(.share$contents))) {
      content <- as.list(.share$contents[i, ])
      assert_that(is.string(content$name) &&
                  !is.na(content$name) &&
                  is.flag(content$active) &&
                  !is.na(content$active))

      .share$db[[content$name]] <- .get_blank_fun(content$name)()

      if (content$active) {
        db_file_path <- .make_db_file_path(content$name)

        if (file.exists(db_file_path)) {
          inform(sprintf("Load list '%s' from file '%s'",
                         content$name,
                         db_file_path))

          .share$db[[content$name]] <- readRDS(db_file_path)
          assert_that(is_tibble(.share$db[[content$name]]))
        } else {
          warn(sprintf("List '%s' seems not exists",
                       content$name))
        }
      }
    }

    invisible()
  }

save_forbes_lists <-
  function() {
    assert_that(is_tibble(.share$contents) &&
                is.list(.share$db))

    for (i in seq_len(nrow(.share$contents))) {
      content <- as.list(.share$contents[i, ])
      assert_that(is.string(content$name) &&
                  !is.na(content$name) &&
                  is.flag(content$active) &&
                  !is.na(content$active))

      if (content$active) {
        db_file_path <- .make_db_file_path(content$name)

        if (!is.null(.share$db[[content$name]])) {
          assert_that(is_tibble(.share$db[[content$name]]))

          inform(sprintf("Save list '%s' to file '%s'",
                         content$name,
                         db_file_path))

          saveRDS(.share$db[[content$name]],
                  file = db_file_path)
        } else {
          warn(sprintf("List %s seems not exists",
                       content$name))
        }
      }
    }

    invisible()
  }

.make_db_file_path <-
  function(name) {
    assert_that(is.string(name) && !is.na(name))

    here("data", "db", paste0(name, ".Rds"))
  }

synchronize_forbes_lists <-
  function() {
    assert_that(is_tibble(.share$contents) &&
                is.scalar(.share$to_year) &&
                is.integer(.share$to_year) &&
                !is.na(.share$to_year) &&
                is.list(.share$db) &&
                is.string(.share$base_url) &&
                !is.na(.share$base_url) &&
                is.string(.share$useragent) &&
                !is.na(.share$useragent) &&
                is.scalar(.share$verbose) &&
                is.integer(.share$verbose) &&
                !is.na(.share$verbose) &&
                is.number(.share$timeout) &&
                !is.na(.share$timeout))

    if (!is.null(.share$session) || !is.null(.share$response))
      abort("Call initialize before synchronize")

    .share$session <-
      session(.share$base_url,
              httr::user_agent(.share$useragent),
              httr::config(verbose = .share$verbose),
              httr::timeout(.share$timeout),
              httr::config(followlocation = 0L),
              httr::set_cookies(notice_gdpr_prefs = ""))

    for (i in seq_len(nrow(.share$contents))) {
      content <- as.list(.share$contents[i, ])
      assert_that(is.string(content$name) &&
                  !is.na(content$name) &&
                  is.string(content$type) &&
                  !is.na(content$type) &&
                  is.scalar(content$from_year) &&
                  is.integer(content$from_year) &&
                  # allow content$from_year NA
                  is.flag(content$active) &&
                  !is.na(content$active))

      .share$db[[content$name]] <- .get_blank_fun(content$name)()

      if (content$active) {
        if (is.na(content$from_year)) {
          .scrape_list(content$name, content$type, 0L)
        } else {
          assert_that(content$from_year <= .share$to_year)

          for (year in seq(content$from_year, .share$to_year)) {
            .scrape_list(content$name, content$type, year)
          }
        }
      }
    }

    invisible()
  }

.scrape_list <-
  function(name, type, year) {
    assert_that(is.list(.share$db) &&
                is.string(name) &&
                !is.na(name) &&
                is.string(type) &&
                !is.na(type) &&
                is.scalar(year) &&
                is.integer(year) &&
                !is.na(year))

    dispatch <-
      list(person = .scrape_person_list,
           org = .scrape_org_list)

    scrape_fun <- dispatch[[type]]
    assert_that(is.function(scrape_fun))

    inform(sprintf("Scraping list '%s' for year: %d", name, year))

    scrape_fun(name, year)
    assert_that(is.list(.share$response))

    rows <- lapply(.share$response, .get_parse_fun(name))
    rows <- as_tibble(do.call(rbind, rows))

    .share$db[[name]] <- rbind(.share$db[[name]], rows)
    .share$response <- NULL

    invisible()
  }

.scrape_person_list <-
  function(name, year) {
    assert_that(is.string(.share$base_url) &&
                !is.na(.share$base_url) &&
                is.string(name) &&
                !is.na(name) &&
                is.scalar(year) &&
                is.integer(year) &&
                !is.na(year))

    path <- sprintf("/forbesapi/person/%s/%d/position/true.json", name, year)
    url <- modify_url(.share$base_url, path = path)
    query <- list(limit = 5000L)

    .graceful_request(url, query)
    assert_that(is.list(.share$response) &&
                is.list(.share$response$personList) &&
                #is.number(.share$response$personList$count) &&
                #!is.na(.share$response$personList$count) &&
                #.share$response$personList$count >= 0 &&
                is.list(.share$response$personList$personsLists))

    .share$response <- .share$response$personList$personsLists

    invisible()
  }

.scrape_org_list <-
  function(name, year) {
    assert_that(is.string(.share$base_url) &&
                !is.na(.share$base_url) &&
                is.string(name) &&
                !is.na(name) &&
                is.scalar(year) &&
                is.integer(year) &&
                !is.na(year))

    path <- sprintf("/forbesapi/org/%s/%d/position/true.json", name, year)
    url <- modify_url(.share$base_url, path = path)
    query <- list(limit = 5000L)

    .graceful_request(url, query)
    assert_that(is.list(.share$response) &&
                is.list(.share$response$organizationList) &&
                #is.number(.share$response$organizationList$count) &&
                #!is.na(.share$response$organizationList$count) &&
                #.share$response$organizationList$count >= 0 &&
                is.list(.share$response$organizationList$organizationsLists))

    .share$response <- .share$response$organizationList$organizationsLists

    invisible()
  }

.graceful_request <-
  function(url, query) {
    assert_that(is.scalar(.share$last_time) &&
                is.POSIXct(.share$last_time) &&
                # allow .share$last_time NA
                is.number(.share$interval) &&
                !is.na(.share$interval) &&
                is.scalar(.share$retries) &&
                is.integer(.share$retries) &&
                !is.na(.share$retries) &&
                is.session(.share$session) &&
                is.string(url) &&
                !is.na(url) &&
                is.list(query))

    for (i in seq_len(.share$retries)) {
      if (i > 1L) inform(sprintf("Retrying failed request, attempt: %d", i))

      .share$response <- NULL

      if (!is.na(.share$last_time)) {
        duration <- difftime(now(), .share$last_time, units = 'sec')
        sleep_time <- .share$interval - duration
        if (sleep_time < 1) sleep_time <- 1

        inform(sprintf("Sleeping %g secs before doing request",
                       as.integer(sleep_time)))
        Sys.sleep(sleep_time)
      }

      .share$last_time <- now()

      try({
        .share$session <- session_jump_to(.share$session, url, query = query)
        stop_for_status(.share$session)

        text <- content(.share$session$response, 'text')
        assert_that(is.string(text) && !is.na(text) && nchar(text) > 1L)

        .share$response <- fromJSON(text, simplifyVector = FALSE)

        break
      }, silent = FALSE)
    }

    invisible()
  }

#
# Params
#

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

get_forbes_list <- function(name) {
  assert_that(is_tibble(.share$contents) &&
              is.list(.share$db) &&
              is.string(name) &&
              !is.na(name) &&
              name %in% .share$contents$name)

  .share$db[[name]]
}

#
# Blank
#

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
           age = integer(),
           birth_date = Date(),
           country = character(),
           citizenship = character(),
           state = character(),
           city = character(),
           source = character(),
           bio = character(),
           about = character(),
           image = character())

.blank_common_org <-
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
    forbes400 <- .blank_common_person()

    forbes400
  }

.blank_global2000 <-
  function() {
    global2000 <- .blank_common_org()

    global2000
  }

#
# Parse
#

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
           last_name = to_string(json$lastName),
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

    billionaires <- .parse_common_person(json)

    fields <-
      c('worth_final',
        'worth_prev',
        'worth_assets',
        'worth_archived')

    billionaires[fields] <-
      tibble(worth_final = to_number(json$finalWorth),
             worth_prev = to_number(json$estWorthPrev),
             worth_assets = to_number(json$privateAssetsWorth),
             worth_archived = to_number(json$archivedWorth))

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

#
# Helpers
#

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
  function(x, sep = "\n") {
    if (is.null(x)) x <- as.character(NA)

    if_else(is.list(x) && length(x) > 0L,
            paste(na.omit(x), collapse = "\n"),
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

