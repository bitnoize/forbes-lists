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

init_forbes_lists <-
  function(active_names,
           from_year = 1990L,
           to_year = as.integer(year(now())),
           base_url = "https://www.forbes.com",
           useragent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
           verbose = 0L,
           timeout = 10,
           interval = 10,
           retries = 10L) {
    assert_that(is.character(active_names) &&
                length(active_names) > 0L &&
                is.scalar(from_year) &&
                is.integer(from_year) &&
                !is.na(from_year) &&
                from_year >= 1990L &&
                is.scalar(to_year) &&
                is.integer(to_year) &&
                !is.na(to_year) &&
                to_year >= 1990L &&
                from_year <= to_year &&
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
    .share$from_year <- from_year
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
                      "midas",
                      "athletes"),
             description = c("Real-time Billionaires",
                             "Billionaires",
                             "The Richest People in America",
                             "The Midas List: Top Tech Investors",
                             "The World's Highest-Paid Athletes"),
             type = c("person",
                      "person",
                      "person",
                      "person",
                      "person"),
             template = c("rtb",
                          "persons",
                          "persons",
                          "persons",
                          "persons"),
             from_year = c(NA,
                           1996L,
                           1996L,
                           2011L,
                           2012L))
    assert_that(!any(duplicated(contents$name)))

    contents %>%
      mutate(active = if_else(name %in% active_names, TRUE, FALSE))
  }

.make_columns <-
  function(template) {
    assert_that(is.scalar(template) && !is.na(template))

    common <-
      c("natural_id",
        "year",
        "timestamp",
        "rank",
        "position")

    person_base <-
      c("person_uri",
        "person_name",
        "person_image",
        "first_name",
        "last_name",
        "gender",
        "status",
        "birth_date",
        "age",
        "bio",
        "about")

    person_geo <-
      c("country",
        "country_of_citizenship",
        "residence_state_region",
        "residence_msa",
        "state",
        "city")

    person_business <-
      c("category",
        "source",
        "industries",
        "embargo")

    person_self_made <-
      c("self_made",
        "self_made_rank")

    person_employment <-
      c("employment_uri",
        "employment_name",
        "employment_title",
        "employment_total_earnings",
        "employment_total_earning_est",
        "employment_salary",
        "employment_salary_est",
        "employment_endorsements",
        "employment_bonus",
        "employment_government",
        "employment_revenue",
        "employment_revenue_year",
        "employment_website",
        "employment_featured_executive")

    dispatch <-
      list("rtb" = c(common,
                     person_base[!(person_base %in% c("first_name",
                                                      "status",
                                                      "age"))],
                     person_geo[!(person_geo %in% c("country",
                                                    "residence_state_region",
                                                    "residence_msa"))],
                     person_business[!(person_business %in% c("category",
                                                              "embargo"))],
                     "final_worth",
                     "est_worth_prev",
                     "private_assets_worth",
                     "archived_worth"),
           "persons" = c(common,
                         person_base,
                         person_geo,
                         person_business,
                         person_self_made,
                         person_employment,
                         "final_worth",
                         "recent_earnings",
                         "other_compensation",
                         "philanthropy_score"))

    columns <- dispatch[[template]]
    assert_that(is.character(columns) &&
                length(columns) > 0L &&
                !any(is.na(columns)))

    columns
  }

load_forbes_lists <-
  function() {
    assert_that(is_tibble(.share$contents) &&
                is.list(.share$db))

    for (i in seq_len(nrow(.share$contents))) {
      content <- as.list(.share$contents[i, ])
      assert_that(is.string(content$name) &&
                  !is.na(content$name) &&
                  is.string(content$template) &&
                  !is.na(content$template) &&
                  is.flag(content$active) &&
                  !is.na(content$active))

      .share$db[[content$name]] <- .blank_list(content$template)

      if (content$active) {
        db_file_path <- .make_db_file_path(content$name)

        if (file.exists(db_file_path)) {
          inform(sprintf("Load list '%s' from file '%s'",
                         content$name,
                         db_file_path))

          .share$db[[content$name]] <- readRDS(db_file_path)
          assert_that(is_tibble(.share$db[[content$name]]))
        } else {
          warn(sprintf("List '%s' file '%s' not exists",
                       content$name,
                       db_file_path))
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
          warn(sprintf("List %s not loaded / fetched yet",
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

fetch_forbes_lists <-
  function() {
    assert_that(is_tibble(.share$contents) &&
                is.scalar(.share$from_year) &&
                is.integer(.share$from_year) &&
                !is.na(.share$from_year) &&
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
      abort("Call init_forbes_lists first")

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
                  is.string(content$template) &&
                  !is.na(content$template) &&
                  is.scalar(content$from_year) &&
                  is.integer(content$from_year) &&
                  # allow content$from_year NA
                  is.flag(content$active) &&
                  !is.na(content$active))

      .share$db[[content$name]] <- .blank_list(content$template)

      if (content$active) {
        if (is.na(content$from_year)) {
          .scrape_list(content$name, content$type, content$template, 0L)
        } else {
          from_year <-
            if_else(.share$from_year >= content$from_year,
                    .share$from_year,
                    content$from_year)
          assert_that(from_year <= .share$to_year)

          for (year in seq(from_year, .share$to_year)) {
            .scrape_list(content$name, content$type, content$template, year)
          }
        }
      }
    }

    invisible()
  }

.scrape_list <-
  function(name, type, template, year) {
    assert_that(is.list(.share$db) &&
                is.string(name) &&
                !is.na(name) &&
                is.string(type) &&
                !is.na(type) &&
                is.string(template) &&
                !is.na(template) &&
                is.scalar(year) &&
                is.integer(year) &&
                !is.na(year))

    dispatch <-
      list(person = .scrape_person_list,
           org = .scrape_org_list)

    scrape_fun <- dispatch[[type]]
    assert_that(is.function(scrape_fun))

    inform(sprintf("Scraping list '%s' for year %d", name, year))

    scrape_fun(name, year)
    assert_that(is.list(.share$response))

    if (length(.share$response) > 0L) {
      rows <- lapply(.share$response, .parse_row, template)
      rows <- as_tibble(do.call(rbind, rows))

      .share$db[[name]] <- rbind(.share$db[[name]], rows)
    } else {
      warn(sprintf("List '%s' for year %d has no rows", name, year))
    }

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
                is.scalar(.share$verbose) &&
                is.integer(.share$verbose) &&
                !is.na(.share$verbose) &&
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

        if (.share$verbose > 0L) {
          inform(sprintf("Sleeping %g secs before doing request",
                         as.integer(sleep_time)))
        }

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

get_forbes_list <-
  function(name, .year) {
    assert_that(is_tibble(.share$contents) &&
                is.list(.share$db) &&
                is.string(name) &&
                !is.na(name) &&
                name %in% .share$contents$name &&
                is.scalar(.year) &&
                is.integer(.year) &&
                !is.na(.year))

    if (is.null(.share$db[[name]]))
      abort(sprintf("List %s not exists, try sync or load", name))

    .share$db[[name]] %>% dplyr::filter(year == .year)
  }

view_forbes_list <-
  function(name, n = 100L)
    page(x, method = 'print', n = n)

.blank_list <-
  function(template) {
    assert_that(is.scalar(template) && !is.na(template))

    columns <- .make_columns(template)
    assert_that(is.character(columns) &&
                length(columns) > 0L &&
                !any(is.na(columns)))

    blank_columns <-
      list('natural_id' = character(),
           'year' = integer(),
           'timestamp' = POSIXct(),
           'rank' = integer(),
           'position' = integer(),
           'person_uri' = character(),
           'person_name' = character(),
           'person_image' = character(),
           'first_name' = character(),
           'last_name' = character(),
           'gender' = character(),
           'status' = character(),
           'birth_date' = Date(),
           'age' = integer(),
           'bio' = character(),
           'about' = character(),
           'country' = character(),
           'country_of_citizenship' = character(),
           'residence_state_region' = character(),
           'residence_msa' = character(),
           'state' = character(),
           'city' = character(),
           'self_made' = logical(),
           'self_made_rank' = integer(),
           'category' = character(),
           'source' = character(),
           'industries' = character(),
           'embargo' = logical(),
           'employment_uri' = character(),
           'employment_name' = character(),
           'employment_title' = character(),
           'employment_total_earnings' = numeric(),
           'employment_total_earning_est' = logical(),
           'employment_salary' = numeric(),
           'employment_salary_est' = logical(),
           'employment_endorsements' = numeric(),
           'employment_bonus' = numeric(),
           'employment_government' = logical(),
           'employment_revenue' = numeric(),
           'employment_revenue_year' = integer(),
           'employment_website' = character(),
           'employment_featured_executive' = logical(),
           'final_worth' = numeric(),
           'est_worth_prev' = numeric(),
           'private_assets_worth' = numeric(),
           'archived_worth' = numeric(),
           'recent_earnings' = numeric(),
           'other_compensation' = numeric(),
           'philanthropy_score' = integer())
    assert_that(length(setdiff(columns, names(blank_columns))) == 0L)

    blank_columns[columns]
  }

.parse_row <-
  function(json, template) {
    assert_that(is.list(json) && is.scalar(template) && !is.na(template))

    columns <- .make_columns(template)
    assert_that(is.character(columns) &&
                length(columns) > 0L &&
                !any(is.na(columns)))

    names(columns) <- columns

    dispatch <-
      list('natural_id' = function() to_string(json$naturalId),
           'year' = function() to_integer(json$year),
           'timestamp' = function() to_timestamp(json$timestamp),
           'rank' = function() to_integer(json$rank),
           'position' = function() to_integer(json$position),
           'person_uri' = function() to_string(json$person$uri),
           'person_name' = function() to_string(json$person$name),
           'person_image' = function() to_string(json$person$squareImage),
           'first_name' = function() to_string(json$firstName),
           'last_name' = function() to_string(json$lastName),
           'gender' = function() to_string(json$gender),
           'status' = function() to_string(json$status),
           'birth_date' = function() to_date(json$birthDate),
           'age' = function() to_integer(json$age),
           'bio' = function() to_text(json$bios, collapse = "\n"),
           'about' = function() to_text(json$abouts, collapse = "\n"),
           'country' = function() to_string(json$country),
           'country_of_citizenship' = function() to_string(json$countryOfCitizenship),
           'residence_state_region' = function() to_string(json$residenceStateRegion),
           'residence_msa' = function() to_string(json$residenceMsa),
           'state' = function() to_string(json$state),
           'city' = function() to_string(json$city),
           'self_made' = function() to_bool(json$selfMade),
           'self_made_rank' = function() to_integer(json$selfMadeRank),
           'category' = function() to_string(json$category),
           'source' = function() to_string(json$source),
           'industries' = function() to_text(json$industries, collapse = ","),
           'embargo' = function() to_bool(json$embargo),
           'employment_uri' = function() to_string(json$employment$uri),
           'employment_name' = function() to_string(json$employment$name),
           'employment_title' = function() to_string(json$employment$title),
           'employment_total_earnings' = function() to_number(json$employment$totalEarnings),
           'employment_total_earning_est' = function() to_bool(json$employment$totalEarningEst),
           'employment_salary' = function() to_number(json$employment$salary),
           'employment_salary_est' = function() to_bool(json$employment$salaryEst),
           'employment_endorsements' = function() to_number(json$employment$endorsements),
           'employment_bonus' = function() to_number(json$employment$bonus),
           'employment_government' = function() to_bool(json$employment$government),
           'employment_revenue' = function() to_number(json$employment$revenue),
           'employment_revenue_year' = function() to_integer(json$employment$revenueYear),
           'employment_website' = function() to_string(json$employment$website),
           'employment_featured_executive' = function() to_bool(json$employment$featuredExecutive),
           'final_worth' = function() to_number(json$finalWorth),
           'est_worth_prev' = function() to_number(json$estWorthPrev),
           'private_assets_worth' = function() to_number(json$privateAssetsWorth),
           'archived_worth' = function() to_number(json$archivedWorth),
           'recent_earnings' = function() to_number(json$recentEarnings),
           'other_compensation' = function() to_number(json$otherCompensation),
           'philanthropy_score' = function() to_integer(json$philanthropyScore))
    assert_that(length(setdiff(columns, names(dispatch))) == 0L)

    parse_column <-
      function(column) {
        parse_fun <- dispatch[[column]]
        assert_that(is.function(parse_fun))

        parse_fun()
      }

    as_tibble(lapply(columns, parse_column))
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
  function(x, collapse = " ") {
    if (is.null(x)) x <- as.character(NA)

    if_else(is.list(x) && length(x) > 0L,
            paste(na.omit(x), collapse = collapse),
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

#to_gender <-
#  function(x) {
#    if (is.null(x)) x <- as.character(NA)
#
#    case_when(is.string(x) && x == "M" ~ "Male",
#              is.string(x) && x == "F" ~ "Female",
#              is.string(x) ~ "Other",
#              .default = as.character(NA))
#  }

