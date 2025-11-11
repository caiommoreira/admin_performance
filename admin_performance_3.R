# admin_performance.R

rm(list = ls())

suppressPackageStartupMessages({
  library(shiny)
  library(dplyr)
  library(dbplyr)
  library(tidyr)
  library(DT)
  library(lubridate)
  library(DBI)
  library(pool)
  library(RMariaDB)
  library(httr)
  library(highcharter)
  library(jsonlite)
  library(mongolite)
})

is_local <- 1
is_debug <- 0

# --------------------- settings ---------------------------------

directory <- tryCatch(paste0(dirname(rstudioapi::getSourceEditorContext()$path), "/"),error = function(e) "./")

source(paste0(directory, "Env.Data.R"))

config <- if (is_debug) getHomEnvConfig() else getProdEnvConfig()

app_title <- "Sensorial – Admin Performance (alpha)"

api_address <- "https://admin.sensorial.life/"

header_key <- config[7]

PER_PAGE <- 30L

capacity_labels <- c(
  "controle-de-impulsividade" = "Controle de Impulsividade",
  "tomada-de-decisao"         = "Decisão",
  "atencao"                   = "Atenção",
  "memoria"                   = "Memória",
  "flexibilidade-cognitiva"   = "Flexibilidade Cognitiva",
  "reacao"                    = "Reação",
  "raciocinio"                = "Raciocínio"
)

# --------------------- connection ---------------------------------

pool <- dbPool(
  drv      = RMariaDB::MariaDB(),
  user     = config[4],
  password = config[5],
  dbname   = config[3],
  host     = config[1],
  port     = as.numeric(config[2])
)
onStop(function() poolClose(pool))

try({
  invisible(DBI::dbGetQuery(pool, "SELECT 1"))
}, silent = TRUE)

# --------------------- minimal tables ---------------------------------

users                 <- tbl(pool, "users")
user_groups           <- tbl(pool, "user_groups")
legal_entity_users    <- tbl(pool, "legal_entity_users")
user_question_answers <- tbl(pool, "user_question_answers") # Q37 para nome

# --------------------- helpers ---------------------------------

# ---- login -----

login_modal <- function() {
  modalDialog(
    title = "Restricted access",
    textInput("login_email", "E-mail", value = ""),
    passwordInput("login_pass", "Password", value = ""),
    div(style = "color:#b00; font-weight:600;", textOutput("login_error")),
    footer = tagList(
      modalButton("Cancel"),
      actionButton("login_confirm", "Sign in", class = "btn btn-primary")
    ),
    easyClose = FALSE, fade = TRUE
  )
}

api_login_get_token <- function(email, password, api_address, header_key) {
  endpoint <- paste0(api_address, "oauth/login")
  body <- list(
    scopes     = c("reports"),
    identifier = email,
    password   = password
  )
  res <- httr::POST(endpoint,
                    body = body,
                    encode = "json",
                    config = httr::add_headers(`X-Secret-Key` = header_key))
  list(status = httr::status_code(res),
       content = tryCatch(httr::content(res, as = "parsed"), error = function(e) NULL))
}

# ---- institution, groups and users -----

api_get_institution_report <- function(token, api_address, header_key) {
  url <- paste0(api_address, "api/v1/reports/institution")
  res <- httr::GET(url,
                   config = httr::add_headers(
                     Authorization = paste("Bearer", token),
                     `X-Secret-Key` = header_key
                   ))
  httr::stop_for_status(res)
  httr::content(res, as = "parsed")
}

get_user_ids_for_institution_or_group <- function(institution_id, group_id_or_all = "ALL") {
  if (identical(group_id_or_all, "ALL")) {
    legal_entity_users %>%
      filter(.data$legal_entity_id == !!as.integer(institution_id)) %>%
      transmute(user_id = as.integer(.data$user_id)) %>%
      distinct() %>% collect() %>% pull(.data$user_id)
  } else {
    gid <- as.integer(group_id_or_all)
    user_groups %>%
      filter(.data$group_id == !!gid) %>%
      transmute(user_id = as.integer(.data$user_id)) %>%
      distinct() %>% collect() %>% pull(.data$user_id)
  }
}

get_names_for_users <- function(uids) {
  if (length(uids) == 0) return(tibble(user_id = integer(), name = character()))
  user_question_answers %>%
    filter(.data$question_id == 37, .data$user_id %in% !!as.integer(uids)) %>%
    group_by(.data$user_id) %>%
    slice_max(order_by = coalesce(.data$updated_at, .data$created_at), n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    transmute(user_id = as.integer(.data$user_id), name = as.character(.data$value)) %>%
    collect()
}

# ---- general -----

hc_with_soft_anim <- function(hc) {
  hc %>% hc_plotOptions(series = list(animation = list(duration = 800)))
}

hc_gauge_pct <- function(value_pct, title_txt) {
  hc_with_soft_anim(
    highchart() %>%
      hc_chart(type = "solidgauge") %>%
      hc_title(text = title_txt, style = list(fontSize = "14px")) %>%
      hc_pane(center = list('50%', '85%'), size = '120%',
              startAngle = -90, endAngle = 90,
              background = list(
                list(outerRadius = '100%', innerRadius = '60%', shape = 'arc')
              )) %>%
      hc_yAxis(min = 0, max = 100, stops = list(
        list(0.3, "#f15c80"),
        list(0.6, "#f7a35c"),
        list(1.0, "#90ed7d")
      ),
      title = list(text = NULL),
      lineWidth = 0, tickInterval = 25,
      labels = list(y = 16)) %>%
      hc_series(
        list(
          name = "Percent",
          data = list(round(as.numeric(value_pct), 2)),
          dataLabels = list(format = '<span style="font-size:18px">{y}%</span>')
        )
      ) %>%
      hc_tooltip(enabled = FALSE)
  )
}

hc_cols_users_members <- function(amount_users, amount_members) {
  df <- tibble::tibble(
    Category = c("Usuários", "Membros"),
    Value = c(amount_users, amount_members)
  )
  hc_with_soft_anim(
    highchart() %>%
      hc_title(text = "Usuários vs Membros") %>%
      hc_xAxis(categories = df$Category) %>%
      hc_yAxis(title = list(text = NULL)) %>%
      hc_add_series(type = "column", data = df$Value, name = "Quantidade") %>%
      hc_plotOptions(column = list(dataLabels = list(enabled = TRUE)))
  )
}

hc_bar_feelings <- function(happy, tired_out, tense, night_of_sleep) {
  df <- tibble::tibble(
    Feeling = c("Feliz", "Cansado", "Tenso", "Noite de sono"),
    Score   = c(happy, tired_out, tense, night_of_sleep) * 100
  )
  hc_with_soft_anim(
    highchart() %>%
      hc_title(text = "Médias de sentimentos") %>%
      hc_xAxis(categories = df$Feeling) %>%
      hc_yAxis(title = list(text = "%"), max = 100) %>%
      hc_add_series(type = "bar", data = round(df$Score, 1), name = "Percentual") %>%
      hc_plotOptions(series = list(dataLabels = list(enabled = TRUE, format = "{point.y:.1f}%")))
  )
}

kpi_card <- function(title, value, subtitle = NULL) {
  div(style="border:1px solid #eee; border-radius:10px; padding:16px; background:#fff; box-shadow:0 1px 3px rgba(0,0,0,0.05);",
      div(style="font-size:13px; color:#666; margin-bottom:6px;", title),
      div(style="font-size:28px; font-weight:700;", value),
      if (!is.null(subtitle)) div(style="font-size:12px; color:#888; margin-top:4px;", subtitle)
  )
}

compute_age_on_date <- function(dob, ref_date) {
  dob <- as.Date(dob)
  ref_date <- as.Date(ref_date)
  
  n <- max(length(dob), length(ref_date))
  if (length(dob) != n)      dob      <- rep(dob, length.out = n)
  if (length(ref_date) != n) ref_date <- rep(ref_date, length.out = n)
  
  out <- rep(NA_integer_, n)
  ok  <- !is.na(dob) & !is.na(ref_date)
  
  if (any(ok)) {
    y <- lubridate::year(ref_date[ok]) - lubridate::year(dob[ok])
    adj <- (lubridate::month(ref_date[ok]) <  lubridate::month(dob[ok])) |
      (lubridate::month(ref_date[ok]) == lubridate::month(dob[ok]) &
         lubridate::day(ref_date[ok])   <  lubridate::day(dob[ok]))
    out[ok] <- as.integer(y - as.integer(adj))
  }
  out
}

get_dobs_for_users <- function(uids) {
  if (length(uids) == 0) return(tibble(user_id = integer(), dob = as.Date(character())))
  
  raw <- user_question_answers %>%
    filter(.data$question_id == 30, .data$user_id %in% !!as.integer(uids)) %>%
    group_by(.data$user_id) %>%
    slice_max(order_by = coalesce(.data$updated_at, .data$created_at), n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(user_id, value) %>%
    collect()
  
  if (!nrow(raw)) return(tibble(user_id = integer(), dob = as.Date(character())))
  
  raw %>%
    mutate(
      user_id = as.integer(user_id),
      dob = parse_dob_vector(value)  # << uso do parser robusto
    ) %>%
    transmute(user_id, dob)
}

parse_dob_vector <- function(x) {
  x <- as.character(x)
  out <- suppressWarnings(lubridate::ymd(x, quiet = TRUE))
  
  # tenta dmy quando ymd falhar
  need <- is.na(out) & grepl("^\\d{1,2}[/-]\\d{1,2}[/-]\\d{2,4}$", x)
  if (any(need)) {
    out[need] <- suppressWarnings(lubridate::dmy(x[need], quiet = TRUE))
  }
  
  # trata "YYYY" apenas (4 dígitos)
  only_year <- is.na(out) & grepl("^\\d{4}$", x)
  if (any(only_year)) {
    out[only_year] <- as.Date(paste0(x[only_year], "-06-30"))
  }
  
  # trata "YYYY/MM/DD" (variação)
  need2 <- is.na(out) & grepl("^\\d{4}[/-]\\d{1,2}[/-]\\d{1,2}$", x)
  if (any(need2)) {
    out[need2] <- suppressWarnings(lubridate::ymd(x[need2], quiet = TRUE))
  }
  
  out
}

color_by_mean <- function(values, mean_val, high_is_good) {
  # azul claro acima da média (ou abaixo, se for tempo), azul escuro no oposto
  col_up   <- "#7cb5ec"  # claro
  col_down <- "#1f4e79"  # escuro
  if (high_is_good) ifelse(values >= mean_val, col_up, col_down) else ifelse(values <= mean_val, col_up, col_down)
}

prep_bar_series <- function(categories, values, high_is_good, bounds) {
  df <- tibble::tibble(cat = categories, val = as.numeric(values)) %>% dplyr::filter(!is.na(val))
  if (!nrow(df)) return(list(cats = character(0), vals = numeric(0)))
  # clamp aos limites
  df$val <- pmin(pmax(df$val, bounds[1]), bounds[2])
  # ordenação: maiores primeiro (ou menores primeiro, se tempo)
  if (high_is_good) df <- dplyr::arrange(df, dplyr::desc(val), cat) else df <- dplyr::arrange(df, val, cat)
  list(cats = df$cat, vals = df$val)
}

axis_with_headroom <- function(bounds, values, pad_frac = 0.08) {
  if (!length(values)) return(list(min = bounds[1], max = bounds[2]))
  rng <- diff(bounds)
  pad <- max(1e-9, pad_frac * rng)
  vmin <- min(values, na.rm = TRUE)
  vmax <- max(values, na.rm = TRUE)
  list(
    min = max(bounds[1], vmin - pad),
    max = min(bounds[2], vmax + pad)
  )
}

bar_datalabels_opts <- function(fmt) {
  list(
    enabled = TRUE,
    format  = fmt,
    inside  = TRUE,
    align   = "right",  # encosta no fim da barra
    x       = -4,       # leve deslocamento para dentro
    crop    = FALSE,
    overflow= "none",
    style   = list(textOutline = "none")
  )
}

pluck_or <- function(x, path, default = NA) {
  tryCatch({
    v <- purrr::pluck(x, !!!path)
    if (is.null(v)) default else v
  }, error = function(...) default)
}

fetch_user_evaluations <- function(api_address, access_token, header_key, user_id) {
  require(httr)
  require(dplyr)
  require(purrr)
  require(tibble)
  
  url0 <- paste0(api_address, "api/v1/reports/users/", as.character(user_id), "/evaluations")
  hdr  <- add_headers(Authorization = paste("Bearer", access_token), `X-Secret-Key` = header_key)
  
  all_rows <- list()
  page_i   <- 0L
  next_url <- url0
  
  while (!is.null(next_url) && nzchar(next_url)) {
    resp <- GET(url = next_url, config = hdr)
    stop_for_status(resp)
    dat  <- content(resp, as = "parsed")
    
    rows <- purrr::map(dat$data, function(x) {
      tibble::tibble(
        evaluation_date     = pluck_or(x, c("evaluation_date"), NA_character_),
        score_id            = pluck_or(x, c("score_id"), NA_character_),
        nrss                = pluck_or(x, c("parameters","nrss"), NA_real_),
        reaction_quality    = pluck_or(x, c("parameters","reaction_quality"), NA_real_),
        decision_quality    = pluck_or(x, c("parameters","decision_quality"), NA_real_),
        attention           = pluck_or(x, c("parameters","attention"), NA_real_),
        impulsivity_control = pluck_or(x, c("parameters","impulsivity_control"), NA_real_),
        rt_avg              = pluck_or(x, c("parameters","rt_avg"), NA_real_),
        dt_avg              = pluck_or(x, c("parameters","dt_avg"), NA_real_),
        stamp               = pluck_or(x, c("parameters","stamp"), NA_character_),
        age                 = pluck_or(x, c("parameters","age"), NA_real_),
        weight              = pluck_or(x, c("parameters","weight"), NA_real_),
        height              = pluck_or(x, c("parameters","height"), NA_real_),
        sex                 = pluck_or(x, c("parameters","sex"), NA_character_),
        rdc_type            = pluck_or(x, c("rdc_type"), NA_character_),
        ref_category_id     = pluck_or(x, c("parameters","performance_reference","data_reference_category_id"), NA),
        reference_mean      = pluck_or(x, c("parameters","performance_reference","mean"), NA_real_),
        reference_sd        = pluck_or(x, c("parameters","performance_reference","sd"), NA_real_),
        happy               = pluck_or(x, c("parameters","base_question_answer","parameters","responses","happy"), NA_real_),
        tired_out           = pluck_or(x, c("parameters","base_question_answer","parameters","responses","tired_out"), NA_real_),
        tense               = pluck_or(x, c("parameters","base_question_answer","parameters","responses","tense"), NA_real_),
        night_of_sleep      = pluck_or(x, c("parameters","base_question_answer","parameters","responses","night_of_sleep"), NA_real_)
      )
    }) %>% dplyr::bind_rows()
    
    all_rows[[length(all_rows)+1]] <- rows
    # paginação
    next_url <- pluck_or(dat, c("links","next"), "")
    page_i   <- page_i + 1L
    if (identical(dat$meta$last_page, page_i)) {
      # se chegamos na última, interrompe
      break
    }
  }
  
  out <- dplyr::bind_rows(all_rows)
  # coerções úteis
  out <- out %>%
    dplyr::mutate(
      user_id = as.integer(user_id),
      evaluation_date = as.Date(evaluation_date)
    ) %>%
    dplyr::arrange(evaluation_date)
  out
}

clamp <- function(x, lo, hi) max(lo, min(hi, x))

page_bounds <- function(n, page, per = PER_PAGE) {
  if (n <= 0) return(c(0L, -1L))
  s <- (page - 1L) * per + 1L
  e <- min(n, s + per - 1L)
  c(s, e)
}

monthly_with_totals <- function(out, month_label = "Mês") {
  # garante coluna de mês como texto (evita problema de fator ao inserir "TOTAL")
  out[[month_label]] <- as.character(out[[month_label]])
  # identifica colunas numéricas (anos)
  num_cols <- setdiff(names(out), month_label)
  # total por linha (meses)
  out$Total <- rowSums(out[num_cols], na.rm = TRUE)
  # total por coluna (anos)
  col_sums <- colSums(out[num_cols], na.rm = TRUE)
  grand    <- sum(col_sums, na.rm = TRUE)
  totals_row <- c(setNames(list("TOTAL"), month_label), as.list(col_sums), Total = grand)
  # une tabela + linha TOTAL
  out2 <- dplyr::bind_rows(out, totals_row)
  # garante numérico nas colunas de ano e Total
  for (cn in c(num_cols, "Total")) out2[[cn]] <- suppressWarnings(as.numeric(out2[[cn]]))
  out2
}

# ---- evals -----------

eval_metric_spec <- function(key) {
  # high_is_good = TRUE para métricas "maiores melhor"; FALSE para tempos (menor melhor)
  switch(key,
         "nrss" = list(label = "Performance Cognitiva", bounds = c(0, 1000), high_is_good = TRUE,  fmt = "{point.y:.0f}"),
         "reaction_quality"  = list(label = "Reação",                     bounds = c(0, 100), high_is_good = TRUE,  fmt = "{point.y:.0f}"),
         "decision_quality"  = list(label = "Decisão",                    bounds = c(0, 100), high_is_good = TRUE,  fmt = "{point.y:.0f}"),
         "attention"         = list(label = "Atenção",                    bounds = c(0, 100), high_is_good = TRUE,  fmt = "{point.y:.0f}"),
         "impulsivity_control" = list(label = "Controle de Impulsividade",bounds = c(0, 100), high_is_good = TRUE,  fmt = "{point.y:.0f}"),
         "peripheral_vision" = list(label = "Visão Periférica",           bounds = c(0, 100), high_is_good = TRUE,  fmt = "{point.y:.0f}"),
         "rt_avg"            = list(label = "Tempo de Reação (ms)",       bounds = c(180, 600), high_is_good = FALSE, fmt = "{point.y:.0f} ms"),
         "dt_avg"            = list(label = "Tempo de Decisão (ms)",      bounds = c(0, 300),   high_is_good = FALSE, fmt = "{point.y:.0f} ms"),
         # default
         list(label = key, bounds = c(0, 100), high_is_good = TRUE, fmt = "{point.y}")
  )
}

hc_circular_bar <- function(value, minmax, title_txt, fmt = "{y}",colors = c("#f15c80","#f7a35c","#90ed7d"),high_is_good = TRUE, size = "90%", inner = "70%") {
  rng <- max(minmax[2] - minmax[1], 1e-9)
  v   <- as.numeric(value)
  v   <- max(minmax[1], min(minmax[2], v))
  stops <- if (high_is_good) list(
    list(0.33, colors[1]), list(0.66, colors[2]), list(1.0, colors[3])
  ) else list( # invert scale: lower is better
    list(0.33, colors[3]), list(0.66, colors[2]), list(1.0, colors[1])
  )
  highchart() %>%
    hc_chart(type = "solidgauge") %>%
    hc_title(text = title_txt, style = list(fontSize = "14px")) %>%
    hc_pane(
      startAngle = 0, endAngle = 360,
      background = list(
        list(outerRadius = size, innerRadius = inner, shape = "arc", borderWidth = 0, backgroundColor = "#f2f2f2")
      )
    ) %>%
    hc_yAxis(
      min = minmax[1], max = minmax[2], lineWidth = 0, tickAmount = 0,
      minorTickInterval = NULL, labels = list(enabled = FALSE), title = list(text = NULL),
      stops = stops
    ) %>%
    hc_series(list(
      name = title_txt,
      data = list(v),
      dataLabels = list(format = sprintf('<span style="font-size:18px">%s</span>', fmt)),
      tooltip = list(pointFormat = sprintf("<b>%s:</b> {point.y}", title_txt))
    )) %>%
    hc_tooltip(enabled = FALSE)
}

hc_speedometer <- function(value, minmax, title_txt, fmt = "{y} ms",colors = c("#90ed7d","#f7a35c","#f15c80"),high_is_good = FALSE) {
  stops <- if (high_is_good) list(
    list(0.33, colors[1]), list(0.66, colors[2]), list(1.0, colors[3])
  ) else list(
    list(0.33, colors[1]), list(0.66, colors[2]), list(1.0, colors[3])
  )
  highchart() %>%
    hc_chart(type = "solidgauge") %>%
    hc_title(text = title_txt, style = list(fontSize = "14px")) %>%
    hc_pane(center = list("50%","85%"), size = "120%", startAngle = -90, endAngle = 90,
            background = list(list(outerRadius="100%", innerRadius="60%", shape="arc"))) %>%
    hc_yAxis(min = minmax[1], max = minmax[2], lineWidth = 0, tickAmount = 3,
             labels = list(y = 18), title = list(text = NULL), stops = stops) %>%
    hc_series(list(
      name = title_txt,
      data = list(round(as.numeric(value), 2)),
      dataLabels = list(format = sprintf('<span style="font-size:18px">%s</span>', fmt))
    )) %>%
    hc_tooltip(enabled = FALSE)
}

hc_polar_rose <- function(categories, values_pct, title_txt = "Humor (rose)", max_pct = 100) {
  
  stopifnot(length(categories) == length(values_pct))
  vals <- as.numeric(values_pct)
  vals[!is.finite(vals)] <- 0
  highchart() %>%
    hc_chart(polar = TRUE, type = "column") %>%
    hc_title(text = title_txt) %>%
    hc_xAxis(categories = categories, tickmarkPlacement = "on", lineWidth = 0) %>%
    hc_yAxis(min = 0, max = max_pct, endOnTick = FALSE, showLastLabel = TRUE,
             gridLineInterpolation = "polygon", lineWidth = 0, tickInterval = 25,
             title = list(text = NULL)) %>%
    hc_plotOptions(column = list(pointPadding = 0, groupPadding = 0.05,
                                 dataLabels = list(enabled = TRUE, format = "{point.y:.0f}%"))) %>%
    hc_add_series(name = "Estado", data = round(vals, 1)) %>%
    hc_tooltip(pointFormat = "<b>{point.category}:</b> {point.y:.0f}%")
}

# ---- minigames -----

games_names <- tryCatch(readRDS(paste0(directory, "games_labels.RDS")),error = function(e) tibble::tibble(game_id = integer(), name = character()))

get_moove_scores_data <- function(sel_users) {
  if (length(sel_users) == 0) return(tibble::tibble())
  url.mongodb <- config[6]
  ids <- as.integer(sel_users)
  
  m <- mongolite::mongo(collection = "moove_scores", url = url.mongodb)
  on.exit(m$disconnect(), add = TRUE)
  
  # Inclui o campo parameters.score_percentiles no projection
  q <- jsonlite::toJSON(list(user_id = list("$in" = as.list(ids))), auto_unbox = TRUE)
  df <- m$find(
    query  = q,
    fields = '{"_id":0,"user_id":1,"game_id":1,"date_time":1,"parameters.score_percentiles":1}'
  )
  
  if (is.null(df) || !nrow(df)) return(tibble::tibble())
  
  df <- tibble::as_tibble(df)
  
  # coloca o score em coluna plana
  if ("parameters" %in% names(df)) {
    df$score <- suppressWarnings(as.numeric(df$parameters$score_percentiles))
    df$parameters <- NULL
  } else {
    # coleções antigas podem não ter parameters; mantém NA
    if (!"score" %in% names(df)) df$score <- NA_real_
  }
  
  df
}

get_user_settings_avg_percentiles <- function(sel_users) {
  if (length(sel_users) == 0) return(tibble::tibble())
  url.mongodb <- config[6]
  ids <- as.integer(sel_users)
  
  m <- mongolite::mongo(collection = "user_settings", url = url.mongodb)
  on.exit(m$disconnect(), add = TRUE)
  
  q <- jsonlite::toJSON(list(user_id = list("$in" = as.list(ids))), auto_unbox = TRUE)
  df <- m$find(
    query  = q,
    fields = '{"_id":0,"user_id":1,"performance.average_percentiles":1}'
  )
  if (is.null(df) || !nrow(df)) return(tibble::tibble())
  
  df <- tibble::as_tibble(df)
  
  # pega diretamente o subdataframe das capacidades
  subdf <- df$performance$average_percentiles
  
  # une user_id com as colunas internas do subdataframe
  out <- bind_cols(
    tibble::tibble(user_id = df$user_id),
    tibble::as_tibble(subdf)
  )
  
  out
}

to_long_percentiles <- function(df) {
  if (is.null(df) || !nrow(df)) {
    return(tibble::tibble(user_id = integer(), capacity = character(), value = numeric()))
  }
  df %>%
    tidyr::pivot_longer(
      cols = tidyselect::any_of(names(capacity_labels)),
      names_to = "capacity",
      values_to = "value"
    ) %>%
    dplyr::mutate(capacity_label = capacity_labels[capacity] %||% capacity)
}

# ---- measurements -----

get_measurement_summaries <- function(sel_users) {
  # Returns a tibble with:
  # user_id, score_id, measurement_id, measurement_name,
  # score, result1..result4, created_at (POSIXct, UTC)
  
  # Early exit
  if (length(sel_users) == 0) {
    return(tibble::tibble(
      user_id = integer(), score_id = character(),
      measurement_id = integer(), measurement_name = character(),
      score = numeric(), result1 = numeric(), result2 = numeric(),
      result3 = numeric(), result4 = numeric(),
      created_at = as.POSIXct(character())
    ))
  }
  
  # Mongo connection (same URL used elsewhere in the app)
  url.mongodb <- config[6]
  m <- mongolite::mongo(collection = "measurement_summarys", url = url.mongodb)
  on.exit(m$disconnect(), add = TRUE)
  
  # Query: only selected users + exclude measurement_id == 217
  q <- list(
    user_id = list("$in" = as.list(as.integer(sel_users))),
    measurement_id = list("$ne" = 217L)
  )
  
  # Project only required fields
  fields <- '{
    "_id": 0,
    "user_id": 1,
    "score_id": 1,
    "measurement_id": 1,
    "parameters.score": 1,
    "parameters.result1": 1,
    "parameters.result2": 1,
    "parameters.result3": 1,
    "parameters.result4": 1,
    "created_at": 1
  }'
  
  raw <- m$find(
    query  = jsonlite::toJSON(q, auto_unbox = TRUE),
    fields = fields
  )
  
  if (is.null(raw) || !nrow(raw)) {
    return(tibble::tibble(
      user_id = integer(), score_id = character(),
      measurement_id = integer(), measurement_name = character(),
      score = numeric(), result1 = numeric(), result2 = numeric(),
      result3 = numeric(), result4 = numeric(),
      created_at = as.POSIXct(character())
    ))
  }
  
  df <- tibble::as_tibble(raw)
  
  df_sub <- df$parameters
  df <- cbind(df %>% select(-parameters),df_sub) %>% filter(!measurement_id %in% c(217,223,225))
  
  ids <- unique(df$measurement_id)
  if (length(ids) == 0) {
    df$measurement_name <- character(nrow(df))
    return(df %>% dplyr::relocate(measurement_name, .after = measurement_id))
  }
  
  msr <- tbl(pool, "measurements") %>%
    dplyr::filter(.data$id %in% !!as.integer(ids)) %>%
    dplyr::select(measurement_id = .data$id, measurement_name = .data$measurement) %>%
    dplyr::collect()
  
  out <- df %>%
    dplyr::left_join(msr, by = "measurement_id") %>%
    dplyr::relocate(measurement_name, .after = measurement_id) %>%
    dplyr::arrange(.data$user_id, .data$measurement_id, .data$created_at)
  
  out
}

fetch_user_measurements <- function(api_address, access_token, header_key, user_id) {
  require(httr); require(dplyr); require(purrr); require(tibble); require(jsonlite)
  url0 <- paste0(api_address, "api/v1/reports/users/", as.character(user_id), "/measurements")
  hdr  <- add_headers(Authorization = paste("Bearer", access_token), `X-Secret-Key` = header_key)
  
  collect_measurements <- function(url0, hdr) {
    all_rows <- list()
    page_i   <- 0L
    next_url <- url0
    
    repeat {
      resp <- httr::GET(url = next_url, config = hdr)
      httr::stop_for_status(resp)
      dat  <- httr::content(resp, as = "parsed")
      
      rows <- purrr::map(dat$data, function(x) {
        tibble::tibble(
          created_at       = pluck_or(x, c("created_at"), NA_character_),
          score_id         = pluck_or(x, c("score_id"), NA_character_),
          measurement_id   = suppressWarnings(as.integer(pluck_or(x, c("measurement_id"), NA))),
          measurement_name = pluck_or(x, c("measurement_name"), NA_character_),
          
          # métrica principal e sub-resultados
          score   = suppressWarnings(as.numeric(pluck_or(x, c("parameters","score"),   NA))),
          result1 = suppressWarnings(as.numeric(pluck_or(x, c("parameters","result1"), NA))),
          result2 = suppressWarnings(as.numeric(pluck_or(x, c("parameters","result2"), NA))),
          result3 = suppressWarnings(as.numeric(pluck_or(x, c("parameters","result3"), NA))),
          result4 = suppressWarnings(as.numeric(pluck_or(x, c("parameters","result4"), NA))),
          
          # labels
          label_score = pluck_or(x, c("parameters","labels","score"), NA_character_),
          label_sec1 = pluck_or(x, c("parameters","labels","result1"), NA_character_),
          label_sec2 = pluck_or(x, c("parameters","labels","result2"), NA_character_),
          label_sec3 = pluck_or(x, c("parameters","labels","result3"), NA_character_),
          label_sec4 = pluck_or(x, c("parameters","labels","result4"), NA_character_),
          
          # referência — tenta em parameters/... e, se não tiver, no topo
          reference_mean = suppressWarnings(as.numeric(
            dplyr::coalesce(
              purrr::pluck(x, "parameters","performance_reference","mean", .default = NULL),
              purrr::pluck(x, "performance_reference","mean", .default = NULL),
              purrr::pluck(x, "performance_reference", 1, "mean", .default = NA_real_)
            )
          )),
          reference_sd = suppressWarnings(as.numeric(
            dplyr::coalesce(
              purrr::pluck(x, "parameters","performance_reference","sd", .default = NULL),
              purrr::pluck(x, "performance_reference","sd", .default = NULL),
              purrr::pluck(x, "performance_reference", 1, "sd", .default = NA_real_)
            )
          )),
          
          # estado basal (quando existir)
          happy          = suppressWarnings(as.numeric(pluck_or(x, c("base_question_answer","parameters","responses","happy"),        NA))),
          tired_out      = suppressWarnings(as.numeric(pluck_or(x, c("base_question_answer","parameters","responses","tired_out"),    NA))),
          tense          = suppressWarnings(as.numeric(pluck_or(x, c("base_question_answer","parameters","responses","tense"),        NA))),
          night_of_sleep = suppressWarnings(as.numeric(pluck_or(x, c("base_question_answer","parameters","responses","night_of_sleep"),NA)))
        )
      }) %>% dplyr::bind_rows()
      
      all_rows[[length(all_rows) + 1L]] <- rows
      page_i  <- page_i + 1L
      
      # avança a paginação de forma segura
      next_url <- pluck_or(dat, c("links","next"), NULL)
      if (is.null(next_url) || !nzchar(next_url)) break
      if (!is.null(dat$meta$last_page) && page_i >= as.integer(dat$meta$last_page)) break
    }
    
    dplyr::bind_rows(all_rows)
  }
  
  df_meas <- collect_measurements(url0, hdr)
  
  df_meas
}

to_millis <- function(x_date) {
  # aceita Date ou POSIXct
  if (inherits(x_date, "Date")) x_date <- as.POSIXct(x_date)
  highcharter::datetime_to_timestamp(x_date)
}

# --------------------- UI() ---------------------------------

ui <- fluidPage(
  tags$head(tags$meta(charset = "utf-8")),
  div(style="text-align:center; margin:16px 0 8px 0;",
      img(src = "sensorial_logo.png", style="max-width:380px; width:40%; height:auto;", alt="Sensorial Logo")
  ),
  fluidRow(
    column(
      width = 12,
      br(),
      uiOutput("ui_status_panel"),
      tabsetPanel(id = "tabs",
                  # ---- overview -----
                  tabPanel("Overview",
                           br(),
                           fluidRow(
                             column(6, highchartOutput("hc_users_members", height = "280px")),
                             column(3, highchartOutput("hc_trainings_pct", height = "280px")),
                             column(3, highchartOutput("hc_eval_members_pct", height = "280px"))
                           ),
                           br(),
                           fluidRow(
                             column(4, uiOutput("kpi_eval_score")),
                             column(8, highchartOutput("hc_feelings", height = "300px"))
                           )
                  ),
                  # ---- evals -----
                  tabPanel(
                    "Avaliações",
                    br(),
                    div(
                      style = "display:flex; justify-content:center; margin: 8px 0 16px 0;",
                      downloadButton("download_evals_xlsx", "Baixar resultados (XLSX)", class = "btn btn-primary")
                    ),
                    br(),
                    fluidRow(align = "center",DTOutput("tbl_mg_monthly_evals",width = "50%")),
                    br(),
                    br(),
                    fluidRow(
                      column(9, uiOutput("ui_eval_metric_tabs")),
                      column(3, div(style="text-align:right; margin-top:6px;", uiOutput("ui_eval_back")))
                    ),
                    highchartOutput("hc_evals", height = "650px"),
                    div(
                      style = "display:flex; justify-content:center; align-items:center; gap:12px; margin: 10px 0 4px 0;",
                      uiOutput("ui_eval_pager")  # <- prev | página X/Y | next
                    ),
                    br(),
                    uiOutput("ui_eval_detail"),
                    br()
                  ),
                  # ---- minigames -----
                  
                  tabPanel(
                    "Minigames",
                    br(),
                    div(
                      style = "display:flex; justify-content:center; margin: 8px 0 16px 0;",
                      downloadButton("download_mg_xlsx", "Baixar Dados Minigames (XLSX)", class = "btn btn-primary")
                    ),
                    br(),
                    # br(),
                    # # status/placeholder (iremos evoluir nos próximos micropassos)
                    # uiOutput("ui_minigames_status"),
                    br(),
                    fluidRow(align = "center",DTOutput("tbl_mg_monthly",width = "33%")),
                    br(),
                    br(),
                    br(),
                    fluidRow(align = "right",uiOutput("ui_mg_back")),
                    highchartOutput("hc_mg_counts", height = "650px"),
                    div(
                      style = "display:flex; justify-content:center; align-items:center; gap:12px; margin: 10px 0 4px 0;",
                      uiOutput("ui_mg_pager")   # ← prev | página X/Y | next
                    ),
                    br(), hr(), br(),
                    fluidRow(align = "center",radioButtons(
                      "sel_capacity",
                      label = "Capacidade cognitiva:",
                      choices = setNames(names(capacity_labels), capacity_labels),
                      selected = "atencao",
                      inline = TRUE
                    )),
                    fluidRow(align = "right",uiOutput("ui_perf_back")),
                    highchartOutput("hc_perf_groups", height = "650px"),
                    div(
                      style = "text-align:center; margin-top:6px;",
                      uiOutput("ui_perf_paging")
                    )
                  ),
                  
                  # ---- measurements -----
                  
                  tabPanel(
                    "Medidas Moove",
                    br(),
                    # uiOutput("ui_mm_status"),
                    div(
                      style = "display:flex; justify-content:center; margin: 8px 0 16px 0;",
                      downloadButton("download_mm_xlsx", "Baixar medidas (XLSX)", class = "btn btn-primary")
                    ),
                    br(),
                    fluidRow(align = "center", DTOutput("tbl_mm_monthly", width = "50%")),
                    br(),
                    br(),
                    br(),
                    fluidRow(
                      column(9, div(style="text-align:center;", uiOutput("ui_mm_metric_tabs"))),
                      column(3, div(style="text-align:right; margin-top:6px;", uiOutput("ui_mm_back")))
                    ),
                    highchartOutput("hc_mm", height = "650px"),
                    div(
                      style = "display:flex; justify-content:center; align-items:center; gap:12px; margin: 10px 0 4px 0;",
                      uiOutput("ui_mm_pager")
                    ),
                    br(),
                    uiOutput("ui_mm_detail")
                  )
                  
                  # ---- end -----
      )
    )
  )
)

# --------------------- server ---------------------------------

server <- function(input, output, session) {
  
  # ===================== reactives =====================
  
  authed          <- reactiveVal(FALSE)
  session_role    <- reactiveVal(NULL)        # "institution" | "trainer" (trainer ficará para próximo passo)
  api_token       <- reactiveVal(NULL)        # string
  institution_raw <- reactiveVal(NULL)        # lista completa do content (parsed)
  
  # ---- institution and groups -----
  
  institution_dt <- reactive({
    x <- institution_raw()
    if (is.null(x)) return(NULL)
    if (!is.null(x$data)) x$data else x
  })
  
  selected_institution_id <- reactive({
    d <- institution_dt(); req(d)
    as.integer(d$institution_id %||% NA_integer_)
  })
  
  groups_from_api <- reactive({
    req(authed(), session_role() == "institution")
    d <- req(institution_dt())
    gl <- d$groups
    if (is.null(gl) || length(gl) == 0) {
      return(tibble(id = integer(), name = character()))
    }
    ids   <- names(gl)
    names <- unlist(gl, use.names = FALSE)
    tibble(
      id = as.integer(ids),
      name = as.character(names)
    ) %>% arrange(name)
  })
  
  # ---- users -----
  
  scope_user_ids <- reactive({
    req(authed(), session_role() == "institution")
    inst_id <- req(selected_institution_id())
    choice  <- input$sel_group %||% "ALL"
    get_user_ids_for_institution_or_group(inst_id, choice)
  })
  
  scope_user_names <- reactive({
    uids <- scope_user_ids()
    nm   <- get_names_for_users(uids)
    tibble(user_id = as.integer(uids)) %>%
      left_join(nm, by = "user_id") %>%
      mutate(name = coalesce(name, paste0("user_", user_id))) %>%
      arrange(name)
  })
  
  selected_user_ids <- reactive({
    req(authed())
    if (!is.null(input$sel_user) && nzchar(as.character(input$sel_user))) {
      return(as.integer(input$sel_user))
    }
    as.integer(scope_user_ids())
  })
  
  # ---- evals -----
  
  evals_joined <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    
    # carrega as tabelas somente quando a aba 'Avaliações' é aberta
    rdcs_tbl   <- tbl(pool, "rdcs")
    rdc_vs_tbl <- tbl(pool, "rdc_vs")
    
    uids <- scope_user_ids()
    if (length(uids) == 0) {
      return(tibble::tibble())
    }
    
    # 1) rdcs (filtrar por users da instituição)
    rdcs_df <- rdcs_tbl %>%
      filter(.data$user_id %in% !!as.integer(uids)) %>%
      select(id, external_id, order_id, score_id, evaluation_date, user_id, session,
             nrss, rt_avg, dt_avg, reaction_quality, decision_quality, attention,
             impulsivity_control, stamp, analysis_version, age, weight, height, sex,
             institution_id, created_at, available) %>%
      collect()
    
    if (!nrow(rdcs_df)) return(tibble::tibble())
    
    # 2) rdc_vs (peripheral_vision) apenas para os ids coletados
    vs_df <- rdc_vs_tbl %>%
      filter(.data$rdc_id %in% !!as.integer(rdcs_df$id)) %>%
      select(rdc_id, peripheral_vision) %>%
      collect()
    
    # join e -1 quando não existir
    rdcs_vs <- rdcs_df %>%
      left_join(vs_df, by = c("id" = "rdc_id")) %>%
      mutate(peripheral_vision = dplyr::coalesce(peripheral_vision, -1))
    
    # 3) nomes (Q37) e DOB (Q30) → idade na data da avaliação
    nm_df  <- get_names_for_users(unique(rdcs_vs$user_id))
    dob_df <- get_dobs_for_users(unique(rdcs_vs$user_id))
    
    rdcs_enriched <- rdcs_vs %>%
      left_join(nm_df,  by = "user_id") %>%
      left_join(dob_df, by = "user_id") %>%
      mutate(
        name = dplyr::coalesce(name, paste0("user_", user_id)),
        age_on_eval = compute_age_on_date(dob, as.Date(evaluation_date))  # <-- mantém este nome de função
      )
    
    # 4) grupos (user_groups) → nomes via groups_from_api()
    g_api <- groups_from_api()
    ug <- user_groups %>%
      filter(.data$user_id %in% !!as.integer(unique(rdcs_enriched$user_id))) %>%
      transmute(user_id = as.integer(.data$user_id), group_id = as.integer(.data$group_id)) %>%
      collect()
    
    if (nrow(ug) && nrow(g_api)) {
      ug_named <- ug %>%
        left_join(g_api %>% rename(group_id = id, group_name = name), by = "group_id") %>%
        group_by(user_id) %>%
        summarise(groups = paste(sort(unique(group_name[!is.na(group_name)])), collapse = ", "),
                  .groups = "drop")
      rdcs_enriched <- rdcs_enriched %>% left_join(ug_named, by = "user_id")
    } else {
      rdcs_enriched$groups <- NA_character_
    }
    
    # 5) ordenar por data desc / user_id
    rdcs_enriched %>% arrange(desc(evaluation_date), user_id)
  })
  
  eval_view_mode <- reactiveVal("groups")
  
  eval_selected_group <- reactiveVal(NA_integer_)
  
  eval_metric_key <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    req(!is.null(input$eval_metric))   # <- evita render inicial “fantasma”
    input$eval_metric
  })
  
  eval_group_stats <- reactive({
    
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    key  <- eval_metric_key()
    spec <- eval_metric_spec(key)
    d <- evals_joined()
    if (!nrow(d)) return(tibble::tibble(group_id = integer(), group_name = character(), value = numeric()))
    if (key == "peripheral_vision") d <- d %>% dplyr::mutate(peripheral_vision = ifelse(peripheral_vision < 0, NA_real_, peripheral_vision))
    
    # 1) média por usuário (evita multiplicar avaliações na junção com grupos)
    d_user <- d %>%
      dplyr::group_by(user_id) %>%
      dplyr::summarise(value = mean(.data[[key]], na.rm = TRUE), .groups = "drop")
    
    # 2) mapeia usuários -> grupos (somente grupos da instituição)
    g_api <- groups_from_api()
    ug <- user_groups %>%
      dplyr::filter(.data$user_id %in% !!as.integer(d_user$user_id)) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id), group_id = as.integer(.data$group_id)) %>%
      dplyr::distinct() %>%
      dplyr::collect()
    
    if (!nrow(ug) || !nrow(g_api)) return(tibble::tibble(group_id = integer(), group_name = character(), value = numeric()))
    
    ug_named <- ug %>%
      dplyr::inner_join(g_api %>% dplyr::rename(group_id = id, group_name = name), by = "group_id")
    
    # 3) junta (user média) x (grupos) → média por grupo
    d_user %>%
      dplyr::left_join(ug_named, by = "user_id", relationship = "many-to-many") %>% # agora é esperado
      dplyr::group_by(group_id, group_name) %>%
      dplyr::summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>% filter(group_name != "" & !is.na(group_name))
  })
  
  eval_user_stats <- reactive({
    
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    gid  <- eval_selected_group(); req(!is.na(gid))
    key  <- eval_metric_key()
    spec <- eval_metric_spec(key)
    d <- evals_joined()
    if (!nrow(d)) return(tibble::tibble(user_id = integer(), name = character(), value = numeric()))
    if (key == "peripheral_vision") d <- d %>% dplyr::mutate(peripheral_vision = ifelse(peripheral_vision < 0, NA_real_, peripheral_vision))
    
    # usuários do grupo (distinct)
    ug_users <- user_groups %>%
      dplyr::filter(.data$group_id == !!as.integer(gid)) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id)) %>%
      dplyr::distinct() %>%
      dplyr::collect()
    
    if (!nrow(ug_users)) return(tibble::tibble(user_id = integer(), name = character(), value = numeric()))
    
    # média por usuário, já com nome
    d %>%
      dplyr::filter(.data$user_id %in% ug_users$user_id) %>%
      dplyr::group_by(user_id, name) %>%
      dplyr::summarise(value = mean(.data[[key]], na.rm = TRUE), .groups = "drop")
  })
  
  eval_selected_user  <- reactiveVal(NA_integer_)
  
  eval_selected_uname <- reactiveVal(NA_character_)
  
  eval_user_ts <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    uid  <- req(eval_selected_user())
    key  <- eval_metric_key()
    spec <- eval_metric_spec(key)
    
    d <- evals_joined()
    if (!nrow(d)) {
      return(tibble::tibble(
        evaluation_date = as.Date(character()),
        score_id        = character(),
        value           = numeric(),
        reference_mean  = numeric(),
        reference_sd    = numeric()
      ))
    }
    
    # keep peripheral_vision visible but handle -1 as NA
    if (key == "peripheral_vision") {
      d <- d %>%
        mutate(peripheral_vision = ifelse(peripheral_vision < 0, NA_real_, peripheral_vision))
    }
    
    d_user <- d %>%
      filter(.data$user_id == !!uid) %>%
      transmute(
        evaluation_date = as.Date(evaluation_date),
        score_id        = as.character(score_id),
        value           = as.numeric(.data[[key]])
      ) %>%
      arrange(evaluation_date)
    
    # join with reference stats only if metric == "nrss"
    if (identical(key, "nrss")) {
      df_full <- get_user_detailed_evals(uid)
      refs <- df_full %>%
        transmute(
          evaluation_date = as.Date(evaluation_date),
          score_id        = as.character(score_id),
          reference_mean  = as.numeric(reference_mean),
          reference_sd    = as.numeric(reference_sd)
        )
      d_user <- d_user %>% left_join(refs, by = c("evaluation_date", "score_id"))
    } else {
      d_user <- d_user %>%
        mutate(reference_mean = NA_real_, reference_sd = NA_real_)
    }
    
    # clamp user values only, not reference
    d_user$value <- pmin(pmax(d_user$value, spec$bounds[1]), spec$bounds[2])
    
    d_user
  })
  
  eval_user_cache <- reactiveVal(new.env(parent = emptyenv()))
  
  eval_selected_score_id <- reactiveVal(NA_character_)
  
  eval_selected_date     <- reactiveVal(as.Date(NA))
  
  get_user_detailed_evals <- function(uid) {
    cache <- eval_user_cache()
    key   <- paste0("u_", uid)
    if (exists(key, envir = cache, inherits = FALSE)) {
      return(get(key, envir = cache, inherits = FALSE))
    }
    tk <- api_token()
    df <- fetch_user_evaluations(
      api_address  = api_address,
      access_token = tk,
      header_key   = header_key,
      user_id      = uid
    )
    assign(key, df, envir = cache)
    df
  }
  
  selected_eval_row <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    uid  <- req(eval_selected_user())
    
    # full timeline from API (already cached)
    df_full <- get_user_detailed_evals(uid) %>%
      dplyr::mutate(
        evaluation_date = as.Date(evaluation_date),
        score_id        = as.character(score_id)
      )
    
    # pick by score_id (preferred) or date
    sid  <- eval_selected_score_id()
    dsel <- eval_selected_date()
    row <- NULL
    if (!is.null(sid) && nzchar(sid)) {
      row <- df_full %>% dplyr::filter(.data$score_id == !!sid) %>% dplyr::slice_tail(n = 1)
    }
    if ((is.null(row) || nrow(row) == 0) && !is.na(dsel)) {
      row <- df_full %>% dplyr::filter(.data$evaluation_date == !!as.Date(dsel)) %>% dplyr::slice_tail(n = 1)
    }
    
    # === JOIN WITH PV FROM DB (evals_joined) ===
    # Keep only columns needed to match and PV, sanitize -1 as NA
    ej_pv <- evals_joined() %>%
      dplyr::filter(.data$user_id == !!uid) %>%
      dplyr::transmute(
        score_id        = as.character(score_id),
        evaluation_date = as.Date(evaluation_date),
        peripheral_vision = as.numeric(dplyr::if_else(peripheral_vision < 0, NA_real_, peripheral_vision))
      )
    
    row <- dplyr::left_join(row, ej_pv, by = c("score_id","evaluation_date"))
    row
  })
  
  evals_for_download <- reactive({
    req(evals_joined())
    evals_joined() %>% select(-c(id,user_id)) %>% 
      transmute(
        `Nome`                     = name,
        `Grupo`                    = groups,
        `Data da avaliação`        = as.Date(evaluation_date),
        `Performance Cognitiva`    = nrss,
        `Tempo de Reação (ms)`     = rt_avg,
        `Tempo de Decisão (ms)`    = dt_avg,
        `Reação`                   = reaction_quality,
        `Decisão`                  = decision_quality,
        `Atenção`                  = attention,
        `Controle de Impulsividade`= impulsivity_control,
        `Visão Periférica`         = peripheral_vision,
        `Idade na avaliação`       = age_on_eval
      )
  })
  
  eval_page <- reactiveVal(1L)
  
  # ---- minigames -----
  
  mg_download_df <- reactive({
    req(authed(), session_role() == "institution")
    
    d <- minigames_df()
    if (is.null(d) || !nrow(d)) {
      return(tibble::tibble(
        user_name = character(), game_name = character(),
        score = numeric(), date = character(), time = character(),
        groups = character()
      ))
    }
    
    # nomes de usuário (Q37)
    nm <- get_names_for_users(unique(as.integer(d$user_id))) %>%
      dplyr::mutate(name = dplyr::coalesce(name, paste0("user_", user_id)))
    
    # nomes de jogos via RDS
    gn <- games_names %>%
      dplyr::transmute(game_id = as.integer(game_id),
                       game_name = as.character(name))
    
    # grupos do usuário (mapeia user_id -> "g1, g2, ...")
    g_api <- groups_from_api()
    ug <- user_groups %>%
      dplyr::filter(.data$user_id %in% !!unique(as.integer(d$user_id))) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id),
                       group_id = as.integer(.data$group_id)) %>%
      dplyr::distinct() %>%
      dplyr::collect()
    
    ug_named <- if (nrow(ug) && nrow(g_api)) {
      ug %>%
        dplyr::left_join(g_api %>% dplyr::rename(group_id = id, group_name = name),
                         by = "group_id") %>%
        dplyr::group_by(user_id) %>%
        dplyr::summarise(
          groups = paste(sort(unique(group_name[!is.na(group_name) & group_name != ""])),
                         collapse = ", "),
          .groups = "drop"
        )
    } else {
      tibble::tibble(user_id = integer(), groups = character())
    }
    
    # formata data/hora
    dt <- as.POSIXct(d$date_time, tz = "UTC")
    
    out <- d %>%
      dplyr::mutate(
        date = format(dt, "%d/%m/%Y"),
        time = substr(date_time, 12, 19)
      ) %>%
      dplyr::arrange(dplyr::desc(date_time), time, user_id, game_id) %>%
      dplyr::left_join(nm %>% dplyr::transmute(user_id, user_name = name),
                       by = "user_id") %>%
      dplyr::left_join(gn, by = "game_id") %>%
      dplyr::left_join(ug_named, by = "user_id") %>%
      dplyr::transmute(
        user_name = dplyr::coalesce(user_name, paste0("user_", user_id)),
        game_name = dplyr::coalesce(game_name, as.character(game_id)),
        score     = 10 * as.numeric(score),
        date, time,
        groups    = dplyr::coalesce(groups, NA_character_)
      ) %>%
      dplyr::filter(!is.na(score))
    
    out
  })
  
  minigames_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Minigames")
    uids <- scope_user_ids()  # já existente: todos os users da legal_entity
    get_moove_scores_data(uids)  # retorna user_id, date_time (UTC), minigame_id
  })
  
  mg_view_mode <- reactiveVal("groups")
  
  mg_selected_group <- reactiveVal(NA_integer_)
  
  mg_group_stats <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Minigames")
    d <- minigames_df()
    req(nrow(d) > 0)
    
    g_api <- groups_from_api()
    ug <- user_groups %>%
      filter(.data$user_id %in% !!unique(d$user_id)) %>%
      transmute(user_id = as.integer(.data$user_id), group_id = as.integer(.data$group_id)) %>%
      distinct() %>%
      collect()
    
    if (!nrow(ug) || !nrow(g_api)) return(tibble(group_id = integer(), group_name = character(), n = integer()))
    
    ug_named <- ug %>%
      inner_join(g_api %>% rename(group_id = id, group_name = name), by = "group_id")
    
    d %>%
      left_join(ug_named, by = "user_id") %>%
      group_by(group_id, group_name) %>%
      summarise(n = n(), .groups = "drop") %>%
      filter(!is.na(group_name) & group_name != "")
  })
  
  mg_user_stats <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Minigames")
    gid <- mg_selected_group(); req(!is.na(gid))
    d <- minigames_df(); req(nrow(d) > 0)
    
    ug_users <- user_groups %>%
      filter(.data$group_id == !!as.integer(gid)) %>%
      transmute(user_id = as.integer(.data$user_id)) %>%
      distinct() %>%
      collect()
    
    nm_df <- get_names_for_users(ug_users$user_id)
    
    d %>%
      filter(user_id %in% ug_users$user_id) %>%
      group_by(user_id) %>%
      summarise(n = n(), .groups = "drop") %>%
      left_join(nm_df, by = "user_id") %>%
      mutate(name = coalesce(name, paste0("user_", user_id)))
  })
  
  mg_page <- reactiveVal(1L)
  
  mg_selected_user  <- reactiveVal(NA_integer_)
  
  mg_selected_uname <- reactiveVal(NA_character_)
  
  mg_user_daily_stats <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Minigames")
    uid <- mg_selected_user(); req(!is.na(uid))
    
    d <- minigames_df()
    d <- d %>% dplyr::filter(.data$user_id == !!uid)
    
    # agrega por dia (mantém date_time com hh:mm:ss na fonte; aqui só agrupa)
    d %>%
      dplyr::mutate(day = as.Date(.data$date_time)) %>%
      dplyr::count(day, name = "n") %>%
      dplyr::arrange(day)
  })
  
  percentiles_long <- reactive({
    req(authed(), session_role() == "institution")
    uids <- scope_user_ids()
    dfw  <- get_user_settings_avg_percentiles(uids)
    to_long_percentiles(dfw)  # colunas: user_id, capacity, value, capacity_label
  })
  
  perf_view_mode   <- reactiveVal("groups")
  
  perf_selected_group <- reactiveVal(NA_integer_)
  
  perf_group_stats <- reactive({
    req(percentiles_long(), authed(), session_role() == "institution")
    cap <- input$sel_capacity; req(cap)
    d <- percentiles_long() %>% filter(capacity == cap)
    
    ug <- user_groups %>%
      transmute(user_id = as.integer(user_id), group_id = as.integer(group_id)) %>%
      distinct() %>%
      collect()
    g_api <- groups_from_api()
    
    d %>%
      inner_join(ug, by = "user_id") %>%
      inner_join(g_api %>% rename(group_id = id, group_name = name), by = "group_id") %>%
      group_by(group_id, group_name) %>%
      summarise(avg = mean(value, na.rm = TRUE), .groups = "drop") %>%
      filter(!is.na(group_name))
  })
  
  perf_user_stats <- reactive({
    req(percentiles_long(), authed(), session_role() == "institution")
    gid <- perf_selected_group(); req(!is.na(gid))
    cap <- input$sel_capacity; req(cap)
    
    d <- percentiles_long() %>% filter(capacity == cap)
    ug <- user_groups %>%
      filter(group_id == !!gid) %>%
      transmute(user_id = as.integer(user_id)) %>%
      distinct() %>%
      collect()
    nm_df <- get_names_for_users(ug$user_id)
    
    d %>%
      filter(user_id %in% ug$user_id) %>%
      group_by(user_id) %>%
      summarise(avg = mean(value, na.rm = TRUE), .groups = "drop") %>%
      left_join(nm_df, by = "user_id") %>%
      mutate(name = coalesce(name, paste0("user_", user_id)))
  })
  
  perf_page <- reactiveVal(1)
  
  # ---- measurements -----
  
  mm_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    uids <- scope_user_ids()
    get_measurement_summaries(uids)
  })
  
  mm_metric_id <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    req(!is.null(input$mm_metric))
    as.integer(input$mm_metric)
  })
  
  mm_available_measures <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    d <- mm_df()
    if (!nrow(d)) return(tibble::tibble(measurement_id = integer(), measurement_name = character()))
    d %>%
      dplyr::distinct(measurement_id, measurement_name) %>%
      dplyr::arrange(measurement_name)
  })
  
  mm_view_mode <- reactiveVal("groups") 
  
  mm_selected_group  <- reactiveVal(NA_integer_)
  
  mm_selected_user   <- reactiveVal(NA_integer_)
  
  mm_selected_uname  <- reactiveVal(NA_character_)
  
  mm_selected_date <- reactiveVal(as.Date(NA))
  
  mm_page            <- reactiveVal(1L)
  
  mm_group_stats <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    d <- mm_df(); req(nrow(d) > 0)
    mid <- mm_metric_id(); req(!is.na(mid))
    
    d <- d %>% dplyr::filter(.data$measurement_id == !!mid)
    
    g_api <- groups_from_api()
    ug <- user_groups %>%
      dplyr::filter(.data$user_id %in% !!unique(d$user_id)) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id), group_id = as.integer(.data$group_id)) %>%
      dplyr::distinct() %>%
      dplyr::collect()
    
    if (!nrow(ug) || !nrow(g_api)) return(tibble::tibble(group_id = integer(), group_name = character(), value = numeric()))
    
    ug_named <- ug %>% dplyr::inner_join(g_api %>% dplyr::rename(group_id = id, group_name = name), by = "group_id")
    
    d %>%
      dplyr::left_join(ug_named, by = "user_id") %>%
      dplyr::group_by(group_id, group_name) %>%
      dplyr::summarise(value = mean(score, na.rm = TRUE), .groups = "drop") %>%
      dplyr::filter(!is.na(group_name) & group_name != "")
  })
  
  mm_user_stats <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    mid <- mm_metric_id(); req(!is.na(mid))
    gid <- mm_selected_group(); req(!is.na(gid))
    
    d <- mm_df(); req(nrow(d) > 0)
    d <- d %>% dplyr::filter(.data$measurement_id == !!mid)
    
    ug_users <- user_groups %>%
      dplyr::filter(.data$group_id == !!gid) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id)) %>%
      dplyr::distinct() %>%
      dplyr::collect()
    
    if (!nrow(ug_users)) return(tibble::tibble(user_id = integer(), name = character(), value = numeric()))
    
    nm_df <- get_names_for_users(ug_users$user_id)
    
    d %>%
      dplyr::filter(.data$user_id %in% ug_users$user_id) %>%
      dplyr::group_by(user_id) %>%
      dplyr::summarise(value = mean(score, na.rm = TRUE), .groups = "drop") %>%
      dplyr::left_join(nm_df, by = "user_id") %>%
      dplyr::mutate(name = dplyr::coalesce(name, paste0("user_", user_id)))
  })
  
  mm_user_measurements_api <- reactiveVal(NULL)
  
  mm_user_cache <- reactiveVal(new.env(parent = emptyenv()))
  
  get_user_measurements_api <- function(uid) {
    cache <- mm_user_cache()
    key   <- paste0("u_", uid)
    if (exists(key, envir = cache, inherits = FALSE)) {
      return(get(key, envir = cache, inherits = FALSE))
    }
    tk <- api_token()
    df <- fetch_user_measurements(
      api_address  = api_address,
      access_token = tk,
      header_key   = header_key,
      user_id      = uid
    )
    assign(key, df, envir = cache)
    df
  }
  
  mm_user_api_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    uid <- mm_selected_user(); req(!is.na(uid))
    mid <- mm_metric_id();     req(!is.na(mid))
    
    df <- get_user_measurements_api(uid)
    
    # ⬇️ garanta o filtro pela medida selecionada
    if (!is.null(df) && nrow(df)) {
      if ("measurement_id" %in% names(df)) {
        df <- df %>% dplyr::filter(as.integer(.data$measurement_id) == !!mid)
      }
    } else {
      return(tibble::tibble(
        date = as.Date(character()),
        score_id = character(), score = numeric(),
        reference_mean = numeric(), reference_sd = numeric(),
        result1 = numeric(), result2 = numeric(), result3 = numeric(), result4 = numeric(),
        label_sec1 = character(), label_sec2 = character(), label_sec3 = character(), label_sec4 = character(),
        happy = numeric(), tired_out = numeric(), tense = numeric(), night_of_sleep = numeric(),
        created_at = as.POSIXct(character())
      ))
    }
    
    # padroniza colunas e ordena
    df %>%
      dplyr::mutate(date = as.Date(created_at)) %>%
      dplyr::transmute(
        date, created_at,
        score_id = as.character(score_id),
        score,
        reference_mean, reference_sd,
        result1, result2, result3, result4, label_score,
        label_sec1, label_sec2, label_sec3, label_sec4,
        happy, tired_out, tense, night_of_sleep
      ) %>%
      dplyr::arrange(date)
  })
  
  mm_user_ts <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    df <- mm_user_api_df()
    req(is.data.frame(df), nrow(df) > 0)
    
    df %>%
      dplyr::transmute(
        date,
        created_at,
        value   = as.numeric(score),
        sid     = as.character(score_id),
        ref_mean = as.numeric(reference_mean),
        ref_sd   = as.numeric(reference_sd),
        ref_low  = dplyr::if_else(is.finite(ref_mean) & is.finite(ref_sd), ref_mean - ref_sd, NA_real_),
        ref_high = dplyr::if_else(is.finite(ref_mean) & is.finite(ref_sd), ref_mean + ref_sd, NA_real_),
        # submétricas (0–100)
        result1, result2, result3, result4,
        label_sec1, label_sec2, label_sec3, label_sec4,
        # estados (0–1 → usaremos % depois)
        happy, tired_out, tense, night_of_sleep
      )
  })
  
  mm_selected_score_id <- reactiveVal(NULL)
  
  mm_selected_row <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    sid  <- mm_selected_score_id()
    dsel <- mm_selected_date()
    df   <- mm_user_api_df()
    
    row <- NULL
    if (!is.null(sid) && nzchar(sid)) {
      row <- df %>% dplyr::filter(.data$score_id == !!sid) %>% dplyr::slice_tail(n = 1)
    }
    if ((is.null(row) || nrow(row) == 0) && !is.na(dsel)) {
      row <- df %>% dplyr::filter(as.Date(.data$date) == !!as.Date(dsel)) %>% dplyr::slice_tail(n = 1)
    }
    
    # Sem seleção válida → devolve data.frame vazio (UI já usa req(nrow(row) > 0))
    if (is.null(row) || nrow(row) == 0) {
      return(tibble::tibble())
    }
    row
  })
  
  mm_download_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    d <- mm_df()
    if (is.null(d) || !nrow(d)) {
      return(tibble::tibble(
        Nome = character(), `Medida Moove` = character(),
        `Pontuação` = numeric(), Data = character(), Grupos = character()
      ))
    }
    
    # nomes dos usuários
    nm <- get_names_for_users(unique(as.integer(d$user_id))) %>%
      dplyr::mutate(name = dplyr::coalesce(name, paste0("user_", user_id))) %>%
      dplyr::transmute(user_id, user_name = name)
    
    # grupos do usuário (user_id -> "g1, g2, ...")
    g_api <- groups_from_api()
    ug <- user_groups %>%
      dplyr::filter(.data$user_id %in% !!unique(as.integer(d$user_id))) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id),
                       group_id = as.integer(.data$group_id)) %>%
      dplyr::distinct() %>%
      dplyr::collect()
    
    ug_named <- if (nrow(ug) && nrow(g_api)) {
      ug %>%
        dplyr::left_join(g_api %>% dplyr::rename(group_id = id, group_name = name),
                         by = "group_id") %>%
        dplyr::group_by(user_id) %>%
        dplyr::summarise(
          groups = paste(sort(unique(group_name[!is.na(group_name) & group_name != ""])),
                         collapse = ", "),
          .groups = "drop"
        )
    } else {
      tibble::tibble(user_id = integer(), groups = character())
    }
    
    df <- d %>%
      dplyr::left_join(nm, by = "user_id") %>%
      dplyr::left_join(ug_named, by = "user_id") %>%
      dplyr::transmute(
        Nome           = dplyr::coalesce(user_name, paste0("user_", .data$user_id)),
        `Medida Moove` = as.character(.data$measurement_name),
        `Pontuação`    = round(as.numeric(.data$score), 0),
        Data           = format(as.POSIXct(.data$created_at, tz = "UTC"), "%d/%m/%Y"),
        Grupos         = dplyr::coalesce(groups, NA_character_)
      ) %>%
      dplyr::arrange(
        dplyr::desc(as.Date(Data, format = "%d/%m/%Y")),
        Nome, `Medida Moove`
      )
    
    df
  })
  
  # ===================== output =====================
  
  # ---- status -----
  
  output$ui_status_panel <- renderUI({
    if (!isTRUE(authed())) {
      return(
        div(style="padding:10px; border:1px solid #eee; border-radius:8px; background:#fafafa; margin-bottom:8px;",
            tags$b("Status"), tags$br(),
            "Aguardando autenticação…"
        )
      )
    }
    rl <- session_role()
    if (identical(rl, "institution")) {
      d <- institution_dt()
      inst_name <- tryCatch(as.character(d$institution_name), error = function(e) NA_character_)
      inst_id   <- tryCatch(as.integer(d$institution_id),   error = function(e) NA_integer_)
      
      div(style="padding:10px; border:1px solid #eee; border-radius:8px; background:#fafafa; margin-bottom:8px;",
          tags$b("Status"), tags$br(),
          span("Instituição: ", inst_name), tags$br()
      )
    } else {
      div(style="padding:10px; border:1px solid #eee; border-radius:8px; background:#fafafa; margin-bottom:8px;",
          tags$b("Status"), tags$br(),
          span("Perfil detectado: Trainer"), tags$br(),
          span(style="color:#b00;", "Relatório de trainer virá em micropasso futuro.")
      )
    }
  })
  
  # ---- overview -----
  
  output$hc_users_members <- renderHighchart({
    req(authed(), session_role() == "institution")
    d <- req(institution_dt())
    amount_users   <- as.numeric(d$amount_users %||% 0)
    amount_members <- as.numeric(d$amount_members %||% 0)
    hc_cols_users_members(amount_users, amount_members)
  })
  
  output$hc_trainings_pct <- renderHighchart({
    req(authed(), session_role() == "institution")
    d <- req(institution_dt())
    pct <- as.numeric(d$percent_trainings_members %||% 0) * 100
    hc_gauge_pct(pct, "Membros treinando (%)")
  })
  
  output$hc_eval_members_pct <- renderHighchart({
    req(authed(), session_role() == "institution")
    d <- req(institution_dt())
    pct <- as.numeric(d$evaluations$members_evaluated_percent %||% 0)
    hc_gauge_pct(pct, "Membros avaliados (%)")
  })
  
  output$kpi_eval_score <- renderUI({
    req(authed(), session_role() == "institution")
    d <- req(institution_dt())
    score <- round(as.numeric(d$evaluations$score_avg %||% NA_real_), 1)
    kpi_card("Score médio de avaliações", ifelse(is.na(score), "—", score))
  })
  
  output$hc_feelings <- renderHighchart({
    req(authed(), session_role() == "institution")
    d <- req(institution_dt())
    fv <- d$feelings_avg
    happy   <- as.numeric(fv$happy         %||% 0)
    tired   <- as.numeric(fv$tired_out     %||% 0)
    tense   <- as.numeric(fv$tense         %||% 0)
    sleep   <- as.numeric(fv$night_of_sleep %||% 0)
    hc_bar_feelings(happy, tired, tense, sleep)
  })
  
  # ---- evals -----
  
  output$tbl_mg_monthly_evals <- DT::renderDT({
    req(authed(), session_role() == "institution")
    df <- evals_joined()
    df$date <- as.Date(df$evaluation_date)
    
    out <- df %>%
      dplyr::mutate(
        year      = lubridate::year(date),
        month_num = lubridate::month(date),
        month_lab = lubridate::month(date, label = TRUE, abbr = TRUE)
      ) %>%
      dplyr::count(year, month_num, month_lab, name = "minigames") %>%
      tidyr::pivot_wider(
        names_from  = year,
        values_from = minigames,
        values_fill = 0
      ) %>%
      dplyr::arrange(month_num) %>%
      dplyr::select(`Mês` = month_lab, dplyr::everything(), -month_num)
    
    out <- monthly_with_totals(out, month_label = "Mês")
    
    DT::datatable(
      out,
      rownames = FALSE,
      options = list(
        paging = FALSE,
        searching = FALSE,
        ordering = FALSE,
        dom = "t"
      )
    )
  })
  
  output$tbl_evals <- DT::renderDT({
    req(evals_joined())
    
    df <- evals_joined() %>%
      transmute(
        id, user_id, name,
        groups,
        evaluation_date = as.Date(evaluation_date),
        nrss,                          # 0-1000
        rt_avg,                        # ms (180-600 limites no gráfico)
        dt_avg,                        # ms (0-300 limites no gráfico)
        reaction_quality,              # 0-100
        decision_quality,              # 0-100
        attention,                     # 0-100
        impulsivity_control,           # 0-100
        peripheral_vision,             # -1 se ausente
        age_on_eval                    # calculado
      )
    
    DT::datatable(
      df,
      rownames = FALSE,
      options = list(
        pageLength = 25,
        order = list(list(3, "desc")),
        dom = "tip",
        scrollX = TRUE
      )
    )
  })
  
  output$ui_eval_metric_tabs <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    # "abas" via nav: usamos radioButtons horizontal para performance e UX melhores
    div(
      style = "width:100%; text-align:center; margin-bottom:6px;",
      radioButtons(
        "eval_metric",
        label = NULL,           # <- sem título
        inline = TRUE,          # mantêm os botões lado a lado
        choices = c(
          "Performance Cognitiva"   = "nrss",
          "Reação"                  = "reaction_quality",
          "Decisão"                 = "decision_quality",
          "Atenção"                 = "attention",
          "Controle de Impulsividade" = "impulsivity_control",
          "Visão Periférica"        = "peripheral_vision",
          "Tempo de Reação (ms)"    = "rt_avg",
          "Tempo de Decisão (ms)"   = "dt_avg"
        ),
        selected = "nrss"
      )
    )
  })
  
  output$ui_eval_back <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    if (identical(eval_view_mode(), "user")) {
      actionButton("btn_eval_back", "Voltar aos usuários", icon = icon("arrow-left"), class = "btn btn-light")
    } else if (identical(eval_view_mode(), "users")) {
      actionButton("btn_eval_back", "Voltar aos grupos", icon = icon("arrow-left"), class = "btn btn-light")
    } else {
      NULL
    }
  })
  
  output$hc_evals <- renderHighchart({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    req(!is.null(input$eval_metric))
    
    key  <- eval_metric_key()
    spec <- eval_metric_spec(key)
    
    if (identical(eval_view_mode(), "groups")) {
      gs <- eval_group_stats()
      
      series <- prep_bar_series(gs$group_name, gs$value, spec$high_is_good, spec$bounds)
      cats <- series$cats
      vals <- series$vals
      
      # paginação
      n  <- length(vals)
      pb <- page_bounds(n, eval_page(), PER_PAGE)
      idx <- if (pb[1] <= pb[2]) seq.int(pb[1], pb[2]) else integer(0)
      cats <- cats[idx]; vals <- vals[idx]
      
      avg  <- if (length(vals)) mean(vals, na.rm = TRUE) else NA_real_
      cols <- color_by_mean(vals, avg, spec$high_is_good)
      
      rng <- axis_with_headroom(spec$bounds, vals)
      
      highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = paste0(spec$label, " — Grupos")) %>%
        hc_xAxis(type = "category", title = list(text = NULL)) %>%
        hc_yAxis(
          min = rng$min,
          max = rng$max,
          title = list(text = NULL),
          plotLines = list(list(color = "#f39c12", width = 2, value = avg, zIndex = 5))
        ) %>%
        hc_plotOptions(
          series = list(animation = list(duration = 700)),
          column = list(
            dataLabels   = list(enabled = TRUE, format = spec$fmt),
            pointPadding = 0.1,
            groupPadding = 0.05,
            cursor       = "pointer",
            point = list(
              events = list(
                click = JS("
                function () {
                  Shiny.setInputValue('hc_eval_group_click',
                    { name: this.name, y: this.y },
                    { priority: 'event' }
                  );
                }")
              )
            )
          )
        ) %>%
        hc_add_series(
          name = spec$label,
          data = purrr::map2(cats, seq_along(vals), function(cat, i) {
            list(name = cat, y = vals[[i]], color = cols[[i]])
          }),
          showInLegend = FALSE
        ) %>%
        hc_tooltip(
          formatter = JS(
            sprintf(
              "function () {
               return '<b>Grupo:</b> ' + this.point.name +
                      '<br/><b>%s:</b> ' + Highcharts.numberFormat(this.point.y, 0);
             }",
              spec$label
            )
          )
        ) %>%
        hc_exporting(enabled = TRUE)
      
    } else if (identical(eval_view_mode(), "users")) {
      
      us <- eval_user_stats()
      
      df <- us %>%
        dplyr::mutate(value = as.numeric(value)) %>%
        dplyr::filter(!is.na(value))
      
      # clamp e ordenação
      df$value <- pmin(pmax(df$value, spec$bounds[1]), spec$bounds[2])
      df <- if (spec$high_is_good) dplyr::arrange(df, dplyr::desc(value), name)
      else                    dplyr::arrange(df, value, name)
      
      cats <- df$name
      vals <- df$value
      ids  <- df$user_id
      
      # paginação
      n  <- length(vals)
      pb <- page_bounds(n, eval_page(), PER_PAGE)
      idx <- if (pb[1] <= pb[2]) seq.int(pb[1], pb[2]) else integer(0)
      cats <- cats[idx]; vals <- vals[idx]; ids <- ids[idx]
      
      avg  <- if (length(vals)) mean(vals, na.rm = TRUE) else NA_real_
      cols <- color_by_mean(vals, avg, spec$high_is_good)
      rng  <- axis_with_headroom(spec$bounds, vals)
      
      highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = paste0(spec$label, " — Usuários do grupo")) %>%
        hc_xAxis(type = "category", title = list(text = NULL)) %>%
        hc_yAxis(
          min = rng$min,
          max = rng$max,
          title = list(text = NULL),
          plotLines = list(list(color = "#f39c12", width = 2, value = avg, zIndex = 5))
        ) %>%
        hc_plotOptions(
          series = list(animation = list(duration = 700)),
          column = list(
            dataLabels   = bar_datalabels_opts(spec$fmt),
            pointPadding = 0.1,
            groupPadding = 0.05,
            cursor       = "pointer",
            point = list(
              events = list(
                click = JS("
                function(){
                  Shiny.setInputValue('hc_eval_user_click',
                    { uid: this.options.uid, name: this.name, y: this.y },
                    { priority: 'event' }
                  );
                }")
              )
            )
          )
        ) %>%
        hc_add_series(
          name = spec$label,
          data = purrr::pmap(
            list(vals, cols, ids, cats),
            function(v, c, id, cat) list(y = v, color = c, uid = id, name = cat)
          ),
          showInLegend = FALSE
        ) %>%
        hc_tooltip(
          pointFormat = paste0("<b>Usuário:</b> {point.name}<br/><b>", spec$label, ":</b> {point.y:.0f}")
        ) %>%
        hc_exporting(enabled = TRUE)
      
    } else if (identical(eval_view_mode(), "user")) {
      
      ts <- eval_user_ts()
      
      key   <- eval_metric_key()
      spec  <- eval_metric_spec(key)
      uname <- eval_selected_uname() %||% "Usuário"
      
      cats      <- format(ts$evaluation_date, "%Y-%m-%d")
      vals      <- as.numeric(ts$value)
      ref_mean  <- as.numeric(ts$reference_mean)
      ref_sd    <- as.numeric(ts$reference_sd)
      sids      <- as.character(ts$score_id)
      
      # paginação
      n  <- length(vals)
      pb <- page_bounds(n, eval_page(), PER_PAGE)
      idx <- if (pb[1] <= pb[2]) seq.int(pb[1], pb[2]) else integer(0)
      cats <- cats[idx]; vals <- vals[idx]; ref_mean <- ref_mean[idx]; ref_sd <- ref_sd[idx]; sids <- sids[idx]
      
      user_mean <- if (length(vals)) mean(vals, na.rm = TRUE) else NA_real_
      cols      <- color_by_mean(vals, user_mean, spec$high_is_good)
      
      # intervalo do eixo
      rng <- if (identical(key, "nrss")) list(min = 0, max = 1000) else axis_with_headroom(spec$bounds, vals)
      
      # dados de erro ±1DP (apenas quando há referência)
      err_data <- purrr::map2(ref_mean, ref_sd, ~{
        if (is.finite(.x) && is.finite(.y)) list(low = .x - .y, high = .x + .y)
        else                                list(low = NA_real_, high = NA_real_)
      })
      
      hc <- highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = paste0(spec$label, " — ", uname)) %>%
        hc_xAxis(type = "category", title = list(text = NULL)) %>%
        hc_yAxis(min = rng$min, max = rng$max, title = list(text = NULL)) %>%
        hc_plotOptions(
          series = list(animation = list(duration = 600)),
          column = list(
            dataLabels   = bar_datalabels_opts(spec$fmt),
            pointPadding = 0.1,
            groupPadding = 0.05,
            cursor       = "pointer",
            point = list(
              events = list(
                click = JS("
                function(){
                  Shiny.setInputValue('hc_eval_user_eval_click',
                    { sid: this.options.sid, date: this.name, y: this.y },
                    { priority: 'event' }
                  );
                }")
              )
            )
          )
        ) %>%
        hc_add_series(
          name = spec$label,
          type = "column",
          data = purrr::pmap(
            list(vals, cols, sids, cats),
            function(v, c, sid, cat) list(y = v, color = c, sid = sid, name = cat)
          ),
          showInLegend = FALSE,
          tooltip = list(pointFormat = paste0("<b>", spec$label, ":</b> {point.y:.0f}"))
        )
      
      # linha da média do usuário (laranja sólida)
      hc <- hc %>%
        hc_add_series(
          type = "line",
          name = "Média do usuário",
          data = rep(user_mean, length(vals)),
          color = "#e67e22",
          lineWidth = 2,
          dashStyle = "Solid",
          marker = list(enabled = FALSE),
          enableMouseTracking = FALSE
        )
      
      # referências apenas para NRSS
      if (identical(key, "nrss") && any(is.finite(ref_mean))) {
        hc <- hc %>%
          hc_add_series(
            type = "spline",
            name = "Ref. média",
            data = ref_mean,
            color = "#7f8c8d",
            dashStyle = "ShortDash",
            lineWidth = 2,
            marker = list(enabled = FALSE)
          )
      }
      if (identical(key, "nrss") && any(is.finite(ref_sd))) {
        hc <- hc %>%
          hc_add_series(
            type = "errorbar",
            name = "Ref. ±1 DP",
            data = err_data,
            whiskerWidth = 5,
            color = "#7f8c8d"
          )
      }
      
      hc %>%
        hc_tooltip(
          formatter = JS(
            sprintf(
              "function(){
               if (this.point && this.point.name !== undefined) {
                 return '<b>Data:</b> ' + this.point.name +
                        '<br/><b>%s:</b> ' + Highcharts.numberFormat(this.point.y, 0);
               }
               return false;
             }",
              spec$label
            )
          )
        ) %>%
        hc_exporting(enabled = TRUE)
    }
  })
  
  output$ui_eval_detail <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    
    # -------- seleciona a linha da avaliação (API) --------
    uid  <- eval_selected_user();   req(!is.na(uid))
    sid  <- eval_selected_score_id()
    dsel <- eval_selected_date()
    
    df_full <- get_user_detailed_evals(uid) %>%
      dplyr::mutate(
        evaluation_date = as.Date(evaluation_date),
        score_id        = as.character(score_id)
      )
    
    base <- NULL
    if (!is.null(sid) && nzchar(sid)) {
      base <- df_full %>% dplyr::filter(.data$score_id == !!sid) %>% dplyr::slice_tail(n = 1)
    }
    if ((is.null(base) || nrow(base) == 0) && !is.na(dsel)) {
      base <- df_full %>% dplyr::filter(.data$evaluation_date == !!as.Date(dsel)) %>% dplyr::slice_tail(n = 1)
    }
    
    # -------- enriquece só com Visão Periférica (DB) --------
    ej <- evals_joined() %>%
      dplyr::filter(.data$user_id == !!uid) %>%
      dplyr::transmute(
        score_id        = as.character(score_id),
        evaluation_date = as.Date(evaluation_date),
        peripheral_vision = as.numeric(dplyr::if_else(peripheral_vision < 0, NA_real_, peripheral_vision))
      )
    
    row <- dplyr::left_join(base, ej, by = c("score_id","evaluation_date"))
    
    # -------- scalars seguros --------
    dstr <- tryCatch(format(row$evaluation_date[1], "%Y-%m-%d"), error = function(...) "—")
    
    pv <- tryCatch(as.numeric(row$peripheral_vision[1]), error = function(...) NA_real_)
    if (length(pv) == 0) pv <- NA_real_
    show_pv <- isTRUE(is.finite(pv) && pv >= 0)
    
    # largura dinâmica: 4 tiles = 25%; 5 tiles = 20%
    width_pct <- if (show_pv) "20%" else "25%"
    
    # helper para um tile circular
    circ_tile <- function(output_id, label = NULL) {
      div(
        style = sprintf("flex: 0 0 %s; max-width:%s; padding:4px;", width_pct, width_pct),
        highchartOutput(output_id, height = "220px")
      )
    }
    
    tagList(
      div(style="margin-top:14px; border:1px solid #eee; border-radius:8px; padding:12px;",
          h4(sprintf("Detalhes da avaliação — %s", dstr)),
          
          # 1) NRSS circular grande
          fluidRow(
            column(12, highchartOutput("hc_det_nrss_circ", height = "340px"))
          ),
          br(),
          
          # 2) submétricas circulares em UMA LINHA (flex)
          div(
            style = "display:flex; gap:8px; flex-wrap:nowrap; width:100%;",
            circ_tile("hc_det_reaction_circ"),
            circ_tile("hc_det_decision_circ"),
            circ_tile("hc_det_attention_circ"),
            circ_tile("hc_det_impulsivity_circ"),
            if (show_pv) circ_tile("hc_det_pv_circ")
          ),
          
          br(),
          
          # 3) velocímetros (labels embaixo)
          fluidRow(
            column(
              6,
              tagList(
                highchartOutput("hc_det_rt_speedo", height = "250px"),
                div(style="text-align:center; margin-top:6px; font-weight:600;", "Tempo de Reação (ms)")
              )
            ),
            column(
              6,
              tagList(
                highchartOutput("hc_det_dt_speedo", height = "250px"),
                div(style="text-align:center; margin-top:6px; font-weight:600;", "Tempo de Decisão (ms)")
              )
            )
          ),
          
          br(),
          
          # 4) Estado (polar rose)
          fluidRow(
            column(12, highchartOutput("hc_det_moods_rose", height = "380px"))
          )
      )
    )
  })
  
  output$ui_det_pv_circ <- renderUI({
    row <- selected_eval_row()
    pv  <- as.numeric(row$peripheral_vision[1] %||% NA_real_)
    if (!is.finite(pv) || pv < 0) return(NULL)
    fluidRow(column(3, highchartOutput("hc_det_pv_circ", height = "220px")))
  })
  
  output$hc_det_nrss_circ <- renderHighchart({
    row <- selected_eval_row()
    hc_circular_bar(value = row$nrss[1], minmax = c(0,1000),
                    title_txt = "Performance Cognitiva (NRSS)",
                    fmt = "{y:.0f}", high_is_good = TRUE, size = "95%", inner = "70%")
  })
  
  output$hc_det_reaction_circ <- renderHighchart({
    row <- selected_eval_row()
    hc_circular_bar(row$reaction_quality[1], c(0,100), "Reação", "{y:.0f}", high_is_good = TRUE,
                    size = "90%", inner = "72%")
  })
  
  output$hc_det_decision_circ <- renderHighchart({
    row <- selected_eval_row()
    hc_circular_bar(row$decision_quality[1], c(0,100), "Decisão", "{y:.0f}", high_is_good = TRUE,
                    size = "90%", inner = "72%")
  })
  
  output$hc_det_attention_circ <- renderHighchart({
    row <- selected_eval_row()
    hc_circular_bar(row$attention[1], c(0,100), "Atenção", "{y:.0f}", high_is_good = TRUE,
                    size = "90%", inner = "72%")
  })
  
  output$hc_det_impulsivity_circ <- renderHighchart({
    row <- selected_eval_row()
    hc_circular_bar(row$impulsivity_control[1], c(0,100), "Controle de Impulsividade", "{y:.0f}",
                    high_is_good = TRUE, size = "90%", inner = "72%")
  })
  
  output$hc_det_pv_circ <- renderHighchart({
    row <- selected_eval_row();  # se você tiver esse helper; senão use a mesma lógica acima dentro deste render
    pv  <- suppressWarnings(as.numeric(row$peripheral_vision[1]))
    
    hc_circular_bar(
      value = pv, minmax = c(0, 100),
      title_txt = "Visão Periférica",
      fmt = "{y:.0f}", high_is_good = TRUE,
      size = "90%", inner = "72%"
    )
  })
  
  output$hc_det_rt_speedo <- renderHighchart({
    row <- selected_eval_row()
    hc_speedometer(row$rt_avg[1], c(180,600), title_txt = "", fmt = "{y:.0f} ms", high_is_good = FALSE) %>%
      hc_title(text = "")
  })
  
  output$hc_det_dt_speedo <- renderHighchart({
    row <- selected_eval_row()
    hc_speedometer(row$dt_avg[1], c(0,300), title_txt = "", fmt = "{y:.0f} ms", high_is_good = FALSE) %>%
      hc_title(text = "")
  })
  
  output$hc_det_moods_rose <- renderHighchart({
    row <- selected_eval_row()
    
    # valores em 0–100%:
    alegria      <- 100 * as.numeric(row$happy[1])
    disposicao   <- 100 * (1 - as.numeric(row$tired_out[1]))  # invertido
    relaxamento  <- 100 * (1 - as.numeric(row$tense[1]))      # invertido
    sono         <- 100 * as.numeric(row$night_of_sleep[1])
    
    cats <- c("Alegria", "Disposição", "Relaxamento", "Última noite de sono")
    vals <- c(alegria,   disposicao,    relaxamento,   sono)
    
    hc_polar_rose(cats, vals, title_txt = "Estado — intensidade (%)", max_pct = 100)
  })
  
  output$download_evals_xlsx <- downloadHandler(filename = function() {
      d <- institution_dt()
      inst <- tryCatch(as.character(d$institution_name), error = function(e) "instituicao")
      sprintf("resultados_avaliacoes_%s_%s.xlsx",
              gsub("[^A-Za-z0-9_-]", "_", inst),
              format(Sys.time(), "%Y%m%d-%H%M"))
    },content = function(file) {
      df <- evals_for_download()
      # garante ordem de colunas como especificado acima:
      openxlsx::write.xlsx(df, file, na = "")
      # se preferir base R:
      # write.csv(df, file, row.names = FALSE, fileEncoding = "UTF-8")
    })
  
  output$ui_eval_pager <- renderUI({
    # calcula total de itens conforme a view atual
    total_items <- 0L
    if (identical(eval_view_mode(), "groups")) {
      gs <- eval_group_stats(); total_items <- nrow(gs)
    } else if (identical(eval_view_mode(), "users")) {
      us <- eval_user_stats();  total_items <- nrow(us)
    } else if (identical(eval_view_mode(), "user")) {
      ts <- eval_user_ts();     total_items <- nrow(ts)
    }
    total_pages <- max(1L, ceiling(total_items / PER_PAGE))
    curr <- clamp(eval_page(), 1L, total_pages); if (curr != eval_page()) eval_page(curr)
    
    tagList(
      actionButton("eval_prev", label = NULL, icon = icon("chevron-left"),
                   class = "btn btn-light", disabled = if (curr <= 1) "disabled"),
      span(sprintf("Página %d de %d", curr, total_pages),
           style="min-width:140px; text-align:center; font-weight:600;"),
      actionButton("eval_next", label = NULL, icon = icon("chevron-right"),
                   class = "btn btn-light", disabled = if (curr >= total_pages) "disabled")
    )
  })
  
  # ---- minigames -----
  
  output$ui_minigames_status <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Minigames")
    n <- tryCatch(nrow(minigames_df()), error = function(...) 0L)
    div(
      style="padding:10px; border:1px solid #eee; border-radius:8px; background:#fafafa;",
      tags$b("Minigames — status"), tags$br(),
      sprintf("Minigames jogados: %s", format(n, big.mark = ".", decimal.mark = ",")),
      tags$br()
    )
  })
  
  output$tbl_mg_monthly <- DT::renderDT({
    req(authed(), session_role() == "institution", input$tabs == "Minigames")
    df <- minigames_df()
    
    df$date <- as.Date(df$date_time)
    df$time <- substr(df$date_time, 12, 19)
    
    out <- df %>%
      dplyr::mutate(
        year      = lubridate::year(date),
        month_num = lubridate::month(date),
        month_lab = lubridate::month(date, label = TRUE, abbr = TRUE)
      ) %>%
      dplyr::count(year, month_num, month_lab, name = "minigames") %>%
      tidyr::pivot_wider(
        names_from  = year,
        values_from = minigames,
        values_fill = 0
      ) %>%
      dplyr::arrange(month_num) %>%
      dplyr::select(`Mês` = month_lab, dplyr::everything(), -month_num)
    
    out <- monthly_with_totals(out, month_label = "Mês")
    
    DT::datatable(
      out,
      rownames = FALSE,
      options = list(
        paging = FALSE,
        searching = FALSE,
        ordering = FALSE,
        dom = "t"
      )
    )
  })
  
  output$hc_mg_counts <- renderHighchart({
    req(authed(), session_role() == "institution", input$tabs == "Minigames")
    
    if (identical(mg_view_mode(), "groups")) {
      df <- mg_group_stats()
      
      # ordena desc
      df <- df %>% dplyr::arrange(dplyr::desc(n), group_name)
      
      # paginação
      n  <- nrow(df)
      pb <- page_bounds(n, mg_page(), PER_PAGE)
      idx <- if (pb[1] <= pb[2]) seq.int(pb[1], pb[2]) else integer(0)
      df <- df[idx, , drop = FALSE]
      
      avg  <- mean(df$n, na.rm = TRUE)
      cols <- color_by_mean(df$n, avg, high_is_good = TRUE)
      
      highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = "Minigames por grupo") %>%
        hc_xAxis(categories = df$group_name) %>%
        hc_yAxis(
          title = list(text = "Quantidade de minigames"),
          plotLines = list(list(color = "#f39c12", width = 2, value = avg, zIndex = 5))
        ) %>%
        hc_add_series(
          name = "Minigames",
          data = purrr::pmap(
            list(df$group_name, df$n, cols),
            function(name, y, color) list(name = name, y = y, color = color)
          ),
          showInLegend = FALSE
        ) %>%
        hc_plotOptions(column = list(
          cursor = "pointer",
          dataLabels = list(enabled = TRUE),
          point = list(events = list(
            click = JS("
            function() {
              Shiny.setInputValue('hc_mg_group_click', { name: this.name }, { priority: 'event' });
            }
          ")
          ))
        ))
    } else if (identical(mg_view_mode(), "users")) {
      df <- mg_user_stats()
      
      # ordena desc
      df <- df %>% dplyr::arrange(dplyr::desc(n), name)
      
      # paginação
      n  <- nrow(df)
      pb <- page_bounds(n, mg_page(), PER_PAGE)
      idx <- if (pb[1] <= pb[2]) seq.int(pb[1], pb[2]) else integer(0)
      df <- df[idx, , drop = FALSE]
      
      avg  <- mean(df$n, na.rm = TRUE)
      cols <- color_by_mean(df$n, avg, high_is_good = TRUE)
      
      highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = "Minigames por usuário") %>%
        hc_xAxis(categories = df$name) %>%
        hc_yAxis(
          title = list(text = "Quantidade de minigames"),
          plotLines = list(list(color = "#f39c12", width = 2, value = avg, zIndex = 5))
        ) %>%
        hc_add_series(
          name = "Minigames",
          data = purrr::pmap(
            list(df$name, df$n, cols, df$user_id),
            function(name, y, color, uid) list(name = name, y = y, color = color, uid = uid)
          ),
          showInLegend = FALSE
        ) %>%
        hc_plotOptions(column = list(
          cursor = "pointer",
          dataLabels = list(enabled = TRUE),
          point = list(events = list(
            click = JS("
            function() {
              Shiny.setInputValue('hc_mg_user_click', { uid: this.options.uid, name: this.name }, { priority: 'event' });
            }
          ")
          ))
        ))
    } else if (identical(mg_view_mode(), "user")) {
      df <- mg_user_daily_stats()
      uname <- mg_selected_uname() %||% "Usuário"
      
      # se estiver vazio, renderiza um chart vazio com título informativo
      if (!nrow(df)) {
        return(
          highchart() %>%
            hc_chart(type = "column") %>%
            hc_title(text = paste0("Minigames por dia — ", uname)) %>%
            hc_subtitle(text = "Sem registros para este usuário.")
        )
      }
      
      cats <- format(df$day, "%Y-%m-%d")
      vals <- df$n
      
      highchart() %>%
        hc_chart(type = "column", inverted = FALSE) %>%
        hc_title(text = paste0("Minigames por dia — ", uname)) %>%
        hc_xAxis(categories = cats, title = list(text = NULL)) %>%
        hc_yAxis(title = list(text = "Quantidade de minigames")) %>%
        hc_add_series(
          name = "Minigames",
          data = vals,
          showInLegend = FALSE
        ) %>%
        hc_plotOptions(column = list(dataLabels = list(enabled = TRUE)))
    }
  })
  
  output$ui_mg_pager <- renderUI({
    total_items <- 0L
    if (identical(mg_view_mode(), "groups")) {
      g <- mg_group_stats(); total_items <- nrow(g)
    } else if (identical(mg_view_mode(), "users")) {
      u <- mg_user_stats();  total_items <- nrow(u)
    }
    total_pages <- max(1L, ceiling(total_items / PER_PAGE))
    curr <- clamp(mg_page(), 1L, total_pages); if (curr != mg_page()) mg_page(curr)
    
    tagList(
      actionButton("mg_prev", label = NULL, icon = icon("chevron-left"),
                   class = "btn btn-light", disabled = if (curr <= 1) "disabled"),
      span(sprintf("Página %d de %d", curr, total_pages),
           style="min-width:140px; text-align:center; font-weight:600;"),
      actionButton("mg_next", label = NULL, icon = icon("chevron-right"),
                   class = "btn btn-light", disabled = if (curr >= total_pages) "disabled")
    )
  })
  
  output$ui_mg_back <- renderUI({
    if (identical(mg_view_mode(), "users")) {
      actionButton("btn_mg_back", "Voltar aos grupos", icon = icon("arrow-left"), class = "btn btn-light")
    } else if (identical(mg_view_mode(), "user")) {
      actionButton("btn_mg_back", "Voltar aos usuários", icon = icon("arrow-left"), class = "btn btn-light")
    } else NULL
  })
  
  output$hc_perf_groups <- renderHighchart({
    req(input$sel_capacity, authed(), session_role() == "institution", input$tabs == "Minigames")
    
    cap_label <- capacity_labels[input$sel_capacity]
    
    if (identical(perf_view_mode(), "groups")) {
      df <- perf_group_stats()
      if (!nrow(df)) return(highchart() %>% hc_title(text = "Sem dados de grupos."))
      
      df <- df %>%
        dplyr::mutate(avg = round(avg, 1)) %>%
        dplyr::arrange(dplyr::desc(avg))
      
      avg_line <- round(mean(df$avg, na.rm = TRUE), 1)
      
      start <- (perf_page() - 1) * PER_PAGE + 1
      end   <- min(nrow(df), start + PER_PAGE - 1)
      if (start <= nrow(df)) {
        df <- df[start:end, , drop = FALSE]
      } else {
        df <- df[0, , drop = FALSE]
      }
      
      highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = paste("Média de", cap_label, "por grupo")) %>%
        hc_xAxis(categories = df$group_name) %>%
        hc_yAxis(
          title = list(text = "Percentil médio"),
          plotLines = list(list(
            value = avg_line, color = "#f39c12", width = 2
          ))
        ) %>%
        hc_add_series(
          name = "Percentil médio",
          data = purrr::map2(df$group_name, df$avg, ~list(name = .x, y = .y)),
          showInLegend = FALSE
        ) %>%
        hc_plotOptions(column = list(
          cursor = "pointer",
          dataLabels = list(enabled = TRUE, format = "{point.y:.1f}"),
          point = list(events = list(
            click = JS("
            function() {
              Shiny.setInputValue('hc_perf_group_click', { name: this.name }, { priority: 'event' });
            }
          "))
          )))
    } 
    else if (identical(perf_view_mode(), "users")) {
      df <- perf_user_stats()
      if (!nrow(df)) return(highchart() %>% hc_title(text = "Sem dados de usuários."))
      
      df <- df %>%
        dplyr::mutate(avg = round(avg, 1)) %>%
        dplyr::arrange(dplyr::desc(avg))
      
      avg_line <- round(mean(df$avg, na.rm = TRUE), 1)
      
      start <- (perf_page() - 1) * PER_PAGE + 1
      end   <- min(nrow(df), start + PER_PAGE - 1)
      if (start <= nrow(df)) {
        df <- df[start:end, , drop = FALSE]
      } else {
        df <- df[0, , drop = FALSE]
      }
      
      highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = paste("Média de", cap_label, "por usuário")) %>%
        hc_xAxis(categories = df$name) %>%
        hc_yAxis(
          title = list(text = "Percentil médio"),
          plotLines = list(list(
            value = avg_line, color = "#f39c12", width = 2
          ))
        ) %>%
        hc_add_series(
          name = "Percentil médio",
          data = purrr::map2(df$name, df$avg, ~list(name = .x, y = .y)),
          showInLegend = FALSE
        ) %>%
        hc_plotOptions(column = list(
          dataLabels = list(enabled = TRUE, format = "{point.y:.1f}")
        ))
    }
  })
  
  output$ui_perf_paging <- renderUI({
    req(perf_view_mode() %in% c("groups","users"))
    total <- if (identical(perf_view_mode(),"groups")) nrow(perf_group_stats()) else nrow(perf_user_stats())
    page_size <- PER_PAGE
    n_pages <- max(1, ceiling(total / page_size))
    cur <- perf_page()
    
    if (n_pages <= 1) return(NULL)
    
    tagList(
      actionButton("btn_perf_prev", "◀", class="btn btn-light btn-sm"),
      span(paste("Página", cur, "de", n_pages)),
      actionButton("btn_perf_next", "▶", class="btn btn-light btn-sm")
    )
  })
  
  output$download_mg_xlsx <- downloadHandler(filename = function() {
      d <- institution_dt()
      inst <- tryCatch(as.character(d$institution_name), error = function(e) "instituicao")
      sprintf("minigames_%s_%s.xlsx",
              gsub("[^A-Za-z0-9_-]", "_", inst),
              format(Sys.time(), "%Y%m%d-%H%M"))
    },content = function(file) {
      df <- mg_download_df()
      if (!requireNamespace("openxlsx", quietly = TRUE)) {
        write.csv(df, file, row.names = FALSE, fileEncoding = "UTF-8")
      } else {
        openxlsx::write.xlsx(df, file, na = "")
      }
    })
  
  # ---- measurements -----
  
  output$ui_mm_status <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    df <- mm_df()
    n_rows <- nrow(df)
    n_users <- dplyr::n_distinct(df$user_id)
    n_meas  <- dplyr::n_distinct(df$measurement_id)
    div(
      style="padding:10px; border:1px solid #eee; border-radius:8px; background:#fafafa;",
      tags$b("Medidas Moove — status"), tags$br(),
      sprintf("Registros: %s", format(n_rows, big.mark = ".", decimal.mark = ",")), tags$br(),
      sprintf("Usuários: %s", format(n_users, big.mark = ".", decimal.mark = ",")), tags$br(),
      sprintf("Tipos de medidas: %s", format(n_meas,  big.mark = ".", decimal.mark = ","))
    )
  })
  
  output$tbl_mm_monthly <- DT::renderDT({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    df <- mm_df()
    
    df$date <- as.Date(df$created_at)
    
    out <- df %>%
      dplyr::mutate(
        year      = lubridate::year(date),
        month_num = lubridate::month(date),
        month_lab = lubridate::month(date, label = TRUE, abbr = TRUE)
      ) %>%
      dplyr::count(year, month_num, month_lab, name = "medidas") %>%
      tidyr::pivot_wider(
        names_from  = year,
        values_from = medidas,
        values_fill = 0
      ) %>%
      dplyr::arrange(month_num) %>%
      dplyr::select(`Mês` = month_lab, dplyr::everything(), -month_num)
    
    out <- monthly_with_totals(out, month_label = "Mês")
    
    DT::datatable(
      out,
      rownames = FALSE,
      options = list(
        paging = FALSE,
        searching = FALSE,
        ordering = FALSE,
        dom = "t"
      )
    )
  })
  
  output$ui_mm_metric_tabs <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    ms <- mm_available_measures(); req(nrow(ms) > 0)
    ch <- stats::setNames(as.integer(ms$measurement_id), ms$measurement_name)
    
    tags$div(
      id = "mm_metric_wrap",
      radioButtons(
        "mm_metric", label = NULL, inline = TRUE,
        choices  = ch,
        selected = as.integer(ch[[1]])
      )
    )
  })
  
  output$ui_mm_back <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    if (identical(mm_view_mode(), "users")) {
      actionButton("btn_mm_back", "Voltar aos grupos", icon = icon("arrow-left"), class = "btn btn-light")
    } else if (identical(mm_view_mode(), "user")) {
      actionButton("btn_mm_back", "Voltar aos usuários", icon = icon("arrow-left"), class = "btn btn-light")
    } else NULL
  })
  
  output$hc_mm <- renderHighchart({
    
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    req(!is.null(input$mm_metric))
    
    bounds <- c(0, 1000)
    fmt    <- "{point.y:.0f}"
    
    if (identical(mm_view_mode(), "groups")) {
      gs <- mm_group_stats()
      
      # ordena e pagina
      df <- gs %>% dplyr::mutate(value = as.numeric(value)) %>% dplyr::filter(!is.na(value))
      df <- df %>% dplyr::arrange(dplyr::desc(value), group_name)
      
      n  <- nrow(df)
      pb <- page_bounds(n, mm_page(), PER_PAGE)
      idx <- if (pb[1] <= pb[2]) seq.int(pb[1], pb[2]) else integer(0)
      df <- df[idx, , drop = FALSE]
      
      avg  <- if (nrow(df)) mean(df$value, na.rm = TRUE) else NA_real_
      cols <- color_by_mean(df$value, avg, high_is_good = TRUE)
      rng  <- axis_with_headroom(bounds, df$value)
      
      highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = "Score médio — por grupo") %>%
        hc_xAxis(type = "category", categories = df$group_name) %>%
        hc_yAxis(min = rng$min, max = rng$max, title = list(text = NULL),
                 plotLines = list(list(color = "#f39c12", width = 2, value = avg, zIndex = 5))) %>%
        hc_plotOptions(
          series = list(animation = list(duration = 700)),
          column = list(
            dataLabels   = bar_datalabels_opts(fmt),
            pointPadding = 0.1, groupPadding = 0.05,
            cursor       = "pointer",
            point = list(events = list(
              click = JS("
              function () {
                Shiny.setInputValue('hc_mm_group_click',
                  { name: this.name },
                  { priority: 'event' }
                );
              }")
            ))
          )
        ) %>%
        hc_add_series(
          name = "Score",
          data = purrr::pmap(list(df$group_name, df$value, cols),
                             function(nm, v, c) list(name = nm, y = v, color = c)),
          showInLegend = FALSE
        ) %>%
        hc_exporting(enabled = TRUE)
      
    } else if (identical(mm_view_mode(), "users")) {
      us <- mm_user_stats()
      df <- us %>% dplyr::mutate(value = as.numeric(value)) %>% dplyr::filter(!is.na(value))
      df <- df %>% dplyr::arrange(dplyr::desc(value), name)
      
      n  <- nrow(df)
      pb <- page_bounds(n, mm_page(), PER_PAGE)
      idx <- if (pb[1] <= pb[2]) seq.int(pb[1], pb[2]) else integer(0)
      df <- df[idx, , drop = FALSE]
      
      avg  <- if (nrow(df)) mean(df$value, na.rm = TRUE) else NA_real_
      cols <- color_by_mean(df$value, avg, high_is_good = TRUE)
      rng  <- axis_with_headroom(bounds, df$value)
      
      highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = "Score médio — usuários do grupo") %>%
        hc_xAxis(type = "category", categories = df$name) %>%
        hc_yAxis(min = rng$min, max = rng$max, title = list(text = NULL),
                 plotLines = list(list(color = "#f39c12", width = 2, value = avg, zIndex = 5))) %>%
        hc_plotOptions(
          series = list(animation = list(duration = 700)),
          column = list(
            dataLabels   = bar_datalabels_opts(fmt),
            pointPadding = 0.1, groupPadding = 0.05,
            cursor       = "pointer",
            point = list(events = list(
              click = JS("
              function(){
                Shiny.setInputValue('hc_mm_user_click',
                  { uid: this.options.uid, name: this.name },
                  { priority: 'event' }
                );
              }")
            ))
          )
        ) %>%
        hc_add_series(
          name = "Score",
          data = purrr::pmap(list(df$name, df$value, cols, df$user_id),
                             function(nm, v, c, uid) list(name = nm, y = v, color = c, uid = uid)),
          showInLegend = FALSE
        ) %>%
        hc_exporting(enabled = TRUE) #################################################################################
      
    } else if (identical(mm_view_mode(), "user")) {
      
      ts <- mm_user_ts(); req(nrow(ts) > 0)
      
      # categorias (datas), valores e ids
      cats <- format(ts$date, "%Y-%m-%d")
      vals <- as.numeric(ts$value)
      sids <- as.character(ts$sid)
      
      # paginação
      n  <- length(vals)
      pb <- page_bounds(n, mm_page(), PER_PAGE)
      idx <- if (pb[1] <= pb[2]) seq.int(pb[1], pb[2]) else integer(0)
      
      cats <- cats[idx]
      vals <- vals[idx]
      sids <- sids[idx]
      
      # referências (usar ponto-a-ponto para spline + errorbar, como na Avaliação)
      ref_mean <- as.numeric(ts$ref_mean[idx])
      ref_sd   <- as.numeric(ts$ref_sd[idx])
      
      # média do usuário (para linha laranja)
      user_mean <- if (length(vals)) mean(vals, na.rm = TRUE) else NA_real_
      
      # coloração por desvio da média do próprio usuário
      cols <- color_by_mean(vals, user_mean, high_is_good = TRUE)
      
      # dados p/ barras (com clique)
      points <- purrr::pmap(
        list(vals, cols, sids, cats),
        function(v, c, sid, cat) list(y = v, color = c, sid = sid, name = cat)
      )
      
      # dados p/ errorbar ±1DP (onde houver referência)
      err_data <- purrr::map2(ref_mean, ref_sd, ~{
        if (is.finite(.x) && is.finite(.y)) list(low = .x - .y, high = .x + .y)
        else                                list(low = NA_real_, high = NA_real_)
      })
      
      uname <- mm_selected_uname() %||% "Usuário"
      
      hc <- highchart() %>%
        hc_chart(type = "column", inverted = TRUE) %>%
        hc_title(text = paste0("Score — ", uname)) %>%
        hc_xAxis(type = "category", categories = cats, title = list(text = NULL)) %>%
        hc_yAxis(min = 0, max = 1000, title = list(text = NULL)) %>%
        hc_plotOptions(
          series = list(animation = list(duration = 600)),
          column = list(
            dataLabels   = bar_datalabels_opts("{point.y:.0f}"),
            pointPadding = 0.1, groupPadding = 0.05,
            cursor       = "pointer",
            point = list(
              events = list(
                click = JS("
                  function(){
                  Shiny.setInputValue('hc_mm_user_eval_click',
                    { sid: this.options.sid, date: this.name, y: this.y },
                    { priority: 'event' }
                  );
                 }
                ")
              )
            )
          )
        ) %>%
        # barras do usuário
        hc_add_series(
          name = "Score",
          type = "column",
          data = points,
          showInLegend = FALSE,
          tooltip = list(pointFormat = "<b>Score:</b> {point.y:.0f}")
        ) %>%
        # linha da média do usuário (laranja)
        hc_add_series(
          type = "line",
          name = "Média do usuário",
          data = rep(user_mean, length(vals)),
          color = "#e67e22", lineWidth = 2, dashStyle = "Solid",
          marker = list(enabled = FALSE), enableMouseTracking = FALSE
        )
      
      # spline da referência média (se houver algum valor finito)
      if (any(is.finite(ref_mean))) {
        hc <- hc %>% hc_add_series(
          type = "spline",
          name = "Ref. média",
          data = ref_mean,
          color = "#7f8c8d",
          dashStyle = "ShortDash",
          lineWidth = 2,
          marker = list(enabled = FALSE)
        )
      }
      
      # barras de erro ±1 DP (se houver algum SD finito)
      if (any(is.finite(ref_sd))) {
        hc <- hc %>% hc_add_series(
          type = "errorbar",
          name = "Ref. ±1 DP",
          data = err_data,
          whiskerWidth = 5,
          color = "#7f8c8d"
        )
      }
      
      hc %>% hc_exporting(enabled = TRUE)
    }
    
    
    
    
  })
  
  output$ui_mm_pager <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    total_items <- 0L
    if (identical(mm_view_mode(), "groups")) {
      g <- mm_group_stats(); total_items <- nrow(g)
    } else if (identical(mm_view_mode(), "users")) {
      u <- mm_user_stats();  total_items <- nrow(u)
    } else if (identical(mm_view_mode(), "user")) {
      t <- mm_user_ts();     total_items <- nrow(t)
    }
    total_pages <- max(1L, ceiling(total_items / PER_PAGE))
    curr <- clamp(mm_page(), 1L, total_pages); if (curr != mm_page()) mm_page(curr)
    
    tagList(
      actionButton("mm_prev", label = NULL, icon = icon("chevron-left"),
                   class = "btn btn-light", disabled = if (curr <= 1) "disabled"),
      span(sprintf("Página %d de %d", curr, total_pages),
           style="min-width:140px; text-align:center; font-weight:600;"),
      actionButton("mm_next", label = NULL, icon = icon("chevron-right"),
                   class = "btn btn-light", disabled = if (curr >= total_pages) "disabled")
    )
  })
  
  output$mm_user_chart <- renderHighchart({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    
    ts <- mm_user_ts()
    
    pts  <- ts$points
    band <- ts$band
    
    # data para HC
    line_data <- pts %>%
      transmute(
        x, y, sid,
        # pacote para o tooltip
        r1 = result1, r2 = result2, r3 = result3, r4 = result4,
        l1 = label_sec1, l2 = label_sec2, l3 = label_sec3, l4 = label_sec4,
        m  = reference_mean, sd = reference_sd
      ) %>%
      highcharter::list_parse2()
    
    arange_data <- NULL
    if (nrow(band) > 0) {
      arange_data <- band %>% highcharter::list_parse2()
    }
    
    highcharter::highchart() %>%
      highcharter::hc_chart(zoomType = "x") %>%
      highcharter::hc_title(text = "Evolução da Medida (score)") %>%
      highcharter::hc_xAxis(type = "datetime") %>%
      highcharter::hc_yAxis(title = list(text = "Score")) %>%
      {
        if (!is.null(arange_data)) {
          . %>% highcharter::hc_add_series(
            type  = "arearange",
            name  = "Referência (±1 DP)",
            data  = arange_data,
            zIndex = 0,
            tooltip = list(valueDecimals = 1),
            fillOpacity = 0.15
          )
        } else .
      } %>%
      highcharter::hc_add_series(
        type = "line",
        name = "Score",
        data = line_data,
        zIndex = 2,
        marker = list(enabled = TRUE, radius = 3),
        events = list(
          click = htmlwidgets::JS(
            "function(e){ 
             if(e && e.point && e.point.sid){
               Shiny.setInputValue('mm_user_point_click', 
                 { sid: e.point.sid, x: e.point.x, nonce: Math.random() },
                 { priority: 'event' }
               );
             }
           }"
          )
        )
      ) %>%
      highcharter::hc_tooltip(
        useHTML = TRUE,
        shared  = FALSE,
        headerFormat = "<span style='font-size:11px'>{point.key}</span><br/>",
        pointFormatter = htmlwidgets::JS(
          "function(){
           var s  = '<b>Score:</b> ' + Highcharts.numberFormat(this.y, 1) + '<br/>';
           if (this.m !== null && this.m !== undefined) {
             s += '<b>Ref. média:</b> ' + Highcharts.numberFormat(this.m,1) + 
                  ' &plusmn; ' + (this.sd ? Highcharts.numberFormat(this.sd,1) : '-') + '<br/>';
           }
           var lbl = function(v,l){ 
             return (v!==null && v!==undefined) ? ('<b>'+(l||'Sub')+':</b> ' + Highcharts.numberFormat(v,1) + '<br/>') : '';
           };
           s += lbl(this.r1, this.l1);
           s += lbl(this.r2, this.l2);
           s += lbl(this.r3, this.l3);
           s += lbl(this.r4, this.l4);
           return s;
         }"
        )
      ) %>%
      highcharter::hc_exporting(enabled = TRUE)
  })
  
  output$ui_mm_detail <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    row <- mm_selected_row(); req(nrow(row) > 0)
    
    dstr <- tryCatch(format(as.Date(row$date[1]), "%Y-%m-%d"), error = function(...) "—")
    
    # nome da medida selecionada
    mid <- mm_metric_id()
    ms  <- mm_available_measures()
    meas_name <- tryCatch(
      as.character(ms$measurement_name[match(mid, ms$measurement_id)]),
      error = function(...) NA_character_
    )
    meas_name <- ifelse(is.na(meas_name) | !nzchar(meas_name), "Medida", meas_name)
    
    width_pct <- "20%"  # 5 tiles em uma linha
    
    circ <- function(output_id) {
      div(style = sprintf("flex:0 0 %s; max-width:%s; padding:4px;", width_pct, width_pct),
          highchartOutput(output_id, height = "220px"))
    }
    
    tagList(
      div(style="margin-top:14px; border:1px solid #eee; border-radius:8px; padding:12px;",
          
          # header agora com o rótulo da medida
          h4(sprintf("Detalhes da medida (%s) — %s", meas_name, dstr)),
          
          fluidRow(column(12, highchartOutput("hc_mm_det_score_circ", height = "340px"))),
          br(),
          
          # 4 circulares centralizados
          div(
            style = "display:flex; gap:16px; flex-wrap:nowrap; width:100%;
                   justify-content:center; align-items:flex-start;",
            circ("hc_mm_det_r1_circ"),
            circ("hc_mm_det_r2_circ"),
            circ("hc_mm_det_r3_circ"),
            circ("hc_mm_det_r4_circ")
          ),
          
          br(),
          fluidRow(column(12, highchartOutput("hc_mm_det_moods_rose", height = "360px")))
      )
    )
  })
  
  output$hc_mm_det_score_circ <- renderHighchart({
    row <- mm_selected_row()
    hc_circular_bar(
      value = as.numeric(row$score[1]), minmax = c(0, 1000),
      title_txt = as.character(row$label_score[1]),
      fmt = "{y:.0f}", high_is_good = TRUE, size = "95%", inner = "70%"
    )
  })
  
  output$hc_mm_det_r1_circ <- renderHighchart({
    row <- mm_selected_row()
    hc_circular_bar(
      value = as.numeric(row$result1[1]), minmax = c(0,100),
      title_txt = as.character(row$label_sec1[1] %||% "Seção 1"),
      fmt = "{y:.0f}", high_is_good = TRUE, size = "90%", inner = "72%"
    )
  })
  
  output$hc_mm_det_r2_circ <- renderHighchart({
    row <- mm_selected_row()
    hc_circular_bar(as.numeric(row$result2[1]), c(0,100),
                    as.character(row$label_sec2[1] %||% "Seção 2"),
                    fmt = "{y:.0f}", high_is_good = TRUE, size = "90%", inner = "72%"
    )
  })
  
  output$hc_mm_det_r3_circ <- renderHighchart({
    row <- mm_selected_row()
    hc_circular_bar(as.numeric(row$result3[1]), c(0,100),
                    as.character(row$label_sec3[1] %||% "Seção 3"),
                    fmt = "{y:.0f}", high_is_good = TRUE, size = "90%", inner = "72%"
    )
  })
  
  output$hc_mm_det_r4_circ <- renderHighchart({
    row <- mm_selected_row()
    hc_circular_bar(as.numeric(row$result4[1]), c(0,100),
                    as.character(row$label_sec4[1] %||% "Seção 4"),
                    fmt = "{y:.0f}", high_is_good = TRUE, size = "90%", inner = "72%"
    )
  })
  
  output$hc_mm_det_moods_rose <- renderHighchart({
    row <- mm_selected_row()
    alegria     <- 100 * as.numeric(row$happy[1])
    disposicao  <- 100 * (1 - as.numeric(row$tired_out[1]))  # invertido
    relaxamento <- 100 * (1 - as.numeric(row$tense[1]))      # invertido
    sono        <- 100 * as.numeric(row$night_of_sleep[1])
    
    cats <- c("Alegria", "Disposição", "Relaxamento", "Última noite de sono")
    vals <- c(alegria,   disposicao,    relaxamento,   sono)
    
    hc_polar_rose(cats, vals, title_txt = "Estado — intensidade (%)", max_pct = 100)
  })
  
  output$download_mm_xlsx <- downloadHandler(filename = function() {
      d <- institution_dt()
      inst <- tryCatch(as.character(d$institution_name), error = function(e) "instituicao")
      sprintf("medidas_moove_%s_%s.xlsx",
              gsub("[^A-Za-z0-9_-]", "_", inst),
              format(Sys.time(), "%Y%m%d-%H%M"))
    },content = function(file) {
      df <- mm_download_df()
      if (!requireNamespace("openxlsx", quietly = TRUE)) {
        write.csv(df, file, row.names = FALSE, fileEncoding = "UTF-8")
      } else {
        openxlsx::write.xlsx(df, file, na = "")
      }
    })
  
  # ===================== observers =====================
  
  # ---- general -----
  
  observeEvent(TRUE, {
    ok <- TRUE
    msg <- NULL
    tryCatch({
      invisible(DBI::dbGetQuery(pool, "SELECT 1"))
    }, error = function(e){
      ok  <<- FALSE
      msg <<- conditionMessage(e)
    })
    if (!ok) {
      showNotification(
        paste0("Falha ao validar conexão com o banco: ", msg,
               " — verifique driver RMariaDB, credenciais e reinicie a sessão R."),
        type = "error", duration = NULL
      )
    }
  }, once = TRUE)
  
  observeEvent(TRUE, { showModal(login_modal()) }, once = TRUE)
  
  observeEvent(input$login_confirm, {
    
    if(is_local){
      email <- "contato@sensorialsports.com"
      pass  <- "senso"
    }else{
      email <- tolower(trimws(input$login_email %||% ""))
      pass  <- input$login_pass %||% ""
    }
    
    if (!nzchar(email)) { output$login_error <- renderText("Informe o e-mail."); return() }
    if (!nzchar(pass))  { output$login_error <- renderText("Informe a senha."); return() }
    
    
    
    # 1) LOGIN → token
    tk <- tryCatch(api_login_get_token(email, pass, api_address, header_key),
                   error = function(e) list(status = 0L, content = NULL))
    
    if (!identical(tk$status, 200L)) {
      output$login_error <- renderText("Falha no login (token). Verifique credenciais.")
      authed(FALSE); return()
    }
    
    token <- tryCatch(tk$content$access_token, error = function(e) NULL)
    if (is.null(token) || !nzchar(token)) {
      output$login_error <- renderText("Token não recebido.")
      authed(FALSE); return()
    }
    api_token(token)
    
    # 2) Relatório de instituição
    inst_resp <- tryCatch(api_get_institution_report(token, api_address, header_key),
                          error = function(e) e)
    
    if (inherits(inst_resp, "error")) {
      # Se falhar aqui, por ora exibimos papel de trainer (integraremos no próximo passo)
      session_role("trainer")
      institution_raw(NULL)
      authed(TRUE); removeModal()
      return()
    }
    
    # OK → papel instituição
    session_role("institution")
    institution_raw(inst_resp)
    authed(TRUE)
    removeModal()
  })
  
  observeEvent(scope_user_names(), {
    req(authed(), session_role() == "institution")
    df <- scope_user_names()
    # choices nomeados: user_id -> name
    ch <- stats::setNames(df$user_id, df$name)
    updateSelectizeInput(
      session, "sel_user",
      choices = ch,
      server  = TRUE,
      selected = character(0) # nenhum selecionado = "todos"
    )
  }, ignoreInit = TRUE)
  
  # ---- evals -----
  
  observeEvent(input$hc_eval_group_click, {
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    clicked_name <- input$hc_eval_group_click$name
    gdf <- groups_from_api()
    gid <- gdf$id[match(clicked_name, gdf$name)]
    if (!is.na(gid)) {
      eval_selected_group(as.integer(gid))
      eval_view_mode("users")
    }
  }, ignoreInit = TRUE)
  
  observeEvent(input$btn_eval_back, {
    if (identical(eval_view_mode(), "user")) {
      # volta para a lista de usuários do grupo
      eval_view_mode("users")
      eval_selected_user(NA_integer_)
      eval_selected_uname(NA_character_)
    } else {
      # volta para grupos
      eval_view_mode("groups")
      eval_selected_group(NA_integer_)
    }
  }, ignoreInit = TRUE)
  
  observeEvent(input$hc_eval_user_click, {
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    uid <- input$hc_eval_user_click$uid
    nm  <- input$hc_eval_user_click$name
    if (!is.null(uid) && !is.na(as.integer(uid))) {
      eval_selected_user(as.integer(uid))
      eval_selected_uname(as.character(nm %||% paste0("user_", uid)))
      eval_view_mode("user")
    }
  }, ignoreInit = TRUE)
  
  observeEvent(input$hc_eval_user_eval_click, {
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    sid  <- input$hc_eval_user_eval_click$sid
    dstr <- input$hc_eval_user_eval_click$date
    if (!is.null(sid)) eval_selected_score_id(as.character(sid))
    if (!is.null(dstr)) eval_selected_date(as.Date(dstr))
  }, ignoreInit = TRUE)
  
  observeEvent(list(eval_view_mode(), eval_metric_key(), eval_selected_group(), eval_selected_user()), {
    eval_page(1L)
  }, ignoreInit = TRUE)
 
  observeEvent(input$eval_prev, { eval_page(clamp(eval_page() - 1L, 1L, 1e6)) })
  
  observeEvent(input$eval_next, { eval_page(eval_page() + 1L) })
  
  output$ui_perf_back <- renderUI({
    if (identical(perf_view_mode(), "users")) {
      actionButton("btn_perf_back", "Voltar aos grupos", icon = icon("arrow-left"), class = "btn btn-light")
    } else NULL
  })
  
  # ---- minigames -----
  
  observeEvent(input$hc_mg_group_click, {
    clicked_name <- input$hc_mg_group_click$name
    gdf <- groups_from_api()
    gid <- gdf$id[match(clicked_name, gdf$name)]
    if (!is.na(gid)) {
      mg_selected_group(as.integer(gid))
      mg_view_mode("users")
    }
  })
  
  observeEvent(mg_view_mode(), { mg_page(1L) }, ignoreInit = TRUE)
  
  observeEvent(input$mg_prev, { mg_page(clamp(mg_page() - 1L, 1L, 1e6)) })
  
  observeEvent(input$mg_next, { mg_page(mg_page() + 1L) })
  
  observeEvent(input$hc_mg_user_click, {
    uid <- input$hc_mg_user_click$uid
    nm  <- input$hc_mg_user_click$name
    if (!is.null(uid) && !is.na(as.integer(uid))) {
      mg_selected_user(as.integer(uid))
      mg_selected_uname(as.character(nm))
      mg_view_mode("user")
    }
  })
  
  observeEvent(input$btn_mg_back, {
    if (identical(mg_view_mode(), "user")) {
      mg_view_mode("users")
      mg_selected_user(NA_integer_)
      mg_selected_uname(NA_character_)
    } else {
      mg_view_mode("groups")
      mg_selected_group(NA_integer_)
    }
  })
  
  observeEvent(input$hc_perf_group_click, {
    clicked <- input$hc_perf_group_click$name
    gdf <- groups_from_api()
    gid <- gdf$id[match(clicked, gdf$name)]
    if (!is.na(gid)) {
      perf_selected_group(as.integer(gid))
      perf_view_mode("users")
      perf_page(1)
    }
  })
  
  observeEvent(input$btn_perf_back, {
    perf_selected_group(NA_integer_)
    perf_view_mode("groups")
    perf_page(1)
  })
  
  observeEvent(input$btn_perf_prev, {
    perf_page(max(1, perf_page() - 1))
  })
  
  observeEvent(input$btn_perf_next, {
    total <- if (identical(perf_view_mode(),"groups")) nrow(perf_group_stats()) else nrow(perf_user_stats())
    n_pages <- max(1, ceiling(total / PER_PAGE))
    perf_page(min(n_pages, perf_page() + 1))
  })
  
  observeEvent(input$sel_capacity, {perf_page(1)
  })
  
  # ---- measurements -----
  
  observeEvent(input$hc_mm_group_click, {
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    clicked_name <- input$hc_mm_group_click$name
    gdf <- groups_from_api()
    gid <- gdf$id[match(clicked_name, gdf$name)]
    if (!is.na(gid)) {
      mm_selected_group(as.integer(gid))
      mm_view_mode("users")
      mm_page(1L)
    }
  })
  
  observeEvent(input$hc_mm_user_click, {
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    uid <- input$hc_mm_user_click$uid
    nm  <- input$hc_mm_user_click$name
    if (!is.null(uid) && !is.na(as.integer(uid))) {
      mm_selected_user(as.integer(uid))
      mm_selected_uname(as.character(nm))
      mm_view_mode("user")
      mm_page(1L)
    }
  })
  
  observeEvent(input$btn_mm_back, {
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    
    if (identical(mm_view_mode(), "user")) {
      mm_view_mode("users")
      mm_selected_user(NA_integer_)
      mm_selected_uname(NA_character_)
    } else {
      mm_view_mode("groups")
      mm_selected_group(NA_integer_)
    }
    
    # zera seleção de avaliação (para os detalhes não ficarem “presos”)
    mm_selected_score_id(NULL)
    mm_selected_date(as.Date(NA))
    
    # volta para a primeira página
    mm_page(1L)
  })
  
  observeEvent(input$mm_prev, { mm_page(clamp(mm_page() - 1L, 1L, 1e6)) })
  
  observeEvent(input$mm_next, { mm_page(mm_page() + 1L) })
  
  observeEvent(list(input$mm_metric, mm_view_mode(), mm_selected_group(), mm_selected_user()), {
    mm_selected_score_id(NULL)
    mm_selected_date(as.Date(NA))
    mm_page(1L)
  }, ignoreInit = TRUE)
  
  observeEvent(input$mm_user_point_click, {
    sid <- input$mm_user_point_click$sid
    if (!is.null(sid) && nzchar(sid)) {
      mm_selected_score_id(sid)
    }
  }, ignoreInit = TRUE)
  
  observeEvent(input$hc_mm_user_eval_click, {
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    sid  <- input$hc_mm_user_eval_click$sid
    dstr <- input$hc_mm_user_eval_click$date
    if (!is.null(sid))  mm_selected_score_id(as.character(sid))
    if (!is.null(dstr)) mm_selected_date(as.Date(dstr))
  }, ignoreInit = TRUE)
  
  # ---- end -----
}

shinyApp(ui, server)
