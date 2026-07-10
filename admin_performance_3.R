# admin_performance.R

rm(list = ls()) #

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
  library(googlesheets4)
  library(ggplot2)
  library(forcats)
  library(scales)
  library(ggimage)
  library(plotly)
})

is_local <- 1
is_debug <- 0
is_manager <- 0

# --------------------- settings ---------------------------------

directory <- tryCatch(paste0(dirname(rstudioapi::getSourceEditorContext()$path), "/"),error = function(e) "./")

source(paste0(directory, "Env.Data.R"))

config <- if (is_debug) getHomEnvConfig() else getProdEnvConfig()

gs4_auth_path <- paste0(directory, "sensorialsports-fa9fdc558dd6.json")
gs4_auth_ok <- FALSE
if (!is.na(gs4_auth_path) && nzchar(gs4_auth_path)) {
  gs4_auth_ok <- tryCatch({
    googlesheets4::gs4_auth(path = gs4_auth_path)
    TRUE
  }, error = function(e) FALSE)
}

TRIAGE_SHEET_ID <- "10knuvOilqvyuW948M5xm8sDnkB2BHxk2fM7R7mOqk5o"
SCREENING_THRESHOLDS_SHEET <- "screening_thresholds"
MINIGAMES_RANKING_SHEET <- "minigames_ranking"
TRAINER_TAGS_SHEET <- "trainer_tags"
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
user_related_users    <- tbl(pool, "user_related_users")
legal_entity_users    <- tbl(pool, "legal_entity_users")
user_question_answers <- tbl(pool, "user_question_answers") # Q37 para nome
user_rankings         <- tbl(pool, "user_rankings")
tag_templates_tbl     <- tbl(pool, "tag_templates")
trainings_tbl         <- tbl(pool, "trainings")
training_tag_completions_tbl <- tbl(pool, "training_tag_completions")
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

get_legal_entity_trainers <- function(sel_legal_entity_id) {
  legal_entity_users %>%
    filter(.data$legal_entity_id %in% !!as.integer(sel_legal_entity_id)) %>%
    transmute(user_id = as.integer(.data$user_id)) %>%
    distinct() %>%
    inner_join(
      users %>%
        filter(.data$user_type == 4) %>%
        transmute(user_id = as.integer(.data$id)),
      by = "user_id"
    ) %>%
    collect() %>%
    pull(.data$user_id)
}

get_legal_entity_trainers_users <- function(sel_legal_entity_trainers) {
  if (length(sel_legal_entity_trainers) == 0) {
    return(tibble::tibble(trainer_id = integer(), user_id = integer()))
  }

  cols <- tryCatch(colnames(user_related_users), error = function(e) character())
  target_col <- c(
    "user_id_related",
    "related_user_id",
    "user_related_user_id",
    "target_user_id",
    "member_user_id",
    "child_user_id"
  )
  target_col <- target_col[target_col %in% cols]

  if (!length(target_col)) {
    user_cols <- setdiff(grep("user", cols, value = TRUE), "user_id")
    target_col <- user_cols[seq_len(min(1L, length(user_cols)))]
  }

  if (!length(target_col)) {
    return(tibble::tibble(trainer_id = integer(), user_id = integer()))
  }

  target_col <- target_col[[1]]

  user_related_users %>%
    filter(.data$user_id %in% !!as.integer(sel_legal_entity_trainers)) %>%
    transmute(
      trainer_id = as.integer(.data$user_id),
      user_id = as.integer(!!rlang::sym(target_col))
    ) %>%
    filter(!is.na(.data$user_id)) %>%
    distinct() %>%
    collect()
}

get_user_ids_for_institution_or_grouping <- function(institution_id, grouping_id_or_all = "ALL", grouping_mode = "groups") {
  if (!identical(grouping_mode, "trainers")) {
    return(get_user_ids_for_institution_or_group(institution_id, grouping_id_or_all))
  }

  if (identical(grouping_id_or_all, "ALL")) {
    return(get_user_ids_for_institution_or_group(institution_id, "ALL"))
  }

  trainer_users <- get_legal_entity_trainers_users(as.integer(grouping_id_or_all))
  unique(as.integer(trainer_users$user_id))
}

grouping_label <- function(mode = "groups", plural = TRUE, title_case = FALSE) {
  out <- if (identical(mode, "trainers")) {
    if (isTRUE(plural)) "treinadores" else "treinador"
  } else {
    if (isTRUE(plural)) "grupos" else "grupo"
  }

  if (isTRUE(title_case)) {
    paste0(toupper(substr(out, 1, 1)), substr(out, 2, nchar(out)))
  } else {
    out
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

get_nickname_for_users <- function(uids) {
  if (length(uids) == 0) return(tibble(user_id = integer(), nickname = character()))
  users %>%
    filter(.data$id %in% !!as.integer(uids)) %>%
    transmute(user_id = as.integer(.data$id), nickname = as.character(.data$user)) %>%
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

triage_tab_label <- function() {
  paste0("Triagem e Ativa", intToUtf8(231), intToUtf8(227), "o")
}

triage_tab_panel <- function() {
  tabPanel(
    triage_tab_label(),
    br(),
    br(),
    fluidRow(align = "center",
      column(3),
      column(
        6,align = "center",
        div(
          style = "display:flex; justify-content:center;",
          radioButtons(
            "triage_bucket_mode",
            label = NULL,
            choices = c("Grupos/Treinadores" = "grouping", "Unidades" = "units"),
            selected = "grouping",
            inline = TRUE
          )
        )
      ),
      column(
        3,
        div(
          style = "display:flex; justify-content:flex-end;",
          textInput("triage_manager_code", label = NULL, value = "", width = "120px")
        )
      )
    ),br(),
    tags$h4("Triagens", style = "text-align:center; font-weight:700;"),
    fluidRow(align = "center", DTOutput("tbl_triage_monthly", width = "33%")),
    br(),
    fluidRow(align = "right", uiOutput("ui_triage_back")),
    highchartOutput("hc_triage_groups", height = "650px"),
    div(
      style = "display:flex; justify-content:center; align-items:center; gap:12px; margin: 10px 0 4px 0;",
      uiOutput("ui_triage_pager")
    ),
    br(),
    uiOutput("ui_triage_detail")
  )
}

triage_report_tab_label <- function() {
  paste0("Relat", intToUtf8(243), "rio Triagem - Ativa", intToUtf8(231), intToUtf8(227), "o")
}

triage_report_tab_panel <- function() {
  tabPanel(
    triage_report_tab_label(),
    br(),
    fluidRow(align = "center",
      column(4),
      column(4,align = "center", selectInput("triage_report_unit", "Unidade", choices = character(0))),
      column(4)
    ),
    br(),
    uiOutput("ui_triage_report_content")
  )
}

emoji_img <- function(filename, size = 18) {
  # filename ex.: "feliz.png" ou "feliz.svg"
  sprintf(
    "<img src='%s' style='height:%dpx;vertical-align:-3px;margin-right:6px;'>",
    filename, as.integer(size)
  )
}

# ---- cache ------

.cache_env <- new.env(parent = emptyenv())

CACHE_TTL <- 60 * 5  # 5 minutos; ajustável

.cache_now <- function() as.numeric(Sys.time())

cache_key <- function(prefix, ..., inst_id = NULL, group_id = NULL, user_id = NULL, extra = NULL) {
  parts <- c(
    prefix,
    if (!is.null(inst_id)) paste0("inst=", inst_id),
    if (!is.null(group_id)) paste0("grp=", group_id),
    if (!is.null(user_id)) paste0("usr=", user_id),
    if (!is.null(extra))   paste0("x=",   extra),
    # inclui is_debug/is_local para evitar reuso indevido entre ambientes
    paste0("dbg=", is_debug),
    paste0("loc=", is_local)
  )
  paste(parts, collapse = "|")
}

cache_set <- function(key, value, ttl = CACHE_TTL) {
  assign(key, list(value = value, expires = .cache_now() + ttl), envir = .cache_env)
  invisible(TRUE)
}

cache_get <- function(key) {
  if (!exists(key, envir = .cache_env, inherits = FALSE)) return(NULL)
  obj <- get(key, envir = .cache_env, inherits = FALSE)
  if (!is.list(obj) || is.null(obj$expires) || is.null(obj$value)) return(NULL)
  if (.cache_now() > obj$expires) {
    # expirou → remove e retorna NULL
    rm(list = key, envir = .cache_env)
    return(NULL)
  }
  obj$value
}

cache_has <- function(key) {
  !is.null(cache_get(key))
}

cache_clear <- function(prefix = NULL) {
  if (is.null(prefix)) {
    rm(list = ls(envir = .cache_env, all.names = TRUE), envir = .cache_env)
    return(invisible(TRUE))
  }
  ks <- ls(envir = .cache_env, all.names = TRUE)
  ks <- ks[startsWith(ks, prefix)]
  if (length(ks)) rm(list = ks, envir = .cache_env)
  invisible(TRUE)
}

.MINIGAMES_MEMO <- new.env(parent = emptyenv())

memo_get_minigames <- function(key, loader_fn) {
  if (exists(key, envir = .MINIGAMES_MEMO, inherits = FALSE)) {
    return(get(key, envir = .MINIGAMES_MEMO, inherits = FALSE))
  }
  val <- loader_fn()
  assign(key, val, envir = .MINIGAMES_MEMO)
  val
}

minigames_cache_key <- function(inst_id, group_choice) {
  sprintf("inst=%s|group=%s", as.character(inst_id %||% "NA"), as.character(group_choice %||% "ALL"))
}

get_moove_scores_data_cached <- function(sel_users, inst_id, group_choice) {
  if (length(sel_users) == 0) return(tibble::tibble())
  key <- paste0("minigames:moove_scores:", minigames_cache_key(inst_id, group_choice))
  memo_get_minigames(key, function() get_moove_scores_data(sel_users))
}

get_moove_scores_raw_data_cached <- function(sel_users, inst_id, group_choice) {
  if (length(sel_users) == 0) return(tibble::tibble())
  key <- paste0("minigames:moove_scores_raw:", minigames_cache_key(inst_id, group_choice))
  memo_get_minigames(key, function() get_moove_scores_raw_data(sel_users))
}

get_moove_scores_game_parameter_data_cached <- function(sel_users, inst_id, group_choice, game_id, game_parameter, parameter_logic = "desc", limit_n = 100L) {
  if (length(sel_users) == 0) return(tibble::tibble())
  key <- paste0(
    "minigames:game_parameter:",
    minigames_cache_key(inst_id, group_choice),
    "|game=", as.character(game_id),
    "|param=", as.character(game_parameter),
    "|logic=", as.character(parameter_logic),
    "|limit=", as.character(limit_n)
  )
  memo_get_minigames(key, function() get_moove_scores_game_parameter_data(sel_users, game_id, game_parameter, parameter_logic = parameter_logic, limit_n = limit_n))
}

get_user_settings_avg_percentiles_cached <- function(sel_users, inst_id, group_choice) {
  if (length(sel_users) == 0) return(tibble::tibble())
  key <- paste0("minigames:avg_percentiles:", minigames_cache_key(inst_id, group_choice))
  memo_get_minigames(key, function() get_user_settings_avg_percentiles(sel_users))
}

.MEASURES_MEMO <- new.env(parent = emptyenv())

memo_get_measures <- function(key, loader_fn) {
  if (exists(key, envir = .MEASURES_MEMO, inherits = FALSE)) {
    return(get(key, envir = .MEASURES_MEMO, inherits = FALSE))
  }
  val <- loader_fn()
  assign(key, val, envir = .MEASURES_MEMO)
  val
}

measures_cache_key <- function(inst_id, group_choice) {
  sprintf("inst=%s|group=%s", as.character(inst_id %||% "NA"), as.character(group_choice %||% "ALL"))
}

get_measurement_summaries_cached <- function(sel_users, inst_id, group_choice) {
  if (length(sel_users) == 0) return(tibble::tibble())
  key <- paste0("measures:summaries:", measures_cache_key(inst_id, group_choice))
  memo_get_measures(key, function() get_measurement_summaries(sel_users))
}

memo_clear_env <- function(env) {
  if (is.environment(env)) rm(list = ls(env, all.names = TRUE), envir = env)
}

memo_clear_all <- function() {
  # se você nomeou os memos assim; ajuste se os nomes diferirem
  if (exists(".MINIGAMES_MEMO",   inherits = FALSE)) memo_clear_env(.MINIGAMES_MEMO)
  if (exists(".PERC_MEMO",        inherits = FALSE)) memo_clear_env(.PERC_MEMO)
  if (exists(".MEASURES_MEMO",    inherits = FALSE)) memo_clear_env(.MEASURES_MEMO)
}

RESPS_TTL <- 15 * 60  # 15 min
.resps_cache_env <- new.env(parent = emptyenv())

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

get_moove_scores_raw_data <- function(sel_users) {
  if (length(sel_users) == 0) return(tibble::tibble())
  url.mongodb <- config[6]
  ids <- as.integer(sel_users)
  
  m <- mongolite::mongo(collection = "moove_scores", url = url.mongodb)
  on.exit(m$disconnect(), add = TRUE)
  
  # Inclui o campo parameters.score_percentiles no projection
  q <- jsonlite::toJSON(list(user_id = list("$in" = as.list(ids)),game_id = 138), auto_unbox = TRUE)
  df <- m$find(
    query  = q,
    fields = '{"_id":0,"user_id":1,"game_id":1,"date_time":1,"parameters.correct_responses_per_minute":1,"parameters.incorrect_responses_per_minute":1,"parameters.average_response_time":1}'
  )

  if (is.null(df) || !nrow(df)) return(tibble::tibble())

  df <- tibble::as_tibble(df)

  if ("parameters" %in% names(df)) {
    df$correct_responses_per_minute <- suppressWarnings(as.numeric(df$parameters$correct_responses_per_minute))
    df$incorrect_responses_per_minute <- suppressWarnings(as.numeric(df$parameters$incorrect_responses_per_minute))
    df$average_response_time <- suppressWarnings(as.numeric(df$parameters$average_response_time))
    df$parameters <- NULL
  } else {
    if (!"correct_responses_per_minute" %in% names(df) && "parameters.correct_responses_per_minute" %in% names(df)) {
      df$correct_responses_per_minute <- suppressWarnings(as.numeric(df[["parameters.correct_responses_per_minute"]]))
    }
    if (!"incorrect_responses_per_minute" %in% names(df) && "parameters.incorrect_responses_per_minute" %in% names(df)) {
      df$incorrect_responses_per_minute <- suppressWarnings(as.numeric(df[["parameters.incorrect_responses_per_minute"]]))
    }
    if (!"average_response_time" %in% names(df) && "parameters.average_response_time" %in% names(df)) {
      df$average_response_time <- suppressWarnings(as.numeric(df[["parameters.average_response_time"]]))
    }
  }

  if (!"correct_responses_per_minute" %in% names(df)) df$correct_responses_per_minute <- NA_real_
  if (!"incorrect_responses_per_minute" %in% names(df)) df$incorrect_responses_per_minute <- NA_real_
  if (!"average_response_time" %in% names(df)) df$average_response_time <- NA_real_

  df %>%
    dplyr::mutate(
      correct_responses_per_minute   = round(.data$correct_responses_per_minute),
      incorrect_responses_per_minute = round(.data$incorrect_responses_per_minute),
      average_response_time          = round(1000 * .data$average_response_time)
    )
}

get_moove_scores_game_parameter_data <- function(sel_users, game_id, game_parameter, parameter_logic = "desc", limit_n = 100L) {
  if (length(sel_users) == 0) return(tibble::tibble())
  url.mongodb <- config[6]
  ids <- as.integer(sel_users)
  game_id <- as.integer(game_id)
  game_parameter <- as.character(game_parameter)
  field_name <- paste0("parameters.", game_parameter)

  m <- mongolite::mongo(collection = "moove_scores", url = url.mongodb)
  on.exit(m$disconnect(), add = TRUE)

  q <- jsonlite::toJSON(list(user_id = list("$in" = as.list(ids)), game_id = game_id), auto_unbox = TRUE)
  fields_json <- sprintf('{"_id":0,"user_id":1,"game_id":1,"date_time":1,"%s":1}', field_name)
  df <- m$find(query = q, fields = fields_json)

  if (is.null(df) || !nrow(df)) return(tibble::tibble())

  df <- tibble::as_tibble(df)

  if ("parameters" %in% names(df) && game_parameter %in% names(df$parameters)) {
    df$metric_value <- suppressWarnings(as.numeric(df$parameters[[game_parameter]]))
    df$parameters <- NULL
  } else if (field_name %in% names(df)) {
    df$metric_value <- suppressWarnings(as.numeric(df[[field_name]]))
  } else {
    df$metric_value <- NA_real_
  }

  df %>%
    dplyr::mutate(metric_value = suppressWarnings(as.numeric(.data$metric_value))) %>%
    dplyr::filter(!is.na(.data$metric_value))
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

triage_threshold_defaults <- function() {
  c(
    triage_correct_yellow   = 72,
    triage_correct_red      = 63,
    triage_incorrect_yellow = 3,
    triage_incorrect_red    = 5,
    triage_rt_yellow        = 690,
    triage_rt_red           = 750
  )
}

find_triage_threshold_sheet_name <- function(sheet_id) {
  if (!isTRUE(gs4_auth_ok)) return(NA_character_)

  sheet_names <- tryCatch(
    googlesheets4::sheet_names(sheet_id),
    error = function(e) character()
  )

  if (!length(sheet_names)) return(NA_character_)

  required_measures <- names(triage_threshold_defaults())

  for (sheet_nm in sheet_names) {
    probe <- tryCatch(
      googlesheets4::read_sheet(sheet_id, sheet = sheet_nm, n_max = 50),
      error = function(e) tibble::tibble()
    )

    if (is.null(probe) || !nrow(probe)) next

        nm_low <- tolower(names(probe))
    has_group_cols <- all(c("group", "group_id") %in% nm_low)
    has_trainer_cols <- all(c("trainer", "trainer_id") %in% nm_low)
    if (!all(c("measure", "value") %in% nm_low) || (!has_group_cols && !has_trainer_cols)) next

    probe_tbl <- tibble::as_tibble(probe)
    names(probe_tbl) <- nm_low
    measures_found <- unique(as.character(probe_tbl$measure))

    if (any(required_measures %in% measures_found)) {
      return(sheet_nm)
    }
  }

  sheet_names[[1]]
}

read_triage_threshold_sheet <- function(sheet_id, grouping_mode = "groups") {
  if (!isTRUE(gs4_auth_ok)) return(tibble::tibble())

  target_sheet <- SCREENING_THRESHOLDS_SHEET

  if (is.na(target_sheet) || !nzchar(target_sheet)) return(tibble::tibble())

  out <- tryCatch(
    googlesheets4::read_sheet(sheet_id, sheet = target_sheet),
    error = function(e) tibble::tibble()
  )

  if (is.null(out) || !nrow(out)) return(tibble::tibble())

  out_tbl <- tibble::as_tibble(out) %>%
    dplyr::rename_with(tolower)

  if (!"measure" %in% names(out_tbl)) out_tbl$measure <- NA_character_
  if (!"value" %in% names(out_tbl)) out_tbl$value <- NA_real_
  if (!"group" %in% names(out_tbl)) out_tbl$group <- NA_character_
  if (!"group_id" %in% names(out_tbl)) out_tbl$group_id <- NA_integer_
  if (!"trainer" %in% names(out_tbl)) out_tbl$trainer <- NA_character_
  if (!"trainer_id" %in% names(out_tbl)) out_tbl$trainer_id <- NA_integer_
  if (!"unit" %in% names(out_tbl)) out_tbl$unit <- NA_character_

  out_tbl <- out_tbl %>%
    dplyr::mutate(
      measure    = as.character(.data$measure),
      value      = suppressWarnings(as.numeric(.data$value)),
      group      = as.character(.data$group),
      group_id   = suppressWarnings(as.integer(.data$group_id)),
      trainer    = as.character(.data$trainer),
      trainer_id = suppressWarnings(as.integer(.data$trainer_id))
    )

  if (identical(grouping_mode, "trainers")) {
    out_tbl <- out_tbl %>%
      dplyr::mutate(
        group = dplyr::coalesce(.data$trainer, .data$group),
        group_id = dplyr::coalesce(.data$trainer_id, .data$group_id)
      )
  }

  out_tbl
}

read_trainer_tags_sheet <- function(sheet_id) {
  if (!isTRUE(gs4_auth_ok)) return(tibble::tibble())

  target_sheet <- TRAINER_TAGS_SHEET

  if (is.na(target_sheet) || !nzchar(target_sheet)) return(tibble::tibble())

  out <- tryCatch(
    googlesheets4::read_sheet(sheet_id, sheet = target_sheet),
    error = function(e) tibble::tibble()
  )

  if (is.null(out) || !nrow(out)) return(tibble::tibble())

  tibble::as_tibble(out) %>%
    dplyr::rename_with(tolower) %>%
    dplyr::mutate(
      trainer_id = suppressWarnings(as.integer(.data$trainer_id)),
      tag_id = suppressWarnings(as.integer(.data$tag_id))
    ) %>%
    dplyr::filter(!is.na(.data$trainer_id), !is.na(.data$tag_id)) %>%
    dplyr::distinct()
}

parse_training_ids_array <- function(x) {
  if (length(x) == 0 || is.null(x)) return(integer())

  val <- x[[1]]

  if (is.null(val) || length(val) == 0) return(integer())

  if (is.list(val)) {
    val <- unlist(val, recursive = TRUE, use.names = FALSE)
  }

  if (is.numeric(val) || is.integer(val)) {
    return(unique(as.integer(val[!is.na(val)])))
  }

  txt <- trimws(as.character(val)[1])
  if (!nzchar(txt) || txt %in% c("NA", "NULL")) return(integer())

  parsed <- tryCatch(jsonlite::fromJSON(txt), error = function(e) NULL)
  if (!is.null(parsed)) {
    parsed_ids <- suppressWarnings(as.integer(unlist(parsed, recursive = TRUE, use.names = FALSE)))
    return(unique(parsed_ids[!is.na(parsed_ids)]))
  }

  matches <- regmatches(txt, gregexpr("[0-9]+", txt))[[1]]
  if (!length(matches)) return(integer())

  unique(as.integer(matches))
}

empty_triage_training_rings_df <- function() {
  tibble::tibble(
    user_id = integer(),
    date = as.Date(character()),
    training_ring_color = character(),
    training_names = character()
  )
}

read_minigames_ranking_sheet <- function(sheet_id) {
  if (!isTRUE(gs4_auth_ok)) return(tibble::tibble())

  out <- tryCatch(
    googlesheets4::read_sheet(sheet_id, sheet = MINIGAMES_RANKING_SHEET),
    error = function(e) tibble::tibble()
  )

  if (is.null(out) || !nrow(out)) return(tibble::tibble())

  tibble::as_tibble(out) %>%
    dplyr::rename_with(tolower) %>%
    dplyr::mutate(
      game_id = suppressWarnings(as.integer(.data$game_id)),
      game_name = as.character(.data$game_name),
      game_parameter = as.character(.data$game_parameter),
      game_parameter_name = as.character(.data$game_parameter_name),
      parameter_logic = tolower(trimws(as.character(.data$parameter_logic)))
    ) %>%
    dplyr::filter(
      !is.na(.data$game_id),
      nzchar(.data$game_name),
      nzchar(.data$game_parameter),
      nzchar(.data$game_parameter_name),
      .data$parameter_logic %in% c("asc", "desc")
    ) %>%
    dplyr::mutate(config_id = paste(.data$game_id, .data$game_parameter, dplyr::row_number(), sep = "__"))
}

resolve_triage_thresholds_for_group <- function(sheet_df, group_id = NA_integer_, group_name = NA_character_) {
  defaults <- triage_threshold_defaults()

  if (is.null(sheet_df) || !nrow(sheet_df)) return(defaults)

  hit <- sheet_df %>%
    dplyr::filter(!is.na(.data$group_id), .data$group_id == !!as.integer(group_id))

  if (!nrow(hit) && !is.na(group_name) && nzchar(group_name)) {
    hit <- sheet_df %>%
      dplyr::filter(tolower(.data$group) == tolower(group_name))
  }

  if (!nrow(hit)) return(defaults)

  vals <- hit %>%
    dplyr::group_by(.data$measure) %>%
    dplyr::summarise(value = dplyr::first(.data$value), .groups = "drop")

  out <- defaults
  matched <- intersect(vals$measure, names(out))
  out[matched] <- vals$value[match(matched, vals$measure)]
  out
}

save_triage_thresholds_for_group <- function(sheet_id, sheet_df, group_id, group_name, values_named, grouping_mode = "groups") {
  if (!isTRUE(gs4_auth_ok)) return(FALSE)

  target_sheet <- SCREENING_THRESHOLDS_SHEET

  if (is.na(target_sheet) || !nzchar(target_sheet)) return(FALSE)

  defaults <- triage_threshold_defaults()
  required_measures <- names(defaults)
  matched <- intersect(names(values_named), required_measures)
  if (!length(matched)) return(FALSE)

  live_df <- tryCatch(
    googlesheets4::read_sheet(sheet_id, sheet = target_sheet),
    error = function(e) tibble::tibble()
  )

  base_df <- tibble::as_tibble(live_df)
  if (!nrow(base_df)) {
    base_df <- tibble::tibble(
      measure = character(),
      value = numeric(),
      group = character(),
      group_id = integer(),
      trainer = character(),
      trainer_id = integer()
    )
  }

  if (!"measure" %in% names(base_df)) base_df$measure <- NA_character_
  if (!"value" %in% names(base_df)) base_df$value <- NA_real_
  if (!"group" %in% names(base_df)) base_df$group <- NA_character_
  if (!"group_id" %in% names(base_df)) base_df$group_id <- NA_integer_
  if (!"trainer" %in% names(base_df)) base_df$trainer <- NA_character_
  if (!"trainer_id" %in% names(base_df)) base_df$trainer_id <- NA_integer_

  base_df <- base_df %>%
    dplyr::mutate(
      measure = as.character(.data$measure),
      value = suppressWarnings(as.numeric(.data$value)),
      group = as.character(.data$group),
      group_id = suppressWarnings(as.integer(.data$group_id)),
      trainer = as.character(.data$trainer),
      trainer_id = suppressWarnings(as.integer(.data$trainer_id))
    )

  out <- base_df
  gid <- as.integer(group_id)
  gname <- as.character(group_name %||% "")
  name_col <- if (identical(grouping_mode, "trainers")) "trainer" else "group"
  id_col <- if (identical(grouping_mode, "trainers")) "trainer_id" else "group_id"

  for (ms in matched) {
    hit_idx <- which(
      !is.na(out[[id_col]]) &
        out[[id_col]] == gid &
        !is.na(out$measure) &
        out$measure == ms
    )

    if (!length(hit_idx) && nzchar(gname)) {
      hit_idx <- which(
        tolower(dplyr::coalesce(out[[name_col]], "")) == tolower(gname) &
          !is.na(out$measure) &
          out$measure == ms
      )
    }

    if (length(hit_idx)) {
      row_i <- hit_idx[[1]]
      out$value[row_i] <- as.numeric(values_named[[ms]])
      out[[name_col]][row_i] <- gname
      out[[id_col]][row_i] <- gid
    } else {
      new_row <- as.list(rep(NA, ncol(out)))
      names(new_row) <- names(out)
      new_row$measure <- ms
      new_row$value <- as.numeric(values_named[[ms]])
      new_row[[name_col]] <- gname
      new_row[[id_col]] <- gid
      out <- dplyr::bind_rows(out, tibble::as_tibble(new_row))
    }
  }

  ok <- tryCatch({
    googlesheets4::sheet_write(data = out, ss = sheet_id, sheet = target_sheet)
    TRUE
  }, error = function(e) FALSE)

  ok
}

compute_triage_default_range <- function(dates, today = Sys.Date()) {
  dates <- as.Date(dates)
  dates <- dates[!is.na(dates)]
  if (!length(dates)) {
    end_date <- as.Date(today)
    return(list(start = end_date - 10, end = end_date))
  }

  default_start <- as.Date(today) - 10
  default_end   <- as.Date(today)

  if (any(dates >= default_start & dates <= default_end)) {
    return(list(start = default_start, end = default_end))
  }

  max_date <- max(dates)
  list(start = max_date - 10, end = max_date)
}

triage_stamp_rank <- function(x) {
  dplyr::case_when(
    x == "red"    ~ 4L,
    x == "orange" ~ 3L,
    x == "yellow" ~ 2L,
    TRUE          ~ 1L
  )
}

build_triage_distribution_plot <- function(df, value_col, title_txt, xlab_txt,
                                           observed_specs = NULL, threshold_specs = NULL,
                                           subtitle_txt = NULL, bins = 30L) {
  vals <- suppressWarnings(as.numeric(df[[value_col]]))
  vals <- vals[is.finite(vals)]

  if (!length(vals)) {
    return(
      ggplot2::ggplot() +
        ggplot2::theme_minimal(base_size = 15) +
        ggplot2::labs(title = title_txt, subtitle = subtitle_txt, x = xlab_txt, y = "Número de sessões")
    )
  }

  h <- graphics::hist(vals, breaks = bins, plot = FALSE)
  y_max <- max(h$counts %||% 0, na.rm = TRUE)
  y_lab_low  <- max(1, 0.06 * y_max)
  y_lab_high <- max(1, 0.90 * y_max)

  mk_specs <- function(df_specs, kind, y_val) {
    if (is.null(df_specs) || !nrow(df_specs)) return(NULL)
    df_specs %>%
      dplyr::mutate(kind = kind, y = y_val)
  }

  lines_df <- dplyr::bind_rows(
    mk_specs(observed_specs, "observed", y_lab_high),
    mk_specs(threshold_specs, "threshold", y_lab_low)
  )

  p <- ggplot2::ggplot(df, ggplot2::aes(x = .data[[value_col]])) +
    ggplot2::geom_histogram(bins = bins, fill = "#5B5FF5", alpha = 0.85, color = "white")

  if (nrow(lines_df)) {
    p <- p +
      ggplot2::geom_vline(
        data = lines_df,
        ggplot2::aes(xintercept = value, color = color, linetype = kind),
        linewidth = 1
      )
  }

  p +
    ggplot2::scale_color_identity() +
    ggplot2::scale_linetype_manual(values = c(observed = "solid", threshold = "dashed")) +
    ggplot2::labs(
      title = title_txt,
      subtitle = subtitle_txt,
      x = xlab_txt,
      y = "Número de sessões"
    ) +
    ggplot2::theme_minimal(base_size = 15) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(face = "bold", hjust = 0.5),
      plot.subtitle = ggplot2::element_text(hjust = 0.5),
      panel.grid.minor = ggplot2::element_blank()
    )
}

format_triage_report_unit_title <- function(unit_name) {
  unit_name <- trimws(as.character(unit_name %||% ""))
  if (!nzchar(unit_name)) return("")
  if (grepl("^Usina\\s+", unit_name, ignore.case = TRUE)) unit_name else paste("Usina", unit_name)
}

build_triage_monthly_table <- function(df, date_col = "date", count_name = "triagens") {
  month_col <- paste0("M", intToUtf8(234), "s")

  if (is.null(df) || !nrow(df) || !date_col %in% names(df)) {
    return(tibble::tibble())
  }

  date_vals <- as.Date(df[[date_col]])
  out <- tibble::tibble(date = date_vals) %>%
    dplyr::filter(!is.na(.data$date)) %>%
    dplyr::mutate(
      year = lubridate::year(.data$date),
      month_num = lubridate::month(.data$date),
      month_lab = lubridate::month(.data$date, label = TRUE, abbr = TRUE)
    ) %>%
    dplyr::count(.data$year, .data$month_num, .data$month_lab, name = count_name) %>%
    tidyr::pivot_wider(
      names_from = "year",
      values_from = count_name,
      values_fill = 0
    ) %>%
    dplyr::arrange(.data$month_num)

  names(out)[names(out) == "month_lab"] <- month_col
  out <- out %>% dplyr::select(-"month_num")

  monthly_with_totals(out, month_label = month_col)
}

report_triage_quantiles <- function(df) {
  q_num <- function(x, probs) {
    stats::quantile(as.numeric(x), probs = probs, na.rm = TRUE, names = FALSE, type = 7)
  }

  list(
    correct_red = q_num(df$correct_responses_per_minute, 0.10),
    correct_yellow = q_num(df$correct_responses_per_minute, 0.25),
    incorrect_yellow = q_num(df$incorrect_responses_per_minute, 0.75),
    incorrect_red = q_num(df$incorrect_responses_per_minute, 0.90),
    rt_yellow = q_num(df$average_response_time, 0.75),
    rt_red = q_num(df$average_response_time, 0.90)
  )
}

classify_triage_sessions_observed <- function(df, qs) {
  df %>%
    dplyr::mutate(
      correct_stamp = dplyr::case_when(
        .data$correct_responses_per_minute <= qs$correct_red ~ "red",
        .data$correct_responses_per_minute < qs$correct_yellow ~ "yellow",
        TRUE ~ "white"
      ),
      incorrect_stamp = dplyr::case_when(
        .data$incorrect_responses_per_minute > qs$incorrect_red ~ "red",
        .data$incorrect_responses_per_minute > qs$incorrect_yellow ~ "yellow",
        TRUE ~ "white"
      ),
      rt_stamp = dplyr::case_when(
        .data$average_response_time > qs$rt_red ~ "red",
        .data$average_response_time > qs$rt_yellow ~ "yellow",
        TRUE ~ "white"
      ),
      yellow_count = (.data$correct_stamp == "yellow") + (.data$incorrect_stamp == "yellow") + (.data$rt_stamp == "yellow"),
      stamp_color = dplyr::case_when(
        .data$correct_stamp == "red" | .data$incorrect_stamp == "red" | .data$rt_stamp == "red" ~ "red",
        .data$yellow_count >= 2 ~ "orange",
        .data$yellow_count == 1 ~ "yellow",
        TRUE ~ "white"
      ),
      severity_rank = triage_stamp_rank(.data$stamp_color)
    )
}

build_triage_heatmap_plot <- function(df) {
  if (is.null(df) || !nrow(df)) {
    return(
      ggplot2::ggplot() +
        ggplot2::theme_minimal(base_size = 14) +
        ggplot2::labs(title = "Mapa de calor das triagens", x = "Hora", y = "Dia da Semana")
    )
  }

  weekday_labels <- c("domingo", "segunda-feira", "terca-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sabado")
  weekday_levels <- rev(weekday_labels)

  grid_df <- tidyr::expand_grid(
    weekday = factor(weekday_levels, levels = weekday_levels),
    hour_num = 0:23
  )

  heat_df <- df %>%
    dplyr::mutate(
      hour_num = suppressWarnings(as.integer(.data$hour)),
      weekday_num = lubridate::wday(.data$date, week_start = 7),
      weekday = factor(weekday_labels[.data$weekday_num], levels = weekday_levels)
    ) %>%
    dplyr::filter(!is.na(.data$hour_num), .data$hour_num >= 0, .data$hour_num <= 23, !is.na(.data$weekday)) %>%
    dplyr::count(.data$weekday, .data$hour_num, name = "n")

  grid_df %>%
    dplyr::left_join(heat_df, by = c("weekday", "hour_num")) %>%
    dplyr::mutate(n = dplyr::coalesce(.data$n, 0L)) %>%
    ggplot2::ggplot(ggplot2::aes(x = .data$hour_num, y = .data$weekday, fill = .data$n)) +
    ggplot2::geom_tile(color = NA) +
    ggplot2::scale_x_continuous(breaks = 0:23) +
    ggplot2::scale_fill_gradient(low = "#440154", high = "#FDE725") +
    ggplot2::labs(title = "Mapa de calor das triagens", x = "Hora", y = "Dia da Semana", fill = "Triagens") +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(face = "bold", hjust = 0.5),
      panel.grid = ggplot2::element_blank()
    )
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

# ---- answers ----

answers_dist_two_pops <- function(df,question_id,pop1_uids, pop1_label,pop2_uids, pop2_label,use_emoji_paths = TRUE,emojis_subdir  = "emojis") {
  
  df_q_base <- df %>%
    dplyr::filter(
      .data$question_id == !!as.numeric(question_id),
      .data$did_not_answer == 0L
    )
  
  if (!nrow(df_q_base)) {
    return(list(
      data = tibble::tibble(),
      meta = list(
        pop1_label = pop1_label,
        pop2_label = pop2_label,
        n1_resp    = 0L,
        n1_users   = 0L,
        n2_resp    = 0L,
        n2_users   = 0L
      )
    ))
  }
  
  make_block <- function(uids, label) {
    if (is.null(uids) || !length(uids)) {
      return(tibble::tibble(
        label_cat       = character(),
        question_answer = character(),
        question_emoji  = character(),
        n               = integer(),
        grupo           = character()
      ))
    }
    df_sub <- df_q_base %>%
      dplyr::filter(.data$user_id %in% !!as.integer(uids))
    
    if (!nrow(df_sub)) {
      return(tibble::tibble(
        label_cat       = character(),
        question_answer = character(),
        question_emoji  = character(),
        n               = integer(),
        grupo           = character()
      ))
    }
    
    # se tiver texto, conta por texto+emoji; se não, por emoji
    if ("question_answer" %in% names(df_sub) &&
        any(nzchar(df_sub$question_answer))) {
      df_sub %>%
        dplyr::count(question_emoji, question_answer, name = "n") %>%
        dplyr::mutate(
          label_cat = question_answer,
          grupo     = label
        )
    } else {
      df_sub %>%
        dplyr::count(question_emoji, name = "n") %>%
        dplyr::mutate(
          question_answer = "",
          label_cat       = question_emoji,
          grupo           = label
        )
    }
  }
  
  block1 <- make_block(pop1_uids, pop1_label)
  block2 <- make_block(pop2_uids, pop2_label)
  
  df_out <- dplyr::bind_rows(block1, block2)
  if (!nrow(df_out)) {
    return(list(
      data = tibble::tibble(),
      meta = list(
        pop1_label = pop1_label,
        pop2_label = pop2_label,
        n1_resp    = 0L,
        n1_users   = 0L,
        n2_resp    = 0L,
        n2_users   = 0L
      )
    ))
  }
  
  df_out <- df_out %>%
    dplyr::group_by(grupo) %>%
    dplyr::mutate(pct = n / sum(n)) %>%
    dplyr::ungroup()
  
  # caminho dos emojis na pasta www/emojis (arquivo físico)
  if (use_emoji_paths && "question_emoji" %in% names(df_out)) {
    df_out <- df_out %>%
      dplyr::mutate(
        img = dplyr::if_else(
          nzchar(question_emoji),
          file.path("www", emojis_subdir, paste0(question_emoji, ".png")),
          NA_character_
        )
      )
  } else {
    df_out$img <- NA_character_
  }
  
  # métricas para caption
  pop1_df <- df_q_base %>% dplyr::filter(.data$user_id %in% !!as.integer(pop1_uids))
  pop2_df <- df_q_base %>% dplyr::filter(.data$user_id %in% !!as.integer(pop2_uids))
  
  meta <- list(
    pop1_label = pop1_label,
    pop2_label = pop2_label,
    n1_resp    = nrow(pop1_df),
    n1_users   = dplyr::n_distinct(pop1_df$user_id),
    n2_resp    = nrow(pop2_df),
    n2_users   = dplyr::n_distinct(pop2_df$user_id)
  )
  
  list(data = df_out, meta = meta)
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
                  tabPanel(
                    "Rankings",
                    br(),
                    tabsetPanel(
                      id = "rankings_tabs",
                      tabPanel(
                        "Ranking Moove",
                        fluidRow(
                          column(12, uiOutput("ui_rankings_scope"))
                        ),
                        fluidRow(
                          column(12, uiOutput("ui_rankings_top3"))
                        ),
                        br(),
                        fluidRow(
                          column(12, DTOutput("tbl_rankings"))
                        )
                      ),
                      tabPanel(
                        "Ranking Minigames",
                        fluidRow(
                          column(12, uiOutput("ui_rankings_minigame_scope"))
                        ),
                        fluidRow(
                          column(12, uiOutput("ui_rankings_minigame_title"))
                        ),
                        fluidRow(
                          column(12, uiOutput("ui_rankings_minigame_top3"))
                        ),
                        br(),
                        fluidRow(
                          column(12, DTOutput("tbl_rankings_minigame"))
                        )
                      )
                    )
                  ),

                  # ---- triage ----- 
                  tabPanel(
                    triage_tab_label(),
                    br(),
                    tags$h4("Triagens", style = "text-align:center; font-weight:700;"),
                    fluidRow(align = "center", DTOutput("tbl_triage_monthly", width = "33%")),
                    br(),
                    fluidRow(align = "right", uiOutput("ui_triage_back")),
                    highchartOutput("hc_triage_groups", height = "650px"),
                    div(
                      style = "display:flex; justify-content:center; align-items:center; gap:12px; margin: 10px 0 4px 0;",
                      uiOutput("ui_triage_pager")
                    ),
                    br(),
                    uiOutput("ui_triage_detail")
                  ),
                  
                                  
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
                  ),
                  
                  # ---- answers ----
                  
                  tabPanel(
                    "Respostas",
                    br(),
                    fluidRow(align = "center",
                      column(6,align = "center",uiOutput("ui_resp_question")),
                      column(3,align = "center",uiOutput("ui_resp_groups")),
                      column(3,align = "center",uiOutput("ui_resp_user"))
                    ),
                    br(),
                    fluidRow(
                      column(
                        12,
                        plotOutput("plt_resp", height = "460px")
                      )
                    )
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
  authed_email    <- reactiveVal(NA_character_)
  triage_tab_visible <- reactiveVal(TRUE)
  triage_report_tab_visible <- reactiveVal(FALSE)
  
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
  
  grouping_mode <- reactive({
    mode <- input$grouping_mode %||% "groups"
    if (identical(mode, "trainers")) "trainers" else "groups"
  })

  grouping_scope_key <- reactive({
    paste0(grouping_mode(), ":", input$sel_group %||% "ALL")
  })

  trainers_from_legal_entity <- reactive({
    req(authed(), session_role() == "institution")
    inst_id <- req(selected_institution_id())
    trainer_ids <- unique(as.integer(get_legal_entity_trainers(inst_id)))

    if (!length(trainer_ids)) {
      return(tibble::tibble(id = integer(), name = character()))
    }

    nm_df <- get_names_for_users(trainer_ids)
    nk_df <- get_nickname_for_users(trainer_ids)

    tibble::tibble(id = trainer_ids) %>%
      dplyr::left_join(nm_df, by = c("id" = "user_id")) %>%
      dplyr::left_join(nk_df, by = c("id" = "user_id")) %>%
      dplyr::mutate(name = dplyr::coalesce(.data$name, .data$nickname, paste0("user_", .data$id))) %>%
      dplyr::select(id, name) %>%
      dplyr::arrange(.data$name)
  })

  grouping_entities <- reactive({
    req(authed(), session_role() == "institution")
    if (identical(grouping_mode(), "trainers")) trainers_from_legal_entity() else groups_from_api()
  })

  grouping_user_links <- reactive({
    req(authed(), session_role() == "institution")

    if (identical(grouping_mode(), "trainers")) {
      trainers <- grouping_entities()
      links <- get_legal_entity_trainers_users(trainers$id)

      if (!nrow(links) || !nrow(trainers)) {
        return(tibble::tibble(user_id = integer(), group_id = integer(), group_name = character()))
      }

      links %>%
        dplyr::transmute(user_id = as.integer(.data$user_id), group_id = as.integer(.data$trainer_id)) %>%
        dplyr::distinct() %>%
        dplyr::left_join(trainers %>% dplyr::rename(group_id = id, group_name = name), by = "group_id") %>%
        dplyr::filter(!is.na(.data$group_name), .data$group_name != "")
    } else {
      g_api <- grouping_entities()
      ug <- user_groups %>%
        dplyr::transmute(user_id = as.integer(.data$user_id), group_id = as.integer(.data$group_id)) %>%
        dplyr::distinct() %>%
        dplyr::collect()

      if (!nrow(ug) || !nrow(g_api)) {
        return(tibble::tibble(user_id = integer(), group_id = integer(), group_name = character()))
      }

      ug %>%
        dplyr::inner_join(g_api %>% dplyr::rename(group_id = id, group_name = name), by = "group_id") %>%
        dplyr::filter(!is.na(.data$group_name), .data$group_name != "")
    }
  })
    # ---- users -----
  
  scope_user_ids <- reactive({
    req(authed(), session_role() == "institution")
    inst_id <- req(selected_institution_id())
    choice  <- input$sel_group %||% "ALL"
    get_user_ids_for_institution_or_grouping(inst_id, choice, grouping_mode())
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

  institution_user_ids <- reactive({
    req(authed(), session_role() == "institution")
    inst_id <- req(selected_institution_id())
    get_user_ids_for_institution_or_group(inst_id, "ALL")
  })

  ranking_base_df <- reactive({
    req(authed(), session_role() == "institution")

    df <- user_rankings %>%
      dplyr::select(id, user_id, score, neurons, created_at, updated_at) %>%
      dplyr::collect()

    if (is.null(df) || !nrow(df)) {
      return(tibble::tibble(
        user_id = integer(),
        display_name = character(),
        score = numeric(),
        neurons = numeric(),
        global_rank = integer(),
        institution_rank = integer(),
        in_institution = logical()
      ))
    }

    df <- tibble::as_tibble(df) %>%
      dplyr::mutate(
        id = suppressWarnings(as.numeric(.data$id)),
        user_id = as.integer(.data$user_id),
        score = as.numeric(.data$score),
        neurons = as.numeric(.data$neurons),
        ref_ts = dplyr::coalesce(.data$updated_at, .data$created_at)
      ) %>%
      dplyr::filter(!is.na(.data$user_id), !is.na(.data$score)) %>%
      dplyr::arrange(.data$user_id, dplyr::desc(.data$ref_ts), dplyr::desc(.data$id)) %>%
      dplyr::group_by(.data$user_id) %>%
      dplyr::slice_head(n = 1) %>%
      dplyr::ungroup()

    nm_df <- get_names_for_users(unique(as.integer(df$user_id)))
    nk_df <- get_nickname_for_users(unique(as.integer(df$user_id)))
    inst_uids <- unique(as.integer(institution_user_ids()))

    out <- df %>%
      dplyr::left_join(nm_df, by = "user_id") %>%
      dplyr::left_join(nk_df, by = "user_id") %>%
      dplyr::mutate(
        display_name = dplyr::coalesce(.data$name, .data$nickname, paste0("user_", .data$user_id)),
        neurons = dplyr::coalesce(.data$neurons, 0)
      ) %>%
      dplyr::arrange(dplyr::desc(.data$score), dplyr::desc(.data$neurons), .data$user_id) %>%
      dplyr::mutate(
        global_rank = dplyr::row_number(),
        in_institution = .data$user_id %in% inst_uids
      )

    inst_rank_df <- out %>%
      dplyr::filter(.data$in_institution) %>%
      dplyr::transmute(user_id = .data$user_id, institution_rank = dplyr::row_number())

    out %>%
      dplyr::left_join(inst_rank_df, by = "user_id") %>%
      dplyr::select(user_id, display_name, score, neurons, global_rank, institution_rank, in_institution)
  })

    ranking_scope_df <- reactive({
    req(authed(), session_role() == "institution")
    mode <- input$ranking_scope %||% "global"
    df <- ranking_base_df()

    if (!nrow(df)) return(df)

    if (identical(mode, "institution")) {
      df %>%
        dplyr::filter(.data$in_institution, !is.na(.data$institution_rank)) %>%
        dplyr::mutate(rank_display = .data$institution_rank) %>%
        dplyr::arrange(.data$rank_display) %>%
        dplyr::slice_head(n = 20)
    } else {
      df %>%
        dplyr::mutate(rank_display = .data$global_rank) %>%
        dplyr::arrange(.data$rank_display) %>%
        dplyr::slice_head(n = 20)
    }
  })

  ranking_minigame_sheet_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Rankings")
    read_minigames_ranking_sheet(TRIAGE_SHEET_ID)
  })

  ranking_minigame_selected_config <- reactive({
    df <- ranking_minigame_sheet_df()
    req(nrow(df) > 0)

    cfg_id <- input$ranking_minigame_config
    if (is.null(cfg_id) || !nzchar(as.character(cfg_id)) || !cfg_id %in% df$config_id) {
      return(df[0, , drop = FALSE])
    }

    df[df$config_id == cfg_id, , drop = FALSE]
  })

  ranking_minigame_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Rankings")
    cfg <- ranking_minigame_selected_config()
    req(nrow(cfg) == 1)

    df <- get_moove_scores_game_parameter_data_cached(
      sel_users = institution_user_ids(),
      inst_id = selected_institution_id(),
      group_choice = "ALL",
      game_id = cfg$game_id[[1]],
      game_parameter = cfg$game_parameter[[1]],
      parameter_logic = cfg$parameter_logic[[1]],
      limit_n = 100L
    )

    if (is.null(df) || !nrow(df)) return(tibble::tibble())

    logic_desc <- identical(cfg$parameter_logic[[1]], "desc")

    df <- df %>%
      dplyr::mutate(
        played_at = suppressWarnings(lubridate::ymd_hms(.data$date_time, tz = "UTC", quiet = TRUE)),
        played_at = dplyr::coalesce(.data$played_at, suppressWarnings(as.POSIXct(.data$date_time, tz = "UTC")))
      )

    if (isTRUE(logic_desc)) {
      df <- df %>% dplyr::arrange(.data$user_id, dplyr::desc(.data$metric_value), dplyr::desc(.data$played_at))
    } else {
      df <- df %>% dplyr::arrange(.data$user_id, .data$metric_value, dplyr::desc(.data$played_at))
    }

    df <- df %>%
      dplyr::group_by(.data$user_id) %>%
      dplyr::slice_head(n = 1) %>%
      dplyr::ungroup()

    nm_df <- get_names_for_users(unique(as.integer(df$user_id)))
    nk_df <- get_nickname_for_users(unique(as.integer(df$user_id)))

    df <- df %>%
      dplyr::left_join(nm_df, by = "user_id") %>%
      dplyr::left_join(nk_df, by = "user_id") %>%
      dplyr::mutate(display_name = dplyr::coalesce(.data$name, .data$nickname, paste0("user_", .data$user_id)))

    if (isTRUE(logic_desc)) {
      df <- df %>% dplyr::arrange(dplyr::desc(.data$metric_value), dplyr::desc(.data$played_at), .data$user_id)
    } else {
      df <- df %>% dplyr::arrange(.data$metric_value, dplyr::desc(.data$played_at), .data$user_id)
    }

    df %>%
      dplyr::mutate(rank_display = dplyr::row_number())
  })
  
  # ---- cache -----
  
  EVAL_CACHE_PREFIX <- "EVAL"
  
  eval_cache_key <- function(suffix = NULL, inst_id = NULL, gid = NULL, uid = NULL, metric = NULL, extra = NULL) {
    cache_key(
      prefix   = paste0(EVAL_CACHE_PREFIX, "|", suffix %||% ""),
      inst_id  = inst_id,
      group_id = gid,
      user_id  = uid,
      extra    = paste0(metric %||% "", if (!is.null(extra)) paste0("|", extra) else "")
    )
  }
  
  ANS_CACHE_PREFIX <- "ANS"
  
  # ---- evals -----
  
  evals_joined <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    inst_id <- req(selected_institution_id())
    
    # tenta o cache
    ck <- eval_cache_key("joined", inst_id = inst_id)
    cached <- cache_get(ck)
    if (!is.null(cached)) return(cached)
    
    # ---------- (código original a partir daqui, sem mudanças de lógica) ----------
    rdcs_tbl   <- tbl(pool, "rdcs")
    rdc_vs_tbl <- tbl(pool, "rdc_vs")
    
    uids <- scope_user_ids()
    if (length(uids) == 0) {
      out <- tibble::tibble()
      cache_set(ck, out); return(out)
    }
    
    rdcs_df <- rdcs_tbl %>%
      filter(.data$user_id %in% !!as.integer(uids)) %>%
      select(id, external_id, order_id, score_id, evaluation_date, user_id, session,
             nrss, rt_avg, dt_avg, reaction_quality, decision_quality, attention,
             impulsivity_control, stamp, analysis_version, age, weight, height, sex,
             institution_id, created_at, available) %>%
      collect()
    if (!nrow(rdcs_df)) { out <- tibble::tibble(); cache_set(ck, out); return(out) }
    
    vs_df <- rdc_vs_tbl %>%
      filter(.data$rdc_id %in% !!as.integer(rdcs_df$id)) %>%
      select(rdc_id, peripheral_vision) %>%
      collect()
    
    rdcs_vs <- rdcs_df %>%
      left_join(vs_df, by = c("id" = "rdc_id")) %>%
      mutate(peripheral_vision = dplyr::coalesce(peripheral_vision, -1))
    
    nm_df  <- get_names_for_users(unique(rdcs_vs$user_id))
    dob_df <- get_dobs_for_users(unique(rdcs_vs$user_id))
    
    rdcs_enriched <- rdcs_vs %>%
      left_join(nm_df,  by = "user_id") %>%
      left_join(dob_df, by = "user_id") %>%
      mutate(
        name = dplyr::coalesce(name, paste0("user_", user_id)),
        age_on_eval = compute_age_on_date(dob, as.Date(evaluation_date))
      )
    
    ug_named <- grouping_user_links() %>%
      dplyr::filter(.data$user_id %in% !!as.integer(unique(rdcs_enriched$user_id))) %>%
      dplyr::group_by(.data$user_id) %>%
      dplyr::summarise(groups = paste(sort(unique(.data$group_name[!is.na(.data$group_name)])), collapse = ", "),
                       .groups = "drop")

    if (nrow(ug_named)) {
      rdcs_enriched <- rdcs_enriched %>% left_join(ug_named, by = "user_id")
    } else {
      rdcs_enriched$groups <- NA_character_
    }
    
    out <- rdcs_enriched %>% arrange(desc(evaluation_date), user_id)
    cache_set(ck, out)
    out
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
    inst_id <- req(selected_institution_id())
    
    ck <- eval_cache_key("group_stats", inst_id = inst_id, metric = key)
    if (!is.null(cache_get(ck))) return(cache_get(ck))
    
    spec <- eval_metric_spec(key)
    d <- evals_joined()
    if (!nrow(d)) { out <- tibble::tibble(group_id = integer(), group_name = character(), value = numeric()); cache_set(ck, out); return(out) }
    if (key == "peripheral_vision") d <- d %>% dplyr::mutate(peripheral_vision = ifelse(peripheral_vision < 0, NA_real_, peripheral_vision))
    
    d_user <- d %>%
      dplyr::group_by(user_id) %>%
      dplyr::summarise(value = mean(.data[[key]], na.rm = TRUE), .groups = "drop")
    
    ug_named <- grouping_user_links() %>%
      dplyr::filter(.data$user_id %in% !!as.integer(d_user$user_id)) %>%
      dplyr::distinct(.data$user_id, .data$group_id, .keep_all = TRUE)

    if (!nrow(ug_named)) { out <- tibble::tibble(group_id = integer(), group_name = character(), value = numeric()); cache_set(ck, out); return(out) }
    
    out <- d_user %>%
      dplyr::left_join(ug_named, by = "user_id", relationship = "many-to-many") %>%
      dplyr::group_by(group_id, group_name) %>%
      dplyr::summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
      dplyr::filter(group_name != "" & !is.na(group_name))
    
    cache_set(ck, out)
    out
  })
  
  eval_user_stats <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    gid  <- eval_selected_group(); req(!is.na(gid))
    key  <- eval_metric_key()
    inst_id <- req(selected_institution_id())
    
    ck <- eval_cache_key("user_stats", inst_id = inst_id, gid = gid, metric = key)
    if (!is.null(cache_get(ck))) return(cache_get(ck))
    
    spec <- eval_metric_spec(key)
    d <- evals_joined()
    if (!nrow(d)) { out <- tibble::tibble(user_id = integer(), name = character(), value = numeric()); cache_set(ck, out); return(out) }
    if (key == "peripheral_vision") d <- d %>% dplyr::mutate(peripheral_vision = ifelse(peripheral_vision < 0, NA_real_, peripheral_vision))
    
    ug_users <- grouping_user_links() %>%
      dplyr::filter(.data$group_id == !!as.integer(gid)) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id)) %>%
      dplyr::distinct()
    if (!nrow(ug_users)) { out <- tibble::tibble(user_id = integer(), name = character(), value = numeric()); cache_set(ck, out); return(out) }
    
    out <- d %>%
      dplyr::filter(.data$user_id %in% ug_users$user_id) %>%
      dplyr::group_by(user_id, name) %>%
      dplyr::summarise(value = mean(.data[[key]], na.rm = TRUE), .groups = "drop")
    
    cache_set(ck, out)
    out
  })
  
  eval_selected_user  <- reactiveVal(NA_integer_)
  
  eval_selected_uname <- reactiveVal(NA_character_)
  
  eval_user_ts <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    uid  <- req(eval_selected_user())
    key  <- eval_metric_key()
    inst_id <- req(selected_institution_id())
    
    ck <- eval_cache_key("user_ts", inst_id = inst_id, uid = uid, metric = key)
    cached <- cache_get(ck)
    if (!is.null(cached)) return(cached)
    
    spec <- eval_metric_spec(key)
    d <- evals_joined()
    if (!nrow(d)) {
      out <- tibble::tibble(
        evaluation_date = as.Date(character()),
        score_id        = character(),
        value           = numeric(),
        reference_mean  = numeric(),
        reference_sd    = numeric()
      )
      cache_set(ck, out); return(out)
    }
    
    if (key == "peripheral_vision") {
      d <- d %>% mutate(peripheral_vision = ifelse(peripheral_vision < 0, NA_real_, peripheral_vision))
    }
    
    d_user <- d %>%
      filter(.data$user_id == !!uid) %>%
      transmute(
        evaluation_date = as.Date(evaluation_date),
        score_id        = as.character(score_id),
        value           = as.numeric(.data[[key]])
      ) %>%
      arrange(evaluation_date)
    
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
      d_user <- d_user %>% mutate(reference_mean = NA_real_, reference_sd = NA_real_)
    }
    
    d_user$value <- pmin(pmax(d_user$value, spec$bounds[1]), spec$bounds[2])
    
    cache_set(ck, d_user)
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
    ug_named <- grouping_user_links() %>%
      dplyr::filter(.data$user_id %in% !!unique(as.integer(d$user_id))) %>%
      dplyr::group_by(.data$user_id) %>%
      dplyr::summarise(
        groups = paste(sort(unique(.data$group_name[!is.na(.data$group_name) & .data$group_name != ""])),
                       collapse = ", "),
        .groups = "drop"
      )
    
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
    uids     <- scope_user_ids()
    inst_id  <- selected_institution_id()
    choice   <- grouping_scope_key()
    get_moove_scores_data_cached(uids, inst_id, choice)
  })
  
  mg_view_mode <- reactiveVal("groups")
  
  mg_selected_group <- reactiveVal(NA_integer_)
  
  mg_group_stats <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Minigames")
    d <- minigames_df()
    req(nrow(d) > 0)
    
        ug_named <- grouping_user_links() %>%
      dplyr::filter(.data$user_id %in% !!unique(as.integer(d$user_id))) %>%
      dplyr::distinct(.data$user_id, .data$group_id, .keep_all = TRUE)

    if (!nrow(ug_named)) return(tibble(group_id = integer(), group_name = character(), n = integer()))
    
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
    
    ug_users <- grouping_user_links() %>%
      dplyr::filter(.data$group_id == !!as.integer(gid)) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id)) %>%
      dplyr::distinct()
    
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
    uids     <- scope_user_ids()
    inst_id  <- selected_institution_id()
    choice   <- grouping_scope_key()
    dfw <- get_user_settings_avg_percentiles_cached(uids, inst_id, choice)
    to_long_percentiles(dfw)
  })
  
  perf_view_mode   <- reactiveVal("groups")
  
  perf_selected_group <- reactiveVal(NA_integer_)
  
  perf_group_stats <- reactive({
    req(percentiles_long(), authed(), session_role() == "institution")
    cap <- input$sel_capacity; req(cap)
    d <- percentiles_long() %>% filter(capacity == cap)
    
        ug <- grouping_user_links() %>%
      dplyr::distinct(.data$user_id, .data$group_id, .keep_all = TRUE)

    d %>%
      inner_join(ug, by = "user_id") %>%
      group_by(group_id, group_name) %>%
      summarise(avg = mean(value, na.rm = TRUE), .groups = "drop") %>%
      filter(!is.na(group_name))
  })
  
  perf_user_stats <- reactive({
    req(percentiles_long(), authed(), session_role() == "institution")
    gid <- perf_selected_group(); req(!is.na(gid))
    cap <- input$sel_capacity; req(cap)
    
    d <- percentiles_long() %>% filter(capacity == cap)
    ug <- grouping_user_links() %>%
      dplyr::filter(.data$group_id == !!gid) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id)) %>%
      dplyr::distinct()
    nm_df <- get_names_for_users(ug$user_id)
    
    d %>%
      filter(user_id %in% ug$user_id) %>%
      group_by(user_id) %>%
      summarise(avg = mean(value, na.rm = TRUE), .groups = "drop") %>%
      left_join(nm_df, by = "user_id") %>%
      mutate(name = coalesce(name, paste0("user_", user_id)))
  })
  
  perf_page <- reactiveVal(1)

  # ---- triage and activation -----

  triage_raw_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    triage_refresh_tick()
    uids    <- scope_user_ids()
    inst_id <- selected_institution_id()
    choice  <- input$sel_group %||% "ALL"

    get_moove_scores_raw_data_cached(uids, inst_id, choice) %>%
      dplyr::mutate(
        played_at = suppressWarnings(lubridate::ymd_hms(.data$date_time, tz = "UTC", quiet = TRUE)),
        played_at = dplyr::coalesce(.data$played_at, suppressWarnings(as.POSIXct(.data$date_time, tz = "UTC"))),
        date      = dplyr::coalesce(as.Date(.data$played_at), as.Date(substr(as.character(.data$date_time), 1, 10))),
        hour      = substr(as.character(.data$date_time), 12, 13)
      )
  })

  triage_bucket_mode <- reactive({
    mode <- input$triage_bucket_mode %||% "grouping"
    if (identical(mode, "units")) "units" else "grouping"
  })

  triage_view_mode <- reactiveVal("groups")

  triage_selected_group <- reactiveVal(NA_integer_)
  triage_selected_unit <- reactiveVal(NA_character_)
  triage_selected_click_date <- reactiveVal(as.Date(NA))
  triage_refresh_tick <- reactiveVal(0L)
  triage_manager_mode <- reactive({
    isTRUE(is_manager == 1) || identical(trimws(as.character(input$triage_manager_code %||% "")), "Senso298")
  })

  triage_page <- reactiveVal(1L)

  triage_units_sheet_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    triage_refresh_tick()
    read_triage_threshold_sheet(TRIAGE_SHEET_ID, grouping_mode = "trainers")
  })

  triage_unit_trainers <- reactive({
    triage_units_sheet_df() %>%
      dplyr::filter(!is.na(.data$unit), nzchar(trimws(.data$unit))) %>%
      dplyr::filter(!is.na(.data$trainer_id), !is.na(.data$trainer), nzchar(trimws(.data$trainer))) %>%
      dplyr::transmute(
        trainer_id = as.integer(.data$trainer_id),
        trainer_name = as.character(.data$trainer),
        unit = as.character(.data$unit)
      ) %>%
      dplyr::distinct(.data$trainer_id, .keep_all = TRUE) %>%
      dplyr::arrange(.data$unit, .data$trainer_name)
  })

  triage_unit_user_links <- reactive({
    trainers <- triage_unit_trainers()

    if (!nrow(trainers)) {
      return(tibble::tibble(user_id = integer(), group_name = character(), unit = character()))
    }

    get_legal_entity_trainers_users(trainers$trainer_id) %>%
      dplyr::inner_join(trainers, by = "trainer_id") %>%
      dplyr::transmute(
        user_id = as.integer(.data$user_id),
        group_name = as.character(.data$unit),
        unit = as.character(.data$unit)
      ) %>%
      dplyr::filter(!is.na(.data$group_name), .data$group_name != "") %>%
      dplyr::distinct()
  })

  triage_sheet_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    triage_refresh_tick()
    read_triage_threshold_sheet(TRIAGE_SHEET_ID, grouping_mode = grouping_mode())
  })

  triage_sheet_group_thresholds <- reactive({
    if (identical(triage_bucket_mode(), "units")) {
      return(triage_threshold_defaults())
    }

    gid <- triage_selected_group()
    if (is.na(gid)) return(triage_threshold_defaults())

    gdf <- grouping_entities()
    gname <- gdf$name[match(as.integer(gid), gdf$id)]
    resolve_triage_thresholds_for_group(
      sheet_df = triage_sheet_df(),
      group_id = gid,
      group_name = gname %||% NA_character_
    )
  })

  triage_group_thresholds <- reactive({
    sheet_vals <- triage_sheet_group_thresholds()

    if (!isTRUE(triage_manager_mode()) || identical(triage_bucket_mode(), "units")) {
      return(sheet_vals)
    }

    c(
      triage_correct_yellow   = as.numeric(input$triage_correct_yellow %||% sheet_vals[["triage_correct_yellow"]]),
      triage_correct_red      = as.numeric(input$triage_correct_red %||% sheet_vals[["triage_correct_red"]]),
      triage_incorrect_yellow = as.numeric(input$triage_incorrect_yellow %||% sheet_vals[["triage_incorrect_yellow"]]),
      triage_incorrect_red    = as.numeric(input$triage_incorrect_red %||% sheet_vals[["triage_incorrect_red"]]),
      triage_rt_yellow        = as.numeric(input$triage_rt_yellow %||% sheet_vals[["triage_rt_yellow"]]),
      triage_rt_red           = as.numeric(input$triage_rt_red %||% sheet_vals[["triage_rt_red"]])
    )
  })

  triage_correct_yellow <- reactive({
    as.numeric(input$triage_correct_yellow %||% triage_threshold_defaults()[["triage_correct_yellow"]])
  })

  triage_correct_red <- reactive({
    as.numeric(input$triage_correct_red %||% triage_threshold_defaults()[["triage_correct_red"]])
  })

  triage_incorrect_yellow <- reactive({
    as.numeric(input$triage_incorrect_yellow %||% triage_threshold_defaults()[["triage_incorrect_yellow"]])
  })

  triage_incorrect_red <- reactive({
    as.numeric(input$triage_incorrect_red %||% triage_threshold_defaults()[["triage_incorrect_red"]])
  })

  triage_rt_yellow <- reactive({
    as.numeric(input$triage_rt_yellow %||% triage_threshold_defaults()[["triage_rt_yellow"]])
  })

  triage_rt_red <- reactive({
    as.numeric(input$triage_rt_red %||% triage_threshold_defaults()[["triage_rt_red"]])
  })

  triage_group_stats <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    d <- triage_raw_df()
    req(nrow(d) > 0)

    ug_named <- if (identical(triage_bucket_mode(), "units")) {
      triage_unit_user_links() %>%
        dplyr::filter(.data$user_id %in% !!unique(as.integer(d$user_id))) %>%
        dplyr::transmute(
          user_id = as.integer(.data$user_id),
          group_id = as.integer(as.factor(.data$group_name)),
          group_name = as.character(.data$group_name)
        ) %>%
        dplyr::distinct(.data$user_id, .data$group_name, .keep_all = TRUE)
    } else {
      grouping_user_links() %>%
        dplyr::filter(.data$user_id %in% !!unique(as.integer(d$user_id))) %>%
        dplyr::distinct(.data$user_id, .data$group_id, .keep_all = TRUE)
    }

    if (!nrow(ug_named)) {
      return(tibble::tibble(group_id = integer(), group_name = character(), n = integer()))
    }

    d %>%
      dplyr::left_join(ug_named, by = "user_id") %>%
      dplyr::group_by(group_id, group_name) %>%
      dplyr::summarise(n = dplyr::n(), .groups = "drop") %>%
      dplyr::filter(!is.na(group_name) & group_name != "")
  })

  triage_selected_group_users <- reactive({
    if (identical(triage_bucket_mode(), "units")) {
      unit_name <- triage_selected_unit()
      req(!is.null(unit_name), !is.na(unit_name), nzchar(unit_name))

      return(
        triage_unit_user_links() %>%
          dplyr::filter(.data$unit == unit_name) %>%
          dplyr::transmute(user_id = as.integer(.data$user_id)) %>%
          dplyr::distinct()
      )
    }

    gid <- triage_selected_group()
    req(!is.na(gid))

    grouping_user_links() %>%
      dplyr::filter(.data$group_id == !!as.integer(gid)) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id)) %>%
      dplyr::distinct()
  })

  triage_selected_group_name <- reactive({
    if (identical(triage_bucket_mode(), "units")) {
      unit_name <- triage_selected_unit()
      req(!is.null(unit_name), !is.na(unit_name), nzchar(unit_name))
      return(as.character(unit_name))
    }

    gid <- triage_selected_group()
    req(!is.na(gid))

    gdf <- grouping_entities()
    as.character(gdf$name[match(as.integer(gid), gdf$id)] %||% "")
  })

  triage_selected_group_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    if (identical(triage_bucket_mode(), "units")) {
      unit_name <- triage_selected_unit()
      req(!is.null(unit_name), !is.na(unit_name), nzchar(unit_name))
    } else {
      gid <- triage_selected_group()
      req(!is.na(gid))
    }

    d <- triage_raw_df()
    ug_users <- triage_selected_group_users()
    req(nrow(ug_users) > 0)

    nm_df <- get_nickname_for_users(ug_users$user_id)

    d %>%
      dplyr::filter(.data$user_id %in% ug_users$user_id) %>%
      dplyr::left_join(nm_df, by = "user_id") %>%
      dplyr::mutate(name = dplyr::coalesce(.data$nickname, paste0("user_", .data$user_id)))
  })

  triage_quantiles <- reactive({
    req(triage_selected_group_df())
    d <- triage_selected_group_df()
    req(nrow(d) > 0)

    q_num <- function(x, probs) {
      stats::quantile(as.numeric(x), probs = probs, na.rm = TRUE, names = FALSE, type = 7)
    }

    list(
      correct   = q_num(d$correct_responses_per_minute, c(0.10, 0.25)),
      incorrect = q_num(d$incorrect_responses_per_minute, c(0.75, 0.90)),
      rt        = q_num(d$average_response_time, c(0.75, 0.90))
    )
  })

  triage_processed_df <- reactive({
    req(triage_selected_group_df())
    d <- triage_selected_group_df()
    req(nrow(d) > 0)

    thr <- triage_group_thresholds()
    correct_yellow   <- as.numeric(thr[["triage_correct_yellow"]])
    correct_red      <- as.numeric(thr[["triage_correct_red"]])
    incorrect_yellow <- as.numeric(thr[["triage_incorrect_yellow"]])
    incorrect_red    <- as.numeric(thr[["triage_incorrect_red"]])
    rt_yellow        <- as.numeric(thr[["triage_rt_yellow"]])
    rt_red           <- as.numeric(thr[["triage_rt_red"]])

    d %>%
      dplyr::mutate(
        correct_stamp = dplyr::case_when(
          .data$correct_responses_per_minute <= correct_red    ~ "red",
          .data$correct_responses_per_minute <  correct_yellow ~ "yellow",
          TRUE                                                 ~ "white"
        ),
        incorrect_stamp = dplyr::case_when(
          .data$incorrect_responses_per_minute > incorrect_red    ~ "red",
          .data$incorrect_responses_per_minute > incorrect_yellow ~ "yellow",
          TRUE                                                    ~ "white"
        ),
        rt_stamp = dplyr::case_when(
          .data$average_response_time > rt_red    ~ "red",
          .data$average_response_time > rt_yellow ~ "yellow",
          TRUE                                    ~ "white"
        ),
        yellow_count = (.data$correct_stamp == "yellow") +
          (.data$incorrect_stamp == "yellow") +
          (.data$rt_stamp == "yellow"),
        stamp_color = dplyr::case_when(
          .data$correct_stamp == "red" | .data$incorrect_stamp == "red" | .data$rt_stamp == "red" ~ "red",
          .data$yellow_count >= 2 ~ "orange",
          .data$yellow_count == 1 ~ "yellow",
          TRUE                    ~ "white"
        ),
        severity_rank = triage_stamp_rank(.data$stamp_color)
      )
  })

  triage_date_bounds <- reactive({
    d <- triage_selected_group_df()
    req(nrow(d) > 0)
    compute_triage_default_range(d$date)
  })

  triage_panel_df <- reactive({
    req(triage_processed_df())
    d <- triage_processed_df()
    req(nrow(d) > 0)
    req(input$triage_date_start, input$triage_date_end)

    start_date <- as.Date(input$triage_date_start)
    end_date   <- as.Date(input$triage_date_end)

    d <- d %>%
      dplyr::filter(.data$date >= start_date, .data$date <= end_date)

    filter_mode <- input$triage_user_filter %||% "period"
    if (!identical(filter_mode, "period")) {
      lookback_hours <- dplyr::case_when(
        filter_mode == "24h" ~ 24,
        filter_mode == "12h" ~ 12,
        filter_mode == "3h"  ~ 3,
        TRUE ~ NA_real_
      )

      if (is.finite(lookback_hours)) {
        cutoff_ts <- Sys.time() - lubridate::hours(lookback_hours)
        eligible_users <- d %>%
          dplyr::filter(!is.na(.data$played_at), .data$played_at >= cutoff_ts) %>%
          dplyr::distinct(.data$user_id) %>%
          dplyr::pull(.data$user_id)

        d <- d %>% dplyr::filter(.data$user_id %in% eligible_users)
      }
    }

    if (!nrow(d)) {
      return(tibble::tibble(
        user_id = integer(),
        name = character(),
        date = as.Date(character()),
        stamp_color = character(),
        hour = character(),
        severity_rank = integer(),
        label_color = character()
      ))
    }

    d %>%
      dplyr::arrange(.data$user_id, .data$date, dplyr::desc(.data$severity_rank), dplyr::desc(.data$played_at)) %>%
      dplyr::group_by(.data$user_id, .data$name, .data$date) %>%
      dplyr::summarise(
        stamp_color = dplyr::first(.data$stamp_color),
        hour = dplyr::first(.data$hour),
        severity_rank = dplyr::first(.data$severity_rank),
        .groups = "drop"
      ) %>%
      dplyr::mutate(
        label_color = dplyr::if_else(.data$stamp_color == "red", "white", "black")
      )
  })

  trainer_tags_sheet_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    triage_refresh_tick()
    read_trainer_tags_sheet(TRIAGE_SHEET_ID)
  })

  triage_training_rings_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    req(input$triage_date_start, input$triage_date_end)

    if (!identical(grouping_mode(), "trainers")) {
      return(empty_triage_training_rings_df())
    }

    trainer_id <- as.integer(triage_selected_group())
    if (is.na(trainer_id)) return(empty_triage_training_rings_df())

    trainer_tags <- trainer_tags_sheet_df()
    tag_ids <- trainer_tags %>%
      dplyr::filter(.data$trainer_id == trainer_id) %>%
      dplyr::pull(.data$tag_id)

    tag_ids <- unique(as.integer(tag_ids[!is.na(tag_ids)]))
    if (!length(tag_ids)) return(empty_triage_training_rings_df())
    tag_id <- tag_ids[[1]]

    tag_template <- tag_templates_tbl %>%
      dplyr::filter(.data$tag_id == !!tag_id | .data$id == !!tag_id) %>%
      dplyr::select(id, tag_id, training_ids_array) %>%
      collect()

    if (!nrow(tag_template)) return(empty_triage_training_rings_df())

    training_ids <- parse_training_ids_array(tag_template$training_ids_array[[1]])
    if (!length(training_ids)) return(empty_triage_training_rings_df())

    training_lookup <- trainings_tbl %>%
      dplyr::filter(.data$id %in% !!as.integer(training_ids)) %>%
      dplyr::transmute(
        training_id = as.integer(.data$id),
        training_name = as.character(.data$name)
      ) %>%
      collect() %>%
      dplyr::right_join(
        tibble::tibble(training_id = as.integer(training_ids)),
        by = "training_id"
      ) %>%
      dplyr::mutate(
        training_order = match(.data$training_id, as.integer(training_ids)),
        training_name = dplyr::coalesce(.data$training_name, paste0("training_", .data$training_id)),
        training_ring_color = dplyr::case_when(
          .data$training_order == 1L ~ "#f2c94c",
          .data$training_order == 2L ~ "#f2994a",
          .data$training_order >= 3L ~ "#eb5757",
          TRUE ~ "#f2c94c"
        )
      )

    user_ids <- unique(as.integer(triage_selected_group_users()$user_id))
    if (!length(user_ids)) return(empty_triage_training_rings_df())

    completions <- training_tag_completions_tbl %>%
      dplyr::filter(
        .data$user_id %in% !!user_ids,
        .data$training_id %in% !!as.integer(training_ids)
      ) %>%
      dplyr::transmute(
        user_id = as.integer(.data$user_id),
        tag_id = as.integer(.data$tag_id),
        training_id = as.integer(.data$training_id),
        completed_at = dplyr::coalesce(.data$created_at, .data$updated_at)
      ) %>%
      collect()

    if (!nrow(completions)) return(empty_triage_training_rings_df())

    if ("tag_id" %in% names(completions) && any(completions$tag_id == tag_id, na.rm = TRUE)) {
      completions <- completions %>%
        dplyr::filter(.data$tag_id == tag_id)
    }

    start_date <- as.Date(input$triage_date_start)
    end_date   <- as.Date(input$triage_date_end)

    completions %>%
      dplyr::mutate(date = as.Date(.data$completed_at)) %>%
      dplyr::filter(!is.na(.data$date), .data$date >= start_date, .data$date <= end_date) %>%
      dplyr::left_join(training_lookup, by = "training_id") %>%
      dplyr::arrange(.data$user_id, .data$date, dplyr::desc(.data$training_order), .data$training_id) %>%
      dplyr::group_by(.data$user_id, .data$date) %>%
      dplyr::summarise(
        training_ring_color = dplyr::first(.data$training_ring_color),
        training_names = paste(unique(.data$training_name), collapse = ", "),
        .groups = "drop"
      )
  })

  triage_training_debug <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    req(input$triage_date_start, input$triage_date_end)

    empty_preview <- tibble::tibble(
      user_id = integer(),
      name = character(),
      date = as.Date(character()),
      hour = character(),
      stamp_color = character(),
      training_ring_color = character(),
      training_names = character()
    )

    if (!identical(grouping_mode(), "trainers")) {
      return(list(lines = c("Agrupamento atual: grupos", "Debug de treino/tag desativado fora do modo treinadores."), preview = empty_preview))
    }

    trainer_id <- as.integer(triage_selected_group())
    if (is.na(trainer_id)) {
      return(list(lines = c("Treinador selecionado: <none>"), preview = empty_preview))
    }

    trainer_tags <- trainer_tags_sheet_df()
    trainer_tag_rows <- trainer_tags %>%
      dplyr::filter(.data$trainer_id == trainer_id)

    tag_ids <- unique(as.integer(trainer_tag_rows$tag_id[!is.na(trainer_tag_rows$tag_id)]))
    tag_id <- if (length(tag_ids)) tag_ids[[1]] else NA_integer_

    tag_template <- if (!is.na(tag_id)) {
      tag_templates_tbl %>%
        dplyr::filter(.data$tag_id == !!tag_id | .data$id == !!tag_id) %>%
        dplyr::select(id, tag_id, training_ids_array) %>%
        collect()
    } else {
      tibble::tibble()
    }

    training_ids <- if (nrow(tag_template)) parse_training_ids_array(tag_template$training_ids_array[[1]]) else integer()

    training_lookup <- if (length(training_ids)) {
      trainings_tbl %>%
        dplyr::filter(.data$id %in% !!as.integer(training_ids)) %>%
        dplyr::transmute(
          training_id = as.integer(.data$id),
          training_name = as.character(.data$name)
        ) %>%
        collect() %>%
        dplyr::right_join(
          tibble::tibble(training_id = as.integer(training_ids)),
          by = "training_id"
        ) %>%
        dplyr::mutate(
          training_order = match(.data$training_id, as.integer(training_ids)),
          training_name = dplyr::coalesce(.data$training_name, paste0("training_", .data$training_id)),
          training_ring_color = dplyr::case_when(
            .data$training_order == 1L ~ "#f2c94c",
            .data$training_order == 2L ~ "#f2994a",
            .data$training_order >= 3L ~ "#eb5757",
            TRUE ~ "#f2c94c"
          )
        )
    } else {
      tibble::tibble(training_id = integer(), training_name = character(), training_order = integer(), training_ring_color = character())
    }

    user_ids <- unique(as.integer(triage_selected_group_users()$user_id))

    completions_raw <- if (length(user_ids) && length(training_ids)) {
      training_tag_completions_tbl %>%
        dplyr::filter(
          .data$user_id %in% !!user_ids,
          .data$training_id %in% !!as.integer(training_ids)
        ) %>%
        dplyr::transmute(
          user_id = as.integer(.data$user_id),
          tag_id = as.integer(.data$tag_id),
          training_id = as.integer(.data$training_id),
          completed_at = dplyr::coalesce(.data$created_at, .data$updated_at)
        ) %>%
        collect()
    } else {
      tibble::tibble(user_id = integer(), tag_id = integer(), training_id = integer(), completed_at = as.POSIXct(character()))
    }

    completions_tag <- completions_raw
    if (!is.na(tag_id) && "tag_id" %in% names(completions_tag) && any(completions_tag$tag_id == tag_id, na.rm = TRUE)) {
      completions_tag <- completions_tag %>%
        dplyr::filter(.data$tag_id == tag_id)
    }

    start_date <- as.Date(input$triage_date_start)
    end_date   <- as.Date(input$triage_date_end)

    completions_range <- completions_tag %>%
      dplyr::mutate(date = as.Date(.data$completed_at)) %>%
      dplyr::filter(!is.na(.data$date), .data$date >= start_date, .data$date <= end_date)

    rings_df <- if (nrow(completions_range)) {
      completions_range %>%
        dplyr::left_join(training_lookup, by = "training_id") %>%
        dplyr::arrange(.data$user_id, .data$date, dplyr::desc(.data$training_order), .data$training_id) %>%
        dplyr::group_by(.data$user_id, .data$date) %>%
        dplyr::summarise(
          training_ring_color = dplyr::first(.data$training_ring_color),
          training_names = paste(unique(.data$training_name), collapse = ", "),
          .groups = "drop"
        )
    } else {
      empty_triage_training_rings_df()
    }

    panel_matches <- if (nrow(rings_df)) {
      triage_panel_df() %>%
        dplyr::select("user_id", "name", "date", "hour", "stamp_color") %>%
        dplyr::inner_join(rings_df, by = c("user_id", "date")) %>%
        dplyr::arrange(.data$date, .data$name)
    } else {
      empty_preview
    }

    lines <- c(
      paste0("trainer_id selecionado: ", trainer_id),
      paste0("linhas trainer_tags para o treinador: ", nrow(trainer_tag_rows)),
      paste0("tag_ids encontrados: ", if (length(tag_ids)) paste(tag_ids, collapse = ", ") else "<nenhum>"),
      paste0("linhas em tag_templates: ", nrow(tag_template)),
      paste0("training_ids extraidos: ", if (length(training_ids)) paste(training_ids, collapse = ", ") else "<nenhum>"),
      paste0("treinos resolvidos em trainings: ", nrow(training_lookup)),
      paste0("usuarios do treinador: ", length(user_ids)),
      paste0("training_tag_completions bruto: ", nrow(completions_raw)),
      paste0("training_tag_completions apos filtro de tag: ", nrow(completions_tag)),
      paste0("training_tag_completions no intervalo de datas: ", nrow(completions_range)),
      paste0("pares user_id + data com treino/tag: ", nrow(rings_df)),
      paste0("matches efetivos com triagens do painel: ", nrow(panel_matches))
    )

    list(lines = lines, preview = panel_matches)
  })

  triage_report_sheet_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_report_tab_label())
    triage_refresh_tick()
    read_triage_threshold_sheet(TRIAGE_SHEET_ID, grouping_mode = "trainers")
  })

  triage_report_units <- reactive({
    df <- triage_report_sheet_df()
    df %>%
      dplyr::filter(!is.na(.data$unit), nzchar(trimws(.data$unit))) %>%
      dplyr::distinct(.data$unit) %>%
      dplyr::arrange(.data$unit) %>%
      dplyr::pull(.data$unit)
  })

  observeEvent(triage_report_units(), {
    units <- triage_report_units()
    selected_unit <- input$triage_report_unit
    if (is.null(selected_unit) || !selected_unit %in% units) {
      selected_unit <- units[[1]] %||% character(0)
    }
    updateSelectInput(session, "triage_report_unit", choices = units, selected = selected_unit)
  }, ignoreInit = FALSE)

  triage_report_unit_trainers <- reactive({
    req(input$triage_report_unit)
    triage_report_sheet_df() %>%
      dplyr::filter(.data$unit == input$triage_report_unit) %>%
      dplyr::filter(!is.na(.data$trainer_id), !is.na(.data$trainer), nzchar(trimws(.data$trainer))) %>%
      dplyr::transmute(
        trainer_id = as.integer(.data$trainer_id),
        trainer_name = as.character(.data$trainer),
        unit = as.character(.data$unit)
      ) %>%
      dplyr::distinct(.data$trainer_id, .keep_all = TRUE) %>%
      dplyr::arrange(.data$trainer_name)
  })

  triage_report_trainer_links <- reactive({
    trainers <- triage_report_unit_trainers()
    req(nrow(trainers) > 0)
    get_legal_entity_trainers_users(trainers$trainer_id) %>%
      dplyr::inner_join(trainers, by = "trainer_id")
  })

  triage_report_raw_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_report_tab_label())
    links <- triage_report_trainer_links()
    req(nrow(links) > 0)

    uids <- unique(as.integer(links$user_id))
    inst_id <- selected_institution_id()
    choice <- paste0("triage-report:", input$triage_report_unit %||% "")

    get_moove_scores_raw_data_cached(uids, inst_id, choice) %>%
      dplyr::mutate(
        played_at = suppressWarnings(lubridate::ymd_hms(.data$date_time, tz = "UTC", quiet = TRUE)),
        played_at = dplyr::coalesce(.data$played_at, suppressWarnings(as.POSIXct(.data$date_time, tz = "UTC"))),
        date = dplyr::coalesce(as.Date(.data$played_at), as.Date(substr(as.character(.data$date_time), 1, 10))),
        hour = substr(as.character(.data$date_time), 12, 13)
      )
  })

  triage_report_reports <- reactive({
    req(authed(), session_role() == "institution", input$tabs == triage_report_tab_label())
    trainers <- triage_report_unit_trainers()
    links <- triage_report_trainer_links()
    inst_id <- selected_institution_id()

    if (!nrow(trainers) || !nrow(links)) {
      return(list())
    }


    trainer_tags_df <- read_trainer_tags_sheet(TRIAGE_SHEET_ID)

    report_list <- vector("list", nrow(trainers))

    for (i in seq_len(nrow(trainers))) {
      trainer_row <- trainers[i, , drop = FALSE]
      trainer_id <- as.integer(trainer_row$trainer_id[[1]])
      trainer_name <- as.character(trainer_row$trainer_name[[1]])

      trainer_links <- links %>%
        dplyr::filter(.data$trainer_id == trainer_id) %>%
        dplyr::distinct(.data$user_id)

      trainer_user_ids <- unique(as.integer(trainer_links$user_id))
      trainer_user_ids <- trainer_user_ids[!is.na(trainer_user_ids)]

      trainer_name_df <- get_names_for_users(trainer_user_ids)
      trainer_nick_df <- get_nickname_for_users(trainer_user_ids)

      trainer_users <- tibble::tibble(user_id = trainer_user_ids) %>%
        dplyr::left_join(trainer_name_df, by = "user_id") %>%
        dplyr::left_join(trainer_nick_df, by = "user_id") %>%
        dplyr::mutate(display_name = dplyr::coalesce(.data$nickname, .data$name, paste0("user_", .data$user_id))) %>%
        dplyr::arrange(.data$display_name)

      trainer_raw_df <- get_moove_scores_raw_data_cached(
        trainer_user_ids,
        inst_id,
        as.character(trainer_id)
      ) %>%
        dplyr::mutate(
          played_at = suppressWarnings(lubridate::ymd_hms(.data$date_time, tz = "UTC", quiet = TRUE)),
          played_at = dplyr::coalesce(.data$played_at, suppressWarnings(as.POSIXct(.data$date_time, tz = "UTC"))),
          date = dplyr::coalesce(as.Date(.data$played_at), as.Date(substr(as.character(.data$date_time), 1, 10))),
          hour = substr(as.character(.data$date_time), 12, 13)
        )

      trainer_df <- trainer_raw_df %>%
        dplyr::left_join(trainer_users, by = "user_id") %>%
        dplyr::mutate(name = dplyr::coalesce(.data$display_name, paste0("user_", .data$user_id)))

      if (!nrow(trainer_df)) {
        report_list[[i]] <- list(
          trainer_id = trainer_id,
          trainer_name = trainer_name,
          user_names = trainer_users$display_name,
          raw_df = tibble::tibble(),
          quantiles = NULL,
          daily_triages = tibble::tibble(),
          demand_df = tibble::tibble(),
          performed_df = tibble::tibble(),
          monthly_df = tibble::tibble(),
          heat_df = tibble::tibble(),
          total_triages = 0L,
          avg_triages_per_day = 0,
          demand_n = 0L,
          activation_n = 0L,
          triage_color_counts = c(yellow = 0L, orange = 0L, red = 0L),
          activation_color_counts = c(yellow = 0L, orange = 0L, red = 0L)
        )
        next
      }

      qs <- report_triage_quantiles(trainer_df)
      processed_df <- classify_triage_sessions_observed(trainer_df, qs)

      daily_triages <- processed_df %>%
        dplyr::arrange(.data$user_id, .data$date, dplyr::desc(.data$severity_rank), dplyr::desc(.data$played_at)) %>%
        dplyr::group_by(.data$user_id, .data$name, .data$date) %>%
        dplyr::summarise(
          stamp_color = dplyr::first(.data$stamp_color),
          hour = dplyr::first(.data$hour),
          severity_rank = dplyr::first(.data$severity_rank),
          .groups = "drop"
        )

      trainer_tag_rows <- trainer_tags_df %>%
        dplyr::filter(.data$trainer_id == trainer_id)
      tag_ids <- unique(as.integer(trainer_tag_rows$tag_id[!is.na(trainer_tag_rows$tag_id)]))
      tag_id <- if (length(tag_ids)) tag_ids[[1]] else NA_integer_

      activations_df <- empty_triage_training_rings_df()
      if (!is.na(tag_id)) {
        tag_template <- tag_templates_tbl %>%
          dplyr::filter(.data$tag_id == !!tag_id | .data$id == !!tag_id) %>%
          dplyr::select(id, tag_id, training_ids_array) %>%
          collect()

        if (nrow(tag_template)) {
          training_ids <- parse_training_ids_array(tag_template$training_ids_array[[1]])

          if (length(training_ids)) {
            training_lookup <- trainings_tbl %>%
              dplyr::filter(.data$id %in% !!as.integer(training_ids)) %>%
              dplyr::transmute(
                training_id = as.integer(.data$id),
                training_name = as.character(.data$name)
              ) %>%
              collect() %>%
              dplyr::right_join(tibble::tibble(training_id = as.integer(training_ids)), by = "training_id") %>%
              dplyr::mutate(
                training_order = match(.data$training_id, as.integer(training_ids)),
                training_name = dplyr::coalesce(.data$training_name, paste0("training_", .data$training_id)),
                training_ring_color = dplyr::case_when(
                  .data$training_order == 1L ~ "#f2c94c",
                  .data$training_order == 2L ~ "#f2994a",
                  .data$training_order >= 3L ~ "#eb5757",
                  TRUE ~ "#f2c94c"
                )
              )

            completions <- training_tag_completions_tbl %>%
              dplyr::filter(
                .data$user_id %in% !!trainer_user_ids,
                .data$training_id %in% !!as.integer(training_ids)
              ) %>%
              dplyr::transmute(
                user_id = as.integer(.data$user_id),
                tag_id = as.integer(.data$tag_id),
                training_id = as.integer(.data$training_id),
                completed_at = dplyr::coalesce(.data$created_at, .data$updated_at)
              ) %>%
              collect()

            if (nrow(completions)) {
              if ("tag_id" %in% names(completions) && any(completions$tag_id == tag_id, na.rm = TRUE)) {
                completions <- completions %>%
                  dplyr::filter(.data$tag_id == tag_id)
              }

              activations_df <- completions %>%
                dplyr::mutate(date = as.Date(.data$completed_at)) %>%
                dplyr::filter(!is.na(.data$date)) %>%
                dplyr::left_join(training_lookup, by = "training_id") %>%
                dplyr::arrange(.data$user_id, .data$date, dplyr::desc(.data$training_order), .data$training_id) %>%
                dplyr::group_by(.data$user_id, .data$date) %>%
                dplyr::summarise(
                  training_ring_color = dplyr::first(.data$training_ring_color),
                  training_names = paste(unique(.data$training_name), collapse = ", "),
                  .groups = "drop"
                )
            }
          }
        }
      }

      demand_df <- daily_triages %>%
        dplyr::filter(.data$stamp_color != "white")

      performed_df <- demand_df %>%
        dplyr::inner_join(activations_df, by = c("user_id", "date"))

      daily_counts <- trainer_df %>%
        dplyr::count(.data$date, name = "n")

      triage_color_counts <- c(
        yellow = sum(demand_df$stamp_color == "yellow", na.rm = TRUE),
        orange = sum(demand_df$stamp_color == "orange", na.rm = TRUE),
        red = sum(demand_df$stamp_color == "red", na.rm = TRUE)
      )

      activation_color_counts <- c(
        yellow = sum(performed_df$training_ring_color == "#f2c94c", na.rm = TRUE),
        orange = sum(performed_df$training_ring_color == "#f2994a", na.rm = TRUE),
        red = sum(performed_df$training_ring_color == "#eb5757", na.rm = TRUE)
      )

      report_list[[i]] <- list(
          trainer_id = trainer_id,
          trainer_name = trainer_name,
          user_names = trainer_users$display_name,
        raw_df = trainer_df,
        quantiles = qs,
        daily_triages = daily_triages,
        demand_df = demand_df,
        performed_df = performed_df,
        monthly_df = build_triage_monthly_table(trainer_df, date_col = "date", count_name = "triagens"),
        heat_df = trainer_df,
        total_triages = nrow(trainer_df),
        avg_triages_per_day = if (nrow(daily_counts)) mean(daily_counts$n) else 0,
        demand_n = nrow(demand_df),
        activation_n = nrow(performed_df),
        triage_color_counts = triage_color_counts,
        activation_color_counts = activation_color_counts
      )
    }

    report_list
  })

  triage_download_base_df <- reactive({
    req(triage_processed_df())
    d <- triage_processed_df()
    req(nrow(d) > 0)
    req(input$triage_date_start, input$triage_date_end)

    start_date <- as.Date(input$triage_date_start)
    end_date   <- as.Date(input$triage_date_end)

    d %>%
      dplyr::filter(.data$date >= start_date, .data$date <= end_date) %>%
      dplyr::mutate(date_download = format(.data$date, "%d/%m/%Y")) %>%
      dplyr::arrange(.data$name, .data$date, .data$played_at) %>%
      dplyr::transmute(
        date_raw = .data$date,
        `Apelido` = name,
        Data = date_download,
        Hora = hour,
        `Respostas Corretas` = correct_responses_per_minute,
        `Respostas Incorretas` = incorrect_responses_per_minute,
        `Tempo de Resposta` = average_response_time,
        `Selo Repostas Corretas` = correct_stamp,
        `Selo Respostas Incorretas` = incorrect_stamp,
        `Selo Tempo de Resposta` = rt_stamp,
        `Selo Geral` = stamp_color
      )
  })

  triage_download_df <- reactive({
    req(triage_download_base_df())

    triage_download_base_df() %>%
      dplyr::select(-.data$date_raw)
  })

  triage_selected_date_download_df <- reactive({
    req(triage_download_base_df())
    sel_date <- triage_selected_click_date()
    req(!is.na(sel_date))

    triage_download_base_df() %>%
      dplyr::filter(.data$date_raw == sel_date) %>%
      dplyr::select(-.data$date_raw)
  })

  triage_panel_dims <- reactive({
    df <- triage_panel_df()
    n_users <- dplyr::n_distinct(df$name)

    list(
      height = max(720L, 40L * n_users + 140L)
    )
  })
  
  
  mm_df <- reactive({
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    inst_id <- selected_institution_id()
    choice  <- input$sel_group %||% "ALL"
    uids    <- scope_user_ids()
    get_measurement_summaries_cached(uids, inst_id, choice)
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
    
        ug_named <- grouping_user_links() %>%
      dplyr::filter(.data$user_id %in% !!unique(as.integer(d$user_id))) %>%
      dplyr::distinct(.data$user_id, .data$group_id, .keep_all = TRUE)

    if (!nrow(ug_named)) return(tibble::tibble(group_id = integer(), group_name = character(), value = numeric()))
    
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
    
    ug_users <- grouping_user_links() %>%
      dplyr::filter(.data$group_id == !!gid) %>%
      dplyr::transmute(user_id = as.integer(.data$user_id)) %>%
      dplyr::distinct()
    
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
    ug_named <- grouping_user_links() %>%
      dplyr::filter(.data$user_id %in% !!unique(as.integer(d$user_id))) %>%
      dplyr::group_by(.data$user_id) %>%
      dplyr::summarise(
        groups = paste(sort(unique(.data$group_name[!is.na(.data$group_name) & .data$group_name != ""])),
                       collapse = ", "),
        .groups = "drop"
      )
    
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
  
  # ---- answers ----
  
  ans_questions <- reactive({
    req(authed(), session_role() == "institution")
    inst_id <- selected_institution_id(); req(inst_id)
    
    ck <- cache_key(
      prefix   = ANS_CACHE_PREFIX,
      inst_id  = inst_id,
      extra    = "questions"
    )
    cached <- cache_get(ck)
    if (!is.null(cached)) return(cached)
    
    questions_tbl         <- tbl(pool, "questions")
    template_questions_tbl <- tbl(pool, "template_questions")
    
    df <- template_questions_tbl %>%
      dplyr::filter(template_id %in% 1:6) %>%              # mantém a lógica dos templates 1–6
      dplyr::select(question_id) %>%
      dplyr::distinct() %>%
      dplyr::inner_join(
        questions_tbl,
        by = c("question_id" = "id")
      ) %>%
      dplyr::select(
        question_id,
        question = title
      ) %>%
      dplyr::mutate(question_id = as.numeric(question_id)) %>%
      dplyr::arrange(question) %>%
      dplyr::collect()
    
    cache_set(ck, df)
    df
  })
  
  ans_base <- reactive({
    req(authed(), session_role() == "institution")
    inst_id <- selected_institution_id(); req(inst_id)
    
    ck <- cache_key(
      prefix   = ANS_CACHE_PREFIX,
      inst_id  = inst_id,
      extra    = "base"
    )
    cached <- cache_get(ck)
    if (!is.null(cached)) return(cached)
    
    # perguntas válidas (templates 1–6)
    q_df <- ans_questions()
    if (!nrow(q_df)) {
      out <- tibble::tibble()
      cache_set(ck, out); return(out)
    }
    q_ids <- as.numeric(q_df$question_id)
    
    # todos os usuários da instituição (sem limitar ao grupo ainda)
    inst_uids <- get_user_ids_for_institution_or_group(inst_id, "ALL")
    if (!length(inst_uids)) {
      out <- tibble::tibble()
      cache_set(ck, out); return(out)
    }
    
    user_question_answers_tbl <- tbl(pool, "user_question_answers")
    question_answers_tbl      <- tbl(pool, "question_answers")
    questions_tbl             <- tbl(pool, "questions")
    
    # respostas brutas dos usuários da instituição para as perguntas de interesse
    uqa_raw <- user_question_answers_tbl %>%
      dplyr::filter(
        .data$question_id %in% !!as.integer(q_ids),
        .data$user_id     %in% !!as.integer(inst_uids)
      ) %>%
      dplyr::select(
        user_id,
        question_id,
        question_answer_id,
        did_not_answer
      ) %>%
      dplyr::collect()
    
    if (!nrow(uqa_raw)) {
      out <- tibble::tibble()
      cache_set(ck, out); return(out)
    }
    
    # ids numéricos, como no script do relatório
    uqa_raw <- uqa_raw %>%
      dplyr::mutate(
        question_id        = as.numeric(question_id),
        question_answer_id = suppressWarnings(
          as.numeric(gsub("[^0-9]", "", as.character(question_answer_id)))
        )
      )
    
    # lookups (answer + emoji, texto da pergunta)
    qa_lookup <- question_answers_tbl %>%
      dplyr::select(id, question_id, title, image) %>%
      dplyr::collect() %>%
      dplyr::mutate(
        id          = as.numeric(id),
        question_id = as.numeric(question_id)
      ) %>%
      dplyr::distinct(id, question_id, .keep_all = TRUE)
    
    q_lookup <- questions_tbl %>%
      dplyr::select(id, title) %>%
      dplyr::collect() %>%
      dplyr::rename(
        question_id = id,
        question    = title
      ) %>%
      dplyr::mutate(question_id = as.numeric(question_id))
    
    # reconstrói base enriquecida com texto da pergunta, resposta e emoji
    df <- uqa_raw %>%
      dplyr::select(
        user_id,
        question_id,
        question_answer_id,
        did_not_answer
      ) %>%
      dplyr::left_join(
        qa_lookup,
        by = c("question_id" = "question_id",
               "question_answer_id" = "id")
      ) %>%
      dplyr::rename(
        question_answer = title,
        question_emoji  = image
      ) %>%
      dplyr::left_join(q_lookup, by = "question_id") %>%
      dplyr::mutate(
        question_answer = dplyr::coalesce(question_answer, ""),
        question_emoji  = dplyr::coalesce(question_emoji, ""),
        question        = dplyr::coalesce(question, "")
      )
    
    cache_set(ck, df)
    df
  })
  
  ans_dist_two_pops <- reactive({
    req(authed(), session_role() == "institution")
    
    df <- ans_base()
    req(is.data.frame(df), nrow(df) > 0)
    
    inst_id <- selected_institution_id()
    req(inst_id)
    
    # ---------------------------------------------
    # 1. ID da pergunta selecionada
    # ---------------------------------------------
    qid <- req(input$resp_question)
    qid <- as.numeric(qid)
    
    df_q <- df %>%
      dplyr::filter(
        .data$question_id == !!qid,
        did_not_answer == 0,
        nzchar(question_answer)
      )
    
    if (!nrow(df_q)) {
      return(tibble::tibble(
        grupo = character(),
        label = character(),
        img   = character(),
        pct   = numeric(),
        n     = integer()
      ))
    }
    
    # ---------------------------------------------
    # 2. Usuários da instituição inteira
    # ---------------------------------------------
    inst_users <- get_user_ids_for_institution_or_group(inst_id, "ALL")
    
    group_choice <- input$resp_group %||% "ALL"
    group_users  <- get_user_ids_for_institution_or_grouping(inst_id, group_choice, grouping_mode())
    bucket_label <- grouping_label(grouping_mode(), plural = FALSE, title_case = TRUE)
    
    user_choice <- input$resp_user %||% "ALL_GROUP"
    
    if (identical(user_choice, "ALL_GROUP") || !nzchar(as.character(user_choice))) {
      pop1_users <- as.integer(group_users)
      pop2_users <- as.integer(inst_users)
      pop1_label <- bucket_label
      pop2_label <- "Instituição"
    } else {
      uid        <- as.integer(user_choice)
      pop1_users <- uid
      pop2_users <- as.integer(group_users)
      pop1_label <- "Usuário"
      pop2_label <- bucket_label
    }
    
    # restringe a base só aos usuários usados na comparação
    keep_ids <- unique(c(pop1_users, pop2_users))
    df_q <- df_q %>% dplyr::filter(.data$user_id %in% !!keep_ids)
    
    if (!nrow(df_q)) {
      return(tibble::tibble(
        grupo = character(),
        label = character(),
        img   = character(),
        pct   = numeric(),
        n     = integer()
      ))
    }
    
    # ---------------------------------------------
    # Função interna para montar distribuição
    # ---------------------------------------------
    make_dist <- function(uids, label_group) {
      if (is.null(uids) || !length(uids)) return(NULL)
      
      sub <- df_q %>%
        dplyr::filter(.data$user_id %in% !!as.integer(uids),
                      nzchar(question_answer))
      
      if (!nrow(sub)) return(NULL)
      
      sub %>%
        dplyr::count(question_answer, question_emoji, name = "n") %>%
        dplyr::mutate(
          grupo = label_group,
          pct   = n / sum(n),
          label = question_answer,
          img   = dplyr::if_else(
            nzchar(question_emoji),
            paste0("emojis/", question_emoji, ".png"),  # arquivo na pasta www/emojis
            NA_character_
          )
        )
    }
    
    df1 <- make_dist(pop1_users, pop1_label)
    df2 <- make_dist(pop2_users, pop2_label)
    
    out <- dplyr::bind_rows(df1, df2)
    
    if (!nrow(out)) {
      tibble::tibble(
        grupo = character(),
        label = character(),
        img   = character(),
        pct   = numeric(),
        n     = integer()
      )
    } else {
      out
    }
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
          span("Instituição: ", inst_name), tags$br(),
          tags$div(style = "margin-top:8px;",
            radioButtons(
              "grouping_mode",
              label = "Agrupamento",
              choices = c("Grupos" = "groups", "Treinadores" = "trainers"),
              selected = input$grouping_mode %||% "groups",
              inline = TRUE
            )
          )
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
  

  output$ui_rankings_scope <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Rankings")
    inst_name <- tryCatch(as.character(institution_dt()$institution_name), error = function(e) "Instituicao")
    selected <- input$ranking_scope %||% "global"

    radioButtons(
      "ranking_scope",
      label = "Escopo do ranking:",
      choices = stats::setNames(c("global", "institution"), c("Global", inst_name)),
      selected = selected,
      inline = TRUE
    )
  })

  output$ui_rankings_top3 <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Rankings")
    df <- ranking_scope_df()

    if (is.null(df) || !nrow(df)) {
      return(div(style = "padding:12px;", "Sem dados para o ranking Moove."))
    }

    top3 <- df %>% dplyr::slice_head(n = 3)

    fluidRow(
      lapply(seq_len(nrow(top3)), function(i) {
        row <- top3[i, , drop = FALSE]
        column(
          width = 4,
          wellPanel(
            tags$div(style = "font-size:18px; font-weight:700;", paste0("#", row$rank_display[[1]], " ", row$display_name[[1]])),
            tags$div(style = "margin-top:6px;", paste0("Neurons: ", scales::comma(row$neurons[[1]], accuracy = 1))),
            tags$div(style = "font-size:24px; font-weight:700; margin-top:10px;", scales::comma(row$score[[1]], accuracy = 1))
          )
        )
      })
    )
  })

  output$tbl_rankings <- DT::renderDT({
    req(authed(), session_role() == "institution", input$tabs == "Rankings")
    df <- ranking_scope_df()
    req(nrow(df) > 0)

    out <- df %>%
      dplyr::transmute(
        Rank = .data$rank_display,
        Usuario = .data$display_name,
        Neurons = round(.data$neurons),
        Score = round(.data$score)
      )

    DT::datatable(
      out,
      rownames = FALSE,
      options = list(pageLength = 50, dom = "tip", ordering = FALSE)
    )
  })

  output$ui_rankings_minigame_scope <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Rankings")
    df <- ranking_minigame_sheet_df()

    if (is.null(df) || !nrow(df)) {
      return(div(style = "padding:12px;", "Sem configuracao na aba minigames_ranking."))
    }

    selected <- input$ranking_minigame_config
    if (is.null(selected) || !nzchar(as.character(selected)) || !selected %in% df$config_id) {
      selected <- ""
    }

    selectInput(
      "ranking_minigame_config",
      label = "Ranking de minigame:",
      choices = c("Selecione" = "", stats::setNames(df$config_id, paste0(df$game_name, " - ", df$game_parameter_name))),
      selected = selected
    )
  })

  output$ui_rankings_minigame_title <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Rankings")
    cfg <- ranking_minigame_selected_config()
    if (!nrow(cfg)) return(NULL)

    tagList(
      tags$h4(paste0("Ranking do ", cfg$game_name[[1]]), style = "text-align:center; font-weight:700;"),
      tags$p(paste0("(", cfg$game_parameter_name[[1]], ")"), style = "text-align:center; margin-top:-6px;")
    )
  })

  output$ui_rankings_minigame_top3 <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == "Rankings")
    cfg <- ranking_minigame_selected_config()
    if (!nrow(cfg)) {
      return(div(style = "padding:12px;", "Selecione um minigame para carregar o ranking."))
    }

    df <- ranking_minigame_df()
    req(nrow(cfg) == 1)

    if (is.null(df) || !nrow(df)) {
      return(div(style = "padding:12px;", "Sem dados para este minigame."))
    }

    top3 <- df %>% dplyr::slice_head(n = 3)

    fluidRow(
      lapply(seq_len(nrow(top3)), function(i) {
        row <- top3[i, , drop = FALSE]
        column(
          width = 4,
          wellPanel(
            tags$div(style = "font-size:18px; font-weight:700;", paste0("#", row$rank_display[[1]], " ", row$display_name[[1]])),
            tags$div(style = "margin-top:6px;", paste0(cfg$game_parameter_name[[1]], ": ", scales::comma(row$metric_value[[1]], accuracy = 0.01)))
          )
        )
      })
    )
  })

  output$tbl_rankings_minigame <- DT::renderDT({
    req(authed(), session_role() == "institution", input$tabs == "Rankings")
    cfg <- ranking_minigame_selected_config()
    req(nrow(cfg) == 1)

    df <- ranking_minigame_df()
    req(nrow(df) > 0)

    out <- df %>%
      dplyr::transmute(
        Rank = .data$rank_display,
        Usuario = .data$display_name,
        Valor = .data$metric_value
      )

    colnames(out)[3] <- cfg$game_parameter_name[[1]]

    DT::datatable(
      out,
      rownames = FALSE,
      options = list(pageLength = 50, dom = "tip", ordering = FALSE)
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

  # ---- triage and activation ----
  output$tbl_triage_monthly <- DT::renderDT({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    df <- triage_raw_df()
    req(nrow(df) > 0)

    out <- df %>%
      dplyr::mutate(
        year      = lubridate::year(.data$date),
        month_num = lubridate::month(.data$date),
        month_lab = lubridate::month(.data$date, label = TRUE, abbr = TRUE)
      ) %>%
      dplyr::count(year, month_num, month_lab, name = "triagens") %>%
      tidyr::pivot_wider(
        names_from  = year,
        values_from = triagens,
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

  output$hc_triage_groups <- renderHighchart({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    df <- triage_group_stats()
    req(nrow(df) > 0)

    df <- df %>% dplyr::arrange(dplyr::desc(.data$n), .data$group_name)

    n  <- nrow(df)
    pb <- page_bounds(n, triage_page(), PER_PAGE)
    idx <- if (pb[1] <= pb[2]) seq.int(pb[1], pb[2]) else integer(0)
    df <- df[idx, , drop = FALSE]

    avg  <- mean(df$n, na.rm = TRUE)
    cols <- color_by_mean(df$n, avg, high_is_good = TRUE)

    highchart() %>%
      hc_chart(type = "column", inverted = TRUE) %>%
      hc_title(text = if (identical(triage_bucket_mode(), "units")) "Triagens por unidade" else paste0("Triagens por ", grouping_label(grouping_mode(), plural = FALSE, title_case = FALSE))) %>%
      hc_xAxis(categories = df$group_name) %>%
      hc_yAxis(
        title = list(text = "Quantidade de triagens"),
        plotLines = list(list(color = "#f39c12", width = 2, value = avg, zIndex = 5))
      ) %>%
      hc_add_series(
        name = "Triagens",
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
              Shiny.setInputValue('hc_triage_group_click', { name: this.name }, { priority: 'event' });
            }
          ")
        ))
      ))
  })

  output$ui_triage_pager <- renderUI({
    total_items <- nrow(triage_group_stats())
    total_pages <- max(1L, ceiling(total_items / PER_PAGE))
    curr <- clamp(triage_page(), 1L, total_pages)
    if (curr != triage_page()) triage_page(curr)

    tagList(
      actionButton("triage_prev", label = NULL, icon = icon("chevron-left"),
                   class = "btn btn-light", disabled = if (curr <= 1) "disabled"),
      span(sprintf("Página %d de %d", curr, total_pages),
           style = "min-width:140px; text-align:center; font-weight:600;"),
      actionButton("triage_next", label = NULL, icon = icon("chevron-right"),
                   class = "btn btn-light", disabled = if (curr >= total_pages) "disabled")
    )
  })

  output$ui_triage_back <- renderUI({
    if (identical(triage_view_mode(), "group")) {
      lbl <- if (identical(triage_bucket_mode(), "units")) "Voltar às unidades" else paste0("Voltar aos ", grouping_label(grouping_mode(), plural = TRUE, title_case = FALSE))
      actionButton("btn_triage_back", lbl, icon = icon("arrow-left"), class = "btn btn-light")
    } else {
      NULL
    }
  })

  output$ui_triage_detail <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())

    if (!identical(triage_view_mode(), "group")) {
      return(
        div(
          style = "padding:12px; border:1px solid #eee; border-radius:8px; background:#fafafa;",
          paste0("Selecione ", if (identical(triage_bucket_mode(), "units")) "uma " else "um ", if (identical(triage_bucket_mode(), "units")) "unidade" else grouping_label(grouping_mode(), plural = FALSE, title_case = FALSE), " no gráfico para ver as distribuições e o painel de triagens.")
        )
      )
    }

    sheet_vals <- triage_sheet_group_thresholds()

    tagList(
      hr(),
      if (isTRUE(triage_manager_mode()) && !identical(triage_bucket_mode(), "units")) {
        tagList(
          fluidRow(align = "center",
            column(
              4,
              plotOutput("plt_triage_correct_dist", height = "420px"),
              numericInput("triage_correct_yellow", "Respostas Corretas (Amarelo)", value = as.numeric(sheet_vals[["triage_correct_yellow"]]), min = 0),
              numericInput("triage_correct_red", "Respostas Corretas (Vermelho)", value = as.numeric(sheet_vals[["triage_correct_red"]]), min = 0)
            ),
            column(
              4,
              plotOutput("plt_triage_incorrect_dist", height = "420px"),
              numericInput("triage_incorrect_yellow", "Respostas Incorretas (Amarelo)", value = as.numeric(sheet_vals[["triage_incorrect_yellow"]]), min = 0),
              numericInput("triage_incorrect_red", "Respostas Incorretas (Vermelho)", value = as.numeric(sheet_vals[["triage_incorrect_red"]]), min = 0)
            ),
            column(
              4,
              plotOutput("plt_triage_rt_dist", height = "420px"),
              numericInput("triage_rt_yellow", "Tempo de Resposta (Amarelo)", value = as.numeric(sheet_vals[["triage_rt_yellow"]]), min = 0),
              numericInput("triage_rt_red", "Tempo de Resposta (Vermelho)", value = as.numeric(sheet_vals[["triage_rt_red"]]), min = 0)
            )
          ),
          fluidRow(
            column(
              12,
              div(
                style = "display:flex; justify-content:center; margin: 12px 0 0 0;",
                actionButton("triage_save_thresholds", "Salvar limites na Google Sheet", icon = icon("floppy-disk"), class = "btn btn-primary")
              )
            )
          )
        )
      },
      br(),
      fluidRow( align = "center",
                column(3),
                column(3, dateInput("triage_date_start", "Data inicial:", value = Sys.Date() - 10)),
                column(3, dateInput("triage_date_end", "Data final:", value = Sys.Date())),
                column(
                  3,
                  div(
                    style = "padding-top: 25px; text-align:center;",
                    actionButton("triage_refresh", "Atualizar dados", icon = icon("rotate-right"), class = "btn btn-default")
                  )
                )
      ),
      fluidRow(
        column(
          12,
          div(
            style = "display:flex; justify-content:center;",
            radioButtons(
              "triage_user_filter",
              label = "Filtrar usuários que realizaram a triagem:",
              choices = c(
                "No período selecionado" = "period",
                "Últimas 24 horas" = "24h",
                "Últimas 12 horas" = "12h",
                "Últimas 3 horas" = "3h"
              ),
              selected = "period",
              inline = TRUE
            )
          )
        )
      ),
      fluidRow(
        column(12, uiOutput("ui_triage_panel_plot"))
      ),
      br(),
      fluidRow(
        column(12, uiOutput("ui_triage_training_debug"))
      ),
      br(),
      div(
        style = "display:flex; justify-content:center; margin: 8px 0 16px 0;",
        downloadButton("download_triage_xlsx", "Baixar triagens derivadas (XLSX)", class = "btn btn-primary")
      )
    )
  })

  output$ui_triage_panel_plot <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    dims <- triage_panel_dims()

    div(
      style = "max-height:900px; overflow-y:auto; overflow-x:hidden; border:1px solid #e5e5e5; border-radius:8px; padding:8px; background:#fff;",
      plotlyOutput("plt_triage_panel", height = paste0(dims$height, "px"), width = "100%")
    )
  })

  output$ui_triage_training_debug <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())

    if (!identical(grouping_mode(), "trainers")) return(NULL)

    div(
      style = "border:1px solid #e5e5e5; border-radius:8px; padding:12px; background:#fff;",
      tags$h4("Debug treino/tag", style = "margin-top:0; text-align:center; font-weight:700;"),
      verbatimTextOutput("txt_triage_training_debug"),
      DTOutput("tbl_triage_training_debug")
    )
  })

  output$plt_triage_correct_dist <- renderPlot({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    d <- triage_selected_group_df()
    qs <- triage_quantiles()
    req(nrow(d) > 0)

    threshold_specs <- tibble::tibble(
      value = c(triage_correct_red(), triage_correct_yellow()),
      label = c(as.character(triage_correct_red()), as.character(triage_correct_yellow())),
      color = c("red", "#F1C40F")
    )

    observed_specs <- tibble::tibble(
      value = c(qs$correct[1], qs$correct[2]),
      label = c(sprintf("P10 %.1f", qs$correct[1]), sprintf("P25 %.1f", qs$correct[2])),
      color = c("red", "#F1C40F")
    )

    subtitle_txt <- sprintf(
      "P10 observado: %.1f | P25 observado: %.1f",
      qs$correct[1], qs$correct[2]
    )

    print(build_triage_distribution_plot(
      d,
      value_col = "correct_responses_per_minute",
      title_txt = "Distribuição das respostas corretas por minuto",
      xlab_txt = "Número de respostas corretas por minuto",
      observed_specs = observed_specs,
      threshold_specs = threshold_specs,
      subtitle_txt = subtitle_txt
    ))
  })

  output$plt_triage_incorrect_dist <- renderPlot({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    d <- triage_selected_group_df()
    qs <- triage_quantiles()
    req(nrow(d) > 0)

    threshold_specs <- tibble::tibble(
      value = c(triage_incorrect_red(), triage_incorrect_yellow()),
      label = c(as.character(triage_incorrect_red()), as.character(triage_incorrect_yellow())),
      color = c("red", "#F1C40F")
    )

    observed_specs <- tibble::tibble(
      value = c(qs$incorrect[2], qs$incorrect[1]),
      label = c(sprintf("P90 %.1f", qs$incorrect[2]), sprintf("P75 %.1f", qs$incorrect[1])),
      color = c("red", "#F1C40F")
    )

    subtitle_txt <- sprintf(
      "P75 observado: %.1f | P90 observado: %.1f",
      qs$incorrect[1], qs$incorrect[2]
    )

    print(build_triage_distribution_plot(
      d,
      value_col = "incorrect_responses_per_minute",
      title_txt = "Distribuição das respostas incorretas por minuto",
      xlab_txt = "Número de respostas incorretas por minuto",
      observed_specs = observed_specs,
      threshold_specs = threshold_specs,
      subtitle_txt = subtitle_txt
    ))
  })

  output$plt_triage_rt_dist <- renderPlot({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    d <- triage_selected_group_df()
    qs <- triage_quantiles()
    req(nrow(d) > 0)

    threshold_specs <- tibble::tibble(
      value = c(triage_rt_red(), triage_rt_yellow()),
      label = c(as.character(triage_rt_red()), as.character(triage_rt_yellow())),
      color = c("red", "#F1C40F")
    )

    observed_specs <- tibble::tibble(
      value = c(qs$rt[2], qs$rt[1]),
      label = c(sprintf("P90 %.1f", qs$rt[2]), sprintf("P75 %.1f", qs$rt[1])),
      color = c("red", "#F1C40F")
    )

    subtitle_txt <- sprintf(
      "P75 observado: %.1f | P90 observado: %.1f",
      qs$rt[1], qs$rt[2]
    )

    print(build_triage_distribution_plot(
      d,
      value_col = "average_response_time",
      title_txt = "Distribuição dos Tempos de Resposta (ms)",
      xlab_txt = "Tempo de resposta (ms)",
      observed_specs = observed_specs,
      threshold_specs = threshold_specs,
      subtitle_txt = subtitle_txt
    ))
  })

  output$plt_triage_panel <- renderPlotly({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    df <- triage_panel_df()
    bucket_label <- if (identical(triage_bucket_mode(), "units")) {
      "unidade"
    } else {
      grouping_label(grouping_mode(), plural = FALSE, title_case = FALSE)
    }
    req(nrow(df) > 0)
    group_name <- triage_selected_group_name()
    req(nzchar(group_name))

    ring_df <- triage_training_rings_df()

    if (nrow(ring_df)) {
      df <- df %>%
        dplyr::left_join(ring_df, by = c("user_id", "date"))
    } else {
      df <- df %>%
        dplyr::mutate(
          training_ring_color = NA_character_,
          training_names = NA_character_
        )
    }

    date_breaks <- sort(unique(df$date))
    name_levels <- sort(unique(as.character(df$name)), na.last = TRUE)

    df <- df %>%
      dplyr::mutate(
        name = factor(.data$name, levels = rev(name_levels)),
        date_label = format(.data$date, "%d/%m/%Y"),
        hover_txt = paste0(
          "Apelido: ", .data$name,
          "<br>Data: ", .data$date_label,
          "<br>Hora: ", .data$hour,
          "<br>Cor: ", .data$stamp_color,
          dplyr::if_else(
            !is.na(.data$training_names) & nzchar(.data$training_names),
            paste0("<br>Treinos da tag: ", .data$training_names),
            ""
          )
        )
      )

    ring_points <- df %>%
      dplyr::filter(!is.na(.data$training_ring_color), .data$training_ring_color != "")

    p <- ggplot2::ggplot(df, ggplot2::aes(x = .data$date, y = .data$name, text = .data$hover_txt))

    if (nrow(ring_points)) {
      p <- p +
        ggplot2::geom_point(
          data = ring_points,
          ggplot2::aes(color = .data$training_ring_color),
          shape = 21,
          fill = NA,
          size = 13.2,
          stroke = 2,
          show.legend = FALSE
        )
    }

    p <- p +
      ggplot2::geom_point(
        ggplot2::aes(fill = .data$stamp_color),
        shape = 21,
        size = 11,
        color = "#d9d9d9",
        stroke = 0.9
      ) +
      ggplot2::geom_text(
        ggplot2::aes(label = .data$hour, color = .data$label_color),
        fontface = "bold",
        size = 3.2
      ) +
      ggplot2::scale_fill_identity() +
      ggplot2::scale_color_identity() +
      ggplot2::scale_x_date(
        breaks = date_breaks,
        labels = function(x) format(x, "%d/%m/%Y")
      ) +
      ggplot2::labs(
        title = paste0("Painel de triagens do(a) ", bucket_label, " ", group_name),
        x = NULL,
        y = NULL
      ) +
      ggplot2::theme_minimal(base_size = 14) +
      ggplot2::theme(
        plot.title = ggplot2::element_text(face = "bold", hjust = 0.5),
        axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
        panel.grid.minor = ggplot2::element_blank()
      )

    plotly::ggplotly(p, tooltip = "text", source = "triage_panel") %>%
      plotly::event_register("plotly_click") %>%
      plotly::layout(
        dragmode = "pan",
        showlegend = FALSE
      )
  })

  output$txt_triage_training_debug <- renderPrint({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    dbg <- triage_training_debug()
    cat(paste(dbg$lines, collapse = "\n"), "\n")
  })

  output$tbl_triage_training_debug <- DT::renderDT({
    req(authed(), session_role() == "institution", input$tabs == triage_tab_label())
    dbg <- triage_training_debug()

    DT::datatable(
      dbg$preview,
      rownames = FALSE,
      options = list(pageLength = 10, scrollX = TRUE)
    )
  })

  output$ui_triage_report_content <- renderUI({
    req(authed(), session_role() == "institution", input$tabs == triage_report_tab_label())
    reports <- triage_report_reports()

    if (!length(reports)) {
      return(
        div(
          style = "padding:12px; border:1px solid #eee; border-radius:8px; background:#fafafa; text-align:center;",
          "Nenhum dado disponivel para a unit selecionada."
        )
      )
    }

    tagList(
      tags$h2(format_triage_report_unit_title(input$triage_report_unit %||% ""), style = "text-align:center; font-weight:700;"),
      lapply(seq_along(reports), function(i) {
        report <- reports[[i]]
        tid <- if (is.na(report$trainer_id)) i else as.integer(report$trainer_id)

        tagList(
          if (i > 1) tags$hr(style = "margin:28px 0;"),
          tags$h3(paste0("Treinador: ", report$trainer_name)),
          tags$p(tags$b("Usuarios: "), format(length(report$user_names), big.mark = ".", decimal.mark = ",")),
          fluidRow(
            column(12, tags$p(tags$b("Numero de triagens realizadas: "), format(report$total_triages, big.mark = ".", decimal.mark = ","))),
            column(12, tags$p(tags$b("Media de triagens por dia: "), format(round(report$avg_triages_per_day, 2), nsmall = 2, decimal.mark = ","))),
            column(12, tags$p(tags$b("Numero de triagens que demandava ativacao: "), format(report$demand_n, big.mark = ".", decimal.mark = ","))),
            column(12, tags$p(tags$b("Numero de ativacoes realizadas: "), format(report$activation_n, big.mark = ".", decimal.mark = ",")))
          ),
          fluidRow(
            column(4, tags$p(tags$b("Numero de triagens classificadas como ativacao amarela: "), format(report$triage_color_counts[["yellow"]], big.mark = ".", decimal.mark = ","))),
            column(4, tags$p(tags$b("Numero de triagens classificadas como ativacao laranja: "), format(report$triage_color_counts[["orange"]], big.mark = ".", decimal.mark = ","))),
            column(4, tags$p(tags$b("Numero de triagens classificadas como ativacao vermelha: "), format(report$triage_color_counts[["red"]], big.mark = ".", decimal.mark = ",")))
          ),
          fluidRow(
            column(4, tags$p(tags$b("Numero de ativacoes amarelas: "), format(report$activation_color_counts[["yellow"]], big.mark = ".", decimal.mark = ","))),
            column(4, tags$p(tags$b("Numero de ativacoes laranjas: "), format(report$activation_color_counts[["orange"]], big.mark = ".", decimal.mark = ","))),
            column(4, tags$p(tags$b("Numero de ativacoes vermelhas: "), format(report$activation_color_counts[["red"]], big.mark = ".", decimal.mark = ",")))
          ),
          tags$h4("Distribuicao das metricas das triagens", style = "margin-top:18px;"),
          fluidRow(
            column(4, plotOutput(paste0("triage_report_correct_", tid), height = "320px")),
            column(4, plotOutput(paste0("triage_report_incorrect_", tid), height = "320px")),
            column(4, plotOutput(paste0("triage_report_rt_", tid), height = "320px"))
          ),
          fluidRow(
            column(
              6,
              tags$h4("Triagens por mes", style = "margin-top:18px;"),
              DTOutput(paste0("triage_report_monthly_", tid))
            ),
            column(
              6,
              tags$h4("Mapa de calor das triagens", style = "margin-top:18px;"),
              plotOutput(paste0("triage_report_heat_", tid), height = "360px")
            )
          )
        )
      })
    )
  })

  observe({
    req(authed(), session_role() == "institution", input$tabs == triage_report_tab_label())
    reports <- triage_report_reports()

    for (i in seq_along(reports)) {
      local({
        report <- reports[[i]]
        tid <- if (is.na(report$trainer_id)) i else as.integer(report$trainer_id)

        output[[paste0("triage_report_correct_", tid)]] <- renderPlot({
          df <- report$raw_df
          if (is.null(df) || !nrow(df)) return(print(ggplot2::ggplot()))
          qs <- report$quantiles
          observed_specs <- tibble::tibble(
            value = c(qs$correct_red, qs$correct_yellow),
            label = c(sprintf("P10 %.1f", qs$correct_red), sprintf("P25 %.1f", qs$correct_yellow)),
            color = c("red", "#F1C40F")
          )
          subtitle_txt <- sprintf("P10 observado: %.1f | P25 observado: %.1f", qs$correct_red, qs$correct_yellow)
          print(build_triage_distribution_plot(df, "correct_responses_per_minute", "Distribuicao das respostas corretas por minuto", "Numero de respostas corretas por minuto", observed_specs = observed_specs, threshold_specs = NULL, subtitle_txt = subtitle_txt))
        })

        output[[paste0("triage_report_incorrect_", tid)]] <- renderPlot({
          df <- report$raw_df
          if (is.null(df) || !nrow(df)) return(print(ggplot2::ggplot()))
          qs <- report$quantiles
          observed_specs <- tibble::tibble(
            value = c(qs$incorrect_red, qs$incorrect_yellow),
            label = c(sprintf("P90 %.1f", qs$incorrect_red), sprintf("P75 %.1f", qs$incorrect_yellow)),
            color = c("red", "#F1C40F")
          )
          subtitle_txt <- sprintf("P75 observado: %.1f | P90 observado: %.1f", qs$incorrect_yellow, qs$incorrect_red)
          print(build_triage_distribution_plot(df, "incorrect_responses_per_minute", "Distribuicao das respostas incorretas por minuto", "Numero de respostas incorretas por minuto", observed_specs = observed_specs, threshold_specs = NULL, subtitle_txt = subtitle_txt))
        })

        output[[paste0("triage_report_rt_", tid)]] <- renderPlot({
          df <- report$raw_df
          if (is.null(df) || !nrow(df)) return(print(ggplot2::ggplot()))
          qs <- report$quantiles
          observed_specs <- tibble::tibble(
            value = c(qs$rt_red, qs$rt_yellow),
            label = c(sprintf("P90 %.1f", qs$rt_red), sprintf("P75 %.1f", qs$rt_yellow)),
            color = c("red", "#F1C40F")
          )
          subtitle_txt <- sprintf("P75 observado: %.1f | P90 observado: %.1f", qs$rt_yellow, qs$rt_red)
          print(build_triage_distribution_plot(df, "average_response_time", "Distribuicao dos Tempos de Resposta (ms)", "Tempo de resposta (ms)", observed_specs = observed_specs, threshold_specs = NULL, subtitle_txt = subtitle_txt))
        })

        output[[paste0("triage_report_monthly_", tid)]] <- DT::renderDT({
          DT::datatable(
            report$monthly_df,
            rownames = FALSE,
            options = list(paging = FALSE, searching = FALSE, ordering = FALSE, dom = "t", scrollX = TRUE)
          )
        })

        output[[paste0("triage_report_heat_", tid)]] <- renderPlot({
          print(build_triage_heatmap_plot(report$heat_df))
        })
      })
    }
  })

  output$download_triage_xlsx <- downloadHandler(
    filename = function() {
      d <- institution_dt()
      inst <- tryCatch(as.character(d$institution_name), error = function(e) "instituicao")
      sprintf(
        "triagens_derivadas_%s_%s.xlsx",
        gsub("[^A-Za-z0-9_-]", "_", inst),
        format(Sys.time(), "%Y%m%d-%H%M")
      )
    },
    content = function(file) {
      df <- triage_download_df()
      if (!requireNamespace("openxlsx", quietly = TRUE)) {
        write.csv(df, file, row.names = FALSE, fileEncoding = "UTF-8")
      } else {
        openxlsx::write.xlsx(df, file, na = "")
      }
    }
  )

  output$download_triage_selected_date_xlsx <- downloadHandler(
    filename = function() {
      d <- institution_dt()
      inst <- tryCatch(as.character(d$institution_name), error = function(e) "instituicao")
      sel_date <- triage_selected_click_date()
      sprintf(
        "triagens_derivadas_%s_%s.xlsx",
        gsub("[^A-Za-z0-9_-]", "_", inst),
        format(as.Date(sel_date), "%Y%m%d")
      )
    },
    content = function(file) {
      df <- triage_selected_date_download_df()
      if (!requireNamespace("openxlsx", quietly = TRUE)) {
        write.csv(df, file, row.names = FALSE, fileEncoding = "UTF-8")
      } else {
        openxlsx::write.xlsx(df, file, na = "")
      }
    }
  )
  
  
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
  
  # ---- answers ----
  
  output$ui_resp_question <- renderUI({
    df <- ans_questions()
    
    df <- df %>% filter(!question_id %in% c(12,15,18,35,36))
    
    if (!nrow(df)) return(NULL)
    
    selectInput(
      "resp_question",
      label = "Pergunta:",
      choices = setNames(df$question_id, df$question),
      selected = df$question_id[1],
      width = "100%"
    )
  })
  
  output$ui_resp_groups <- renderUI({
    groups_df <- grouping_entities()
    req(nrow(groups_df) > 0)

    group_choices <- groups_df$id
    names(group_choices) <- groups_df$name
    current <- input$resp_group %||% ""
    selected <- if (nzchar(as.character(current)) && current %in% as.character(group_choices)) current else group_choices[1]

    selectInput(
      "resp_group",
      label = paste0(grouping_label(grouping_mode(), plural = FALSE, title_case = TRUE), ":"),
      choices = group_choices,
      selected = selected,
      width = "100%"
    )
  })
  
  output$ui_resp_user <- renderUI({
    req(input$resp_group)
    
    inst_id <- selected_institution_id()
    gid     <- input$resp_group
    
    group_uids <- get_user_ids_for_institution_or_grouping(inst_id, gid, grouping_mode())
    group_uids <- get_names_for_users(group_uids)
    
    choices_all <- c("Todos do agrupamento" = "ALL_GROUP")
    choices <- choices_all
    
    if (nrow(group_uids) > 0) {
      extra_choices <- group_uids$user_id
      names(extra_choices) <- group_uids$name
      choices <- c(choices_all, extra_choices)
    }
    
    selectInput(
      "resp_user",
      label = "Usuário:",
      choices = choices,
      selected = "ALL_GROUP",
      width = "100%"
    )
  })
  
  output$plt_resp <- renderPlot({
    
    df <- ans_dist_two_pops()
    req(nrow(df) > 0)
    
    # -------------------------- nomes "bonitos" --------------------------
    question_title <- ans_questions()$question[
      ans_questions()$question_id == as.integer(input$resp_question)
    ]
    
    inst_name   <- institution_dt()$institution_name %||% "Instituição"
    
    bucket_label <- grouping_label(grouping_mode(), plural = FALSE, title_case = TRUE)
    grouping_df  <- grouping_entities()
    group_label  <- grouping_df$name[match(as.integer(input$resp_group), grouping_df$id)] %||% bucket_label
    
    if (is.null(input$resp_user) || input$resp_user == "ALL_GROUP") {
      user_label <- "Todos do agrupamento"
    } else {
      user_label <- get_names_for_users(as.integer(input$resp_user))$name
    }
    
    # -------------------------- limpeza básica --------------------------
    df <- df %>%
      dplyr::filter(!is.na(grupo))   # some com NA na legenda
    
    max_pct <- max(df$pct, na.rm = TRUE)
    
    # níveis fixos para o fill
    values_all <- c(stats::setNames("#93C5FD", bucket_label), "Instituição" = "#4B5563", "Usuário" = "#da4a11")
    
    df <- df %>%
      dplyr::mutate(
        grupo = factor(grupo, levels = names(values_all))
      )
    
    # -------------------------- labels da legenda --------------------------
    legend_labels <- character(0)
    
    if (bucket_label %in% as.character(df$grupo)) {
      legend_labels[bucket_label] <- paste0(bucket_label, ": ", group_label)
    }
    if ("Instituição" %in% as.character(df$grupo)) {
      legend_labels["Instituição"] <- paste0("Instituição: ", inst_name)
    }
    if ("Usuário" %in% as.character(df$grupo)) {
      legend_labels["Usuário"] <- paste0("Usuário: ", user_label)
    }
    
    used_levels <- intersect(names(values_all), as.character(unique(df$grupo)))
    
    # -------------------------- ggplot --------------------------
    p <- ggplot(
      df,
      aes(
        x    = forcats::fct_reorder(label, pct, .fun = max),
        y    = pct,
        fill = grupo
      )
    ) +
      geom_col(position = position_dodge(width = 0.7)) +
      coord_flip() +
      geom_text(
        aes(label = scales::percent(pct, accuracy = 0.1), group = grupo),
        position = position_dodge(width = 0.7),
        hjust    = -0.15,
        size     = 4.5
      ) +
      scale_y_continuous(
        labels = scales::percent_format(accuracy = 1),
        limits = c(0, max_pct + 0.1)
      ) +
      scale_fill_manual(
        values = values_all[used_levels],
        breaks = used_levels,
        labels = legend_labels[used_levels],
        drop   = FALSE
      ) +
      labs(
        title = question_title,  # pergunta como título
        x     = NULL,
        y     = NULL,
        fill  = NULL
      ) +
      theme_minimal(base_size = 16) +
      theme(
        text               = element_text(size = 16),
        axis.text.x        = element_text(size = 14),
        axis.text.y        = element_text(size = 14),
        legend.text        = element_text(size = 14),
        legend.title       = element_text(size = 14),
        legend.position    = "bottom",
        plot.title         = element_text(size = 18, face = "bold", hjust = 0.5),
        panel.grid.major.y = element_blank()
      )
    
    # -------------------------- emojis --------------------------
    df_emojis <- df %>%
      dplyr::distinct(label, img) %>%
      dplyr::filter(!is.na(img), nzchar(img))
    
    if (nrow(df_emojis) > 0) {
      # caminho de arquivo (www/emojis/...)
      df_emojis <- df_emojis %>%
        dplyr::mutate(img_fs = file.path("www", img))
      
      p <- p +
        ggimage::geom_image(
          data        = df_emojis,
          aes(x = label, y = 0, image = img_fs),
          inherit.aes = FALSE,
          size        = 0.06,
          asp         = 1.3
        )
    }
    
    p
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
  
  observeEvent(TRUE, {
    removeTab(inputId = "tabs", target = triage_tab_label())
    triage_tab_visible(FALSE)
  }, once = TRUE)

  observeEvent(TRUE, { showModal(login_modal()) }, once = TRUE)
  
  observeEvent(input$login_confirm, {
    
    if(is_local){
      email <- "contato@sensorialsports.com"
      pass  <- "senso"
      email <- "bruno.bember@sesisp.org.br"
      pass  <- "sesivolei1"
      email <- "luana@cityvida.com.br"
      pass  <- "CityVida07"
      # email <- "deise.superaonline@franquiasupera.com.br"
      # pass  <- "Cc8888"
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
      authed_email(NA_character_)
      authed(FALSE); return()
    }
    
    token <- tryCatch(tk$content$access_token, error = function(e) NULL)
    if (is.null(token) || !nzchar(token)) {
      output$login_error <- renderText("Token não recebido.")
      authed_email(NA_character_)
      authed(FALSE); return()
    }
    api_token(token)
    authed_email(email)
    
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

  observe({
    req(authed(), session_role() == "institution")
    can_see_triage <- identical(tolower(authed_email() %||% ""), "luana@cityvida.com.br")

    if (can_see_triage && !isTRUE(triage_tab_visible())) {
      insertTab(
        inputId = "tabs",
        tab = triage_tab_panel(),
        target = "Medidas Moove",
        position = "before",
        select = FALSE
      )
      triage_tab_visible(TRUE)
    }

    if (can_see_triage && !isTRUE(triage_report_tab_visible())) {
      insertTab(
        inputId = "tabs",
        tab = triage_report_tab_panel(),
        target = triage_tab_label(),
        position = "after",
        select = FALSE
      )
      triage_report_tab_visible(TRUE)
    }

    if (!can_see_triage && isTRUE(triage_report_tab_visible())) {
      removeTab(inputId = "tabs", target = triage_report_tab_label())
      triage_report_tab_visible(FALSE)
      if (identical(input$tabs, triage_report_tab_label())) {
        updateTabsetPanel(session, "tabs", selected = "Medidas Moove")
      }
    }

    if (!can_see_triage && isTRUE(triage_tab_visible())) {
      removeTab(inputId = "tabs", target = triage_tab_label())
      triage_tab_visible(FALSE)
      if (identical(input$tabs, triage_tab_label())) {
        updateTabsetPanel(session, "tabs", selected = "Medidas Moove")
      }
    }
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

  observeEvent(grouping_mode(), {
    memo_clear_all()
    cache_clear(EVAL_CACHE_PREFIX)
    eval_selected_group(NA_integer_)
    eval_selected_user(NA_integer_)
    mg_selected_group(NA_integer_)
    mg_selected_user(NA_integer_)
    perf_selected_group(NA_integer_)
    triage_selected_group(NA_integer_)
    mm_selected_group(NA_integer_)
    mm_selected_user(NA_integer_)
    eval_view_mode("groups")
    mg_view_mode("groups")
    perf_view_mode("groups")
    triage_view_mode("groups")
    mm_view_mode("groups")
    eval_page(1L)
    mg_page(1L)
    perf_page(1)
    triage_page(1L)
    mm_page(1L)
  }, ignoreInit = TRUE)
  
  # ---- cache ----
  
  observeEvent(list(authed(), session_role(), selected_institution_id()), {
    cache_clear(EVAL_CACHE_PREFIX)
  }, ignoreInit = TRUE)
  
  observeEvent(authed(), {
    if (isTRUE(authed())) memo_clear_all()
  }, ignoreInit = TRUE)
  
  observeEvent(list(api_token(), selected_institution_id()), {
    if (!is.null(api_token())) memo_clear_all()
  }, ignoreInit = TRUE)
  
  session$onSessionEnded(function(...) {
    memo_clear_all()
  })
  
  # ---- evals -----
  
  observeEvent(input$hc_eval_group_click, {
    req(authed(), session_role() == "institution", input$tabs == "Avaliações")
    clicked_name <- input$hc_eval_group_click$name
    gdf <- grouping_entities()
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
    gdf <- grouping_entities()
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
    gdf <- grouping_entities()
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

  # ---- triage and activation ----
  observeEvent(input$hc_triage_group_click, {
    clicked_name <- input$hc_triage_group_click$name
    if (identical(triage_bucket_mode(), "units")) {
      if (!is.null(clicked_name) && nzchar(as.character(clicked_name))) {
        triage_selected_unit(as.character(clicked_name))
        triage_selected_group(NA_integer_)
        triage_view_mode("group")
        triage_page(1L)
      }
    } else {
      gdf <- grouping_entities()
      gid <- gdf$id[match(clicked_name, gdf$name)]
      if (!is.na(gid)) {
        triage_selected_group(as.integer(gid))
        triage_selected_unit(NA_character_)
        triage_view_mode("group")
        triage_page(1L)
      }
    }
  })

  observeEvent(input$btn_triage_back, {
    triage_selected_group(NA_integer_)
    triage_selected_unit(NA_character_)
    triage_view_mode("groups")
    triage_page(1L)
  })

  observeEvent(triage_view_mode(), {
    triage_page(1L)
  }, ignoreInit = TRUE)

  observeEvent(input$triage_prev, {
    triage_page(clamp(triage_page() - 1L, 1L, 1e6))
  })

  observeEvent(input$triage_next, {
    triage_page(triage_page() + 1L)
  })

  observeEvent(triage_selected_group_df(), {
    d <- triage_selected_group_df()
    req(nrow(d) > 0)

    rng <- triage_date_bounds()
    updateDateInput(session, "triage_date_start", value = rng$start, min = min(d$date, na.rm = TRUE), max = max(d$date, na.rm = TRUE))
    updateDateInput(session, "triage_date_end", value = rng$end, min = min(d$date, na.rm = TRUE), max = max(d$date, na.rm = TRUE))
  }, ignoreInit = TRUE)

  observeEvent(list(triage_selected_group(), triage_sheet_df(), triage_view_mode(), triage_manager_mode(), triage_bucket_mode()), {
    req(identical(triage_view_mode(), "group"))
    req(!identical(triage_bucket_mode(), "units"))
    req(isTRUE(triage_manager_mode()))
    vals <- triage_sheet_group_thresholds()

    updateNumericInput(session, "triage_correct_yellow", value = as.numeric(vals[["triage_correct_yellow"]]))
    updateNumericInput(session, "triage_correct_red", value = as.numeric(vals[["triage_correct_red"]]))
    updateNumericInput(session, "triage_incorrect_yellow", value = as.numeric(vals[["triage_incorrect_yellow"]]))
    updateNumericInput(session, "triage_incorrect_red", value = as.numeric(vals[["triage_incorrect_red"]]))
    updateNumericInput(session, "triage_rt_yellow", value = as.numeric(vals[["triage_rt_yellow"]]))
    updateNumericInput(session, "triage_rt_red", value = as.numeric(vals[["triage_rt_red"]]))
  }, ignoreInit = TRUE)

  observeEvent(list(input$triage_date_start, input$triage_date_end), {
    if (is.null(input$triage_date_start) || is.null(input$triage_date_end)) return()
    start_date <- as.Date(input$triage_date_start)
    end_date   <- as.Date(input$triage_date_end)
    if (any(is.na(c(start_date, end_date))) || isTRUE(start_date <= end_date)) return()
    updateDateInput(session, "triage_date_end", value = start_date)
  }, ignoreInit = TRUE)

  observeEvent(input$triage_refresh, {
    req(authed(), session_role() == "institution")
    memo_clear_all()
    triage_refresh_tick(isolate(triage_refresh_tick()) + 1L)
    showNotification("Dados de Triagem e Ativação atualizados.", type = "message", duration = 3)
  }, ignoreInit = TRUE)

  observeEvent(input$triage_save_thresholds, {
    req(authed(), session_role() == "institution", isTRUE(triage_manager_mode()))
    req(!identical(triage_bucket_mode(), "units"))
    gid <- triage_selected_group()
    req(!is.na(gid))

    gdf <- grouping_entities()
    gname <- gdf$name[match(as.integer(gid), gdf$id)] %||% ""

    vals <- c(
      triage_correct_yellow   = triage_correct_yellow(),
      triage_correct_red      = triage_correct_red(),
      triage_incorrect_yellow = triage_incorrect_yellow(),
      triage_incorrect_red    = triage_incorrect_red(),
      triage_rt_yellow        = triage_rt_yellow(),
      triage_rt_red           = triage_rt_red()
    )

        ok <- save_triage_thresholds_for_group(
      sheet_id = TRIAGE_SHEET_ID,
      sheet_df = triage_sheet_df(),
      group_id = as.integer(gid),
      group_name = as.character(gname),
      values_named = vals,
      grouping_mode = grouping_mode()
    )

    if (isTRUE(ok)) {
      triage_refresh_tick(isolate(triage_refresh_tick()) + 1L)
      showNotification("Limites da triagem salvos na Google Sheet.", type = "message", duration = 4)
    } else {
      showNotification("Não foi possível salvar os limites da triagem na Google Sheet.", type = "error", duration = 6)
    }
  }, ignoreInit = TRUE)

  observeEvent(plotly::event_data("plotly_click", source = "triage_panel"), {
    ev <- plotly::event_data("plotly_click", source = "triage_panel")
    req(!is.null(ev), nrow(ev) > 0)

    available_dates <- sort(unique(triage_panel_df()$date))
    req(length(available_dates) > 0)

    raw_x <- ev$x[[1]]
    clicked_date <- suppressWarnings(as.Date(raw_x, origin = "1970-01-01"))
    if (is.na(clicked_date)) clicked_date <- suppressWarnings(as.Date(raw_x))
    if (is.na(clicked_date)) {
      clicked_num <- suppressWarnings(as.numeric(raw_x))
      if (is.finite(clicked_num)) {
        clicked_date <- as.Date(clicked_num / 86400000, origin = "1970-01-01")
      }
    }
    req(!is.na(clicked_date))

    nearest_date <- available_dates[which.min(abs(as.numeric(available_dates - clicked_date)))]
    triage_selected_click_date(as.Date(nearest_date))

    showModal(
      modalDialog(
        title = "Baixar Triagens da Data Selecionada",
        sprintf("Data selecionada: %s", format(as.Date(nearest_date), "%d/%m/%Y")),
        "Este download é separado do botão principal e baixará somente os dados desta data.",
        footer = tagList(
          modalButton("Cancelar"),
          downloadButton("download_triage_selected_date_xlsx", "Confirmar e baixar", class = "btn btn-primary")
        ),
        easyClose = TRUE,
        fade = TRUE
      )
    )
  }, ignoreInit = TRUE)
  
  
  observeEvent(input$hc_mm_group_click, {
    req(authed(), session_role() == "institution", input$tabs == "Medidas Moove")
    clicked_name <- input$hc_mm_group_click$name
    gdf <- grouping_entities()
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





