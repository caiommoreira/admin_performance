# Métricas da avaliação cognitiva na escala descrita pelo T.O.V.A.

COGNITIVE_ARTICLE_REFERENCES <- list(
  list(
    label = "T.O.V.A. Professional Manual",
    url = "https://files.tovatest.com/installers/development/manuals/9/Professional%20Manual.pdf"
  ),
  list(
    label = "Greenberg e Waldman (1993) - Developmental Normative Data on the T.O.V.A.",
    url = "https://doi.org/10.1111/j.1469-7610.1993.tb01105.x"
  ),
  list(
    label = "Leark, Wallace e Fitzgerald (2004) - Test-retest reliability of the T.O.V.A.",
    url = "https://doi.org/10.1177/1073191104269186"
  ),
  list(
    label = "Applicability of the T.O.V.A. in Brazilian adults",
    url = "https://pmc.ncbi.nlm.nih.gov/articles/PMC6289477/"
  )
)

COGNITIVE_ARTICLE_RULES <- c(
  "Cada documento concluído do Moove e cada sessão completa do legado/VR correspondem a uma avaliação.",
  "Os níveis 2 e 3 são divididos cronologicamente em dois quartos cada. Q1 e Q2 formam a metade 1; Q3 e Q4 formam a metade 2.",
  "Omissões e comissões são apresentadas em quantidade e percentual. Respostas antecipatórias, abaixo de 150 ms, são excluídas dos respectivos denominadores.",
  "Tempo de resposta e variabilidade usam somente respostas corretas aos estímulos-alvo. A variabilidade usa o desvio padrão populacional descrito no manual.",
  "D-prime é calculado por qnorm(taxa de acertos) - qnorm(taxa de falsos alarmes), limitando as taxas a 0,00001 e 0,99999.",
  "Os níveis 1 e 4 contêm somente estímulos-alvo. Por isso, recebem métricas próprias e taxa de acertos no lugar de d-prime. O nível 4 corresponde à mão não dominante no legado/VR.",
  "Não são aplicadas normas, escores z ou escores padronizados do T.O.V.A., pois as avaliações Moove e VR possuem quantidades de estímulos diferentes do protocolo normativo completo."
)

cognitive_empty_trials <- function() {
  tibble::tibble(
    source = character(),
    user_id = integer(),
    assessment_id = character(),
    evaluation_date = as.POSIXct(character()),
    level = integer(),
    source_block = integer(),
    trial = integer(),
    target = integer(),
    detection = integer(),
    rt_ms = numeric(),
    row_order = integer()
  )
}

cognitive_empty_scores <- function() {
  tibble::tibble(
    source = character(),
    user_id = integer(),
    assessment_id = character(),
    evaluation_date = as.POSIXct(character())
  )
}

cognitive_as_datetime <- function(x) {
  if (inherits(x, "POSIXt")) return(as.POSIXct(x, tz = "UTC"))
  suppressWarnings(as.POSIXct(
    as.character(x),
    tz = "UTC",
    tryFormats = c(
      "%Y-%m-%d %H:%M:%OS",
      "%Y-%m-%dT%H:%M:%OS",
      "%Y-%m-%dT%H:%M:%OSZ",
      "%Y-%m-%d"
    )
  ))
}

cognitive_population_sd <- function(x) {
  x <- as.numeric(x)
  x <- x[is.finite(x)]
  if (!length(x)) return(NA_real_)
  sqrt(mean((x - mean(x))^2))
}

cognitive_safe_percent <- function(numerator, denominator) {
  if (!is.finite(denominator) || denominator <= 0) return(NA_real_)
  100 * numerator / denominator
}

cognitive_d_prime <- function(hit_rate, false_alarm_rate) {
  if (!is.finite(hit_rate) || !is.finite(false_alarm_rate)) return(NA_real_)
  hit_rate <- min(max(hit_rate, 0.00001), 0.99999)
  false_alarm_rate <- min(max(false_alarm_rate, 0.00001), 0.99999)
  stats::qnorm(hit_rate) - stats::qnorm(false_alarm_rate)
}

cognitive_normalize_trials <- function(df) {
  if (is.null(df) || !nrow(df)) return(cognitive_empty_trials())

  df %>%
    dplyr::mutate(
      user_id = suppressWarnings(as.integer(.data$user_id)),
      level = suppressWarnings(as.integer(.data$level)),
      source_block = suppressWarnings(as.integer(.data$source_block)),
      trial = suppressWarnings(as.integer(.data$trial)),
      target = suppressWarnings(as.integer(.data$target)),
      detection = suppressWarnings(as.integer(.data$detection)),
      rt_ms = suppressWarnings(as.numeric(.data$rt_ms)),
      evaluation_date = cognitive_as_datetime(.data$evaluation_date),
      detection = dplyr::if_else(is.finite(.data$rt_ms) & .data$rt_ms < 150, 4L, .data$detection),
      row_order = dplyr::row_number()
    ) %>%
    dplyr::filter(
      !is.na(.data$user_id),
      !is.na(.data$level),
      .data$target %in% c(0L, 1L)
    )
}

cognitive_trials_to_tibble <- function(x) {
  while (
    is.list(x) && !inherits(x, "data.frame") && length(x) == 1L &&
      (is.null(names(x)) || all(!nzchar(names(x))))
  ) {
    x <- x[[1]]
  }

  tryCatch(
    tibble::as_tibble(x),
    error = function(e) tibble::as_tibble(as.data.frame(x, stringsAsFactors = FALSE))
  )
}
cognitive_trials_from_mongo <- function(mongo_url, user_ids) {
  ids_num <- unique(as.integer(user_ids))
  ids_num <- ids_num[is.finite(ids_num)]
  if (!length(ids_num) || is.null(mongo_url) || !nzchar(mongo_url)) {
    return(cognitive_empty_trials())
  }

  ids_chr <- as.character(ids_num)
  query <- jsonlite::toJSON(
    list(
      "parameters.conclusion" = 1,
      "$or" = list(
        list(user_id = list("$in" = as.list(ids_num))),
        list(user_id = list("$in" = as.list(ids_chr)))
      )
    ),
    auto_unbox = TRUE
  )

  scores <- mongolite::mongo(collection = "cogscore4m_scores", url = mongo_url)
  on.exit(scores$disconnect(), add = TRUE)
  raw <- scores$find(
    query = query,
    fields = '{"_id":1,"user_id":1,"created_at":1,"parameters.levels":1,"parameters.conclusion":1}'
  )

  if (is.null(raw) || !nrow(raw) || !"parameters" %in% names(raw)) {
    return(cognitive_empty_trials())
  }

  rows <- lapply(seq_len(nrow(raw)), function(i) {
    levels <- raw$parameters$levels[[i]]$series
    if (is.null(levels) || !length(levels)) return(NULL)

    assessment_id <- if ("_id" %in% names(raw)) {
      as.character(raw[["_id"]][[i]])
    } else {
      paste0("moove_", raw$user_id[[i]], "_", i)
    }

    level_rows <- lapply(seq_along(levels), function(level_no) {
      trials <- cognitive_trials_to_tibble(levels[[level_no]])
      if (!nrow(trials)) return(NULL)

      names_map <- c(
        attempt = "trial",
        stimulus_type = "target",
        response_time = "rt_ms",
        detection = "detection"
      )
      for (old_name in names(names_map)) {
        if (old_name %in% names(trials)) names(trials)[names(trials) == old_name] <- names_map[[old_name]]
      }
      if ("date_time_register" %in% names(trials)) {
        names(trials)[names(trials) == "date_time_register"] <- "trial_date"
      } else if ("created_at" %in% names(trials)) {
        names(trials)[names(trials) == "created_at"] <- "trial_date"
      }
      needed <- c("trial", "target", "rt_ms", "detection", "trial_date")
      if (!all(needed %in% names(trials))) return(NULL)

      trials %>%
        dplyr::transmute(
          source = "Moove",
          user_id = raw$user_id[[i]],
          assessment_id = assessment_id,
          evaluation_date = .data$trial_date,
          level = level_no,
          source_block = NA_integer_,
          trial = .data$trial,
          target = .data$target,
          detection = .data$detection,
          rt_ms = .data$rt_ms
        )
    })

    dplyr::bind_rows(level_rows)
  })

  out <- dplyr::bind_rows(rows)
  if (!nrow(out)) return(cognitive_empty_trials())

  out <- cognitive_normalize_trials(out)
  out %>%
    dplyr::group_by(.data$source, .data$user_id, .data$assessment_id) %>%
    dplyr::mutate(evaluation_date = min(.data$evaluation_date, na.rm = TRUE)) %>%
    dplyr::ungroup()
}

cognitive_legacy_connection <- function(config_legacy) {
  DBI::dbConnect(
    RMariaDB::MariaDB(),
    host = config_legacy[1],
    port = as.integer(config_legacy[2]),
    dbname = config_legacy[3],
    user = config_legacy[4],
    password = config_legacy[5]
  )
}

cognitive_trials_from_legacy <- function(pool, config_legacy, user_ids) {
  ids <- unique(as.integer(user_ids))
  ids <- ids[is.finite(ids)]
  if (!length(ids)) return(cognitive_empty_trials())

  mapping <- dplyr::tbl(pool, "users") %>%
    dplyr::filter(.data$id %in% !!ids, !is.na(.data$external_id), .data$external_id != "") %>%
    dplyr::select(user_id = .data$id, external_user_id = .data$external_id) %>%
    dplyr::collect() %>%
    dplyr::mutate(external_user_id = as.character(.data$external_user_id))
  if (!nrow(mapping)) return(cognitive_empty_trials())

  con <- cognitive_legacy_connection(config_legacy)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  quoted_ids <- as.character(DBI::dbQuoteString(con, mapping$external_user_id))
  sql <- paste0(
    "SELECT ID AS row_id, UserID AS external_user_id, estagio AS level, ",
    "bloco AS source_block, tentativa AS trial, alvoOuDistrator AS target, ",
    "deteccao AS detection, tempoReacao AS rt_seconds, Data AS evaluation_date ",
    "FROM ScoreTB WHERE UserID IN (", paste(quoted_ids, collapse = ","), ") ",
    "ORDER BY UserID, Data, ID"
  )
  raw <- DBI::dbGetQuery(con, sql)
  if (!nrow(raw)) return(cognitive_empty_trials())

  raw <- raw %>%
    dplyr::mutate(
      external_user_id = as.character(.data$external_user_id),
      evaluation_date = cognitive_as_datetime(.data$evaluation_date)
    ) %>%
    dplyr::left_join(mapping, by = "external_user_id") %>%
    dplyr::filter(!is.na(.data$user_id)) %>%
    dplyr::arrange(.data$user_id, .data$evaluation_date, .data$row_id) %>%
    dplyr::group_by(.data$user_id) %>%
    dplyr::mutate(
      gap_minutes = as.numeric(difftime(.data$evaluation_date, dplyr::lag(.data$evaluation_date), units = "mins")),
      new_assessment = dplyr::row_number() == 1L |
        as.Date(.data$evaluation_date) != as.Date(dplyr::lag(.data$evaluation_date)) |
        (!is.na(.data$gap_minutes) & .data$gap_minutes > 60),
      assessment_no = cumsum(dplyr::coalesce(.data$new_assessment, TRUE))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(.data$user_id, .data$assessment_no) %>%
    dplyr::mutate(
      assessment_start = min(.data$evaluation_date, na.rm = TRUE),
      assessment_id = paste0(
        "vr_", .data$user_id, "_",
        format(.data$assessment_start, "%Y%m%d%H%M%S", tz = "UTC")
      ),
      assessment_rows = dplyr::n()
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(.data$assessment_rows >= 540L) %>%
    dplyr::transmute(
      source = "Legado/VR",
      user_id = .data$user_id,
      assessment_id = .data$assessment_id,
      evaluation_date = .data$assessment_start,
      level = .data$level,
      source_block = .data$source_block,
      trial = .data$trial,
      target = .data$target,
      detection = .data$detection,
      rt_ms = 1000 * suppressWarnings(as.numeric(.data$rt_seconds))
    )

  cognitive_normalize_trials(raw)
}

cognitive_period_metrics <- function(df) {
  n_targets <- sum(df$target == 1L, na.rm = TRUE)
  n_nontargets <- sum(df$target == 0L, na.rm = TRUE)
  anticipatory <- df$detection == 4L
  anticipatory[is.na(anticipatory)] <- FALSE
  anticipatory_target <- anticipatory & df$target == 1L
  anticipatory_nontarget <- anticipatory & df$target == 0L
  hits <- df$detection == 1L & df$target == 1L & !anticipatory
  misses <- df$detection == 0L & df$target == 1L & !anticipatory
  commissions <- df$detection == 2L & df$target == 0L & !anticipatory
  correct_nonresponses <- df$detection == 3L & df$target == 0L & !anticipatory

  target_denominator <- n_targets - sum(anticipatory_target)
  nontarget_denominator <- n_nontargets - sum(anticipatory_nontarget)
  hit_rate <- if (target_denominator > 0) sum(hits) / target_denominator else NA_real_
  false_alarm_rate <- if (nontarget_denominator > 0) sum(commissions) / nontarget_denominator else NA_real_
  correct_rt <- df$rt_ms[hits & is.finite(df$rt_ms)]

  post_commission_rt <- numeric()
  commission_idx <- which(commissions)
  target_idx <- which(df$target == 1L & !anticipatory)
  if (length(commission_idx) && length(target_idx)) {
    post_target_idx <- vapply(commission_idx, function(idx) {
      candidates <- target_idx[target_idx > idx]
      if (length(candidates)) candidates[[1]] else NA_integer_
    }, integer(1))
    post_target_idx <- unique(post_target_idx[!is.na(post_target_idx)])
    post_target_idx <- post_target_idx[df$detection[post_target_idx] == 1L]
    post_commission_rt <- df$rt_ms[post_target_idx]
    post_commission_rt <- post_commission_rt[is.finite(post_commission_rt)]
  }

  c(
    omissions_n = sum(misses),
    omissions_pct = cognitive_safe_percent(sum(misses), target_denominator),
    commissions_n = sum(commissions),
    commissions_pct = cognitive_safe_percent(sum(commissions), nontarget_denominator),
    response_time_ms = if (length(correct_rt)) mean(correct_rt) else NA_real_,
    response_time_sd_ms = cognitive_population_sd(correct_rt),
    correct_responses_n = sum(hits),
    correct_nonresponses_n = sum(correct_nonresponses),
    anticipatory_n = sum(anticipatory),
    anticipatory_pct = cognitive_safe_percent(sum(anticipatory), nrow(df)),
    anticipatory_target_n = sum(anticipatory_target),
    anticipatory_nontarget_n = sum(anticipatory_nontarget),
    post_commission_n = length(post_commission_rt),
    post_commission_response_time_ms = if (length(post_commission_rt)) mean(post_commission_rt) else NA_real_,
    post_commission_response_time_sd_ms = cognitive_population_sd(post_commission_rt),
    d_prime = cognitive_d_prime(hit_rate, false_alarm_rate)
  )
}

cognitive_single_target_metrics <- function(df) {
  anticipatory <- df$detection == 4L
  anticipatory[is.na(anticipatory)] <- FALSE
  valid_target <- df$target == 1L & !anticipatory
  hits <- valid_target & df$detection == 1L
  misses <- valid_target & df$detection == 0L
  denominator <- sum(valid_target)
  correct_rt <- df$rt_ms[hits & is.finite(df$rt_ms)]

  c(
    anticipatory_n = sum(anticipatory),
    anticipatory_pct = cognitive_safe_percent(sum(anticipatory), nrow(df)),
    response_time_ms = if (length(correct_rt)) mean(correct_rt) else NA_real_,
    response_time_sd_ms = cognitive_population_sd(correct_rt),
    misses_n = sum(misses),
    misses_pct = cognitive_safe_percent(sum(misses), denominator),
    hit_rate_pct = cognitive_safe_percent(sum(hits), denominator)
  )
}

cognitive_split_level_quarters <- function(df, level_no, labels) {
  level_df <- df %>%
    dplyr::filter(.data$level == !!level_no) %>%
    dplyr::arrange(.data$source_block, .data$trial, .data$row_order)
  if (!nrow(level_df)) return(list())

  split_point <- ceiling(nrow(level_df) / 2)
  list(
    stats::setNames(list(level_df[seq_len(split_point), , drop = FALSE]), labels[[1]]),
    stats::setNames(list(level_df[seq.int(split_point + 1L, nrow(level_df)), , drop = FALSE]), labels[[2]])
  ) %>% unlist(recursive = FALSE)
}

cognitive_score_assessment <- function(df) {
  if (!nrow(df)) return(NULL)

  analysis_df <- if (identical(df$source[[1]], "Legado/VR")) {
    df %>% dplyr::filter(!(.data$level %in% c(2L, 3L) & .data$source_block == 0L))
  } else {
    df
  }

  quarters <- c(
    cognitive_split_level_quarters(analysis_df, 2L, c("q1", "q2")),
    cognitive_split_level_quarters(analysis_df, 3L, c("q3", "q4"))
  )
  if (!all(c("q1", "q2", "q3", "q4") %in% names(quarters))) return(NULL)

  periods <- c(
    quarters,
    list(
      h1 = dplyr::bind_rows(quarters$q1, quarters$q2),
      h2 = dplyr::bind_rows(quarters$q3, quarters$q4),
      total = dplyr::bind_rows(quarters$q1, quarters$q2, quarters$q3, quarters$q4)
    )
  )

  values <- c()
  for (period_name in names(periods)) {
    metrics <- cognitive_period_metrics(periods[[period_name]])
    names(metrics) <- paste(period_name, names(metrics), sep = "_")
    values <- c(values, metrics)
  }

  for (level_no in c(1L, 4L)) {
    level_df <- analysis_df %>% dplyr::filter(.data$level == !!level_no)
    metrics <- if (nrow(level_df)) cognitive_single_target_metrics(level_df) else {
      stats::setNames(rep(NA_real_, 7L), names(cognitive_single_target_metrics(analysis_df[0, , drop = FALSE])))
    }
    names(metrics) <- paste0("level", level_no, "_", names(metrics))
    values <- c(values, metrics)
  }

  identity <- tibble::tibble(
    source = as.character(df$source[[1]]),
    user_id = as.integer(df$user_id[[1]]),
    assessment_id = as.character(df$assessment_id[[1]]),
    evaluation_date = as.POSIXct(df$evaluation_date[[1]], tz = "UTC")
  )
  dplyr::bind_cols(identity, tibble::as_tibble_row(as.list(values)))
}

cognitive_scores_from_trials <- function(trials) {
  if (is.null(trials) || !nrow(trials)) return(cognitive_empty_scores())
  scored <- trials %>%
    dplyr::group_by(.data$source, .data$user_id, .data$assessment_id) %>%
    dplyr::group_split() %>%
    lapply(cognitive_score_assessment)
  scored <- scored[!vapply(scored, is.null, logical(1))]
  if (!length(scored)) return(cognitive_empty_scores())
  dplyr::bind_rows(scored) %>%
    dplyr::arrange(.data$evaluation_date, .data$user_id, .data$source)
}

get_cognitive_article_scores <- function(pool, config, user_ids) {
  moove <- tryCatch(
    cognitive_trials_from_mongo(config[6], user_ids),
    error = function(e) {
      warning("Failed to calculate Mongo cognitive assessments: ", conditionMessage(e))
      cognitive_empty_trials()
    }
  )
  legacy_user_ids <- dplyr::tbl(pool, "rdcs") %>%
    dplyr::filter(
      .data$user_id %in% !!as.integer(user_ids),
      .data$analysis_version == "v20180228"
    ) %>%
    dplyr::distinct(.data$user_id) %>%
    dplyr::collect() %>%
    dplyr::pull(.data$user_id)
  legacy <- if (length(legacy_user_ids)) {
    tryCatch(
      cognitive_trials_from_legacy(pool, getLegacyEnvConfig(), legacy_user_ids),
      error = function(e) {
        warning("Failed to calculate legacy cognitive assessments: ", conditionMessage(e))
        cognitive_empty_trials()
      }
    )
  } else {
    cognitive_empty_trials()
  }
  cognitive_scores_from_trials(dplyr::bind_rows(moove, legacy))
}
cognitive_article_export_labels <- function(columns) {
  labels <- c(
    source = "Origem",
    assessment_id = "ID da avaliação",
    evaluation_date = "Data da avaliação"
  )

  period_labels <- c(
    q1 = "Q1", q2 = "Q2", q3 = "Q3", q4 = "Q4",
    h1 = "Metade 1", h2 = "Metade 2", total = "Total"
  )
  metric_labels <- c(
    omissions_n = "Omissões (n)",
    omissions_pct = "Omissões (%)",
    commissions_n = "Comissões (n)",
    commissions_pct = "Comissões (%)",
    response_time_ms = "Tempo de resposta (ms)",
    response_time_sd_ms = "Variabilidade do tempo de resposta (ms)",
    correct_responses_n = "Respostas corretas (n)",
    correct_nonresponses_n = "Não-respostas corretas (n)",
    anticipatory_n = "Antecipatórias (n)",
    anticipatory_pct = "Antecipatórias (%)",
    anticipatory_target_n = "Antecipatórias em alvos (n)",
    anticipatory_nontarget_n = "Antecipatórias em não-alvos (n)",
    post_commission_n = "Respostas pós-comissão (n)",
    post_commission_response_time_ms = "Tempo pós-comissão (ms)",
    post_commission_response_time_sd_ms = "Variabilidade pós-comissão (ms)",
    d_prime = "D-prime"
  )

  for (period in names(period_labels)) {
    for (metric in names(metric_labels)) {
      labels[[paste(period, metric, sep = "_")]] <- paste(period_labels[[period]], metric_labels[[metric]], sep = " - ")
    }
  }

  single_labels <- c(
    anticipatory_n = "Antecipatórias (n)",
    anticipatory_pct = "Antecipatórias (%)",
    response_time_ms = "Tempo de resposta (ms)",
    response_time_sd_ms = "Variabilidade do tempo de resposta (ms)",
    misses_n = "Misses (n)",
    misses_pct = "Misses (%)",
    hit_rate_pct = "Taxa de acertos (%)"
  )
  for (level_no in c(1L, 4L)) {
    level_label <- if (level_no == 1L) "Nível 1" else "Nível 4 - mão não dominante"
    for (metric in names(single_labels)) {
      labels[[paste0("level", level_no, "_", metric)]] <- paste(level_label, single_labels[[metric]], sep = " - ")
    }
  }

  out <- columns
  matched <- match(columns, names(labels))
  out[!is.na(matched)] <- unname(labels[matched[!is.na(matched)]])
  out
}
