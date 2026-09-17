# Pontuação 3D-MOT / NeuroSpace usada na aba Medidas Moove.

neurospace_empty_scores <- function() {
  tibble::tibble(
    user_id = integer(),
    score_id = character(),
    template_id = integer(),
    created_at = as.POSIXct(character()),
    key = character(),
    value = numeric(),
    secondary = list()
  )
}

neurospace_as_datetime <- function(x) {
  if (inherits(x, "POSIXt")) return(as.POSIXct(x, tz = "UTC"))
  if (inherits(x, "Date")) return(as.POSIXct(x, tz = "UTC"))

  if (is.numeric(x)) {
    out <- as.POSIXct(x, origin = "1970-01-01", tz = "UTC")
    if (is.finite(x) && x > 100000000000) {
      out <- as.POSIXct(x / 1000, origin = "1970-01-01", tz = "UTC")
    }
    return(out)
  }

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

neurospace_numeric_col <- function(df, col, default = NA_real_) {
  if (!col %in% names(df)) return(rep(default, nrow(df)))
  suppressWarnings(as.numeric(df[[col]]))
}

neurospace_score_id <- function(raw, i, created_at) {
  if ("_id" %in% names(raw)) {
    id <- as.character(raw[["_id"]][[i]])
    if (!is.na(id) && nzchar(id)) return(id)
  }

  uid <- as.integer(raw[["user_id"]][[i]])
  stamp <- if (is.na(created_at)) {
    paste0("row_", i)
  } else {
    format(created_at, "%Y%m%d%H%M%OS3", tz = "UTC")
  }
  paste("neurospace_230", uid, stamp, i, sep = "_")
}

neurospace_trials_to_tibble <- function(trials) {
  if (is.null(trials) || !NROW(trials)) return(tibble::tibble())

  out <- tryCatch(
    tibble::as_tibble(trials),
    error = function(e) tibble::as_tibble(as.data.frame(trials, stringsAsFactors = FALSE))
  )

  if (!nrow(out)) return(tibble::tibble())
  out
}

neurospace_fetch_trials <- function(mongo_url, user_ids, game_id = "230") {
  ids <- unique(as.character(user_ids))
  ids <- ids[!is.na(ids) & nzchar(ids)]
  if (!length(ids) || is.null(mongo_url) || !nzchar(mongo_url)) {
    return(tibble::tibble())
  }

  moove_scores <- mongolite::mongo(collection = "moove_scores", url = mongo_url)
  on.exit(moove_scores$disconnect(), add = TRUE)

  query <- jsonlite::toJSON(
    list(user_id = list("$in" = as.list(ids)), game_id = as.character(game_id)),
    auto_unbox = TRUE
  )
  fields <- paste(
    '{"_id":1,"user_id":1,"game_id":1,"created_at":1,',
    '"date_time":1,"neuroAttempts":1}'
  )

  raw <- moove_scores$find(query = query, fields = fields)
  if (is.null(raw) || !nrow(raw) || !"neuroAttempts" %in% names(raw)) {
    return(tibble::tibble())
  }

  dplyr::bind_rows(lapply(seq_len(nrow(raw)), function(i) {
    trials <- neurospace_trials_to_tibble(raw$neuroAttempts[[i]])
    if (!nrow(trials)) return(NULL)

    created_at <- if ("created_at" %in% names(raw)) raw$created_at[[i]] else NA
    if ((length(created_at) == 0 || is.na(created_at)) && "date_time" %in% names(raw)) {
      created_at <- raw$date_time[[i]]
    }
    created_at <- neurospace_as_datetime(created_at)

    trials %>%
      dplyr::mutate(
        user_id = as.integer(raw$user_id[[i]]),
        score_id = neurospace_score_id(raw, i, created_at),
        created_at = created_at,
        neurospace_assessment_id = i,
        trial = dplyr::row_number(),
        .before = 1
      )
  }))
}

neurospace_score_assessment <- function(df) {
  if (!nrow(df)) return(NULL)

  if ("attempt" %in% names(df)) {
    df <- df %>% dplyr::arrange(.data$attempt, .data$trial)
  } else {
    df <- df %>% dplyr::arrange(.data$trial)
  }

  target_balls <- neurospace_numeric_col(df, "number_of_target_balls", 4)
  selected_balls <- neurospace_numeric_col(df, "correct_selected_balls")
  speed <- neurospace_numeric_col(df, "ball_speed")
  start_time <- neurospace_numeric_col(df, "time_start_selecting_normalized")
  finish_time <- neurospace_numeric_col(df, "time_finish_selecting_normalized")

  correct <- is.finite(selected_balls) & is.finite(target_balls) & selected_balls == target_balls
  reversal_idx <- which(correct[-1] != correct[-length(correct)]) + 1L
  reversal_speeds <- speed[reversal_idx]
  last_reversal_speeds <- tail(reversal_speeds[is.finite(reversal_speeds) & reversal_speeds > 0], 4)

  score <- if (length(last_reversal_speeds) == 4L) {
    exp(mean(log(last_reversal_speeds)))
  } else {
    NA_real_
  }

  selection_time <- finish_time - start_time
  selection_time[!is.finite(selection_time) | selection_time < 0] <- NA_real_

  tibble::tibble(
    user_id = as.integer(df$user_id[[1]]),
    score_id = as.character(df$score_id[[1]]),
    template_id = NA_integer_,
    created_at = as.POSIXct(df$created_at[[1]], tz = "UTC"),
    key = "neurospace_3dmot",
    value = as.numeric(score),
    secondary = list(list(
      "Percentual de acertos" = 100 * mean(correct),
      "Total de reversões" = length(reversal_idx),
      "Velocidade máxima" = suppressWarnings(max(speed, na.rm = TRUE)),
      "Velocidade média" = suppressWarnings(mean(speed, na.rm = TRUE)),
      "Tempo médio de seleção" = suppressWarnings(mean(selection_time, na.rm = TRUE))
    ))
  )
}

neurospace_scores_from_trials <- function(trials) {
  if (!nrow(trials)) return(neurospace_empty_scores())

  scored <- trials %>%
    dplyr::group_by(.data$user_id, .data$score_id, .data$created_at, .data$neurospace_assessment_id) %>%
    dplyr::group_split() %>%
    lapply(neurospace_score_assessment)
  scored <- scored[!vapply(scored, is.null, logical(1))]

  if (!length(scored)) return(neurospace_empty_scores())

  dplyr::bind_rows(scored) %>%
    dplyr::filter(is.finite(.data$value)) %>%
    dplyr::arrange(.data$user_id, .data$key, .data$created_at, .data$score_id)
}

get_neurospace_article_scores <- function(mongo_url, user_ids) {
  trials <- neurospace_fetch_trials(mongo_url = mongo_url, user_ids = user_ids, game_id = "230")
  neurospace_scores_from_trials(trials)
}