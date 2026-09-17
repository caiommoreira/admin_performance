# Pontuações originais das medidas usadas na aba Medidas Moove.
# Este arquivo concentra regras de cálculo e textos exibidos na interface.

ARTICLE_MEASURES <- tibble::tribble(
  ~key, ~template_id, ~name, ~main_label, ~range_label, ~digits, ~higher_is_good,
  "neurospace_3dmot", NA_integer_, "Avaliação de Memória de Trabalho", "Limiar de velocidade 3D-MOT", "velocidade", 2L, TRUE,
  "cfq", 15L, "Questionário de Falhas Cognitivas", "Pontuação total", "0 a 100", 0L, FALSE,
  "psqi", 18L, "Índice de Qualidade do Sono de Pittsburgh", "Índice global", "0 a 21", 0L, FALSE,
  "whoqol", 19L, "Questionário de Qualidade de Vida", "Qualidade de vida geral", "1 a 5", 0L, TRUE,
  "ipaq", 20L, "Questionário Internacional de Atividade Física", "Atividade física total", "MET-min/semana", 0L, NA,
  "pss", 21L, "Escala de Percepção de Estresse", "Pontuação total", "0 a 40", 0L, FALSE,
  "asrs", 22L, "Questionário de Foco e Autocontrole", "Pontuação total", "0 a 72", 0L, FALSE
)

ARTICLE_MEASURE_TEXTS <- list(
  neurospace_3dmot = list(
    title = "3D-MOT",
    calculation = "Cada tentativa é considerada correta quando todas as bolas-alvo são selecionadas. A medida principal é a média geométrica das velocidades nas quatro últimas reversões entre as 20 tentativas.",
    domains = "Resultados secundários: percentual de acertos, total de reversões, velocidade máxima, velocidade média e tempo médio de seleção"
  ),
  cfq = list(
    title = "Questionário de Falhas Cognitivas (CFQ-25)",
    calculation = "Somam-se as 25 respostas, pontuadas de 0 (nunca) a 4 (quase sempre). Total de 0 a 100; valores maiores representam maior frequência de falhas cognitivas.",
    domains = "Domínios de Wallace: Memória (itens 3, 6, 12, 13, 16, 17, 18 e 23; 0-32), Distração (1, 2, 3, 4, 15, 19, 21, 22 e 25; 0-36), Deslizes (5, 8, 9, 10, 11, 14 e 24; 0-28) e Nomes (7 e 20; 0-8). O item 3 participa de Memória e Distração."
  ),
  psqi = list(
    title = "Índice de Qualidade do Sono de Pittsburgh (PSQI)",
    calculation = "O índice global é a soma de sete componentes, cada um de 0 a 3: qualidade subjetiva, latência, duração, eficiência habitual, distúrbios, uso de medicação e disfunção diurna. Total de 0 a 21; valores maiores indicam pior qualidade do sono.",
    domains = "A questão qualitativa 5j não integra esta aplicação e é considerada zero no componente Distúrbios do sono. A eficiência é calculada por horas dormidas / horas na cama x 100."
  ),
  whoqol = list(
    title = "WHOQOL-bref", calculation = "A medida principal exibida é a resposta ao item 1, qualidade de vida geral, de 1 a 5. Para os domínios, os itens 3, 4 e 26 são invertidos; calcula-se a média dos itens e transforma-se o resultado para 0 a 100.",
    domains = "Domínios: Físico (7 itens), Psicológico (6), Relações sociais (3) e Meio ambiente (8). Exige-se ao menos 21 dos 26 itens e, em cada domínio, no máximo dois itens ausentes (um em Relações sociais)."
  ),
  ipaq = list(
    title = "IPAQ curto", calculation = "MET-min/semana = 8,0 x minutos x dias de atividade vigorosa + 4,0 x minutos x dias de atividade moderada + 3,3 x minutos x dias de caminhada. Atividades com menos de 10 minutos são zeradas e durações acima de 180 minutos/dia são truncadas.",
    domains = "Também são apresentados caminhada, atividade moderada, atividade vigorosa, categoria (baixa, moderada ou alta) e tempo sentado médio por dia. Protocolos com soma diária acima de 960 minutos são inválidos."
  ),
  pss = list(
    title = "Escala de Percepção de Estresse (PSS-10)", calculation = "Somam-se os dez itens, de 0 a 4. Os itens 4, 5, 7 e 8 são invertidos (4 - resposta). Total de 0 a 40; valores maiores indicam maior estresse percebido.",
    domains = "A pontuação padrão é unidimensional; não são exibidos domínios secundários."
  ),
  asrs = list(
    title = "ASRS-v1.1 (18 itens)", calculation = "Somam-se as 18 respostas, de 0 (nunca) a 4 (muito frequentemente). Total de 0 a 72; valores maiores representam maior frequência de sintomas.",
    domains = "Desatenção: itens 1 a 9 (0-36). Hiperatividade/impulsividade: itens 10 a 18 (0-36). A pontuação é informativa e não constitui diagnóstico."
  )
)


ARTICLE_MEASURE_REFERENCES <- list(
  neurospace_3dmot = list(
    list(label="Faubert (2013) - 3D-MOT e aprendizagem perceptivo-cognitiva", url="https://doi.org/10.1038/srep01154"),
    list(label="Legault, Allard e Faubert (2013) - treinamento 3D-MOT", url="https://doi.org/10.3389/fpsyg.2013.00323"),
    list(label="Protocolo 3D-MOT - 20 tentativas e média geométrica das últimas quatro reversões", url="https://pmc.ncbi.nlm.nih.gov/articles/PMC7678459/")
  ),
  cfq = list(
    list(label="Broadbent et al. (1982) - CFQ original", url="https://doi.org/10.1111/j.2044-8260.1982.tb01421.x"),
    list(label="Wallace, Kass e Stanny (2002) - quatro fatores", url="https://doi.org/10.1080/00221300209602098")
  ),
  psqi = list(
    list(label="Buysse et al. (1989) - PSQI original", url="https://doi.org/10.1016/0165-1781(89)90047-4"),
    list(label="University of Pittsburgh - instrumento e algoritmo", url="https://www.sleep.pitt.edu/research/measures-and-study-instruments")
  ),
  whoqol = list(
    list(label="World Health Organization - WHOQOL-BREF", url="https://www.who.int/tools/whoqol/whoqol-bref"),
    list(label="WHOQOL-BREF - manual de pontuação", url="https://www.who.int/docs/default-source/substance-use/who-msd-msb-00-2b.pdf")
  ),
  ipaq = list(
    list(label="IPAQ - protocolo oficial de pontuação", url="https://sites.google.com/view/ipaq/score")
  ),
  pss = list(
    list(label="Cohen, Kamarck e Mermelstein (1983) - PSS", url="https://doi.org/10.2307/2136404")
  ),
  asrs = list(
    list(label="Kessler et al. (2005) - ASRS", url="https://doi.org/10.1017/S0033291704002892"),
    list(label="Harvard Medical School - ASRS", url="https://www.hcp.med.harvard.edu/ncs/asrs.php")
  )
)

article_empty_scores <- function() {
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

article_measure_info <- function(key) {
  ARTICLE_MEASURES %>% dplyr::filter(.data$key == !!key) %>% dplyr::slice(1)
}

article_parse_answer_id <- function(x) {
  x <- as.character(x)
  out <- suppressWarnings(as.integer(sub(".*?\\\"id\\\"[[:space:]]*:[[:space:]]*([0-9]+).*", "\\1", x)))
  out[!grepl("\\\"id\\\"[[:space:]]*:", x)] <- NA_integer_
  out
}

article_number_from_text <- function(x) {
  x <- iconv(tolower(as.character(x)), from = "", to = "ASCII//TRANSLIT")
  out <- suppressWarnings(as.numeric(sub("^.*?([0-9]+([.,][0-9]+)?).*$", "\\1", x)))
  out[grepl("nenhum|nenhuma|menos que 1 hora", x)] <- 0
  out
}

article_answer_value <- function(question_id, answer_title, answer_rank, raw_value) {
  q <- as.integer(question_id)
  title <- iconv(tolower(as.character(answer_title)), from = "", to = "ASCII//TRANSLIT")
  value <- suppressWarnings(as.numeric(raw_value))
  rank0 <- as.numeric(answer_rank) - 1
  out <- dplyr::coalesce(value, rank0)

  cfq <- c("nunca" = 0, "raramente" = 1, "ocasionalmente" = 2,
           "frequentemente" = 3, "quase sempre" = 4)
  pss <- c("nunca" = 0, "quase nunca" = 1, "as vezes" = 2,
           "frequente" = 3, "muito frequente" = 4)
  asrs <- c("nunca" = 0, "raramente" = 1, "algumas vezes" = 2,
            "frequentemente" = 3, "muito frequentemente" = 4)

  idx <- q >= 51 & q <= 75 & title %in% names(cfq)
  out[idx] <- unname(cfq[title[idx]])
  idx <- q >= 182 & q <= 191 & title %in% names(pss)
  out[idx] <- unname(pss[title[idx]])
  idx <- q >= 530 & q <= 547 & title %in% names(asrs)
  out[idx] <- unname(asrs[title[idx]])

  structured <- q %in% c(139:142, 192:199)
  out[structured] <- article_number_from_text(title[structured])
  out
}

article_fetch_raw_answers <- function(pool, user_ids) {
  user_ids <- unique(as.integer(user_ids))
  user_ids <- user_ids[is.finite(user_ids)]
  if (!length(user_ids)) return(tibble::tibble())
  question_ids <- c(51:75, 139:181, 182:199, 530:547)
  sql <- sprintf(
    paste(
      "SELECT id, user_id, question_id, question_answer_id, value,",
      "did_not_answer, score_id, created_at, updated_at",
      "FROM user_question_answers",
      "WHERE user_id IN (%s) AND question_id IN (%s)"
    ), paste(user_ids, collapse = ","), paste(question_ids, collapse = ",")
  )
  raw <- DBI::dbGetQuery(pool, sql)
  if (!nrow(raw)) return(tibble::tibble())

  qa <- DBI::dbGetQuery(
    pool,
    sprintf(
      paste("SELECT id AS answer_id, question_id, title AS answer_title",
            "FROM question_answers WHERE deleted_at IS NULL AND question_id IN (%s)"),
      paste(question_ids, collapse = ",")
    )
  ) %>%
    dplyr::group_by(.data$question_id) %>%
    dplyr::arrange(.data$answer_id, .by_group = TRUE) %>%
    dplyr::mutate(answer_rank = dplyr::row_number()) %>%
    dplyr::ungroup()

  raw %>%
    dplyr::mutate(
      answer_id = article_parse_answer_id(.data$question_answer_id),
      created_at = as.POSIXct(.data$created_at),
      template_id = suppressWarnings(as.integer(sub("^.*_([0-9]+)_[0-9]+$", "\\1", .data$score_id)))
    ) %>%
    dplyr::left_join(qa, by = c("answer_id", "question_id")) %>%
    dplyr::mutate(answer = article_answer_value(.data$question_id, .data$answer_title,
                                                 .data$answer_rank, .data$value),
                  answer = dplyr::if_else(as.integer(.data$did_not_answer) == 1L,
                                          NA_real_, .data$answer))
}

article_wide_assessment <- function(df) {
  if (!nrow(df)) return(tibble::tibble())
  dates <- df %>%
    dplyr::group_by(.data$user_id, .data$score_id, .data$template_id) %>%
    dplyr::summarise(created_at = max(.data$created_at, na.rm = TRUE), .groups = "drop")
  df %>%
    dplyr::arrange(.data$id) %>%
    dplyr::group_by(.data$user_id, .data$score_id, .data$template_id, .data$question_id) %>%
    dplyr::summarise(answer = dplyr::last(.data$answer), .groups = "drop") %>%
    tidyr::pivot_wider(names_from = "question_id", values_from = "answer",
                       names_prefix = "q", values_fn = dplyr::last) %>%
    dplyr::left_join(dates, by = c("user_id", "score_id", "template_id"))
}

article_q <- function(row, ids) {
  cols <- paste0("q", ids)
  out <- rep(NA_real_, length(cols))
  present <- cols %in% names(row)
  out[present] <- as.numeric(row[1, cols[present], drop = TRUE])
  stats::setNames(out, ids)
}

article_component <- function(x, breaks) {
  if (!is.finite(x)) return(NA_real_)
  as.numeric(cut(x, breaks = breaks, labels = FALSE, right = TRUE)) - 1
}

score_article_assessment <- function(row) {
  tid <- as.integer(row$template_id[1])
  base <- list(user_id = as.integer(row$user_id[1]), score_id = as.character(row$score_id[1]),
               template_id = tid, created_at = as.POSIXct(row$created_at[1]))
  result <- NULL

  if (tid == 15L) {
    x <- article_q(row, 51:75)
    valid <- sum(is.finite(x)) == 25L
    result <- c(base, list(key = "cfq", value = if (valid) sum(x) else NA_real_,
      secondary = list(
        "Memória" = sum(x[as.character(c(53,56,62,63,66,67,68,73))]),
        "Distração" = sum(x[as.character(c(51,52,53,54,65,69,71,72,75))]),
        "Deslizes" = sum(x[as.character(c(55,58,59,60,61,64,74))]),
        "Nomes" = sum(x[as.character(c(57,70))])
      )))
  } else if (tid == 21L) {
    x <- article_q(row, 182:191)
    valid <- sum(is.finite(x)) == 10L
    x[as.character(c(185,186,188,189))] <- 4 - x[as.character(c(185,186,188,189))]
    result <- c(base, list(key = "pss", value = if (valid) sum(x) else NA_real_, secondary = list()))
  } else if (tid == 22L) {
    x <- article_q(row, 530:547)
    valid <- sum(is.finite(x)) == 18L
    result <- c(base, list(key = "asrs", value = if (valid) sum(x) else NA_real_,
      secondary = list("Desatenção" = sum(x[1:9]), "Hiperatividade/impulsividade" = sum(x[10:18]))))
  } else if (tid == 19L) {
    x <- article_q(row, 156:181)
    valid <- sum(is.finite(x)) >= 21L
    x[as.character(c(158,159,181))] <- 6 - x[as.character(c(158,159,181))]
    domain <- function(ids, minimum) {
      z <- x[as.character(ids)]
      if (sum(is.finite(z)) < minimum) return(NA_real_)
      ((mean(z, na.rm = TRUE) * 4) - 4) * 100 / 16
    }
    result <- c(base, list(key = "whoqol", value = if (valid) x["156"] else NA_real_,
      secondary = list(
        "Domínio físico" = domain(c(158,159,165,170,171,172,173), 6),
        "Domínio psicológico" = domain(c(160,161,162,166,174,181), 5),
        "Relações sociais" = domain(c(175,176,177), 2),
        "Meio ambiente" = domain(c(163,164,167,168,169,178,179,180), 6)
      )))
  } else if (tid == 18L) {
    x <- article_q(row, 139:155)
    valid <- sum(is.finite(x)) == 17L
    bedtime <- x["139"]; wake <- x["141"]
    hours_in_bed <- (wake - bedtime) %% 24
    efficiency <- if (is.finite(hours_in_bed) && hours_in_bed > 0) 100 * x["142"] / hours_in_bed else NA_real_
    c1 <- unname(x["155"])
    c2a <- dplyr::case_when(x["140"] <= 15 ~ 0, x["140"] <= 30 ~ 1, x["140"] <= 60 ~ 2, TRUE ~ 3)
    c2 <- article_component(c2a + x["143"], c(-Inf, 0, 2, 4, Inf))
    c3 <- dplyr::case_when(x["142"] > 7 ~ 0, x["142"] >= 6 ~ 1, x["142"] >= 5 ~ 2, TRUE ~ 3)
    c4 <- dplyr::case_when(efficiency > 85 ~ 0, efficiency >= 75 ~ 1, efficiency >= 65 ~ 2, TRUE ~ 3)
    c5 <- article_component(sum(x[as.character(144:151)]), c(-Inf, 0, 9, 18, Inf))
    c6 <- unname(x["152"])
    c7 <- article_component(x["153"] + x["154"], c(-Inf, 0, 2, 4, Inf))
    components <- c("Qualidade subjetiva"=c1, "Latência"=c2, "Duração"=c3,
                    "Eficiência habitual"=c4, "Distúrbios do sono"=c5,
                    "Uso de medicação"=c6, "Disfunção diurna"=c7)
    result <- c(base, list(key = "psqi", value = if (valid && all(is.finite(components))) sum(components) else NA_real_,
                           secondary = as.list(components)))
  } else if (tid == 20L) {
    x <- article_q(row, 192:199)
    active <- x[as.character(192:197)]
    valid <- all(is.finite(active))
    days <- pmin(c(x["192"], x["194"], x["196"]), 7)
    mins <- pmin(c(x["193"], x["195"], x["197"]), 180)
    mins[mins < 10] <- 0
    invalid_daily <- !valid || sum(mins, na.rm = TRUE) > 960
    mets <- c(vigorous = 8 * days[1] * mins[1], moderate = 4 * days[2] * mins[2], walking = 3.3 * days[3] * mins[3])
    total <- sum(mets)
    high <- isTRUE((mets[1] >= 1500 && days[1] >= 3) || (total >= 3000 && sum(days) >= 7))
    moderate <- isTRUE((days[1] >= 3 && mins[1] >= 20) ||
      (sum(days[2:3]) >= 5 && sum(mins[2:3]) >= 30) || (total >= 600 && sum(days) >= 5))
    category <- if (high) "Alta" else if (moderate) "Moderada" else "Baixa"
    sitting <- if (all(is.finite(x[c("198", "199")]))) (x["198"] * 5 + x["199"] * 2) * 60 / 7 else NA_real_
    result <- c(base, list(key = "ipaq", value = if (valid && !invalid_daily) total else NA_real_,
      secondary = list("Caminhada (MET-min/semana)"=unname(mets[3]), "Moderada (MET-min/semana)"=unname(mets[2]),
                       "Vigorosa (MET-min/semana)"=unname(mets[1]), "Categoria"=category,
                       "Tempo sentado médio (min/dia)"=unname(sitting))))
  }
  result
}

article_scores_from_raw <- function(raw) {
  wide <- article_wide_assessment(raw)
  if (!nrow(wide)) return(article_empty_scores())
  scored <- lapply(seq_len(nrow(wide)), function(i) score_article_assessment(wide[i, , drop = FALSE]))
  scored <- scored[!vapply(scored, is.null, logical(1))]
  if (!length(scored)) return(article_empty_scores())
  dplyr::bind_rows(lapply(scored, function(x) tibble::tibble(
    user_id=x$user_id, score_id=x$score_id, template_id=x$template_id,
    created_at=x$created_at, key=x$key, value=as.numeric(x$value), secondary=list(x$secondary)
  ))) %>%
    dplyr::filter(is.finite(.data$value)) %>%
    dplyr::arrange(.data$user_id, .data$key, .data$created_at, .data$score_id)
}

get_article_measure_scores <- function(pool, user_ids, mongo_url = NULL) {
  questionnaire_scores <- article_scores_from_raw(article_fetch_raw_answers(pool, user_ids))
  neurospace_scores <- article_empty_scores()

  if (!is.null(mongo_url) && nzchar(mongo_url) && exists("get_neurospace_article_scores", mode = "function")) {
    neurospace_scores <- tryCatch(
      get_neurospace_article_scores(mongo_url, user_ids),
      error = function(e) article_empty_scores()
    )
  }

  dplyr::bind_rows(questionnaire_scores, neurospace_scores) %>%
    dplyr::arrange(.data$user_id, .data$key, .data$created_at, .data$score_id)
}