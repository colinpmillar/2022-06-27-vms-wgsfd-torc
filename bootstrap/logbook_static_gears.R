#' Logbook data from the ICES VMS and Logbook Database
#'
#' Logbook data from the ICES VMS and Logbook Database
#' for static gears only, from the 2021 datacall
#'
#' @name logbook_static_gears
#' @format csv file
#' @tafOriginator ICES
#' @tafYear 2021
#' @tafAccess Restricted
#' @tafSource script

library(icesTAF)
library(icesVMS)
library(icesVocab)
library(dplyr)

# get static gear data
static_gears <-
  list(
    "Active_lines" = c("LHP", "LHM", "LTL", "LVT"),
    "Passive_lines" = c("LLS", "LLD", "LL", "LX"),
    "Nets" = c("GNS", "GND", "GNC", "GTR", "GTN", "GEN"),
    "Traps" = c("FPN", "FPO", "FYK")
  )

static_gears <-
  data.frame(
    gearCode = unlist(static_gears, use.names = FALSE),
    gearGroup = rep(names(static_gears), sapply(static_gears, length))
  )


# download
# note to access download a token for the current R session using
# update_token({ices username})

# to get all years (year = 0)
logbook_static <-
  get_logbook(
    gear_code = static_gears$gearCode, year = 0#, datacall = 2021
  )

logbook_static <-
  logbook_static %>%
  select(
    year, month, icesRectangle, gearCode,
    fishingDays, totweight,
    vmsEnabled
  ) %>%
  left_join(static_gears)

# save
write.taf(logbook_static, quote = TRUE)
