## Preprocess data, write TAF data tables

## Before:
## After:

library(icesTAF)

mkdir("data")

logbook <- read.taf(taf.data.path("logbook_static_gears", "logbook_static.csv"))

head(logbook)


