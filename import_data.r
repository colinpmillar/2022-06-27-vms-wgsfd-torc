# packages
library(RODBC)
library(icesTAF)
library(jsonlite)
library(dplyr)
library(icesVMS)
library(icesVocab)
library(stringi)
# settings
config <- read_json("config.json", simplifyVector = TRUE)

# create directories
mkdir(config$data_dir)

# connect to DB
conn <- odbcDriverConnect(connection = config$db_connection)

year <- c(2019, 2020, 2021)

sqlq <- "SELECT
                        [recordtype]
                        ,[country]
                        ,[year]
                        ,[month]
                        ,[ICES_rectangle]
                        ,[gear_code]
                        ,[LE_MET_level5]
                        ,[LowerMeshSize]
                        ,[UpperMeshSize]
                        ,[LE_MET_level6]
                        ,[fishing_days]
                        ,[vessel_length_category]
                        ,[vms_enabled]
                        ,[kw_fishing_days]
                        ,[totweight]
                        ,[totvalue]
                        ,[UniqueVessels]
                        ,[AnonVessels]
                        ,[importDate]
                        ,[SessionID]
                        FROM [VMS].[dbo].[_ICES_VMS_Datacall_LE] WHERE year in (2019,2020, 2021)"
# sqlq <- sprintf("SELECT * FROM [DATSU].dbo.tbl_145_21928_LE WHERE country = '%s' AND sessionID in () order by year, ICES_rectangle, gear_code, LE_MET_level6, month, vessel_length_category, fishing_days, vms_enabled", country)
fname <- paste0(config$data_dir, "/ICES_LE_", ".csv")
# sqlq <- sprintf("SELECT distinct country  FROM [DATSU].[dbo].[_ICES_VMS_Datacall_LE] order by year, ICES_rectangle, gear_code, LE_MET_level6, month, vessel_length_category, fishing_days, vms_enabled", country)
# fetch
out <- sqlQuery(conn, sqlq)
dim(out)
# save to file
write.table(out, file = fname, row.names = FALSE, col.names = TRUE, sep = ",")

#checks
names(out)

# select only the culumns we need
df_LE <- out %>% select(c("ICES_rectangle", "gear_code", "fishing_days", "vms_enabled", "totweight"))   
tibble(df_LE)

# import the shapefile with the mapping statrec -> ices areas
library(sf)
shape <- st_read(dsn = "Data/ICES_StatRec_mapto_ICES_Areas", 
    layer = "StatRec_map_Areas_Full_20170124")

# join the two df on the ices rectagle
joined_df <- left_join(df_LE, shape, 
              by = c("ICES_rectangle" = "ICESNAME"))

# select culumns we need             
df <- joined_df %>% select(c("ICES_rectangle", "gear_code", "fishing_days", "vms_enabled", "totweight","Area_27","MaxPer"))

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

# map static gear to gear codes
df_gear_grups <- left_join(df, static_gears,  by = c("gear_code" = "gearCode"))

# get rid of data not relevant to static gear codes
df_gear_grups_noNA <- df_gear_grups %>% na.omit()

# simplify area 27 column
area_simplified <- stri_extract_first_regex(c(df_gear_grups_noNA$Area_27), "[0-9]+")
area_simplified <- paste0("27.", area_simplified)
df_gear_grups_noNA$area_27_simplified <- area_simplified
#check
tibble(df_gear_grups_noNA)

#adjust totweight to MaxPer
df_gear_grups_noNA$totweight_adj <- (df_gear_grups_noNA$totweight*df_gear_grups_noNA$MaxPer)/100

# group by area_27_simplified & gearGroup and summarize based on the columns present in the tab report
df1 <- df_gear_grups_noNA %>%
    select(fishing_days, totweight, totweight_adj, area_27_simplified, gearGroup, vms_enabled) %>%
    group_by(area_27_simplified, gearGroup) %>%
    summarize(
        fishing_days_vmsYes = round(sum(fishing_days[vms_enabled == "Y"]),0),
        fishing_days_vmsNO = round(sum(fishing_days[vms_enabled == "N"]),0),
        totweight_vmsYes = round(sum(totweight[vms_enabled == "Y"]),0),
        totweight_vmsNO = round(sum(totweight[vms_enabled == "N"]),0),
        totweight_adj_vmsYes = round(sum(totweight_adj[vms_enabled == "Y"]),0),
        totweight_adj_vmsNO = round(sum(totweight_adj[vms_enabled == "N"]),0)
    ) 

# calculate percentages of fishing days with vms 
df1$perc_fish_days <-   round((df1$fishing_days_vmsYes / (df1$fishing_days_vmsYes+df1$fishing_days_vmsNO))  *100, 0)         #round(((df1$fishing_days_vmsYes / df1$fishing_days_vmsNO) / df1$fishing_days_vmsYes * 100), 0) # need to finish this
df1 <- df1 %>% relocate(perc_fish_days, .after = fishing_days_vmsNO)



# calculate percentages of totweight with vms
df1$perc_totweight <-   round((df1$totweight_vmsYes / (df1$totweight_vmsYes + df1$totweight_vmsNO))  *100, 0)

# calculate percentages of totweight adjusted for stat-rec percentages
df1$perc_totweight_adj <-   round((df1$totweight_adj_vmsYes / (df1$totweight_adj_vmsYes + df1$totweight_adj_vmsNO))  *100, 0)

df1 <- df1 %>% select(-c(totweight_vmsYes,totweight_vmsNO,perc_totweight))

# save table as csv
write.table(df1, file = "table_report_adj.csv", row.names = FALSE, col.names = TRUE, sep = ",")

