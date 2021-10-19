# Telemetry: Process ringed seal dive data for Navy TAD Request
# S. Hardy, 23 April 2020

# Load libraries
library(RPostgres)
library(tidyverse)
library(sf)
library(dplyr)
library(lubridate)
library(janitor)

# Functions
`%notin%` <- Negate(`%in%`)

sequence_by_hour <- function(start, end) {
  start <- trunc(start, unit = "hours")
  end <- trunc(end, unit = "hours")
  dt <- seq(start, end, by = "hour")
  starts <- pmax(start, as.POSIXct(dt, frac = 0))
  ends <- starts + 59*60 + 59 
  unique(data.frame(start_hr = starts, end_hr = ends))
}

calculate_times_005to010 <- function(dive_times) {
  dive_times_005to010 <- dive_times %>%
    filter(mid_bottom <= 10) %>%
    mutate(min_5to10 = min_per_hour,
           min_11to30 = 0,
           min_31to50 = 0,
           min_51to70 = 0,
           min_71to100 = 0,
           min_101to150 = 0,
           min_151to200 = 0,
           min_201to250 = 0,
           min_251to300 = 0,
           min_301to400 = 0,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_011to030 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    filter(mid_bottom > 10 & mid_bottom <= 30 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/16 * percent_not_diving,
           min_11to30 = min_per_hour * percent_diving + min_per_hour * 10/16 * percent_not_diving,
           min_31to50 = 0,
           min_51to70 = 0,
           min_71to100 = 0,
           min_101to150 = 0,
           min_151to200 = 0,
           min_201to250 = 0,
           min_251to300 = 0,
           min_301to400 = 0,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_031to050 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    filter(mid_bottom > 30 & mid_bottom <= 50 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/36 * percent_not_diving,
           min_11to30 = min_per_hour * 20/36 * percent_not_diving,
           min_31to50 = min_per_hour * percent_diving + min_per_hour * 10/36 * percent_not_diving,
           min_51to70 = 0,
           min_71to100 = 0,
           min_101to150 = 0,
           min_151to200 = 0,
           min_201to250 = 0,
           min_251to300 = 0,
           min_301to400 = 0,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_051to070 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    filter(mid_bottom > 50 & mid_bottom <= 70 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/56 * percent_not_diving,
           min_11to30 = min_per_hour * 20/56 * percent_not_diving,
           min_31to50 = min_per_hour * 20/56 * percent_not_diving,
           min_51to70 = min_per_hour * percent_diving + min_per_hour * 10/56 * percent_not_diving,
           min_71to100 = 0,
           min_101to150 = 0,
           min_151to200 = 0,
           min_201to250 = 0,
           min_251to300 = 0,
           min_301to400 = 0,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_071to100 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    dplyr::filter(mid_bottom > 70 & mid_bottom <= 100 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/81 * percent_not_diving,
           min_11to30 = min_per_hour * 20/81 * percent_not_diving,
           min_31to50 = min_per_hour * 20/81 * percent_not_diving,
           min_51to70 = min_per_hour * 20/81 * percent_not_diving,
           min_71to100 = min_per_hour * percent_diving + min_per_hour * 15/81 * percent_not_diving,
           min_101to150 = 0,
           min_151to200 = 0,
           min_201to250 = 0,
           min_251to300 = 0,
           min_301to400 = 0,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_101to150 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    dplyr::filter(mid_bottom > 100 & mid_bottom <= 150 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/121 * percent_not_diving,
           min_11to30 = min_per_hour * 20/121 * percent_not_diving,
           min_31to50 = min_per_hour * 20/121 * percent_not_diving,
           min_51to70 = min_per_hour * 20/121 * percent_not_diving,
           min_71to100 = min_per_hour * 30/121 * percent_not_diving,
           min_101to150 = min_per_hour * percent_diving + min_per_hour * 25/121 * percent_not_diving,
           min_151to200 = 0,
           min_201to250 = 0,
           min_251to300 = 0,
           min_301to400 = 0,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_151to200 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    dplyr::filter(mid_bottom > 150 & mid_bottom <= 200 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/171 * percent_not_diving,
           min_11to30 = min_per_hour * 20/171 * percent_not_diving,
           min_31to50 = min_per_hour * 20/171 * percent_not_diving,
           min_51to70 = min_per_hour * 20/171 * percent_not_diving,
           min_71to100 = min_per_hour * 30/171 * percent_not_diving,
           min_101to150 = min_per_hour * 50/171 * percent_not_diving,
           min_151to200 = min_per_hour * percent_diving + min_per_hour * 25/171 * percent_not_diving,
           min_201to250 = 0,
           min_251to300 = 0,
           min_301to400 = 0,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_201to250 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    dplyr::filter(mid_bottom > 200 & mid_bottom <= 250 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/221 * percent_not_diving,
           min_11to30 = min_per_hour * 20/221 * percent_not_diving,
           min_31to50 = min_per_hour * 20/221 * percent_not_diving,
           min_51to70 = min_per_hour * 20/221 * percent_not_diving,
           min_71to100 = min_per_hour * 30/221 * percent_not_diving,
           min_101to150 = min_per_hour * 50/221 * percent_not_diving,
           min_151to200 = min_per_hour * 50/221 * percent_not_diving,
           min_201to250 = min_per_hour * percent_diving + min_per_hour * 25/221 * percent_not_diving,
           min_251to300 = 0,
           min_301to400 = 0,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_251to300 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    dplyr::filter(mid_bottom > 250 & mid_bottom <= 300 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/271 * percent_not_diving,
           min_11to30 = min_per_hour * 20/271 * percent_not_diving,
           min_31to50 = min_per_hour * 20/271 * percent_not_diving,
           min_51to70 = min_per_hour * 20/271 * percent_not_diving,
           min_71to100 = min_per_hour * 30/271 * percent_not_diving,
           min_101to150 = min_per_hour * 50/271 * percent_not_diving,
           min_151to200 = min_per_hour * 50/271 * percent_not_diving,
           min_201to250 = min_per_hour * 50/271 * percent_not_diving,
           min_251to300 = min_per_hour * percent_diving + min_per_hour * 25/271 * percent_not_diving,
           min_301to400 = 0,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_301to400 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    dplyr::filter(mid_bottom > 300 & mid_bottom <= 350 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/346 * percent_not_diving,
           min_11to30 = min_per_hour * 20/346 * percent_not_diving,
           min_31to50 = min_per_hour * 20/346 * percent_not_diving,
           min_51to70 = min_per_hour * 20/346 * percent_not_diving,
           min_71to100 = min_per_hour * 30/346 * percent_not_diving,
           min_101to150 = min_per_hour * 50/346 * percent_not_diving,
           min_151to200 = min_per_hour * 50/346 * percent_not_diving,
           min_201to250 = min_per_hour * 50/346 * percent_not_diving,
           min_251to300 = min_per_hour * 50/346 * percent_not_diving,
           min_301to400 = min_per_hour * percent_diving + min_per_hour * 50/346 * percent_not_diving,
           min_401to500 = 0,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_401to500 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    dplyr::filter(mid_bottom > 400 & mid_bottom <= 500 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/446 * percent_not_diving,
           min_11to30 = min_per_hour * 20/446 * percent_not_diving,
           min_31to50 = min_per_hour * 20/446 * percent_not_diving,
           min_51to70 = min_per_hour * 20/446 * percent_not_diving,
           min_71to100 = min_per_hour * 30/446 * percent_not_diving,
           min_101to150 = min_per_hour * 50/446 * percent_not_diving,
           min_151to200 = min_per_hour * 50/446 * percent_not_diving,
           min_201to250 = min_per_hour * 50/446 * percent_not_diving,
           min_251to300 = min_per_hour * 50/446 * percent_not_diving,
           min_301to400 = min_per_hour * 100/446 * percent_not_diving,
           min_401to500 = min_per_hour * percent_diving + min_per_hour * 50/446 * percent_not_diving,
           min_501to600 = 0,
           min_g600 = 0)
}

calculate_times_501to600 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    dplyr::filter(mid_bottom > 500 & mid_bottom <= 600 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/546 * percent_not_diving,
           min_11to30 = min_per_hour * 20/546 * percent_not_diving,
           min_31to50 = min_per_hour * 20/546 * percent_not_diving,
           min_51to70 = min_per_hour * 20/546 * percent_not_diving,
           min_71to100 = min_per_hour * 30/546 * percent_not_diving,
           min_101to150 = min_per_hour * 50/546 * percent_not_diving,
           min_151to200 = min_per_hour * 50/546 * percent_not_diving,
           min_201to250 = min_per_hour * 50/546 * percent_not_diving,
           min_251to300 = min_per_hour * 50/546 * percent_not_diving,
           min_301to400 = min_per_hour * 100/446 * percent_not_diving,
           min_401to500 = min_per_hour * 100/546 * percent_not_diving,
           min_501to600 = min_per_hour * percent_diving + min_per_hour * 50/546 * percent_not_diving,
           min_g600 = 0)
}

calculate_times_g600 <- function(dive_times, dive_shape, percent_not_diving, percent_diving) {
  dive_times %>%
    dplyr::filter(mid_bottom > 600 & shape == dive_shape) %>%
    mutate(min_5to10 = min_per_hour * 6/596 * percent_not_diving,
           min_11to30 = min_per_hour * 20/596 * percent_not_diving,
           min_31to50 = min_per_hour * 20/596 * percent_not_diving,
           min_51to70 = min_per_hour * 20/596 * percent_not_diving,
           min_71to100 = min_per_hour * 30/596 * percent_not_diving,
           min_101to150 = min_per_hour * 50/596 * percent_not_diving,
           min_151to200 = min_per_hour * 50/596 * percent_not_diving,
           min_201to250 = min_per_hour * 50/596 * percent_not_diving,
           min_251to300 = min_per_hour * 50/596 * percent_not_diving,
           min_301to400 = min_per_hour * 100/446 * percent_not_diving,
           min_401to500 = min_per_hour * 100/596 * percent_not_diving,
           min_501to600 = min_per_hour * 100/596 * percent_not_diving,
           min_g600 = min_per_hour * percent_diving)
}

# Connect to DB and get starting data
con <- RPostgres::dbConnect(Postgres(), 
                            dbname = Sys.getenv("pep_db"), 
                            host = Sys.getenv("pep_ip"), 
                            port = Sys.getenv("pep_port"),
                            user = Sys.getenv("pep_user"), 
                            password = rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_user"), sep = "")))

behav <- RPostgres::dbGetQuery(con, "SELECT b.* FROM telem.tbl_wc_behav b INNER JOIN telem.tbl_deploy d ON b.deploy_id = d.id WHERE b.deployid LIKE \'HF%\' AND start_dt >= deploy_dt AND start_dt < d.end_dt ORDER BY id")  %>%
  mutate(start_dt = lubridate::with_tz(start_dt, tz = "UTC")) %>%
  mutate(end_dt = lubridate::with_tz(end_dt, tz = "UTC")) %>%
  mutate(min_msg = difftime(end_dt, start_dt, units = "mins"))

RPostgres::dbDisconnect(con)
rm(con)

# QA/QC message, dive and surface data
messages <- behav %>%
  filter(what == "Message") %>%
  select("id", "deployid") %>%
  mutate(number = 1) %>%
  group_by(deployid) %>%
  mutate(message_id = cumsum(number)) %>% 
  ungroup %>%
  select("id", "message_id")

behav <- behav %>%
  left_join(messages, by = "id") %>%
  fill(message_id) 

rm(messages)

qa_filter <- behav %>%
  filter(duration_max > 2700 & what == "Dive") %>%
  select(id, deploy_id, message_id, message_count) %>%
  mutate(qa_status = "exclude_tooLong_dive")

qa_filter <- behav %>%
  filter(duration_max > 259200 & what == "Surface") %>%
  select(id, deploy_id, message_id, message_count) %>%
  mutate(qa_status = "exclude_tooLong_surface") %>%
  dplyr::bind_rows(qa_filter)

qa_filter <- behav %>%
  filter(duration_max > 1296000 & what == "Message") %>%
  select(id, deploy_id, message_id, message_count) %>%
  mutate(qa_status = "exclude_tooLong_message") %>%
  dplyr::bind_rows(qa_filter)

qa_filter <- qa_filter %>%
  select(deploy_id, message_id) %>%
  distinct() %>%
  mutate(error = "TRUE")

qa_overlap <- behav %>%
  filter(what != "Message") %>%
  select("id", "deploy_id", "deployid", "message_id", "message_count", "start_dt", "end_dt", 
         "what", "depth_max", "duration_max", "shallow", "deep") %>%
  mutate(prev_deployid = ifelse(!is.na(lag(deployid)), lag(deployid), deployid),
         prev_what = ifelse(!is.na(lag(deployid)), 
                            ifelse(deployid == lag(deployid), lag(what), what), 
                            what),
         prev_start = ifelse(!is.na(lag(deployid)),
                             ifelse(deployid == lag(deployid), lag(start_dt), start_dt), 
                             start_dt),
         prev_end = ifelse(!is.na(lag(deployid)), 
                           ifelse(deployid == lag(deployid), lag(end_dt), start_dt), 
                           start_dt)) %>%
  mutate(prev_start = as.POSIXct(prev_start, origin = '1970-01-01', tz = "UTC"),
         prev_end = as.POSIXct(prev_end, origin = '1970-01-01', tz = "UTC")) %>%
  mutate(start_dt2prev_end = difftime(start_dt, prev_end, units = "mins")) %>%
  mutate(start_dtU = ifelse(start_dt2prev_end <= 1 & start_dt2prev_end >= -1, prev_end, start_dt)) %>%
  mutate(start_dtU = as.POSIXct(start_dtU, origin = '1970-01-01', tz = "UTC")) %>%
  mutate(start_dtU2prev_end = difftime(start_dtU, prev_end, units = "mins"),
         flagU = ifelse(start_dtU < prev_end, "error", "okay")) %>%
  filter(flagU == "error" ) %>%
  select(deploy_id, message_id) %>%
  mutate(error = "TRUE") %>%
  distinct()

qa_remove <- rbind(qa_filter, qa_overlap) %>%
  distinct()

behav <- behav %>%
  left_join(qa_remove, by = c("deploy_id", "message_id")) %>%
  filter(is.na(error)) 

rm(qa_overlap, qa_remove, qa_filter)

# Create dataset of full hours of data for calculating TAD
msg <- behav %>%
  dplyr::filter(what == "Message")

msg_times <- msg %>%
  group_by(id) %>%
  do(sequence_by_hour(.$start_dt, .$end_dt)) %>%
  ungroup %>%
  left_join(msg, by = "id") %>%
  mutate(prev_deployid = ifelse(!is.na(lag(deployid)), lag(deployid), deployid),
         prev_what = ifelse(!is.na(lag(deployid)), 
                            ifelse(deployid == lag(deployid), lag(what), what), 
                            what),
         prev_start = ifelse(!is.na(lag(deployid)),
                             ifelse(deployid == lag(deployid), lag(start_dt), start_dt), 
                             start_dt),
         prev_end = ifelse(!is.na(lag(deployid)), 
                           ifelse(deployid == lag(deployid), lag(end_dt), start_dt), 
                           start_dt)) %>%
  mutate(prev_start = as.POSIXct(prev_start, origin = '1970-01-01', tz = "UTC"),
         prev_end = as.POSIXct(prev_end, origin = '1970-01-01', tz = "UTC")) %>%
  mutate(start_dt2prev_end = difftime(start_dt, prev_end, units = "mins")) %>%
  mutate(start_dt = ifelse(start_dt2prev_end <= 1.5 & start_dt2prev_end >= -1.5, prev_end, start_dt)) %>%
  mutate(start_dt = as.POSIXct(start_dt, origin = '1970-01-01', tz = "UTC")) %>%
  mutate(min_per_hour = ifelse(start_dt >= start_hr & end_dt <= end_hr, (end_dt - start_dt),
                               ifelse(start_dt >= start_hr & end_dt > end_hr, (end_hr - start_dt)/60,
                                      ifelse(start_dt < start_hr & end_dt <= end_hr, (end_dt - start_hr)/60, 60)))) %>%
  group_by(deployid, start_hr, end_hr) %>%
  summarize(min_per_hour = sum(min_per_hour, na.rm = TRUE)) %>%
  #dplyr::filter(min_per_hour == 60) %>%
  ungroup()

# Update behav dataset to not have overlapping times between dives and surfacings
behav_corrected <- behav %>%
  dplyr::filter(what != "Message") %>%
  mutate(prev_deployid = ifelse(!is.na(lag(deployid)), lag(deployid), deployid),
         prev_what = ifelse(!is.na(lag(deployid)), 
                            ifelse(deployid == lag(deployid), lag(what), what), 
                            what),
         prev_start = ifelse(!is.na(lag(deployid)),
                             ifelse(deployid == lag(deployid), lag(start_dt), start_dt), 
                             start_dt),
         prev_end = ifelse(!is.na(lag(deployid)), 
                           ifelse(deployid == lag(deployid), lag(end_dt), start_dt), 
                           start_dt)) %>%
  mutate(prev_start = as.POSIXct(prev_start, origin = '1970-01-01', tz = "UTC"),
         prev_end = as.POSIXct(prev_end, origin = '1970-01-01', tz = "UTC")) %>%
  mutate(start_dt2prev_end = difftime(start_dt, prev_end, units = "mins")) %>%
  mutate(start_dt = ifelse(start_dt2prev_end <= 1.5 & start_dt2prev_end >= -1.5, prev_end, start_dt)) %>%
  mutate(start_dt = as.POSIXct(start_dt, origin = '1970-01-01', tz = "UTC")) 

# Calculate time within depth bins for surface messages
sfc <- behav_corrected %>%
  filter(what == "Surface") %>%
  select("id", "deployid", "start_dt", "end_dt", "what", "min_msg", "shallow", "deep") %>%
  mutate(shallow = shallow/60, 
         deep = deep/60) %>%
  mutate(prop_shallow = shallow/(shallow + deep),
         prop_deep = deep/(shallow + deep))

sfc_times <- sfc %>%
  group_by(id) %>%
  do(sequence_by_hour(.$start_dt, .$end_dt)) %>%
  ungroup %>%
  left_join(sfc, by = "id") %>%
  mutate(min_0to4 = ifelse(start_dt >= start_hr & end_dt <= end_hr, difftime(end_dt, start_dt, units = "mins"),
                           ifelse(start_dt >= start_hr & end_dt > end_hr, difftime(end_hr, start_dt, units = "mins"),
                                  ifelse(start_dt < start_hr & end_dt <= end_hr, difftime(end_dt, start_hr, units = "mins"), 60)))) %>% 
  group_by(deployid, start_hr) %>% 
  summarize(min_0to4 = sum(min_0to4, na.rm = TRUE)) %>%
  ungroup

# Calculate time within depth bins for dive messages
dive <- behav_corrected %>%
  filter(what == "Dive") %>%
  filter(min_msg < 45) %>%
  filter(depth_max < 800) %>%
  select("id", "deployid", "start_dt", "end_dt", "what", "shape", "depth_max", "min_msg")

dive_times <- dive %>%
  group_by(id) %>%
  do(sequence_by_hour(.$start_dt, .$end_dt)) %>%
  ungroup %>%
  left_join(dive, by = "id") %>%
  mutate(min_per_hour = ifelse(start_dt >= start_hr & end_dt <= end_hr, difftime(end_dt, start_dt, units = "mins"),
                               ifelse(start_dt >= start_hr & end_dt > end_hr, difftime(end_hr, start_dt, units = "mins"),
                                      ifelse(start_dt < start_hr & end_dt <= end_hr, difftime(end_dt, start_hr, units = "mins"), 60))),
         bottom = depth_max * 0.8,
         mid_bottom = depth_max * 0.9)

### Calculate dive times: 5-10 m
dive_times_005to010 <- calculate_times_005to010(dive_times) 

### Calculate dive times: SQUARE shape
dive_times_011to030S <- calculate_times_011to030(dive_times, "Square", 0.25, 0.75) 
dive_times_031to050S <- calculate_times_031to050(dive_times, "Square", 0.25, 0.75) 
dive_times_051to070S <- calculate_times_051to070(dive_times, "Square", 0.25, 0.75) 
dive_times_071to100S <- calculate_times_071to100(dive_times, "Square", 0.25, 0.75) 
dive_times_101to150S <- calculate_times_101to150(dive_times, "Square", 0.25, 0.75) 
dive_times_151to200S <- calculate_times_151to200(dive_times, "Square", 0.25, 0.75) 
dive_times_201to250S <- calculate_times_201to250(dive_times, "Square", 0.25, 0.75) 
dive_times_251to300S <- calculate_times_251to300(dive_times, "Square", 0.25, 0.75) 
dive_times_301to400S <- calculate_times_301to400(dive_times, "Square", 0.25, 0.75) 
dive_times_401to500S <- calculate_times_401to500(dive_times, "Square", 0.25, 0.75) 
dive_times_501to600S <- calculate_times_501to600(dive_times, "Square", 0.25, 0.75) 
dive_times_g600S <- calculate_times_g600(dive_times, "Square", 0.25, 0.75) 

### Calculate dive times: U shape
dive_times_011to030U <- calculate_times_011to030(dive_times, "U", 0.65, 0.35) 
dive_times_031to050U <- calculate_times_031to050(dive_times, "U", 0.65, 0.35)  
dive_times_051to070U <- calculate_times_051to070(dive_times, "U", 0.65, 0.35)  
dive_times_071to100U <- calculate_times_071to100(dive_times, "U", 0.65, 0.35)  
dive_times_101to150U <- calculate_times_101to150(dive_times, "U", 0.65, 0.35) 
dive_times_151to200U <- calculate_times_151to200(dive_times, "U", 0.65, 0.35)  
dive_times_201to250U <- calculate_times_201to250(dive_times, "U", 0.65, 0.35)  
dive_times_251to300U <- calculate_times_251to300(dive_times, "U", 0.65, 0.35)  
dive_times_301to400U <- calculate_times_301to400(dive_times, "U", 0.65, 0.35)  
dive_times_401to500U <- calculate_times_401to500(dive_times, "U", 0.65, 0.35)  
dive_times_501to600U <- calculate_times_501to600(dive_times, "U", 0.65, 0.35) 
dive_times_g600U <- calculate_times_g600(dive_times, "U", 0.65, 0.35)  

### Calculate dive times: V shape
dive_times_011to030V <- calculate_times_011to030(dive_times, "V", 0.9, 0.1) 
dive_times_031to050V <- calculate_times_031to050(dive_times, "V", 0.9, 0.1)  
dive_times_051to070V <- calculate_times_051to070(dive_times, "V", 0.9, 0.1)  
dive_times_071to100V <- calculate_times_071to100(dive_times, "V", 0.9, 0.1)  
dive_times_101to150V <- calculate_times_101to150(dive_times, "V", 0.9, 0.1) 
dive_times_151to200V <- calculate_times_151to200(dive_times, "V", 0.9, 0.1)  
dive_times_201to250V <- calculate_times_201to250(dive_times, "V", 0.9, 0.1)  
dive_times_251to300V <- calculate_times_251to300(dive_times, "V", 0.9, 0.1)  
dive_times_301to400V <- calculate_times_301to400(dive_times, "V", 0.9, 0.1)  
dive_times_401to500V <- calculate_times_401to500(dive_times, "V", 0.9, 0.1)  
dive_times_501to600V <- calculate_times_501to600(dive_times, "V", 0.9, 0.1) 
dive_times_g600V <- calculate_times_g600(dive_times, "V", 0.9, 0.1) 

### Merge dive times data together and delete individual datasets
dive_times_calc <- rbind(dive_times_005to010, 
                         dive_times_011to030S, dive_times_011to030U, dive_times_011to030V, 
                         dive_times_031to050S, dive_times_031to050U, dive_times_031to050V,
                         dive_times_051to070S, dive_times_051to070U, dive_times_051to070V,
                         dive_times_071to100S, dive_times_071to100U, dive_times_071to100V, 
                         dive_times_101to150S, dive_times_101to150U, dive_times_101to150V, 
                         dive_times_151to200S, dive_times_151to200U, dive_times_151to200V,
                         dive_times_201to250S, dive_times_201to250U, dive_times_201to250V, 
                         dive_times_251to300S, dive_times_251to300U, dive_times_251to300V,
                         dive_times_301to400S, dive_times_301to400U, dive_times_301to400V,
                         dive_times_401to500S, dive_times_401to500U, dive_times_401to500V,
                         dive_times_501to600S, dive_times_501to600U, dive_times_501to600V,
                         dive_times_g600S, dive_times_g600U, dive_times_g600V)

rm(dive_times_005to010, 
   dive_times_011to030S, dive_times_011to030U, dive_times_011to030V, 
   dive_times_031to050S, dive_times_031to050U, dive_times_031to050V,
   dive_times_051to070S, dive_times_051to070U, dive_times_051to070V,
   dive_times_071to100S, dive_times_071to100U, dive_times_071to100V, 
   dive_times_101to150S, dive_times_101to150U, dive_times_101to150V, 
   dive_times_151to200S, dive_times_151to200U, dive_times_151to200V,
   dive_times_201to250S, dive_times_201to250U, dive_times_201to250V, 
   dive_times_251to300S, dive_times_251to300U, dive_times_251to300V,
   dive_times_301to400S, dive_times_301to400U, dive_times_301to400V,
   dive_times_401to500S, dive_times_401to500U, dive_times_401to500V,
   dive_times_501to600S, dive_times_501to600U, dive_times_501to600V,
   dive_times_g600S, dive_times_g600U, dive_times_g600V)  

### Summarize time in depth bins by hour
dive_times_calc <- dive_times_calc %>%
  group_by(deployid, start_hr) %>% 
  summarize(min_5to10 = sum(min_5to10, na.rm = TRUE),
            min_11to30 = sum(min_11to30, na.rm = TRUE),
            min_31to50 = sum(min_31to50, na.rm = TRUE),
            min_51to70 = sum(min_51to70, na.rm = TRUE),
            min_71to100 = sum(min_71to100, na.rm = TRUE),
            min_101to150 = sum(min_101to150, na.rm = TRUE),
            min_151to200 = sum(min_151to200, na.rm = TRUE),
            min_201to250 = sum(min_201to250, na.rm = TRUE),
            min_251to300 = sum(min_251to300, na.rm = TRUE),
            min_301to400 = sum(min_301to400, na.rm = TRUE),
            min_401to500 = sum(min_401to500, na.rm = TRUE),
            min_501to600 = sum(min_501to600, na.rm = TRUE),
            min_g600 = sum(min_g600, na.rm = TRUE)) %>%
  ungroup

# Create final TAD dataset
tad_times <- msg_times %>%
  left_join(sfc_times, by = c("deployid", "start_hr")) %>%
  left_join(dive_times_calc, by = c("deployid", "start_hr")) %>%
  mutate_if(is.numeric, ~replace_na(., 0)) %>%
  mutate(min_total = min_0to4 + min_5to10 + min_11to30 + min_31to50 +
           min_51to70 + min_71to100 + min_101to150 + min_151to200 +
           min_201to250 + min_251to300 + min_301to400 +
           min_401to500 + min_501to600 + min_g600)

tad <- tad_times %>%
  summarize(min_0to4 = sum(min_0to4, na.rm = TRUE),
            min_5to10 = sum(min_5to10, na.rm = TRUE),
            min_11to30 = sum(min_11to30, na.rm = TRUE),
            min_31to50 = sum(min_31to50, na.rm = TRUE),
            min_51to70 = sum(min_51to70, na.rm = TRUE),
            min_71to100 = sum(min_71to100, na.rm = TRUE),
            min_101to150 = sum(min_101to150, na.rm = TRUE),
            min_151to200 = sum(min_151to200, na.rm = TRUE),
            min_201to250 = sum(min_201to250, na.rm = TRUE),
            min_251to300 = sum(min_251to300, na.rm = TRUE),
            min_301to400 = sum(min_301to400, na.rm = TRUE),
            min_401to500 = sum(min_401to500, na.rm = TRUE),
            min_501to600 = sum(min_501to600, na.rm = TRUE),
            min_g600 = sum(min_g600, na.rm = TRUE),
            min_total = sum(min_total, na.rm = TRUE)) %>%
  mutate(pct_0to4 = min_0to4/min_total,
          pct_5to10 = min_5to10/min_total,
          pct_11to30 = min_11to30/min_total,
          pct_31to50 = min_31to50/min_total,
          pct_51to70 = min_51to70/min_total,
          pct_71to100 = min_71to100/min_total,
          pct_101to150 = min_101to150/min_total,
          pct_151to200 = min_151to200/min_total,
          pct_201to250 = min_201to250/min_total,
          pct_251to300 = min_251to300/min_total,
          pct_301to400 = min_301to400/min_total,
          pct_401to500 = min_401to500/min_total,
          pct_501to600 = min_501to600/min_total,
          pct_g600 = min_g600/min_total) 