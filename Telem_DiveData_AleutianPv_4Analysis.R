# Telemetry: Process dive data for Aleutian harbor seal analysis
# S. Hardy, 19 March 2020

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
  # ends <- starts + 59*60 + 59 
  ends <- starts + 3600 
  unique(data.frame(start_hr = starts, end_hr = ends))
}

# Connect to DB and get starting data
con <- RPostgres::dbConnect(Postgres(), 
                            dbname = Sys.getenv("pep_db"), 
                            host = Sys.getenv("pep_ip"), 
                            port = Sys.getenv("pep_port"),
                            user = Sys.getenv("pep_admin"), 
                            password = rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_admin"), sep = "")),
                            options="-c search_path=telem")

behav_start <- RPostgres::dbGetQuery(con, "SELECT b.* FROM telem.tbl_wc_behav_qa b INNER JOIN telem.tbl_deploy d ON b.deploy_id = d.id WHERE (b.deployid LIKE \'PV2014%\' OR b.deployid LIKE \'PV2015%\' OR b.deployid LIKE \'PV2016%\') AND start_dt >= deploy_dt AND start_dt < d.end_dt ORDER BY id")  %>%
  mutate(start_dt = lubridate::with_tz(start_dt, tz = "UTC")) %>%
  mutate(end_dt = lubridate::with_tz(end_dt, tz = "UTC")) 

# QA/QC message, dive and surface data
messages <- behav_start %>%
  filter(what == "Message") %>%
  select("id", "deployid") %>%
  mutate(number = 1) %>%
  group_by(deployid) %>%
  mutate(message_id = cumsum(number)) %>% 
  ungroup %>%
  select("id", "message_id")

behav_start <- behav_start %>%
  left_join(messages, by = "id") %>%
  fill(message_id)

qa_filter <- behav_start %>%
  filter(depth_max > 500) %>%
  select(id, deploy_id, message_id, message_count) %>%
  mutate(qa_status = "exclude_tooDeep")

qa_filter <- behav_start %>%
  filter(duration_max > 2700 & what == "Dive") %>%
  select(id, deploy_id, message_id, message_count) %>%
  mutate(qa_status = "exclude_tooLong_dive") %>%
  dplyr::bind_rows(qa_filter)

qa_filter <- behav_start %>%
  filter(duration_max > 259200 & what == "Surface") %>%
  select(id, deploy_id, message_id, message_count) %>%
  mutate(qa_status = "exclude_tooLong_surface") %>%
  dplyr::bind_rows(qa_filter)

qa_filter <- behav_start %>%
  filter(duration_max > 1296000 & what == "Message") %>%
  select(id, deploy_id, message_id, message_count) %>%
  mutate(qa_status = "exclude_tooLong_message") %>%
  dplyr::bind_rows(qa_filter)

qa_filter <- qa_filter %>%
  select(deploy_id, message_id) %>%
  distinct() %>%
  mutate(error = "TRUE")

behav_adj_time <- behav_start %>%
  left_join(qa_filter, by = c("deploy_id", "message_id")) %>%
  filter(is.na(error)) %>%
  filter(what != "Message") %>%
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
  mutate(min_msg = difftime(end_dt, start_dtU, units = "mins")) %>%
  mutate(start_dtU2prev_end = difftime(start_dtU, prev_end, units = "mins"),
         flagU = ifelse(start_dtU < prev_end | min_msg <= 0, "error", "okay"))

qa_filter_time <- behav_adj_time %>%
  filter(flagU == "error" ) %>%
  select(deploy_id, message_id) %>%
  distinct() %>%
  mutate(error = "TRUE")

behav_final <- behav_adj_time %>%
  select(id, deploy_id, deployid, ptt, depth_sensor, source, instr, message_count, start_dt, start_dtU, end_dt, what, number, shape, depth_min, depth_max,
         duration_min, duration_max, shallow, deep, message_id) %>%
  left_join(qa_filter_time, by = c("deploy_id", "message_id")) %>%
  filter(is.na(error)) %>%
  mutate(min_msg = difftime(end_dt, start_dtU, units = "mins")) %>%
  select(id, deploy_id, deployid, ptt, depth_sensor, source, instr, message_count, start_dt, start_dtU, end_dt, what, number, shape, depth_min, depth_max,
         duration_min, duration_max, shallow, deep, min_msg, message_id) 

rm(behav_start, behav_adj_time)

# Process dive data from behav_final
dive <- behav_final %>%
  filter(what == "Dive") %>%
  select("id", "deployid", "start_dtU", "end_dt", "what", "shape", "depth_max", "min_msg")

dive_times <- dive %>%
  group_by(id) %>%
  do(sequence_by_hour(.$start_dtU, .$end_dt)) %>%
  ungroup %>%
  left_join(dive, by = "id") %>%
  mutate(min_per_hour = ifelse(start_dtU >= start_hr & end_dt <= end_hr, difftime(end_dt, start_dtU, units = "mins"),
                               ifelse(start_dtU >= start_hr & end_dt > end_hr, difftime(end_hr, start_dtU, units = "mins"),
                                      ifelse(start_dtU < start_hr & end_dt <= end_hr, difftime(end_dt, start_hr, units = "mins"), 60)))) %>%
  filter(min_per_hour > 0)
  
# Summarize dive data
### Summarize minutes spent diving per hour
dive_summ <- dive_times %>%
  group_by(deployid, start_hr) %>% 
  summarize(minutes_diving = sum(min_per_hour, na.rm = TRUE),
            depth_min = min(depth_max, na.rm = TRUE)) %>% 
  ungroup()

### Summarize dive depth
dive_summ <- dive_times %>%
  group_by(deployid, start_hr) %>% 
  summarize(depth_median = median(depth_max, na.rm = TRUE), 
            depth_avg = mean(depth_max, na.rm = TRUE), 
            depth_sd = sd(depth_max, na.rm = TRUE),
            depth_max = max(depth_max, na.rm = TRUE)) %>% 
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr")) 

### Summarize number of dives by shape
dive_summ <- dive_times %>%
  group_by(deployid, start_hr, shape) %>% 
  tally() %>%
  spread(shape, n, fill = NA, convert = FALSE) %>%
  rename(count_square = Square,
         count_u = U, 
         count_v = V) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr"))

### Summarize dive time by depth bin
dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max <= 20, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below20m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr")) %>%
  mutate(percent_below20m = minutes_below20m/minutes_diving)

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 20 & depth_max <= 30, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below30m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr")) 

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 30 & depth_max <= 40, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below40m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr")) 

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 40 & depth_max <= 50, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below50m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr")) 

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 50 & depth_max <= 75, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below75m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr")) 

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 75 & depth_max <= 100, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below100m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr")) 

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 100 & depth_max <= 150, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below150m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr")) 

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 150 & depth_max <= 200, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below200m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr"))

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 200 & depth_max <= 250, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below250m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr"))

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 250 & depth_max <= 300, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below300m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr"))

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 300 & depth_max <= 350, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below350m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr"))

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 350 & depth_max <= 400, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_below400m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr"))

dive_summ <- dive_times %>%
  mutate(minutes_below = ifelse(depth_max > 400, min_per_hour, 0)) %>%
  group_by(deployid, start_hr) %>%
  summarize(minutes_above400m = sum(minutes_below, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr"))

### Summarize dive duration
dive_summ <- dive_times %>%
  group_by(deployid, start_hr) %>% 
  summarize(duration_min = min(min_msg, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr"))

dive_summ <- dive_times %>%
  group_by(deployid, start_hr) %>% 
  summarize(duration_median = median(min_msg, na.rm = TRUE), 
            duration_avg = mean(min_msg, na.rm = TRUE), 
            duration_sd = sd(min_msg, na.rm = TRUE),
            duration_max = max(min_msg, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr"))

### Summarize dive depth and duration by dive shape
dive_summ_shape <- dive_times %>%
  group_by(deployid, start_hr, shape) %>% 
  summarize(depth_min = min(depth_max, na.rm = TRUE)) %>%
  ungroup()

dive_summ_shape <- dive_times %>%
  group_by(deployid, start_hr, shape) %>% 
  summarize(depth_median = median(depth_max, na.rm = TRUE), 
            depth_avg = mean(depth_max, na.rm = TRUE), 
            depth_sd = sd(depth_max, na.rm = TRUE),
            depth_max = max(depth_max, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ_shape, by = c("deployid", "start_hr", "shape")) 

dive_summ_shape <- dive_times %>%
  group_by(deployid, start_hr, shape) %>% 
  summarize(duration_min = min(min_msg, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ_shape, by = c("deployid", "start_hr", "shape"))

dive_summ <- dive_times %>%
  group_by(deployid, start_hr, shape) %>% 
  summarize(duration_median = median(min_msg, na.rm = TRUE), 
            duration_avg = mean(min_msg, na.rm = TRUE), 
            duration_sd = sd(min_msg, na.rm = TRUE),
            duration_max = max(min_msg, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(dive_summ_shape, by = c("deployid", "start_hr", "shape")) %>%
  group_by(deployid, start_hr) %>% 
  pivot_wider(id_cols = c("deployid", "start_hr"), 
              names_from = shape, 
              values_from = c("depth_min", "depth_median", "depth_avg", "depth_sd", "depth_max",
                              "duration_min", "duration_median", "duration_avg", 
                              "duration_sd", "duration_max")) %>%
  ungroup() %>%
  left_join(dive_summ, by = c("deployid", "start_hr")) 

rm(dive_summ_shape)

# Process surface from behav
sfc <- behav_final %>%
  filter(what == "Surface") %>%
  select("id", "deployid", "start_dtU", "end_dt", "what", "min_msg", "shallow", "deep") %>%
  mutate(shallow = shallow/60, 
         deep = deep/60) %>%
  mutate(prop_shallow = shallow/(shallow + deep),
         prop_deep = deep/(shallow + deep))

sfc_times <- sfc %>%
  group_by(id) %>%
  do(sequence_by_hour(.$start_dtU, .$end_dt)) %>%
  ungroup %>%
  left_join(sfc, by = "id") %>%
  mutate(min_per_hour = ifelse(start_dtU >= start_hr & end_dt <= end_hr, difftime(end_dt, start_dtU, units = "mins"),
                               ifelse(start_dtU >= start_hr & end_dt > end_hr, difftime(end_hr, start_dtU, units = "mins"),
                                      ifelse(start_dtU < start_hr & end_dt <= end_hr, difftime(end_dt, start_hr, units = "mins"), 60)))) %>%
  filter(min_per_hour > 0)

# Summarize surface data
### Summarize minutes spent surfacing per hour
sfc_summ <- sfc_times %>%
  group_by(deployid, start_hr) %>% 
  summarize(minutes_surface = sum(min_per_hour, na.rm = TRUE)) %>% 
  ungroup()

sfc_summ <- sfc_times %>%
  mutate(minutes_shallow = prop_shallow * min_per_hour,
         minutes_deep = prop_deep * min_per_hour) %>%
  group_by(deployid, start_hr) %>% 
  summarize(minutes_shallow = sum(minutes_shallow, na.rm = TRUE),
            minutes_deep = sum(minutes_deep, na.rm = TRUE),) %>%
  ungroup() %>%
  left_join(sfc_summ, by = c("deployid", "start_hr"))

# Merge surface and dive summary data together, calculate final variables and clean up columns
summ <- sfc_summ %>%
  full_join(dive_summ, by = c("deployid", "start_hr")) %>%
  janitor::clean_names(case = "snake") %>%
  mutate(minutes_surface = ifelse(is.na(minutes_surface), 0, minutes_surface)) %>%
  mutate(minutes_diving = ifelse(is.na(minutes_diving), 0, minutes_diving)) %>%
  mutate(minutes_data = minutes_surface + minutes_diving) %>%
  mutate(percent_diving = minutes_diving/minutes_data) %>%
  select(deployid, start_hr, minutes_data, minutes_surface, minutes_diving, percent_diving,
         minutes_shallow, minutes_deep,
         depth_min, depth_max, depth_avg, depth_sd, depth_median,
         duration_min, duration_max, duration_avg, duration_sd, duration_median,
         minutes_below20m, minutes_below30m, minutes_below40m, minutes_below50m, minutes_below75m,
         minutes_below100m, minutes_below150m, minutes_below200m, minutes_below250m, minutes_below300m,
         minutes_below350m, minutes_below400m, minutes_above400m, percent_below20m,
         count_square, depth_min_square, depth_max_square, depth_avg_square, depth_sd_square, depth_median_square,
         duration_min_square, duration_max_square, duration_avg_square, duration_sd_square, duration_median_square,
         count_u, depth_min_u, depth_max_u, depth_avg_u, depth_sd_u, depth_median_u,
         duration_min_u, duration_max_u, duration_avg_u, duration_sd_u, duration_median_u,
         count_v, depth_min_v, depth_max_v, depth_avg_v, depth_sd_v, depth_median_v,
         duration_min_v, duration_max_v, duration_avg_v, duration_sd_v, duration_median_v
         ) %>%
  mutate(duration_min = as.numeric(duration_min)) %>%
  mutate(duration_max = as.numeric(duration_max)) %>%
  mutate(duration_avg = as.numeric(duration_avg)) %>%
  mutate(duration_sd = as.numeric(duration_sd)) %>%
  mutate(duration_median = as.numeric(duration_median)) %>%
  mutate(duration_min_square = as.numeric(duration_min_square)) %>%
  mutate(duration_max_square = as.numeric(duration_max_square)) %>%
  mutate(duration_avg_square = as.numeric(duration_avg_square)) %>%
  mutate(duration_sd_square = as.numeric(duration_sd_square)) %>%
  mutate(duration_median_square = as.numeric(duration_median_square)) %>%
  mutate(duration_min_u = as.numeric(duration_min_u)) %>%
  mutate(duration_max_u = as.numeric(duration_max_u)) %>%
  mutate(duration_avg_u = as.numeric(duration_avg_u)) %>%
  mutate(duration_sd_u = as.numeric(duration_sd_u)) %>%
  mutate(duration_median_u = as.numeric(duration_median_u)) %>%
  mutate(duration_min_v = as.numeric(duration_min_v)) %>%
  mutate(duration_max_v = as.numeric(duration_max_v)) %>%
  mutate(duration_avg_v = as.numeric(duration_avg_v)) %>%
  mutate(duration_sd_v = as.numeric(duration_sd_v)) %>%
  mutate(duration_median_v = as.numeric(duration_median_v))

summ <- as.data.frame(summ)

# Write dive results to DB
RPostgres::dbSendQuery(con, "DELETE FROM res_harborseal_dive")
RPostgres::dbWriteTable(con, "res_harborseal_dive", summ, append = TRUE, row.names = FALSE)

RPostgres::dbDisconnect(con)
rm(con)