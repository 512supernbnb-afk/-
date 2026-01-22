rm(list=ls())
library(dplyr)
library(readr)
library(stringr)
library(jsonlite)
library(fs)

# 1. 建立測站資訊對照表 (Metadata)
stations_info <- data.frame(
  id = c("C2M910", "C0M730", "G2L020", "467480"),
  name = c("嘉義大學", "嘉義市東區", "農試嘉義分所", "嘉義"),
  lon = c(120.47779, 120.452408, 120.465684, 120.42479),
  lat = c(23.473087, 23.459414, 23.486776, 23.497684),
  stringsAsFactors = FALSE
)

# 設定資料根目錄 (請依實際情況修改)
base_dir <- "C://Users//KJChen//OneDrive//Desktop//嘉義市氣象站//data"

# 儲存所有處理後的年資料
all_yearly_data <- list()

# 2. 遍歷每個測站
for (i in 1:nrow(stations_info)) {
  st_name <- stations_info$name[i]
  st_id <- stations_info$id[i]
  
  # 搜尋該測站目錄下的所有 CSV
  # 假設路徑結構為 data/站名/xxx.csv
  target_files <- dir_ls(path = file.path(base_dir, st_name), glob = "*.csv", recurse = TRUE)
  
  if (length(target_files) == 0) {
    cat(sprintf("找不到測站 [%s] 的 CSV 檔案，跳過。\n", st_name))
    next
  }
  
  cat(sprintf("正在處理 [%s]，共 %d 個檔案...\n", st_name, length(target_files)))
  
  # 讀取並合併該測站所有 CSV
  station_raw_data <- target_files %>%
    lapply(function(f) {
      # 讀取 CSV，跳過第一行 Metadata，直接讀取 header
      # 根據您提供的範例，header 在第 2 行
      df <- read_csv(f, skip = 1, col_types = cols(.default = "c"), show_col_types = FALSE)
      
      # 提取需要的欄位：降水量(Precp)、氣溫(Temperature)
      # 確保欄位名稱正確，有些 CSV 可能是中文或英文，這裡假設是您範例中的英文
      df %>%
        select(ObsTime, Precp, Temperature) %>%
        mutate(
          # 從檔名或內容提取年份 (這裡簡單用 ObsTime 處理)
          # 假設 ObsTime 是 "1", "2"... 需配合檔名判斷年份，
          # 但更穩健的方法是解析檔名: "467480-1990-03.csv"
          year = as.integer(str_extract(basename(f), "\\d{4}")),
          month = as.integer(str_extract(basename(f), "(?<=-)\\d{2}"))
        )
    }) %>%
    bind_rows()
  
  # 3. 資料清理與計算年統計
  station_yearly <- station_raw_data %>%
    mutate(
      Precp = as.numeric(ifelse(Precp %in% c("T", "X", "/", "null"), 0, Precp)),
      Temperature = as.numeric(ifelse(Temperature %in% c("X", "/", "null"), NA, Temperature))
    ) %>%
    group_by(year) %>%
    summarise(
      total_rain = sum(Precp, na.rm = TRUE),
      avg_temp = mean(Temperature, na.rm = TRUE),
      data_count = n() # 紀錄資料筆數，可用於過濾資料不足的年份
    ) %>%
    filter(!is.na(year)) %>%
    ungroup() %>%
    mutate(
      station_id = st_id,
      station_name = st_name,
      lat = stations_info$lat[i],
      lon = stations_info$lon[i]
    )
  
  all_yearly_data[[i]] <- station_yearly
}

# 合併所有測站資料
final_df <- bind_rows(all_yearly_data)

# 4. 計算長期氣候平均值 (用於計算全域距平)
climatology <- final_df %>%
  summarise(
    global_avg_rain = mean(total_rain, na.rm = TRUE),
    global_avg_temp = mean(avg_temp, na.rm = TRUE)
  )

# 輸出成 JSON 結構
output_json <- list(
  metadata = stations_info,
  climatology = climatology, # 長期平均
  records = final_df # 歷年資料
)

write_json(output_json, "C://Users//KJChen//OneDrive//Desktop//嘉義市氣象站//datachiayi_weather_summary.json", pretty = TRUE, auto_unbox = TRUE)
cat("轉換完成！請使用 chiayi_weather_summary.json\n")