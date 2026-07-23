# README

## 1. Tổng quan

Thư mục này chứa các file CSV dữ liệu chất lượng không khí theo **mức trạm** đã được tổ chức ở dạng **wide format**.  
Mỗi dòng tương ứng với **một mốc thời gian tại một trạm**, và mỗi chất ô nhiễm được đặt trong một cột riêng.

Cấu trúc này phù hợp để làm đầu vào cho bài toán ước lượng PM2.5 bằng học máy/học sâu và thuận tiện để ghép thêm dữ liệu khí tượng hoặc đặc trưng vệ tinh trong các bước tiếp theo.

## 2. Các file hiện có

| file_name | location_id | location_name | rows | start_utc | end_utc | latitude | longitude |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2161292_LuuQuangVu.csv | 2161292 | Số 46, phố Lưu Quang Vũ | 16414 | 2024-01-29 17:00:00 UTC | 2026-04-20 23:00:00 UTC | 21.0152 | 105.7999 |
| 2161306_MinhKhai.csv | 2161306 | Minh Khai - Bắc Từ Liêm | 9653 | 2024-01-29 17:00:00 UTC | 2026-04-20 23:00:00 UTC | 21.05 | 105.74 |

Các file metadata đã tạo kèm:
- `DATA_DICTIONARY.md`: mô tả chi tiết ý nghĩa từng cột
- `DATA_DICTIONARY.csv`: data dictionary dạng máy đọc được
- `STATION_SUMMARY.csv`: bảng tóm tắt phạm vi thời gian và mức độ thiếu dữ liệu

## 3. Schema chung

Hai file CSV hiện tại cùng dùng một schema gồm 10 cột:

1. `datetime_utc`
2. `location_id`
3. `location_name`
4. `latitude`
5. `longitude`
6. `CO mass µg/m³`
7. `NO₂ mass µg/m³`
8. `O₃ mass µg/m³`
9. `PM2.5 µg/m³`
10. `SO₂ mass µg/m³`

Khóa ghép khuyến nghị:
- `datetime_utc + location_id`

Kiểm tra nhanh trên 2 file hiện tại:
- Không có dòng trùng toàn bộ
- Không có khóa `datetime_utc + location_id` bị trùng
- Mỗi file chỉ chứa 1 trạm
- `latitude` và `longitude` cố định trong từng file

## 4. Tình trạng thiếu dữ liệu

| file_name | CO_missing_% | NO2_missing_% | O3_missing_% | PM25_missing_% | SO2_missing_% |
| --- | --- | --- | --- | --- | --- |
| 2161292_LuuQuangVu.csv | 11.05 | 34.04 | 25.8 | 3.59 | 65.57 |
| 2161306_MinhKhai.csv | 31.96 | 73.66 | 19.04 | 31.07 | 48.26 |

Diễn giải nhanh:
- `PM2.5 µg/m³` nên được dùng làm biến mục tiêu chính cho bài toán hồi quy.
- `NO₂ mass µg/m³` và `SO₂ mass µg/m³` có tỷ lệ thiếu khá cao, nên cân nhắc như biến phụ hoặc áp dụng chiến lược xử lý missing rõ ràng.
- Vì dữ liệu đang ở wide format, file phù hợp cho việc tạo lag feature, rolling statistics, resampling và merge với khí tượng.

## 5. Cách dùng gợi ý trong pipeline PM2.5

### 5.1 Biến mục tiêu
- `PM2.5 µg/m³`

### 5.2 Biến đầu vào phía trạm
Có thể dùng làm feature:
- `CO mass µg/m³`
- `NO₂ mass µg/m³`
- `O₃ mass µg/m³`
- `SO₂ mass µg/m³`

### 5.3 Biến khóa để ghép đa nguồn
Giữ nguyên các cột sau:
- `datetime_utc`: trục thời gian chuẩn
- `location_id`: khóa trạm
- `latitude`, `longitude`: phục vụ ghép theo không gian

Gợi ý ghép dữ liệu:
- Ghép với feature vệ tinh theo trạm và mốc thời gian
- Ghép với khí tượng theo UTC
- Tạo thêm lag feature như `pm25_lag_1h`, `pm25_lag_3h`, `pm25_lag_24h`

## 6. Tiền xử lý khuyến nghị

1. Parse `datetime_utc` thành timezone-aware datetime ở UTC.
2. Sắp xếp theo `location_id`, `datetime_utc`.
3. Kiểm tra xem dữ liệu đã là hourly chưa; nếu chưa thì resample về giờ.
4. Với supervised learning, lọc những dòng thiếu `PM2.5 µg/m³`.
5. Đánh giá missingness trước khi quyết định:
   - xóa cột,
   - xóa dòng,
   - forward-fill,
   - interpolation,
   - hoặc dùng mô hình chịu được NaN.
6. Chuẩn hóa các biến số bằng thống kê từ train set.
7. Lưu dữ liệu đã xử lý sang thư mục `processed/` thay vì ghi đè file gốc.

## 7. Ví dụ đọc và gộp dữ liệu bằng Python

```python
import pandas as pd

files = [
    "2161292_LuuQuangVu.csv",
    "2161306_MinhKhai.csv"
]

dfs = []
for f in files:
    df = pd.read_csv(f)
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    dfs.append(df)

data = pd.concat(dfs, ignore_index=True)
data = data.sort_values(["location_id", "datetime_utc"]).reset_index(drop=True)

# Giữ lại các dòng có target PM2.5
train_df = data.dropna(subset=["PM2.5 µg/m³"]).copy()

# Ví dụ tạo lag 1 giờ
train_df["pm25_lag_1h"] = train_df.groupby("location_id")["PM2.5 µg/m³"].shift(1)

print(train_df.head())
```

## 8. Cấu trúc thư mục gợi ý

```text
project_root/
├── data/
│   ├── raw/
│   │   ├── 2161292_LuuQuangVu.csv
│   │   └── 2161306_MinhKhai.csv
│   ├── metadata/
│   │   ├── DATA_DICTIONARY.md
│   │   ├── DATA_DICTIONARY.csv
│   │   └── STATION_SUMMARY.csv
│   └── processed/
├── notebooks/
├── src/
└── README.md
```

## 9. Lưu ý thực hành

- Tên cột hiện có chứa ký tự đặc biệt và đơn vị (`µg/m³`), tốt cho báo cáo nhưng hơi bất tiện trong code.  
  Nên cân nhắc đổi sang tên thân thiện hơn như:
  - `co_ugm3`
  - `no2_ugm3`
  - `o3_ugm3`
  - `pm25_ugm3`
  - `so2_ugm3`

- Vì mỗi file hiện chỉ chứa 1 trạm, khi train mô hình đa trạm bạn nên gộp chúng thành một bảng chung theo dạng:
  - 1 dòng = 1 thời điểm × 1 trạm

- Khi mở rộng pipeline, nên document thêm các feature phát sinh như:
  - lag features,
  - rolling mean,
  - biến khí tượng,
  - feature vệ tinh,
  - missing flags,
  - QA flags.

## 10. Bối cảnh project

README này được viết để phù hợp với hướng nghiên cứu multimodal PM2.5 của bạn: kết hợp dữ liệu trạm, dữ liệu vệ tinh và khí tượng; đồng thời giữ `datetime_utc`, `location_id`, và tọa độ trạm làm nền cho bước đồng bộ hóa đa nguồn sau này.
