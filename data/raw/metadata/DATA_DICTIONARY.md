# DATA DICTIONARY

## Phạm vi áp dụng

Data dictionary này mô tả schema chung cho hai file:

- `2161292_LuuQuangVu.csv`
- `2161306_MinhKhai.csv`

Mỗi dòng là **một mốc thời gian tại một trạm** theo wide format.

## Định nghĩa các cột

| column_name | data_type | unit | nullable | role | description | example | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| datetime_utc | datetime64[ns, UTC] hoặc chuỗi ISO 8601 nếu chưa parse | Mốc thời gian UTC | Không | Khóa thời gian chính | Thời điểm quan trắc ở múi giờ UTC; nên parse bằng pandas.to_datetime(..., utc=True). | 2024-01-29 17:00:00+00:00 | Nên dùng cùng location_id làm khóa ghép. |
| location_id | int64 | Mã định danh | Không | Mã trạm | Mã số duy nhất của trạm/điểm quan trắc. | 2161292 | Trong bộ hiện tại, mỗi file chỉ chứa 1 trạm. |
| location_name | string | Văn bản | Không | Tên trạm | Tên hiển thị của trạm quan trắc. | Số 46, phố Lưu Quang Vũ | Hữu ích cho trực quan hóa và báo cáo. |
| latitude | float64 | Độ thập phân | Không | Tọa độ trạm | Vĩ độ WGS84 của trạm. | 21.0152 | Gần như cố định trong từng file. |
| longitude | float64 | Độ thập phân | Không | Tọa độ trạm | Kinh độ WGS84 của trạm. | 105.7999 | Gần như cố định trong từng file. |
| CO mass µg/m³ | float64 | µg/m³ | Có | Biến ô nhiễm | Nồng độ CO theo đơn vị khối lượng. | 748.2 | Có missing; nên kiểm tra trước khi gộp đa nguồn. |
| NO₂ mass µg/m³ | float64 | µg/m³ | Có | Biến ô nhiễm | Nồng độ NO₂ theo đơn vị khối lượng. | 7.32 | Tỷ lệ thiếu khá cao trong dữ liệu hiện tại. |
| O₃ mass µg/m³ | float64 | µg/m³ | Có | Biến ô nhiễm | Nồng độ O₃ theo đơn vị khối lượng. | 26.62 | Có thể dùng như co-pollutant feature. |
| PM2.5 µg/m³ | float64 | µg/m³ | Có | Biến mục tiêu | Nồng độ bụi mịn PM2.5. | 48.63 | Ứng viên chính cho target regression. |
| SO₂ mass µg/m³ | float64 | µg/m³ | Có | Biến ô nhiễm | Nồng độ SO₂ theo đơn vị khối lượng. | 1.70 | Một số file có missing rất cao. |

## Metadata mức file/trạm

| file_name | location_id | location_name | latitude | longitude |
| --- | --- | --- | --- | --- |
| 2161292_LuuQuangVu.csv | 2161292 | Số 46, phố Lưu Quang Vũ | 21.0152 | 105.7999 |
| 2161306_MinhKhai.csv | 2161306 | Minh Khai - Bắc Từ Liêm | 21.05 | 105.74 |

## Quy tắc dữ liệu và kiểm tra

| Quy tắc | Mô tả |
|---|---|
| Khóa ghép | `datetime_utc + location_id` nên là duy nhất |
| Múi giờ | `datetime_utc` nên giữ ở UTC để dễ ghép đa nguồn |
| Phạm vi file | Mỗi file hiện tại chứa 1 trạm |
| Tọa độ | `latitude` và `longitude` nên gần như không đổi trong từng file |
| Kiểu dữ liệu số | Các cột chất ô nhiễm nên được parse thành `float64` |
| Missing values | Được phép tồn tại nhưng phải xử lý rõ trước khi train |

## Gợi ý dùng cho mô hình

| Chủ đề | Khuyến nghị |
|---|---|
| Biến mục tiêu chính | `PM2.5 µg/m³` |
| Khóa ghép với khí tượng | `datetime_utc` + tọa độ trạm hoặc `location_id` |
| Khóa ghép với vệ tinh | tọa độ trạm + quy tắc match thời gian |
| Feature baseline | CO, NO₂, O₃, SO₂, lagged PM2.5 |
| Kiểm soát chất lượng | Kiểm tra missingness, gap thời gian, giá trị âm bất hợp lý |
| Quy ước tên cột | Nên đổi sang alias ASCII-safe trong code train |

## Alias tên cột khuyến nghị

| Tên cột gốc | Alias đề xuất |
|---|---|
| `datetime_utc` | `datetime_utc` |
| `location_id` | `location_id` |
| `location_name` | `location_name` |
| `latitude` | `latitude` |
| `longitude` | `longitude` |
| `CO mass µg/m³` | `co_ugm3` |
| `NO₂ mass µg/m³` | `no2_ugm3` |
| `O₃ mass µg/m³` | `o3_ugm3` |
| `PM2.5 µg/m³` | `pm25_ugm3` |
| `SO₂ mass µg/m³` | `so2_ugm3` |

## Ví dụ import và đổi tên cột

```python
import pandas as pd

rename_map = {
    "CO mass µg/m³": "co_ugm3",
    "NO₂ mass µg/m³": "no2_ugm3",
    "O₃ mass µg/m³": "o3_ugm3",
    "PM2.5 µg/m³": "pm25_ugm3",
    "SO₂ mass µg/m³": "so2_ugm3",
}

df = pd.read_csv("2161292_LuuQuangVu.csv")
df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
df = df.rename(columns=rename_map)
```
