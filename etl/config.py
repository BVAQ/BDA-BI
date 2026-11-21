"""
Module: config.py
Mục đích: Quản lý cấu hình tập trung cho toàn bộ Pipeline ETL.
Nguyên lý: Single Source of Truth cho các hằng số, đường dẫn và thiết lập BigQuery.
"""

import os
from pathlib import Path

# Thư mục gốc dự án
BASE_DIR = Path(__file__).resolve().parent.parent

# Đường dẫn thư mục dữ liệu các tầng (Medallion Architecture)
DATA_DIR = BASE_DIR / "data"
RAW_DATA_FILE = DATA_DIR / "DataCoSupplyChainDataset.csv"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Cấu hình Google Cloud BigQuery
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dataco-supplychain-bi")
BQ_DATASET_ID = "dataco_logistics_gold"
BQ_LOCATION = "US"

# Bảng mã đọc file gốc
RAW_FILE_ENCODING = "ISO-8859-1"

# Danh sách 5 cặp cột trùng lặp 100% trong DataCo cần loại bỏ ở Silver
REDUNDANT_COLUMNS_TO_DROP = [
    "Order Profit Per Order",      # Trùng 100% với 'Benefit per order'
    "Sales per customer",          # Trùng 100% với 'Order Item Total'
    "Product Category Id",        # Trùng 100% với 'Category Id'
    "Order Customer Id",          # Trùng 100% với 'Customer Id'
    "Order Item Cardprod Id"      # Trùng 100% với 'Product Card Id'
]

# Các cột khuyết thiếu >85% hoặc 100% rác
EMPTY_COLUMNS_TO_DROP = [
    "Product Description",        # 100% Null (180,519 / 180,519)
    "Order Zipcode"               # 86.24% Null (155,679 / 180,519)
]

# Các cột chứa thông tin định danh cá nhân nhạy cảm (PII) cần loại bỏ để bảo mật
PII_COLUMNS_TO_DROP = [
    "Customer Password",
    "Customer Email",
    "Customer Fname",
    "Customer Lname",
    "Customer Street",
    "Product Image"
]

# Từ điển chuẩn hóa tên địa danh quốc tế bị lỗi ký tự '?' do bảng mã Latin-1
CITY_REPAIR_MAP = {
    "Kahramanmara?": "Kahramanmaraş",
    "Klaip?da": "Klaipėda",
    "Bra?ov": "Brașov",
    "Gy?r": "Győr",
    "Aew?l-li": "Aewol-li",
    "Cox’s B?z?r": "Cox's Bazar",
    "Cox's B?z?r": "Cox's Bazar"
}

# Đảm bảo các thư mục tồn tại
for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)

