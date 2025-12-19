# Data Import API 整合指南

如何將 Data Import Pipeline 整合到另一個 FastAPI 專案。

## 專案結構

```
your-project/
├── app/
│   ├── main.py                    # FastAPI 入口
│   ├── api/
│   │   └── data_import.py         # /imports API 路由
│   ├── models/
│   │   └── imports.py             # Pydantic 模型
│   └── services/
│       ├── __init__.py            # 匯出服務
│       ├── connectors.py          # 連接器、設定載入
│       ├── processors.py          # 欄位切割、檔案分割
│       ├── neo4j_loader.py        # Neo4j 載入
│       └── import_pipeline.py     # Pipeline 主邏輯
└── conf/
    ├── settings.yaml              # DROPBOX_DIR, Neo4j 連線
    ├── column_map.yaml            # 欄位對應設定
    └── data_map.csv               # 資料來源對應
```

## 需要複製的檔案

### 1. 核心服務 (必須)

| 檔案 | 說明 |
|------|------|
| `app/services/__init__.py` | 匯出 Pipeline 類別 |
| `app/services/connectors.py` | 檔案連接器、設定載入器 |
| `app/services/processors.py` | ColumnCutter、FileSplitter |
| `app/services/neo4j_loader.py` | Neo4j LOAD CSV |
| `app/services/import_pipeline.py` | 主要 Pipeline 邏輯 |

### 2. API 路由

| 檔案 | 說明 |
|------|------|
| `app/api/data_import.py` | `/api/v1/imports` POST/GET 端點 |
| `app/models/imports.py` | ImportStatus 模型 |

### 3. 設定檔 (必須)

| 檔案 | 說明 |
|------|------|
| `conf/settings.yaml` | DROPBOX_DIR、Neo4j 連線設定 |
| `conf/column_map.yaml` | 欄位索引、名稱、分隔符 |
| `conf/data_map.csv` | 資料來源、連線方式、路徑模板 |

## 快速複製指令

```bash
# 設定來源路徑
SRC=/Users/francis/dev_citi/microservice_data_import

# 在目標專案目錄執行
mkdir -p app/services app/api app/models conf

# 複製服務層
cp $SRC/app/services/__init__.py app/services/
cp $SRC/app/services/connectors.py app/services/
cp $SRC/app/services/processors.py app/services/
cp $SRC/app/services/neo4j_loader.py app/services/
cp $SRC/app/services/import_pipeline.py app/services/

# 複製 API 路由
cp $SRC/app/api/data_import.py app/api/

# 複製模型
cp $SRC/app/models/imports.py app/models/

# 複製設定檔
cp $SRC/conf/settings.yaml conf/
cp $SRC/conf/column_map.yaml conf/
cp $SRC/conf/data_map.csv conf/
```

## 整合到 main.py

在你的 FastAPI `main.py` 加入：

```python
from fastapi import FastAPI
from app.api.data_import import router as data_import_router

app = FastAPI(title="Your App")

# 加入 data import 路由
app.include_router(data_import_router)
```

## 依賴套件

在 `requirements.txt` 加入：

```
neo4j>=5.0.0
pyyaml>=6.0
pandas>=2.0.0  # 可選，沒有會用 CSV fallback
```

## 設定檔調整

### conf/settings.yaml

```yaml
DROPBOX_DIR: /your/project/path/mnt/nas

DATABASES:
  neo4j:
    NEO4J_URI: bolt://localhost:7687
    USER: neo4j
    PASSWORD: your_password
    DATABASE: neo4j
```

### conf/data_map.csv

修改 `source_file_path_template` 配合你的檔案路徑：

```csv
domain_type,domain_name,connector_type,connector_params,source_file_path_template
CURATED,Credit Risk Transactions,linux,"{""server_name"": ""localhost""}",/your/path/olympus_credit_txn_{cob_date}.dat
```

## API 使用方式

### 啟動 Import

```bash
curl -X POST http://localhost:9000/api/v1/imports \
  -H "Content-Type: application/json" \
  -d '{
    "domain_type": "CURATED",
    "domain_name": "Credit Risk Transactions",
    "cob_date": "2024-11-06"
  }'
```

回應：
```json
{
  "workflow_id": "abc-123-def",
  "status": "pending",
  "detail": null
}
```

### 查詢狀態

```bash
curl http://localhost:9000/api/v1/imports/{workflow_id}
```

回應：
```json
{
  "workflow_id": "abc-123-def",
  "status": "completed",
  "detail": "Successfully processed Credit Risk Transactions for 20241106"
}
```

## Neo4j 設定 (macOS)

如果使用 Neo4j Desktop，需要建立 symbolic link：

```bash
# 找到 Neo4j import 目錄
find ~/Library/Application\ Support/Neo4j\ Desktop -name "import" -type d

# 建立 link (在所有 Neo4j 資料庫)
for dir in ~/Library/Application\ Support/Neo4j\ Desktop/Application/relate-data/dbmss/*/import; do
  ln -sf /your/project/path/mnt/nas/20241106 "$dir/20241106"
done
```

## Pipeline 流程

```
POST /imports
     │
     ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Fetch     │ → │  Cut Cols   │ → │   Split     │ → │  Load Neo4j │
│  Source     │    │  (cut/py)   │    │  by GFCID   │    │   MERGE     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
     │                   │                  │                  │
     ▼                   ▼                  ▼                  ▼
 從遠端取得         提取指定欄位       按 GFCID 分割      載入 Neo4j
 原始檔案          轉換分隔符         成多個檔案         建立節點關係
```
