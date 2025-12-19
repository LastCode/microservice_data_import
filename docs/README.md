# Data Import Microservice

數據匯入微服務 - 使用 FastAPI 框架建構的 RESTful API 服務。

## 專案概述

這是一個 **Data Import Microservice（數據匯入微服務）**，主要功能包括：

1. **數據匯入任務管理** - 提供 RESTful API 來創建和追蹤批量數據匯入任務
2. **多數據源支援** - 可從本地文件、API、資料庫等來源讀取數據
3. **Neo4j 圖資料庫整合** - 數據持久化和血緣追蹤
4. **元數據 API** - 為前端提供下拉選單數據（domain types、domains、periods）

## 核心流程

```
用戶選擇 domain type → domain name → COB dates（營業日期）
                              ↓
                    提交匯入請求 (POST /imports)
                              ↓
                    後台執行匯入管線 (Background Task)
                              ↓
                    前端透過 workflow_id 查詢狀態
```

## 技術棧

| 類別 | 技術 |
|------|------|
| Web 框架 | FastAPI (v0.115.2) |
| ASGI 伺服器 | Uvicorn (v0.30.6) |
| 資料庫 | Neo4j (v5.23.1) |
| 資料驗證 | Pydantic (v2.9.2) |
| Python 版本 | 3.12 |

### 開發工具

- **Black** - 程式碼格式化
- **Ruff** - Linter
- **Pytest** - 單元測試
- **Mypy** - 靜態類型檢查

## 專案結構

```
microservice_data_import/
├── app/
│   ├── main.py                    # FastAPI 應用入口
│   ├── config.py                  # 配置管理（環境變量）
│   ├── api/                       # API 路由層
│   │   ├── data_import.py         # 數據匯入路由
│   │   ├── metadata.py            # 元數據路由
│   │   ├── default.py             # 健康檢查等
│   │   └── variance_analysis.py   # 方差分析（待實現）
│   ├── services/                  # 業務邏輯層
│   │   ├── import_pipeline.py     # 核心匯入管線
│   │   ├── metadata.py            # 元數據服務
│   │   └── workflows.py           # Workflow 管理
│   ├── models/                    # Pydantic 數據模型
│   │   ├── imports.py
│   │   └── workflows.py
│   └── storage/
│       └── graph.py               # Neo4j 驅動程序
├── conf/
│   ├── data_map.csv               # 域名到數據源的映射配置
│   └── settings.yaml              # 服務配置文件
├── docs/                          # 文檔
├── tests/                         # 測試
├── requirements.txt               # 依賴清單
└── .env.example                   # 環境變量範例
```

## API 端點

### 數據匯入

| 方法 | 端點 | 說明 |
|------|------|------|
| POST | `/imports` | 創建匯入任務 |
| GET | `/imports/{workflow_id}` | 查詢匯入狀態 |

### 元數據

| 方法 | 端點 | 說明 |
|------|------|------|
| GET | `/api/v1/metadata/domain-types` | 獲取域類型列表 |
| GET | `/api/v1/metadata/domains?domain_type=...` | 獲取特定類型的域名列表 |
| GET | `/api/v1/metadata/periods?domain_type=...&domain_name=...` | 獲取可用 COB 日期 |

### 其他

| 方法 | 端點 | 說明 |
|------|------|------|
| GET | `/health` | 健康檢查 |
| POST | `/api/v1/workflows` | 創建 workflow |

## 快速開始

### 環境設置

```bash
# 建立虛擬環境
python -m venv .venv
source .venv/bin/activate

# 安裝依賴
pip install -r requirements.txt

# 配置環境變量
cp .env.example .env
# 編輯 .env 設定 Neo4j 連線資訊
```

### 環境變量

| 變量名 | 說明 | 預設值 |
|--------|------|--------|
| `IMPORT_APP_NAME` | 應用名稱 | - |
| `IMPORT_UPLOAD_DIR` | 上傳目錄 | - |
| `IMPORT_NEO4J_URI` | Neo4j 連接 URI | - |
| `IMPORT_NEO4J_USER` | Neo4j 用戶名 | - |
| `IMPORT_NEO4J_PASSWORD` | Neo4j 密碼 | - |

### 啟動服務

```bash
# 開發模式（熱重載）
uvicorn app.main:app --reload

# 指定端口
uvicorn app.main:app --reload --port 8000
```

### API 文檔

啟動服務後，訪問以下地址查看自動生成的 API 文檔：

- Swagger UI: http://127.0.0.1:8000/docs
- ReDoc: http://127.0.0.1:8000/redoc

## 使用範例

### 創建匯入任務

```bash
curl -X POST http://127.0.0.1:8000/imports \
  -H "Content-Type: application/json" \
  -d '{
    "domain_type": "API",
    "domain_name": "MyDomain",
    "cob_date_1": "2024-09-01",
    "cob_date_2": "2024-09-02"
  }'
```

回應：
```json
{
  "workflow_id": "abc123...",
  "status": "pending"
}
```

### 查詢匯入狀態

```bash
curl http://127.0.0.1:8000/imports/<workflow_id>
```

回應：
```json
{
  "workflow_id": "abc123...",
  "status": "completed",
  "detail": "Import completed successfully"
}
```

### 獲取元數據

```bash
# 獲取域類型
curl http://127.0.0.1:8000/api/v1/metadata/domain-types

# 獲取域名列表
curl "http://127.0.0.1:8000/api/v1/metadata/domains?domain_type=API"

# 獲取可用日期
curl "http://127.0.0.1:8000/api/v1/metadata/periods?domain_type=API&domain_name=MyDomain"
```

## 匯入管線架構

匯入管線（ImportPipeline）包含以下步驟：

1. **ParameterValidator** - 驗證必填參數（domain_type、domain_name、cob_date）
2. **Neo4jTransactionChecker** - 檢查 Neo4j 中是否已存在該數據
3. **DataMapResolver** - 從 `conf/data_map.csv` 解析數據源配置
4. **FetcherRegistry + DataFetcher** - 根據數據源類型調用相應的 fetcher
5. **DataTransformer** - 數據清洗、欄位選擇、分檔處理
6. **DataLoader** - 建立索引、創建節點、創建關係

### Workflow 狀態

| 狀態 | 說明 |
|------|------|
| `PENDING` | 等待執行 |
| `IN_PROGRESS` | 執行中 |
| `COMPLETED` | 完成 |
| `FAILED` | 失敗 |

## 開發指南

### 程式碼品質檢查

```bash
# 格式化程式碼
black app tests

# Lint 檢查
ruff app tests

# 執行測試
pytest -q

# 類型檢查
mypy app
```

### 新增數據源 Fetcher

1. 在 `app/services/import_pipeline.py` 中創建新的 Fetcher 類
2. 實作 `fetch(source_identifier: str) -> pd.DataFrame` 方法
3. 在 `FetcherRegistry` 中註冊新的 fetcher

```python
class MyNewFetcher(DataFetcher):
    def fetch(self, source_identifier: str) -> pd.DataFrame:
        # 實作數據獲取邏輯
        pass

# 註冊
registry = FetcherRegistry()
registry.register("my_source_type", MyNewFetcher())
```

## 開發狀態

### 已完成

- API 框架和路由結構
- 參數驗證和模型定義
- 元數據查詢（從 Neo4j）
- 本地文件 Fetcher
- 異步任務執行和狀態追蹤
- Workflow ID 生成

### 待實現

- [ ] `Neo4jTransactionChecker.exists()` - Neo4j Cypher 查詢
- [ ] `DataTransformer` - 數據轉換邏輯
- [ ] `DataLoader` - Neo4j 節點和關係創建
- [ ] 方差分析 API
- [ ] 狀態持久化（目前為記憶體）
- [ ] 其他數據源 Fetcher（API、S3、資料庫等）

## License

[待補充]