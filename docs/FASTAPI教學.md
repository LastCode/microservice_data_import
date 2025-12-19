# FastAPI 教學：以 Data Import Microservice 為例

本教學示範如何用 FastAPI 建立匯入微服務：提交匯入任務（JSON），背景跑管線，並可查詢 workflow 狀態；同時提供 metadata 路由。

## 1. 開發環境與安裝
- Python 3.12：`python3 -m venv .venv && source .venv/bin/activate`
- 安裝依賴：`pip install -r requirements.txt`
- 啟動服務：`uvicorn app.main:app --reload`
- 健康檢查：`curl http://127.0.0.1:8000/health`

## 2. 專案骨架
```
.env.example         # 環境變數範例（含 Neo4j）
requirements.txt     # 依賴
conf/
└─ data_map.csv      # domain 對應 data source 的設定
metadata_service/    # 選用：獨立的 metadata FastAPI 服務
├─ main.py
└─ api/metadata.py
app/
├─ main.py           # FastAPI 入口（組裝 routers）
├─ api/              # 路由分組
│  ├─ default.py     # /health、/api/v1/workflows
│  ├─ data_import.py # /imports 背景管線
│  ├─ metadata.py    # /api/v1/metadata/*
│  └─ variance_analysis.py
├─ config.py         # Pydantic Settings（含 Neo4j）
├─ models/           # Pydantic 模型
│  ├─ imports.py
│  └─ workflows.py
├─ services/         # 匯入管線、metadata、workflows
│  ├─ import_pipeline.py
│  ├─ metadata.py
│  └─ workflows.py
└─ storage/          # 外部資源
   └─ graph.py       # Neo4j driver helper
```

app/ 下的幾個folders 的用途和 convention
```


• 簡述 app/ 內各資料夾的用途與慣例：

  - app/main.py：FastAPI 入口，建立 FastAPI 實例並 include_router 組裝路由，通常不放業務邏輯。
  - app/api/：路由分組。每個檔案對應一組領域路由（如 data_import.py、metadata.py、default.py）。用 APIRouter，並在 main.py 中掛載。
  - app/services/：服務層／業務邏輯。包含匯入管線、metadata 查詢、workflow 建立等。慣例是將純邏輯與 I/O 封裝在這裡，避免直接耦合到 FastAPI。
  - app/models/：Pydantic 模型（請求/回應/內部資料結構）。命名以功能分檔，避免模型散落。
  - app/storage/：外部資源存取封裝（例如 Neo4j driver helper）。慣例是把連線/客戶端初始化與存取封裝在這裡，供 services 使用。
  - app/api/__init__.py、app/services/__init__.py：集中 export，便於 import。

  一般約定：

  - 路由層只處理輸入驗證、呼叫 service、回傳結果，避免業務邏輯。
  - services 封裝業務邏輯、I/O；storage 封裝外部連線。
  - models 放 Pydantic schema，清晰分檔。
  - 依領域/責任拆檔，避免過大檔案與交叉耦合。

```
## 3. 設定與環境變數
`config.py` 使用 `pydantic-settings`，支援：
- `IMPORT_APP_NAME`、`IMPORT_UPLOAD_DIR`
- `IMPORT_NEO4J_URI`、`IMPORT_NEO4J_USER`、`IMPORT_NEO4J_PASSWORD`
參考 `.env.example` 填入連線設定。

## 4. 主要模型
- `ImportStatus`：`workflow_id`、`status`、`detail`。
- `WorkflowCreate` / `WorkflowCreated`：工作流建立資料（含 alias 容錯）。

## 5. 匯入管線骨架（`services/import_pipeline.py`）
- 流程：參數驗證 → Neo4j 交易檢查（TODO）→ data_map 解析 → fetcher 取檔 → transformer 清洗/切欄/分檔 → loader 建索引/節點/關係（TODO）。
- 預設元件：`LocalFileFetcher`、`PassthroughTransformer`、`NoOpLoader`。`build_default_pipeline()` 會組合並使用記憶體狀態儲存。
- TODO：實作 Neo4j 檢查、實際 fetcher/transformer/loader、平行化與持久化狀態。

## 6. 路由與使用方式
- `/imports`（POST，JSON）：`{"domain_type","domain_name","cob_date_1","cob_date_2"}`，觸發背景管線；管線一次處理一個 COB，會依序跑 cob_date_1、cob_date_2，回傳 `workflow_id` 與狀態 `pending/in_progress`。
- `/imports/{workflow_id}`（GET）：查詢匯入狀態。
- `/api/v1/workflows`（POST）：建立 workflow（兼容前端欄位拼字）。
- `/api/v1/metadata/*`：domain types/domains/periods。
- `/health`：健康檢查。

### 範例請求
提交匯入任務：
```bash
curl -X POST http://127.0.0.1:8000/imports \
  -H "Content-Type: application/json" \
  -d '{"domain_type":"API","domain_name":"MyDomain","cob_date_1":"2024-09-01","cob_date_2":"2024-09-02"}'
```
查詢狀態：
```bash
curl http://127.0.0.1:8000/imports/<workflow_id>
```
Metadata：
```bash
curl "http://127.0.0.1:8000/api/v1/metadata/domains?domain_type=API"
```

## 7. 開發建議
- 依 TODO 填實 Neo4j 查詢、fetcher/transformer/loader；若需持久化，換成 DB/快取取代記憶體 dict。
- 補充測試（pytest）與 lint（ruff/black）。
- 如需獨立 metadata 服務，可啟動 `metadata_service.main:app`（預設 port 8001）。
