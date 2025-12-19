# Data Import Microservice (FastAPI)

一個專注於批次匯入的 FastAPI 服務，支援提交匯入任務（JSON 參數），在背景執行管線並可查詢 workflow 狀態；同時提供 metadata 查詢。

## 1) 快速開始
- 建立並啟用虛擬環境（Python 3.12）：`python3 -m venv .venv && source .venv/bin/activate`
- 安裝依賴（請先建立 `requirements.txt`，至少含 fastapi、uvicorn、python-multipart、pydantic、black、ruff、pytest）：`pip install -r requirements.txt`
- 啟動開發伺服器：`uvicorn app.main:app --reload`
- 健康檢查：`curl http://127.0.0.1:8000/health`

## 2) 專案結構 (建議)
```
.
├─ app/
│  ├─ main.py            # FastAPI 入口 (組裝 routers)
│  ├─ api/               # 路由分組 (imports/metadata/default)
│  ├─ services/          # 匯入管線、metadata、workflow
│  └─ storage/           # Neo4j driver helper
├─ metadata_service/     # 選擇性獨立 metadata 服務
├─ conf/data_map.csv     # domain 對應 data source 設定
├─ requirements.txt
└─ README.md
```

## 3) API 快速測試
- 提交匯入任務（背景執行管線；會依序處理 cob_date_1、cob_date_2）：  
  `curl -X POST http://127.0.0.1:8000/imports -H "Content-Type: application/json" -d '{"domain_type":"API","domain_name":"MyDomain","cob_date_1":"2024-09-01","cob_date_2":"2024-09-02"}'`
- 查詢匯入狀態：`curl http://127.0.0.1:8000/imports/<workflow_id>`
- 健康檢查：`curl http://127.0.0.1:8000/health`
- Metadata 範例：`curl "http://127.0.0.1:8000/api/v1/metadata/domains?domain_type=API"`

## 5) 開發慣例
- 格式化：`black app tests`
- Lint：`ruff app tests`
- 測試：`pytest -q`
- 型別檢查 (可選)：`mypy app`

## 6) 下一步建議
- 以 `app/services/import_pipeline.py` 為骨架補齊 Neo4j 檢查、fetcher/transformer/loader 實作並加上平行化。
- 將 workflow 狀態持久化（DB/快取）取代記憶體。
- 在 `.env` 填入 Neo4j 連線設定（參考 `.env.example`）。
