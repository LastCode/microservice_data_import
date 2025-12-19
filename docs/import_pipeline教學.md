# 匯入管線（Import Pipeline）教學

本文件說明如何使用 `app/services/import_pipeline.py` 的管線框架，讓匯入任務在背景執行，並由 API 追蹤 workflow 狀態。

## 1. 架構概念
- 入口路由：`POST /imports`（`app/api/data_import.py`），接收 JSON payload：`domain_type`、`domain_name`、`cob_date_1`、`cob_date_2`；管線一次只處理單一 COB，會依序跑兩次（cob_date_1、cob_date_2）。
- 管線核心：`ImportPipeline.run()`，流程為：
  1) 驗證參數 (`ParameterValidator`)  
  2) 檢查 Neo4j 是否已有交易資料 (`Neo4jTransactionChecker.exists`)  
  3) 解析 `conf/data_map.csv` 取得來源設定 (`DataMapResolver`)  
  4) 依 `source_type` 取 fetcher 下載/讀取原始檔 (`FetcherRegistry` + `DataFetcher`)  
  5) 轉換/切欄/依 GFCID 分檔 (`DataTransformer`)  
  6) 建索引/節點/關係 (`DataLoader`)  
  7) 更新狀態為 `completed` 或 `failed`
- 背景執行：`BackgroundTasks` 觸發 `_run_pipeline()`，前端透過 `/imports/{workflow_id}` 輪詢 `ImportStatus`。

## 2. 主要類別與職責
- `ImportRequest`：封裝 `domain_type`、`domain_name`、`cob_date`（單一日期）。
- `WorkflowStatus` / `WorkflowState`：工作狀態與訊息。
- `ParameterValidator`：欄位存在與日期邏輯檢查。
- `Neo4jTransactionChecker`：TODO，應實作 Cypher 查詢判斷是否已有交易資料。
- `DataMapResolver`：讀取 `conf/data_map.csv`，解析 `data_source_type`、`pyhysical_name`、`data_source_parameters`。
- `FetcherRegistry` / `DataFetcher`：依 source type 取得 fetcher。預設註冊 `LocalFileFetcher`（檢查檔案存在）。
- `DataTransformer`：TODO；預設 `PassthroughTransformer` 不做任何轉換。
- `DataLoader`：TODO；預設 `NoOpLoader` 不做任何寫入。
- `build_default_pipeline()`：組合上述元件，並接受 `status_store`（用於查詢狀態）。

## 3. API 如何啟動背景任務
- 路由：`app/api/data_import.py`
  - `ImportJobRequest` 驗證輸入。
  - `create_import()` 產生 `workflow_id`，先寫入狀態為 `pending`，呼叫 `background_tasks.add_task(_run_pipeline, workflow_id, payload)`。
  - `_run_pipeline()` 將 payload 轉為 `ImportRequest`，呼叫 `pipeline.run()`。成功會更新狀態為 `done/completed` 等；發生例外會標記 `failed` 並記錄錯誤訊息。
  - 查詢：`GET /imports/{workflow_id}` 回傳最新的 `ImportStatus`。

## 4. 如何接上實作
- Neo4j 檢查：在 `Neo4jTransactionChecker.exists` 實作 Cypher，例如查某 domain/cob 的交易節點數量 > 0。
- Fetcher：依 `data_source_type` 實作對應 fetcher（S3、HTTP、DB 等），並用 `fetcher_registry.register("<type>", MyFetcher())`。
- Transformer：實作 `clean` / `cut_columns` / `split_by_gfcid`。
- Loader：實作 `ensure_indexes_and_constraints` / `create_nodes` / `create_relationships`，可用平行化方式。
- 狀態儲存：目前為記憶體 dict；若需持久化，換成 DB 或快取（Redis）。

## 5. 測試路徑
- 啟動主服務：`uvicorn app.main:app --reload --port 8000`
- 建立匯入任務：
  ```bash
  curl -X POST http://127.0.0.1:8000/imports \
    -H "Content-Type: application/json" \
    -d '{"domain_type":"API","domain_name":"MyDomain","cob_date_1":"2024-09-01","cob_date_2":"2024-09-02"}'
  ```
  回應包含 `workflow_id`。
- 查詢狀態：
  ```bash
  curl http://127.0.0.1:8000/imports/<workflow_id>
  ```

## 6. 備註
- `/imports` 現在只接受 JSON 參數並觸發管線，不處理直接上傳檔案。
- 任何舊的檔案上傳 helper（如 `save_file`）已移除。
