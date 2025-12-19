# Todo
我這裡要做的是data import的功能，主要包含以下幾個部分：

## 流程：
a. User 從的UI domain_type dropdown選取 domain_type
b. User 從的UI domain_name dropdown選取 domain_name（這個 list 是根據 domain_type 動態載入的）
c. User 從的UI period dropdown選取 COB date 1（這里需要提供一個 cob dates function 動態載入的）
d. User 從的UI period dropdown選取 COB date 2（這里需要提供一個 cob dates function 動態載入的）
e. User 點擊 "Import Data" 按鈕，觸發 data import API call
f. API 接收到 request 後，啟動一個異步任務來執行 data import process，並立即回傳一個 task_id 給前端
g. 前端使用這個 task_id 定期輪詢 API 來查詢任務狀態
   - data import 任務可能需要一段時間來完成
   - 任務狀態包括：pending, in_progress, completed, failed
   - 當任務完成後，前端可以顯示匯入結果或錯誤訊息
   - 前端也可以提供一個取消任務的功能（可選）
   - 任務完成後，匯入的資料會被存儲在資料庫中，供後續分析使用
i. data import 的工作事項：
   - 根據 domain_type 和 domain_name 從 conf/data_map.xlsx 讀取對應的 remote data source 設定
   - 根據 data source type 使用不同的 fetcher class 來取得資料
   - 將取得的資料進行必要的轉換和清洗
   - 將清洗後的資料存入資料庫
   - 記錄匯入過程的日誌和錯誤訊息
   - 更新任務狀態以供前端查詢

## APIs:
 以下是基於 TODO 的 API 命名與分組建議（保持 REST 風格、名詞化、路徑一致性），你可直接挑選或微調：

### 建議的主要資源：imports / workflows

  - POST /api/v1/imports                        ：建立一次匯入流程，產生 workflow_id（含 domain_type、domain_name、cob_date_1/2）。
  - GET  /api/v1/imports/{workflow_id}          ：查匯入狀態（pending/in_progress/completed/failed、錯誤訊息）。
  - POST /api/v1/imports/{workflow_id}/cancel   ：取消任務。（可選）
                                      
（若要分子任務，可在內部使用 job_id，但對外維持 workflow_id 即可。）

### Metadata (下拉選單)

  1. 建議的最佳命名（含 metadata）：

  - GET /api/v1/metadata/domain-types
  - GET /api/v1/metadata/domains?domain_type=...
  - GET /api/v1/metadata/periods?domain_type=...&domain_name=...

  2. 匯入主流程：
      - POST /api/v1/imports                    （啟動流程，回傳 workflow_id）
      - GET  /api/v1/imports/{workflow_id}
      - POST /api/v1/imports/{workflow_id}/cancel（可選）

  維持這個規則：path 全部 kebab-case（小寫 + -），query 參數 snake_case，避免混用 _/-。


  命名原則說明

  - 路徑用複數名詞：避免 /import，改用 /imports。
  - 狀態查詢用 GET 單一資源：GET /imports/{workflow_id}。
  - 下拉/元資料獨立在 /metadata（或 /lookups），避免與匯入主流程混用。
  - Query 參數用來過濾（例如 ?domain_type=），避免把類型硬編進路徑除非有階層需要。

  建議的啟動 payload 範例（POST /imports）

  {
    "domain_type": "equity",
    "domain_name": "us_equity",
    "cob_date_1": "2024-12-05",
    "cob_date_2": "2024-12-06"
  }

  若需要多階段子任務

  - 仍以 workflow_id 為對外主鍵。
  - 內部可有 job_id（parse/validate/persist）記錄在 DB/快取，但不必暴露到 API，或僅在 debug 訊息中呈現。

  這樣命名可以保持一致、可閱讀且易於擴充（未來加 cancel/retry 都能貼上同一路徑結構）。







