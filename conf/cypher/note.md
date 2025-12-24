# Cypher 查詢說明文檔

## 概述

這些 Cypher 查詢用於在 **Transaction 資料匯入完成後**，進行資料的後處理和聚合。

## 資料流程圖

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Data Import Pipeline                            │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  1. Fetch    │  從來源伺服器取得 CSV 檔案                                │
│  2. Cut      │  擷取需要的欄位                                          │
│  3. Split    │  分割大檔案為小檔案                                       │
│  4. Load     │  使用 LOAD CSV 匯入 Neo4j (建立 Transaction 節點)         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Post-Processing (這些 CQL 檔案)                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
   ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
   │ cypher_02   │          │ cypher_03   │          │ cypher_04   │
   │ Summary     │          │ Summary     │          │ Summary     │
   │ _CAGID      │          │ _GFCID      │          │ _NETTINGID  │
   └─────────────┘          └─────────────┘          └─────────────┘
          │                         │                         │
          └─────────────────────────┼─────────────────────────┘
                                    ▼
                            ┌─────────────┐
                            │ cypher_05   │
                            │ 建立關係    │
                            └─────────────┘
```

---

## 檔案說明

### cypher_01.cql - 資料庫配置 & Index 設定

**目的**: 在匯入資料前，設定資料庫參數和建立索引以提升效能。

**內容**:
- 設定交易超時時間為 100 分鐘 (處理大量資料)
- 為 Transaction 節點建立索引 (transaction_id, cagid, gfcid, netting_id, cob_date)
- 為 Summary 節點建立索引
- 建立唯一約束 (Constraint)

**執行時機**: 資料匯入前 (只需執行一次)

---

### cypher_02_summary_cagid.cql - Summary_CAGID 聚合

**目的**: 將 Transaction 資料按 `(cagid, cob_date)` 聚合，建立 Summary_CAGID 節點。

**聚合邏輯**:
```
Transaction (多筆) ──聚合──> Summary_CAGID (一筆)
     │                              │
     │  cagid = "A001"              │  cagid = "A001"
     │  cob_date = "2025-06-30"     │  cob_date = "2025-06-30"
     │  amount1 = 100               │  amount1 = 300 (sum)
     │  amount1 = 200               │
```

**執行時機**: Transaction 資料匯入完成後

---

### cypher_03_summary_gfcid.cql - Summary_GFCID 聚合

**目的**: 將 Transaction 資料按 `(gfcid, cob_date)` 聚合，建立 Summary_GFCID 節點。

**與 Summary_CAGID 差異**:
- 聚合 Key: `gfcid` (而非 `cagid`)
- 額外保留: `cagid`, `obligor_name` 欄位

**執行時機**: Transaction 資料匯入完成後

---

### cypher_04_summary_nettingid.cql - Summary_NETTINGID 聚合

**目的**: 將 Transaction 資料按 `(nettingid, cob_date)` 聚合，建立 Summary_NETTINGID 節點。

**與其他 Summary 差異**:
- 聚合 Key: `nettingid`
- 額外保留: `cagid`, `gfcid`, `obligor_name` 欄位

**執行時機**: Transaction 資料匯入完成後

---

### cypher_05_create_relationships.cql - 建立節點關係

**目的**: 在 Summary 節點之間建立關係，形成階層結構。

**建立的關係**:
```
(Summary_CAGID) ─[:CONTAINS_GFCID]─> (Summary_GFCID)
```

**Graph 結構**:
```
        Summary_CAGID (cagid: A001)
              │
              │ CONTAINS_GFCID
              ▼
        Summary_GFCID (gfcid: G001)
        Summary_GFCID (gfcid: G002)
```

**執行時機**: 所有 Summary 節點建立完成後

---

## 執行順序

```
1. cypher_01.cql          ← 設定索引 (匯入前)
2. [Data Import Pipeline] ← 匯入 Transaction 資料
3. cypher_02.cql          ← 建立 Summary_CAGID
4. cypher_03.cql          ← 建立 Summary_GFCID
5. cypher_04.cql          ← 建立 Summary_NETTINGID
6. cypher_05.cql          ← 建立關係
```

---

## 與 Data Import 的關係

| 階段 | 執行者 | 說明 |
|------|--------|------|
| Import | `neo4j_loader.py` | 將 CSV 匯入為 Transaction 節點 |
| Post-Process | 這些 CQL 檔案 | 聚合 Transaction 為 Summary 節點 |

**neo4j_loader.py 做的事**:
```cypher
LOAD CSV WITH HEADERS FROM 'file://...' AS row
MERGE (t:Transaction {transaction_id: row.transaction_id})
ON CREATE SET t.gfcid = row.gfcid, t.cagid = row.cagid, ...
MERGE (t)-[:TRANSACTIONS]->(g:Summary_GFCID)
```

**這些 CQL 做的事**:
- 將多筆 Transaction 聚合成 Summary 節點
- 計算各種金額欄位的總和 (約 270 個欄位)
- 建立 Summary 節點之間的階層關係

---

## 最終 Graph 結構

```
                    ┌──────────────────┐
                    │  Summary_CAGID   │
                    │  (按 cagid 聚合)  │
                    └────────┬─────────┘
                             │ CONTAINS_GFCID
                             ▼
                    ┌──────────────────┐
                    │  Summary_GFCID   │
                    │  (按 gfcid 聚合)  │
                    └────────┬─────────┘
                             │ TRANSACTIONS
                             ▼
                    ┌──────────────────┐
                    │   Transaction    │
                    │   (原始交易資料)  │
                    └──────────────────┘
```
