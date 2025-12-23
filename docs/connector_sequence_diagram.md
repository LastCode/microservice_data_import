# Connector Sequence Diagram

## 概覽

此文檔展示 Connector 模組與外部伺服器之間的交互時序圖。

---

## 完整交互流程

```
┌──────────┐     ┌──────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│  Client  │     │ FastAPI  │     │ImportPipeline│     │  Connector  │     │External Server│
│          │     │          │     │              │     │             │     │(NAS/SFTP)    │
└────┬─────┘     └────┬─────┘     └──────┬───────┘     └──────┬──────┘     └──────┬───────┘
     │                │                  │                    │                   │
     │ POST /imports  │                  │                    │                   │
     │───────────────>│                  │                    │                   │
     │                │                  │                    │                   │
     │                │ BackgroundTask   │                    │                   │
     │                │─────────────────>│                    │                   │
     │                │                  │                    │                   │
     │ 202 Accepted   │                  │                    │                   │
     │<───────────────│                  │                    │                   │
     │                │                  │                    │                   │
     │                │                  │ _fetch_file()      │                   │
     │                │                  │───────────────────>│                   │
     │                │                  │                    │                   │
     │                │                  │                    │ fetch(src, dest)  │
     │                │                  │                    │──────────────────>│
     │                │                  │                    │                   │
     │                │                  │                    │   File Content    │
     │                │                  │                    │<──────────────────│
     │                │                  │                    │                   │
     │                │                  │    FetchResult     │                   │
     │                │                  │<───────────────────│                   │
     │                │                  │                    │                   │
     │                │                  │ (continue to Cut,  │                   │
     │                │                  │  Split, Load...)   │                   │
     │                │                  │                    │                   │
     │ GET /imports/{id}                 │                    │                   │
     │───────────────>│                  │                    │                   │
     │                │                  │                    │                   │
     │   Status       │                  │                    │                   │
     │<───────────────│                  │                    │                   │
     │                │                  │                    │                   │
```

---

## LinuxConnector 時序圖

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant API as FastAPI<br/>/api/v1/imports
    participant Pipeline as ImportPipeline
    participant Factory as ConnectorFactory
    participant Linux as LinuxConnector
    participant NAS as NAS/Local FS<br/>(Mounted Storage)
    participant Local as Local Disk<br/>(DROPBOX_DIR)

    Client->>API: POST /api/v1/imports<br/>{domain_type, domain_name, cob_date}
    API->>API: Generate workflow_id
    API-->>Client: 202 {workflow_id, status: pending}

    API->>Pipeline: BackgroundTask: run(request)
    activate Pipeline

    Pipeline->>Pipeline: Load settings.yaml
    Pipeline->>Pipeline: Resolve data_map.csv<br/>connector_type: "linux"

    Pipeline->>Factory: create("linux", config, settings)
    Factory-->>Pipeline: LinuxConnector instance

    Pipeline->>Linux: fetch(source_path, dest_path)
    activate Linux

    Linux->>NAS: Check file exists<br/>Path(source_path).exists()
    NAS-->>Linux: True

    Linux->>NAS: Read file<br/>shutil.copy2(source, dest)
    NAS-->>Linux: File content

    Linux->>Local: Write file to DROPBOX_DIR
    Local-->>Linux: Write complete

    Linux->>Local: Get file size<br/>dest_path.stat().st_size
    Local-->>Linux: file_size (bytes)

    Linux-->>Pipeline: FetchResult<br/>{success: true, local_path, bytes_transferred}
    deactivate Linux

    Pipeline->>Pipeline: Continue to Cut, Split, Load...
    deactivate Pipeline

    Client->>API: GET /api/v1/imports/{workflow_id}
    API-->>Client: {status: completed, detail: ...}
```

---

## SFTPConnector 時序圖

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant API as FastAPI<br/>/api/v1/imports
    participant Pipeline as ImportPipeline
    participant Factory as ConnectorFactory
    participant SFTP as SFTPConnector
    participant Remote as Remote Server<br/>(SSH/SFTP)
    participant Local as Local Disk<br/>(DROPBOX_DIR)

    Client->>API: POST /api/v1/imports<br/>{domain_type, domain_name, cob_date}
    API->>API: Generate workflow_id
    API-->>Client: 202 {workflow_id, status: pending}

    API->>Pipeline: BackgroundTask: run(request)
    activate Pipeline

    Pipeline->>Pipeline: Load settings.yaml
    Pipeline->>Pipeline: Resolve data_map.csv<br/>connector_type: "sftp"

    Pipeline->>Factory: create("sftp", config, settings)
    Factory-->>Pipeline: SFTPConnector instance

    Pipeline->>SFTP: fetch(source_path, dest_path)
    activate SFTP

    SFTP->>SFTP: Get server credentials<br/>from LINUX_SERVERS

    SFTP->>Remote: scp user@host:source dest<br/>(subprocess.run, timeout=300s)
    activate Remote

    Note over SFTP,Remote: SSH Authentication<br/>(Key-based or Password)

    Remote-->>SFTP: File transferred
    deactivate Remote

    SFTP->>Local: Verify file exists
    Local-->>SFTP: File confirmed

    SFTP->>Local: Get file size<br/>dest_path.stat().st_size
    Local-->>SFTP: file_size (bytes)

    SFTP-->>Pipeline: FetchResult<br/>{success: true, local_path, bytes_transferred}
    deactivate SFTP

    Pipeline->>Pipeline: Continue to Cut, Split, Load...
    deactivate Pipeline

    Client->>API: GET /api/v1/imports/{workflow_id}
    API-->>Client: {status: completed, detail: ...}
```

---

## 錯誤處理時序圖

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant API as FastAPI
    participant Pipeline as ImportPipeline
    participant Connector as Connector
    participant External as External Server

    Client->>API: POST /api/v1/imports
    API-->>Client: 202 {workflow_id, status: pending}

    API->>Pipeline: BackgroundTask: run(request)
    activate Pipeline

    Pipeline->>Connector: fetch(source, dest)
    activate Connector

    alt File Not Found
        Connector->>External: Check file exists
        External-->>Connector: File not found
        Connector-->>Pipeline: FetchResult {success: false, error: "not found"}
    else Connection Timeout
        Connector->>External: scp command
        External--xConnector: Timeout (300s)
        Connector-->>Pipeline: FetchResult {success: false, error: "timed out"}
    else Permission Denied
        Connector->>External: scp command
        External-->>Connector: Permission denied
        Connector-->>Pipeline: FetchResult {success: false, error: "permission denied"}
    else Success
        Connector->>External: Transfer file
        External-->>Connector: File content
        Connector-->>Pipeline: FetchResult {success: true, ...}
    end
    deactivate Connector

    alt Fetch Failed
        Pipeline->>Pipeline: raise RuntimeError
        Pipeline->>API: Update status: FAILED
    else Fetch Success
        Pipeline->>Pipeline: Continue pipeline
        Pipeline->>API: Update status: COMPLETED
    end
    deactivate Pipeline

    Client->>API: GET /api/v1/imports/{workflow_id}
    API-->>Client: {status: failed/completed, detail: ...}
```

---

## 系統架構圖

```
                                    ┌─────────────────────────────────────────┐
                                    │           External Systems              │
                                    ├─────────────────────────────────────────┤
                                    │                                         │
                                    │   ┌─────────────┐   ┌─────────────┐    │
                                    │   │  NAS/Mount  │   │ SFTP Server │    │
                                    │   │  Storage    │   │ (Remote)    │    │
                                    │   └──────┬──────┘   └──────┬──────┘    │
                                    │          │                 │           │
                                    └──────────┼─────────────────┼───────────┘
                                               │                 │
                              ┌────────────────┴─────────────────┴────────────────┐
                              │                    Network                         │
                              └────────────────────────────┬──────────────────────┘
                                                           │
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              Microservice (Data Import)                                  │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│   ┌─────────────┐      ┌──────────────────┐      ┌─────────────────────────────────┐   │
│   │   Client    │      │    FastAPI       │      │        Service Layer            │   │
│   │  (Browser/  │─────>│  /api/v1/imports │─────>│                                 │   │
│   │   Postman)  │      │                  │      │  ┌───────────────────────────┐  │   │
│   └─────────────┘      └──────────────────┘      │  │     ImportPipeline        │  │   │
│                                                   │  │                           │  │   │
│                                                   │  │  ┌─────────────────────┐  │  │   │
│                                                   │  │  │  ConnectorFactory   │  │  │   │
│                                                   │  │  └──────────┬──────────┘  │  │   │
│                                                   │  │             │             │  │   │
│                                                   │  │     ┌───────┴───────┐     │  │   │
│                                                   │  │     │               │     │  │   │
│                                                   │  │  ┌──▼────┐    ┌─────▼──┐  │  │   │
│                                                   │  │  │Linux  │    │ SFTP   │  │  │   │
│                                                   │  │  │Connec.│    │Connec. │  │  │   │
│                                                   │  │  └───┬───┘    └───┬────┘  │  │   │
│                                                   │  │      │            │       │  │   │
│                                                   │  └──────┼────────────┼───────┘  │   │
│                                                   └─────────┼────────────┼──────────┘   │
│                                                             │            │              │
│   ┌─────────────────────────────────────────────────────────┼────────────┼───────────┐  │
│   │                      Local Storage                      │            │           │  │
│   │                                                         ▼            ▼           │  │
│   │   ┌─────────────────────────────────────────────────────────────────────────┐   │  │
│   │   │                        DROPBOX_DIR                                       │   │  │
│   │   │                    /Users/.../mnt/nas/                                   │   │  │
│   │   │                                                                          │   │  │
│   │   │   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐               │   │  │
│   │   │   │ fetched  │  │   cut    │  │  split   │  │  split   │  ...          │   │  │
│   │   │   │ file.csv │─>│ file.csv │─>│ file_1   │  │ file_2   │               │   │  │
│   │   │   └──────────┘  └──────────┘  └──────────┘  └──────────┘               │   │  │
│   │   │                                     │              │                    │   │  │
│   │   └─────────────────────────────────────┼──────────────┼────────────────────┘   │  │
│   └─────────────────────────────────────────┼──────────────┼────────────────────────┘  │
│                                             │              │                           │
│   ┌─────────────────────────────────────────┼──────────────┼────────────────────────┐  │
│   │                Neo4j Graph Database     │              │                        │  │
│   │                                         ▼              ▼                        │  │
│   │   ┌─────────────────────────────────────────────────────────────────────────┐  │  │
│   │   │  LOAD CSV → MERGE Transaction nodes → CREATE relationships             │  │  │
│   │   └─────────────────────────────────────────────────────────────────────────┘  │  │
│   └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 資料流向表

| 步驟 | 來源 | 目標 | 資料 | 協議/方式 |
|------|------|------|------|-----------|
| 1 | Client | FastAPI | HTTP POST Request | REST API |
| 2 | FastAPI | ImportPipeline | ImportRequest | Python 函數調用 |
| 3 | ImportPipeline | ConnectorFactory | config, settings | Python 函數調用 |
| 4a | LinuxConnector | NAS/Mount | 讀取文件 | 文件系統 (shutil.copy2) |
| 4b | SFTPConnector | Remote Server | 傳輸文件 | SSH/SCP (subprocess) |
| 5 | Connector | DROPBOX_DIR | 寫入文件 | 本地文件系統 |
| 6 | Pipeline | Neo4j | CSV 資料 | Bolt Protocol (LOAD CSV) |
| 7 | FastAPI | Client | HTTP Response | REST API |

---

## 相關文件

| 文件 | 說明 |
|------|------|
| `app/api/data_import.py` | API 端點定義 |
| `app/services/import_pipeline.py` | Pipeline 編排 |
| `app/services/connectors.py` | Connector 實現 |
| `conf/settings.yaml` | 伺服器認證配置 |
| `conf/data_map.csv` | Domain 到 Connector 映射 |
