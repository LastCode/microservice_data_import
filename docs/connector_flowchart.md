# Connector 流程圖

## 概覽

此文檔描述了數據連接器模組 (`app/services/connectors.py`) 的完整處理流程。

---

## 功能說明

### 用途
Connector 模組負責從各種數據源獲取文件，支援本地文件系統和遠端 SFTP 傳輸。

### 支援的連接器類型

| 類型 | 類別 | 用途 |
|------|------|------|
| `linux` / `local` | `LinuxConnector` | 本地文件系統、掛載的 NAS |
| `sftp` / `scp` | `SFTPConnector` | 遠端 SSH/SFTP 伺服器 |

---

## 主流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         _fetch_file() 調用入口                               │
│                    (app/services/import_pipeline.py:263)                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  從 data_config 取得配置                                                     │
│  ├── connector_type: "linux" | "sftp"                                       │
│  ├── connector_params: {server_name: "..."}                                 │
│  └── source_file_path_template: "/path/to/{cob_date}/file.csv"              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  創建 ConnectorConfig                                                        │
│  ConnectorConfig(                                                           │
│      connector_type = "linux" | "sftp",                                     │
│      server_name = "server1",                                               │
│      params = {...}                                                         │
│  )                                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
╔═════════════════════════════════════════════════════════════════════════════╗
║                         ConnectorFactory.create()                            ║
║                        (connectors.py:184-190)                              ║
╚═════════════════════════════════════════════════════════════════════════════╝
                                      │
                    ┌─────────────────┴─────────────────┐
                    │                                   │
          connector_type?                      connector_type?
          "linux" | "local"                    "sftp" | "scp"
                    │                                   │
                    ▼                                   ▼
┌───────────────────────────────┐     ┌───────────────────────────────┐
│       LinuxConnector          │     │       SFTPConnector           │
│      (connectors.py:63)       │     │      (connectors.py:98)       │
│                               │     │                               │
│  用於:                        │     │  用於:                        │
│  • 本地文件系統               │     │  • 遠端 SSH/SFTP 伺服器       │
│  • 掛載的 NAS                 │     │  • 需要認證的遠端主機         │
└───────────────────────────────┘     └───────────────────────────────┘
                    │                                   │
                    ▼                                   ▼
┌───────────────────────────────┐     ┌───────────────────────────────┐
│        connector.fetch()      │     │        connector.fetch()      │
└───────────────────────────────┘     └───────────────────────────────┘
                    │                                   │
                    ▼                                   ▼
            ┌───────────────────────────────────────────────┐
            │                 FetchResult                   │
            │  { success, local_path, bytes_transferred }   │
            └───────────────────────────────────────────────┘
```

---

## LinuxConnector.fetch() 流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LinuxConnector.fetch(source, dest)                        │
│                         (connectors.py:66-91)                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                              ┌───────────────┐
                              │ source 存在？ │
                              └───────┬───────┘
                               NO     │     YES
                                ▼     │      ▼
┌────────────────────────┐           │     ┌────────────────────────┐
│ FetchResult(           │           │     │ 創建目標目錄           │
│   success=False,       │           │     │ dest.parent.mkdir()    │
│   error="not found"    │           │     └────────────────────────┘
│ )                      │           │                  │
└────────────────────────┘           │                  ▼
                                     │     ┌────────────────────────┐
                                     │     │ shutil.copy2()         │
                                     │     │ 複製文件 (保留元數據)   │
                                     │     └────────────────────────┘
                                     │                  │
                                     │                  ▼
                                     │     ┌────────────────────────┐
                                     │     │ 計算文件大小           │
                                     │     │ file_size = stat().size│
                                     │     └────────────────────────┘
                                     │                  │
                                     │                  ▼
                                     │     ┌────────────────────────┐
                                     │     │ FetchResult(           │
                                     │     │   success=True,        │
                                     │     │   local_path=dest,     │
                                     │     │   bytes_transferred    │
                                     │     │ )                      │
                                     │     └────────────────────────┘
                                     │
                              ┌──────┴───────┐
                              │  Exception?  │
                              └──────┬───────┘
                                     ▼
                        ┌────────────────────────┐
                        │ FetchResult(           │
                        │   success=False,       │
                        │   error=str(e)         │
                        │ )                      │
                        └────────────────────────┘
```

---

## SFTPConnector.fetch() 流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SFTPConnector.fetch(source, dest)                         │
│                         (connectors.py:118-155)                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  _get_server_settings()                                                      │
│  從 settings.yaml 的 LINUX_SERVERS 取得認證資訊                              │
│  { user: "username", password: "xxx", ... }                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                              ┌───────┴────────┐
                              │ settings 存在？│
                              └───────┬────────┘
                               NO     │     YES
                                ▼     │      ▼
┌────────────────────────┐           │     ┌────────────────────────┐
│ FetchResult(           │           │     │ 創建目標目錄           │
│   success=False,       │           │     │ dest.parent.mkdir()    │
│   error="No settings"  │           │     └────────────────────────┘
│ )                      │           │                  │
└────────────────────────┘           │                  ▼
                                     │     ┌────────────────────────┐
                                     │     │ 組裝 SCP 命令          │
                                     │     │ ["scp",                │
                                     │     │  "user@host:source",   │
                                     │     │  "dest"]               │
                                     │     └────────────────────────┘
                                     │                  │
                                     │                  ▼
                                     │     ┌────────────────────────┐
                                     │     │ subprocess.run()       │
                                     │     │ timeout=300秒          │
                                     │     └────────────────────────┘
                                     │                  │
                                     │         ┌───────┴───────┐
                                     │         │ returncode=0? │
                                     │         └───────┬───────┘
                                     │          NO     │     YES
                                     │           ▼     │      ▼
                                     │  ┌──────────────┤     ┌────────────────────────┐
                                     │  │ FetchResult( │     │ FetchResult(           │
                                     │  │  success=    │     │   success=True,        │
                                     │  │   False,     │     │   local_path=dest,     │
                                     │  │  error=      │     │   bytes_transferred    │
                                     │  │   stderr     │     │ )                      │
                                     │  │ )            │     └────────────────────────┘
                                     │  └──────────────┘
                                     │
                              ┌──────┴───────┐
                              │  Timeout?    │
                              └──────┬───────┘
                                     ▼
                        ┌────────────────────────┐
                        │ FetchResult(           │
                        │   success=False,       │
                        │   error="timed out"    │
                        │ )                      │
                        └────────────────────────┘
```

---

## 類別關係圖

```
                    ┌─────────────────────┐
                    │   ConnectorConfig   │
                    │   (dataclass)       │
                    ├─────────────────────┤
                    │ connector_type: str │
                    │ server_name: str    │
                    │ params: Dict        │
                    └─────────────────────┘
                              │
                              │ 傳入
                              ▼
                    ┌─────────────────────┐
                    │  ConnectorFactory   │
                    ├─────────────────────┤
                    │ _connectors = {     │
                    │   "linux": Linux,   │
                    │   "sftp": SFTP      │
                    │ }                   │
                    ├─────────────────────┤
                    │ create()            │
                    │ register()          │
                    └─────────────────────┘
                              │
                              │ 創建
                              ▼
                    ┌─────────────────────┐
                    │   BaseConnector     │
                    │   (ABC 抽象類)      │
                    ├─────────────────────┤
                    │ config              │
                    │ settings            │
                    ├─────────────────────┤
                    │ fetch() [abstract]  │
                    │ test_connection()   │
                    └─────────────────────┘
                              △
                              │ 繼承
              ┌───────────────┴───────────────┐
              │                               │
┌─────────────────────┐         ┌─────────────────────┐
│   LinuxConnector    │         │   SFTPConnector     │
├─────────────────────┤         ├─────────────────────┤
│                     │         │ server_settings     │
├─────────────────────┤         ├─────────────────────┤
│ fetch()             │         │ fetch()             │
│  └─ shutil.copy2()  │         │  └─ scp command     │
│ test_connection()   │         │ test_connection()   │
│  └─ return True     │         │  └─ ssh test        │
└─────────────────────┘         └─────────────────────┘
              │                               │
              │                               │
              ▼                               ▼
┌─────────────────────┐         ┌─────────────────────┐
│    FetchResult      │         │    FetchResult      │
│    (dataclass)      │         │    (dataclass)      │
├─────────────────────┤         ├─────────────────────┤
│ success: bool       │         │ success: bool       │
│ local_path: Path    │         │ local_path: Path    │
│ error: str          │         │ error: str          │
│ bytes_transferred   │         │ bytes_transferred   │
└─────────────────────┘         └─────────────────────┘
```

---

## 配置解析流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DataMapResolver                                   │
│                         (connectors.py:198-244)                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ 讀取
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          conf/data_map.csv                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ domain_type │ domain_name │ connector_type │ source_file_path_template      │
├─────────────┼─────────────┼────────────────┼────────────────────────────────┤
│ API         │ Credit Risk │ linux          │ /mnt/nas/{cob_date}/credit.csv │
│ CORE        │ Market Data │ sftp           │ /data/{cob_date}/market.csv    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ 返回
                                      ▼
                        { connector_type, source_file_path_template, ... }
```

---

## 相關文件

| 文件 | 說明 |
|------|------|
| `app/services/connectors.py` | Connector 實現 |
| `app/services/import_pipeline.py` | Pipeline 調用 Connector |
| `conf/settings.yaml` | SFTP 伺服器認證配置 |
| `conf/data_map.csv` | Domain 到 Connector 的映射 |

---

## Mermaid 流程圖

```mermaid
flowchart TD
    A[_fetch_file 調用] --> B[讀取 data_config]
    B --> C[創建 ConnectorConfig]
    C --> D[ConnectorFactory.create]

    D --> E{connector_type?}
    E -->|linux/local| F[LinuxConnector]
    E -->|sftp/scp| G[SFTPConnector]

    F --> H[shutil.copy2]
    G --> I[scp command]

    H --> J{成功?}
    I --> J

    J -->|Yes| K[FetchResult: success=True]
    J -->|No| L[FetchResult: success=False]
```

```mermaid
classDiagram
    class ConnectorConfig {
        +str connector_type
        +str server_name
        +Dict params
    }

    class FetchResult {
        +bool success
        +Path local_path
        +str error
        +int bytes_transferred
    }

    class BaseConnector {
        <<abstract>>
        +ConnectorConfig config
        +Dict settings
        +fetch() FetchResult
        +test_connection() bool
    }

    class LinuxConnector {
        +fetch() FetchResult
        +test_connection() bool
    }

    class SFTPConnector {
        +Dict server_settings
        +fetch() FetchResult
        +test_connection() bool
    }

    class ConnectorFactory {
        +Dict _connectors
        +create() BaseConnector
        +register()
    }

    BaseConnector <|-- LinuxConnector
    BaseConnector <|-- SFTPConnector
    ConnectorFactory --> BaseConnector : creates
    BaseConnector --> FetchResult : returns
    ConnectorFactory --> ConnectorConfig : uses
```
