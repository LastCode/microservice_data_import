# /api/v1/imports Sequence Diagram

## Overview

This document shows the complete sequence diagrams for interactions between the `/api/v1/imports` API and all external systems.

---

## System Components

| Component | Type | Description |
|-----------|------|-------------|
| Client | External | Initiates API requests (Browser/Postman) |
| FastAPI | Internal | REST API service (Port 9090) |
| ImportPipeline | Internal | ETL workflow orchestration |
| Connector | Internal | File fetching (Linux/SFTP) |
| Processor | Internal | Data processing (Cut/Split) |
| Neo4jLoader | Internal | Data loading |
| NAS/SFTP Server | External | Source data storage |
| Neo4j Database | External | Graph database |

---

## Complete ETL Pipeline Sequence Diagram

```
┌────────┐┌────────┐┌──────────────┐┌───────────┐┌───────────┐┌───────────┐┌─────────────┐┌─────────┐
│ Client ││FastAPI ││ImportPipeline││ Connector ││ Processor ││Neo4jLoader││External     ││  Neo4j  │
│        ││        ││              ││           ││           ││           ││Server(NAS)  ││Database │
└───┬────┘└───┬────┘└──────┬───────┘└─────┬─────┘└─────┬─────┘└─────┬─────┘└──────┬──────┘└────┬────┘
    │         │            │              │            │            │             │            │
    │ POST    │            │              │            │            │             │            │
    │/imports │            │              │            │            │             │            │
    │────────>│            │              │            │            │             │            │
    │         │            │              │            │            │             │            │
    │         │BackgroundTask             │            │            │             │            │
    │         │───────────>│              │            │            │             │            │
    │         │            │              │            │            │             │            │
    │ 202     │            │              │            │            │             │            │
    │<────────│            │              │            │            │             │            │
    │         │            │              │            │            │             │            │
    │         │            │ ═══════════════════════════════════════════════════════════════  │
    │         │            │ ║                    STEP 1: FETCH                            ║  │
    │         │            │ ═══════════════════════════════════════════════════════════════  │
    │         │            │              │            │            │             │            │
    │         │            │ fetch()      │            │            │             │            │
    │         │            │─────────────>│            │            │             │            │
    │         │            │              │            │            │             │            │
    │         │            │              │ copy/scp   │            │             │            │
    │         │            │              │───────────────────────────────────────>│            │
    │         │            │              │            │            │             │            │
    │         │            │              │  file data │            │             │            │
    │         │            │              │<───────────────────────────────────────│            │
    │         │            │              │            │            │             │            │
    │         │            │ FetchResult  │            │            │             │            │
    │         │            │<─────────────│            │            │             │            │
    │         │            │              │            │            │             │            │
    │         │            │ ═══════════════════════════════════════════════════════════════  │
    │         │            │ ║                    STEP 2: CUT                              ║  │
    │         │            │ ═══════════════════════════════════════════════════════════════  │
    │         │            │              │            │            │             │            │
    │         │            │ cut_columns()│            │            │             │            │
    │         │            │─────────────────────────>│            │             │            │
    │         │            │              │            │            │             │            │
    │         │            │              │   (local disk I/O)     │             │            │
    │         │            │              │            │            │             │            │
    │         │            │ ProcessResult│            │            │             │            │
    │         │            │<─────────────────────────│            │             │            │
    │         │            │              │            │            │             │            │
    │         │            │ ═══════════════════════════════════════════════════════════════  │
    │         │            │ ║                    STEP 3: SPLIT                            ║  │
    │         │            │ ═══════════════════════════════════════════════════════════════  │
    │         │            │              │            │            │             │            │
    │         │            │ split_file() │            │            │             │            │
    │         │            │─────────────────────────>│            │             │            │
    │         │            │              │            │            │             │            │
    │         │            │              │   (local disk I/O)     │             │            │
    │         │            │              │            │            │             │            │
    │         │            │ ProcessResult│            │            │             │            │
    │         │            │<─────────────────────────│            │             │            │
    │         │            │              │            │            │             │            │
    │         │            │ ═══════════════════════════════════════════════════════════════  │
    │         │            │ ║                    STEP 4: LOAD                             ║  │
    │         │            │ ═══════════════════════════════════════════════════════════════  │
    │         │            │              │            │            │             │            │
    │         │            │ load_files() │            │            │             │            │
    │         │            │─────────────────────────────────────>│             │            │
    │         │            │              │            │            │             │            │
    │         │            │              │            │            │ LOAD CSV   │            │
    │         │            │              │            │            │────────────────────────>│
    │         │            │              │            │            │             │            │
    │         │            │              │            │            │ nodes/rels │            │
    │         │            │              │            │            │<────────────────────────│
    │         │            │              │            │            │             │            │
    │         │            │ LoadResult   │            │            │             │            │
    │         │            │<─────────────────────────────────────│             │            │
    │         │            │              │            │            │             │            │
    │         │ status:    │              │            │            │             │            │
    │         │ completed  │              │            │            │             │            │
    │         │<───────────│              │            │            │             │            │
    │         │            │              │            │            │             │            │
    │ GET     │            │              │            │            │             │            │
    │/imports/│            │              │            │            │             │            │
    │  {id}   │            │            │            │            │             │            │
    │────────>│            │              │            │            │             │            │
    │         │            │              │            │            │             │            │
    │ status  │            │              │            │            │             │            │
    │<────────│            │              │            │            │             │            │
    │         │            │              │            │            │             │            │
```

---

## Mermaid Sequence Diagram - Complete Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant API as FastAPI<br/>Port 9090
    participant Pipeline as ImportPipeline
    participant Connector as Connector<br/>(Linux/SFTP)
    participant Processor as DataProcessor<br/>(Cut/Split)
    participant Loader as Neo4jLoader
    participant External as External Server<br/>(NAS/SFTP)
    participant Neo4j as Neo4j<br/>Database

    %% Request Phase
    rect rgb(200, 220, 250)
        Note over Client,API: Request Phase
        Client->>API: POST /api/v1/imports<br/>{domain_type, domain_name, cob_date}
        API->>API: Generate workflow_id (UUID)
        API->>API: Store status: PENDING
        API-->>Client: 202 Accepted<br/>{workflow_id, status: pending}
    end

    %% Background Processing
    API->>Pipeline: BackgroundTask: run(request)
    activate Pipeline
    Pipeline->>Pipeline: status: IN_PROGRESS
    Pipeline->>Pipeline: Load configurations<br/>(settings.yaml, data_map.csv, column_map.yaml)

    %% Step 1: Fetch
    rect rgb(255, 230, 200)
        Note over Pipeline,External: Step 1: FETCH
        Pipeline->>Pipeline: status: FETCHING
        Pipeline->>Connector: _fetch_file(request, config)
        activate Connector

        alt LinuxConnector
            Connector->>External: shutil.copy2(source, dest)
        else SFTPConnector
            Connector->>External: scp user@host:source dest
        end

        External-->>Connector: File content transferred
        Connector->>Connector: Calculate bytes_transferred
        Connector-->>Pipeline: FetchResult {success, local_path, bytes}
        deactivate Connector
    end

    %% Step 2: Cut
    rect rgb(220, 250, 220)
        Note over Pipeline,Processor: Step 2: CUT
        Pipeline->>Pipeline: status: CUTTING
        Pipeline->>Processor: cut_columns(file, config)
        activate Processor
        Processor->>Processor: Unix cut command<br/>or Python fallback
        Processor->>Processor: Add CSV headers (optional)
        Processor-->>Pipeline: ProcessResult {success, output_path, rows}
        deactivate Processor
    end

    %% Step 3: Split
    rect rgb(250, 250, 200)
        Note over Pipeline,Processor: Step 3: SPLIT
        Pipeline->>Pipeline: status: SPLITTING
        Pipeline->>Processor: split_file(file, column)
        activate Processor
        Processor->>Processor: Split by GFCID/BOOK_ID
        Processor->>Processor: Create multiple output files
        Processor-->>Pipeline: ProcessResult {success, output_paths[]}
        deactivate Processor
    end

    %% Step 4: Load
    rect rgb(250, 220, 220)
        Note over Pipeline,Neo4j: Step 4: LOAD
        Pipeline->>Pipeline: status: LOADING
        Pipeline->>Loader: load_files(file_paths)
        activate Loader

        loop For each split file
            Loader->>Neo4j: LOAD CSV FROM 'file://...'
            Loader->>Neo4j: MERGE (t:Transaction {...})
            Loader->>Neo4j: MATCH (s:Summary_GFCID)
            Loader->>Neo4j: CREATE (t)-[:BELONGS_TO]->(s)
            Neo4j-->>Loader: nodes_created, rels_created
        end

        Loader-->>Pipeline: LoadResult {success, nodes, relationships}
        deactivate Loader
    end

    %% Completion
    Pipeline->>Pipeline: status: COMPLETED
    Pipeline->>API: Update STATUSES[workflow_id]
    deactivate Pipeline

    %% Status Query
    rect rgb(230, 230, 250)
        Note over Client,API: Status Query Phase
        Client->>API: GET /api/v1/imports/{workflow_id}
        API-->>Client: {status: completed, detail: ...}
    end
```

---

## External System Interaction Details

### 1. Interaction with NAS/SFTP Server

```mermaid
sequenceDiagram
    participant Pipeline as ImportPipeline
    participant Linux as LinuxConnector
    participant SFTP as SFTPConnector
    participant NAS as NAS<br/>(Mounted)
    participant Remote as SFTP Server<br/>(Remote)

    alt connector_type = "linux"
        Pipeline->>Linux: fetch(source, dest)
        Linux->>NAS: Path(source).exists()
        NAS-->>Linux: True/False
        Linux->>NAS: shutil.copy2(source, dest)
        NAS-->>Linux: File copied
        Linux-->>Pipeline: FetchResult
    else connector_type = "sftp"
        Pipeline->>SFTP: fetch(source, dest)
        SFTP->>SFTP: Load credentials from settings.yaml
        SFTP->>Remote: scp -o ConnectTimeout=5 user@host:source dest
        Note over SFTP,Remote: SSH Authentication
        Remote-->>SFTP: File transferred (timeout=300s)
        SFTP-->>Pipeline: FetchResult
    end
```

### 2. Interaction with Neo4j Database

```mermaid
sequenceDiagram
    participant Pipeline as ImportPipeline
    participant Loader as Neo4jLoader
    participant Driver as Neo4j Driver
    participant Neo4j as Neo4j Database<br/>(Bolt:7687)

    Pipeline->>Loader: load_files(file_paths, base_path)
    activate Loader

    Loader->>Driver: get_driver()
    Driver-->>Loader: Neo4j driver instance

    loop For each file in file_paths
        Loader->>Neo4j: BEGIN TRANSACTION

        Loader->>Neo4j: LOAD CSV WITH HEADERS<br/>FROM 'file://base_path/file.csv'

        Loader->>Neo4j: MERGE (t:Transaction {id: row.id})<br/>SET t.gfcid = row.gfcid, ...

        Loader->>Neo4j: MATCH (s:Summary_GFCID {gfcid: row.gfcid})

        Loader->>Neo4j: CREATE (t)-[:BELONGS_TO]->(s)

        Neo4j-->>Loader: {nodes_created: N, rels_created: M}

        Loader->>Neo4j: COMMIT
    end

    Loader->>Driver: close()
    Loader-->>Pipeline: LoadResult {success, nodes_created, relationships_created}
    deactivate Loader
```

---

## Error Handling Sequence Diagram

```mermaid
sequenceDiagram
    participant Client
    participant API as FastAPI
    participant Pipeline as ImportPipeline
    participant Connector
    participant Processor
    participant Loader as Neo4jLoader
    participant External as External Systems

    Client->>API: POST /api/v1/imports
    API-->>Client: 202 {workflow_id, status: pending}

    API->>Pipeline: BackgroundTask
    activate Pipeline

    alt Validation Error
        Pipeline->>Pipeline: _validate_request()
        Pipeline->>Pipeline: Missing domain_type/name/cob_date
        Pipeline->>API: status: FAILED, detail: "Missing required fields"
    else Config Not Found
        Pipeline->>Pipeline: data_map_resolver.resolve()
        Pipeline->>Pipeline: No config for domain
        Pipeline->>API: status: FAILED, detail: "No data source configuration found"
    else Fetch Failed
        Pipeline->>Connector: fetch()
        Connector->>External: Transfer file
        External--xConnector: Error (timeout/not found/permission denied)
        Connector-->>Pipeline: FetchResult {success: false, error: "..."}
        Pipeline->>API: status: FAILED, detail: "Failed to fetch source file"
    else Cut Failed
        Pipeline->>Processor: cut_columns()
        Processor-->>Pipeline: ProcessResult {success: false, error: "..."}
        Pipeline->>API: status: FAILED, detail: "Column cutting failed"
    else Split Failed
        Pipeline->>Processor: split_file()
        Processor-->>Pipeline: ProcessResult {success: false, error: "..."}
        Pipeline->>API: status: FAILED, detail: "File splitting failed"
    else Load Failed
        Pipeline->>Loader: load_files()
        Loader->>External: Neo4j LOAD CSV
        External--xLoader: Connection error / Query error
        Loader-->>Pipeline: LoadResult {success: false, error: "..."}
        Pipeline->>API: status: COMPLETED (with warnings)
    else Success
        Pipeline->>API: status: COMPLETED
    end

    deactivate Pipeline

    Client->>API: GET /api/v1/imports/{workflow_id}
    API-->>Client: {status, detail}
```

---

## Status Transition Sequence Diagram

```mermaid
sequenceDiagram
    participant Client
    participant API as FastAPI
    participant Pipeline as ImportPipeline
    participant Store as STATUSES Dict

    Client->>API: POST /api/v1/imports
    API->>Store: STATUSES[id] = {status: PENDING}
    API-->>Client: 202 {status: pending}

    API->>Pipeline: BackgroundTask
    activate Pipeline

    Pipeline->>Store: STATUSES[id] = {status: IN_PROGRESS}

    Pipeline->>Pipeline: Step 1: Fetch
    Pipeline->>Store: status: FETCHING

    Pipeline->>Pipeline: Step 2: Cut
    Pipeline->>Store: status: CUTTING

    Pipeline->>Pipeline: Step 3: Split
    Pipeline->>Store: status: SPLITTING

    Pipeline->>Pipeline: Step 4: Load
    Pipeline->>Store: status: LOADING

    Pipeline->>Store: status: COMPLETED
    deactivate Pipeline

    loop Polling
        Client->>API: GET /api/v1/imports/{id}
        API->>Store: Get STATUSES[id]
        Store-->>API: {status, detail}
        API-->>Client: {status, detail}
    end
```

---

## System Interaction Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                      EXTERNAL SYSTEMS                                            │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                 │
│    ┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐                 │
│    │  NAS Storage    │         │  SFTP Server    │         │  Neo4j Database │                 │
│    │  (Mounted)      │         │  (Remote)       │         │  (Bolt:7687)    │                 │
│    │                 │         │                 │         │                 │                 │
│    │  /mnt/nas/      │         │  ssh://host:22  │         │  bolt://host    │                 │
│    └────────┬────────┘         └────────┬────────┘         └────────┬────────┘                 │
│             │                           │                           │                          │
│             │ shutil.copy2              │ scp command               │ Cypher LOAD CSV          │
│             │                           │                           │                          │
└─────────────┼───────────────────────────┼───────────────────────────┼──────────────────────────┘
              │                           │                           │
              │                           │                           │
┌─────────────┼───────────────────────────┼───────────────────────────┼──────────────────────────┐
│             │                           │                           │                          │
│             ▼                           ▼                           ▼                          │
│    ┌─────────────────────────────────────────────────────────────────────────────────────┐    │
│    │                              MICROSERVICE                                            │    │
│    ├─────────────────────────────────────────────────────────────────────────────────────┤    │
│    │                                                                                     │    │
│    │   ┌───────────────┐                                                                 │    │
│    │   │    Client     │                                                                 │    │
│    │   │  (External)   │                                                                 │    │
│    │   └───────┬───────┘                                                                 │    │
│    │           │ HTTP                                                                    │    │
│    │           ▼                                                                         │    │
│    │   ┌───────────────┐      ┌───────────────┐      ┌───────────────┐                  │    │
│    │   │   FastAPI     │─────>│ImportPipeline │─────>│   Connector   │──────────────────┼────┼──> NAS/SFTP
│    │   │  Port 9090    │      │               │      │               │                  │    │
│    │   └───────────────┘      │               │      └───────────────┘                  │    │
│    │                          │               │                                          │    │
│    │                          │               │      ┌───────────────┐                  │    │
│    │                          │               │─────>│ DataProcessor │                  │    │
│    │                          │               │      │  (Cut/Split)  │                  │    │
│    │                          │               │      └───────────────┘                  │    │
│    │                          │               │                                          │    │
│    │                          │               │      ┌───────────────┐                  │    │
│    │                          │               │─────>│  Neo4jLoader  │──────────────────┼────┼──> Neo4j
│    │                          │               │      │               │                  │    │
│    │                          └───────────────┘      └───────────────┘                  │    │
│    │                                                                                     │    │
│    │   ┌─────────────────────────────────────────────────────────────────────────────┐  │    │
│    │   │                           LOCAL STORAGE                                      │  │    │
│    │   │                         (DROPBOX_DIR)                                        │  │    │
│    │   │                                                                              │  │    │
│    │   │    [fetched.csv] ──> [cut.csv] ──> [split_1.csv] [split_2.csv] ...          │  │    │
│    │   │                                                                              │  │    │
│    │   └─────────────────────────────────────────────────────────────────────────────┘  │    │
│    │                                                                                     │    │
│    └─────────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Communication Protocols and Ports

| Interaction | Protocol | Port | Authentication |
|-------------|----------|------|----------------|
| Client → FastAPI | HTTP/HTTPS | 9090 | None (internal) |
| LinuxConnector → NAS | NFS/SMB | 445/2049 | Mount credentials |
| SFTPConnector → Remote | SSH/SCP | 22 | SSH Key / Password |
| Neo4jLoader → Neo4j | Bolt | 7687 | Username/Password |

---

## Data Flow Summary

| Step | Source | Target | Data | Method |
|------|--------|--------|------|--------|
| 1 | Client | FastAPI | HTTP Request | REST API |
| 2 | FastAPI | ImportPipeline | ImportRequest | Python call |
| 3 | ImportPipeline | Connector | Config | Python call |
| 4 | Connector | NAS/SFTP | File request | copy/scp |
| 5 | NAS/SFTP | DROPBOX_DIR | File data | File transfer |
| 6 | Processor | DROPBOX_DIR | Cut file | Local I/O |
| 7 | Processor | DROPBOX_DIR | Split files | Local I/O |
| 8 | Neo4jLoader | Neo4j | CSV data | LOAD CSV |
| 9 | FastAPI | Client | HTTP Response | REST API |

---

## Related Files

| File | Description |
|------|-------------|
| `app/api/data_import.py` | API endpoint definitions |
| `app/services/import_pipeline.py` | Pipeline orchestration |
| `app/services/connectors.py` | Connector implementation |
| `app/services/processors.py` | Data processing (Cut/Split) |
| `app/services/neo4j_loader.py` | Neo4j loading |
| `conf/settings.yaml` | Configuration (DB, Server credentials) |
| `conf/data_map.csv` | Domain mapping |
| `conf/column_map.yaml` | Column mapping configuration |
