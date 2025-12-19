我有另一個 project   要用到  這個 data  import 的api。我應該把哪些檔案加入另一個FastAPI  project？                                                                                       
                                                                                                                                                                                           
                                                                                                                                                                                           
⏺ 這是你需要的檔案清單：                                                                                                                                                                   
                                                                                                                                                                                           
  核心服務 (必須)                                                                                                                                                                          
                                                                                                                                                                                           
  app/services/                                                                                                                                                                            
  ├── __init__.py                                                                                                                                                                          
  ├── connectors.py      # 連接器、設定載入                                                                                                                                                
  ├── processors.py      # 欄位切割、檔案分割                                                                                                                                              
  ├── neo4j_loader.py    # Neo4j 載入                                                                                                                                                      
  └── import_pipeline.py # Pipeline 主邏輯                                                                                                                                                 
                                                                                                                                                                                           
  設定檔 (必須)                                                                                                                                                                            
                                                                                                                                                                                           
  conf/                                                                                                                                                                                    
  ├── settings.yaml      # DROPBOX_DIR, Neo4j 連線                                                                                                                                         
  ├── column_map.yaml    # 欄位對應設定                                                                                                                                                    
  └── data_map.csv       # 資料來源對應                                                                                                                                                    
                                                                                                                                                                                           
  API Route (參考)                                                                                                                                                                         
                                                                                                                                                                                           
  # 在你的 FastAPI app 加入                                                                                                                                                                
  from app.services.import_pipeline import ImportPipeline, ImportRequest, build_default_pipeline                                                                                           
                                                                                                                                                                                           
  @router.post("/imports")                                                                                                                                                                 
  async def create_import(request_data: dict, background_tasks: BackgroundTasks):                                                                                                          
      pipeline = build_default_pipeline()                                                                                                                                                  
      request = ImportRequest(                                                                                                                                                             
          domain_type=request_data["domain_type"],                                                                                                                                         
          domain_name=request_data["domain_name"],                                                                                                                                         
          cob_date=request_data["cob_date"]                                                                                                                                                
      )                                                                                                                                                                                    
      background_tasks.add_task(pipeline.run, request)                                                                                                                                     
      return {"status": "accepted"}                                                                                                                                                        
                                                                                                                                                                                           
  依賴套件 (requirements.txt)                                                                                                                                                              
                                                                                                                                                                                           
  neo4j>=5.0.0                                                                                                                                                                             
  pyyaml>=6.0                                                                                                                                                                              
  pandas>=2.0.0  # 可選，沒有會用 CSV fallback                                                                                                                                             
                                                                                                                                                                                           
  ---                                                                                                                                                                                      
  快速複製指令                                                                                                                                                                             
                                                                                                                                                                                           
  # 在目標 project 目錄執行                                                                                                                                                                
  mkdir -p app/services conf                                                                                                                                                               
                                                                                                                                                                                           
  cp /Users/francis/dev_citi/microservice_data_import/app/services/{__init__,connectors,processors,neo4j_loader,import_pipeline}.py app/services/                                          
                                                                                                                                                                                           
  cp /Users/francis/dev_citi/microservice_data_import/conf/{settings.yaml,column_map.yaml,data_map.csv} conf/                                                                              
                                                                                                                                                                                           
  然後修改 conf/settings.yaml 和 conf/data_map.csv 配合新專案的路徑即可。                                                                                                                  
                                                                                                                                                                                           
  要我幫你建立一個獨立的 Python package 方便重用嗎？          