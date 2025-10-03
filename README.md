# Data Engineering Interview Project: Online Retail II ETL/ELT Pipeline

這是英國線上零售商 2009/12/01 - 2011/12/09 交易數據的資料倉儲建置與自動化 ETL 實作專案。

---

## 🎯 專案目標

目標是將原始 CSV 交易資料轉化為一個可供分析的 **Star Schema** 資料倉儲，並建立自動化 **ETL/ELT** 流程，以支援每日銷售、客戶分析和地區分佈等即時報表需求。

---

## 🛠️ 技術棧 (Technology Stack)

| 類別 | 工具 | 目的 |
| :--- | :--- | :--- |
| **協調與排程** | Prefect 2.x | 每日 ETL 流程自動化、監控與告警。 |
| **資料庫** | ClickHouse | 作為 OLAP 資料倉儲，提供快速的聚合查詢和報表支持 (經考官允許使用)。 |
| **語言與函式庫** | Python, Poetry | ETL 核心邏輯開發與依賴管理。 |
| **基礎設施** | k3d, ArgoCD | **(詳見 `interview-infrastructure` 專案)** 專案運行所需的本地 Kubernetes 環境與 GitOps 管理。 |
| **視覺化** | Metabase | 報表與儀表板呈現。 |

---

## 🚀 環境部署與設置

**⚠️ 前提條件：** 本 pipeline 假設基礎設施（Prefect Server, ClickHouse Server, Metabase）已透過 `zhweiliu/interview-infrastructure` 專案部署完成。

### 1. 取得原始資料

請從 **UCI Online Retail II** 資料集連結下載原始 CSV 檔案，並將其放置於 Prefect Agent 可存取的位置。

> 來源: [https://archive.ics.uci.edu/dataset/502/online+retail+ii](https://archive.ics.uci.edu/dataset/502/online+retail+ii)

### 2. Python 環境與依賴安裝

使用 Poetry 建立虛擬環境並安裝所有依賴：

```bash
# Clone the repository
git clone [https://github.com/zhweiliu/interview-pipeline.git](https://github.com/zhweiliu/interview-pipeline.git)
cd interview-pipeline

# Install dependencies using Poetry
poetry install
poetry shell
```

### 3. 設定 Prefect Flow
設定 Prefect 遠端連線： 確保您的本地環境已配置正確的 PREFECT_API_URL 和 PREFECT_API_KEY 以連接到 k8s Cluster 中的 Prefect Server。

建立 Prefect Block： 根據您的 ClickHouse 連線資訊，在 Prefect UI 中建立 ClickHouse 連線 Block。

部署 Flow： 執行以下指令將 ETL 流程部署到 Prefect Server：

```bash
# 部署主要的 ETL/ELT Flow
prefect deployment build ./online_retails/online_retail_flow.py:online_retail_etl_flow --name "Online Retail Daily ETL" --apply

# 部署資料品質監控與告警 Flow
prefect deployment build ./utilx/notification_flow.py:data_quality_alert_flow --name "Data Quality Check & Alert" --apply
```

### 4. 執行與排程

- 手動執行： 您可以直接在 Prefect UI 中點擊部署的 Flow 進行手動執行測試。
- 自動排程： ETL 流程已設定為每日 Cron 排程執行。

---

## 🗃️ 資料倉儲模型與設計假設 (Data Vault 2.0 & Star Schema)

本專案採用 **Data Vault 2.0** 框架進行分層建模，最終層 **Marts** 採用 **Star Schema** 設計。所有資料庫均選用 **ClickHouse** (接受其不支援 Foreign Key 實體約束的特性)。

### 1. 資料庫與分層架構

| 資料庫名稱 | 目的 (Data Vault 2.0) | 備註 |
| :--- | :--- | :--- |
| **raw** | 存放原始資料 (Landing) 和 **PSA** (Persistent Staging Area) | **預先計算所有 Hash Key (Load Date, Hash Key, Hash Diff)**，作為進入 Vault 的準備。 |
| **vault** | 存放 Hub, Link, Satellite 資料表 | 包含業務鍵 (Business Keys)、關係 (Links) 和描述性資料 (Satellites)。 |
| **marts** | 存放所有 **Fact Table** 和 **Dim Table** | 依據 Star Schema 建模，直接供下游報表 (Metabase) 使用。 |
| **quality** | 存放 Data Quality 檢查結果 | 記錄每日銷售總額範圍、缺失客戶 ID 比例等監控數據。 |

> **DDL 腳本：** 所有資料庫與表格的初始化 SQL 語法位於 `./online_retails/ddl/init.sql`。

### 2. Marts 層 Star Schema 設計假設 (針對 Fact_Sales)

| 考題要求 | 設計決策 | 決策解釋 |
| :--- | :--- | :--- |
| **Sale ID (PK)** | **Hash Key (InvoiceNo + StockCode + InvoiceDate)** | 作為 Marts 層 Fact 表的主鍵，使用 Data Vault 結構中的 Link Hash Key，保證每筆明細的唯一性。 |
| **Invoice No (FK)** | **保留為普通欄位** | 由於缺乏 Dim_Invoice 表，此欄位在 Fact_Sales 中僅作為業務識別碼保留，**不設置為 FK**。 |
| **Product Key** (StockCode) | **Hash ID (基於 StockCode)** | 在 **raw** 層預計算 StockCode 的 Hash Key。**StockCode 字母尾綴視為獨立的 SKU**（例如 $84029G$ 和 $84029E$ 是兩個不同的產品），避免在 Marts 層出現聚合失真。 |
| **Time Key** (InvoiceDate) | **String (基於 InvoiceDate 的 hash)** | time_key 為 InvoiceDate 的 hash_key。 Dim_Time 粒度為**年**/**月**/**天**/**星期幾**，Dim_Time 無法支持小時級別分析。 |
| **退貨處理** | **獨立 Fact_Sale_Returns 表** | 將所有 InvoiceNo 以 C 開頭或 Quantity < 0 的記錄，從 Fact_Sales 中**排除**，並載入到 **Fact_Sale_Returns** 表中 (與 Fact_Sales 結構相同)。 |

### 3. 自動化與監控機制 (Prometheus/Grafana)

* **監控腳本：** 指標數據腳本位於 `./online_retails/tasks/metrics/notification.py`。
* **指標傳遞：** 該腳本負責計算需要監控的指標數據（如銷售總額、缺失客戶 ID 比例）並將其傳遞給 **Prometheus**。
* **視覺化與告警：** 最終在 **Grafana** (部署於 [interview-infrastructure](https://github.com/zhweiliu/interview-infrastructure) 專案) 中呈現監控儀表板，並透過 Grafana 的內建 Alert 機制發送告警。

### 資料清理邏輯

* **空值處理：** 丟棄 StockCode、 InvoiceDate 為空的行。 將 Customer ID 空行轉變為 Default Value `0` ，用於計算 Customer ID 缺失比例。
* **重複資料：** 依據 (InvoiceNo + StockCode + InvoiceDate) 組合進行去重。
* **基本轉換：** 計算 TotalAmount = Quantity $\times$ UnitPrice。
* **異常標記：** UnitPrice $\le$ 0 或 Quantity > 1000 的行將被標記，並透過 Grafana **輸出警示**。

---

## 💡 成果展示 (視覺化)

以下是使用 **Metabase** 連接 ClickHouse 資料倉儲所產生的關鍵報表截圖：

*  **每日銷售趨勢 (折線圖)**
*  **國家別銷售排行 (長條圖) (過去 30 天)** 
*  **熱門商品銷售 Top 10 (過去 30 天)**

![metabase-dashboard.png](pic/metabase-dashboard.png)

