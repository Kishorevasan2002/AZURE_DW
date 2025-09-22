# End-to-End ESG Fleet Management Analytics Platform

An **end-to-end data pipeline** built on the **Azure cloud** to ingest, process, and analyze real-time fleet performance and **Environmental, Social, and Governance (ESG) metrics**.  
The solution delivers actionable insights through a live **Power BI dashboard** connected to a scalable **data lakehouse**.

---

## 📌 Project Overview
This project addresses the growing need for businesses to monitor not only their **operational efficiency** but also their **environmental impact**.  
The platform provides a **near real-time view** of a simulated logistics fleet, answering key business questions such as:

- Driver performance  
- Truck efficiency  
- Regional operations  
- CO₂ emissions
- Fuel Efficiency

The solution is built on a **modern, cloud-native data stack**, demonstrating a robust implementation of the **Medallion Architecture** for a scalable and reliable **data lakehouse**.
## Key Features & KPIs
The platform provides insights into:

- **Operational KPIs**: Total Deliveries, Avg Delivery Duration, Avg Speed, Trip Distance  
- **ESG Metrics**: Total CO₂ Emitted (kg), CO₂ Efficiency (kg/km), Fuel Efficiency (km/L)  
- **Driver Analytics**: Individual driver performance & efficiency  
- **Fleet Analytics**: Truck performance by model, emission standard, maintenance status  
- **Regional Analysis**: KPIs sliced by region & traffic index  

---

## Data Model
The **Medallion Architecture** ensures progressive data refinement.

- **landing stage:** `raw_json` 
- **bronze Table:** `raw_converted to (delta)` 
- **Silver Fact Table:** `delivery_summary`  
- **Dimension Tables:** `driver_details`, `truck_details`, `location_details` 
- **Final Gold Table:** `enriched_delivery_performance`  
  - Wide, denormalized table with **20+ performance metrics and attributes**

---

## Architecture
The platform follows a **streaming + batch processing architecture**, moving data through **Bronze, Silver, and Gold layers**.
<img width="2274" height="854" alt="diagram-export-9-20-2025-11_30_45-AM" src="https://github.com/user-attachments/assets/6a3b36fb-42b9-4e22-a6cc-ffc1ddaedf18" />

**Data Flow:**
1. **Ingestion** – A Python simulator generates JSON data (fleet deliveries & truck telemetry) → sent to **Azure Event Hubs**.  
2. **Stream Processing** – **Azure Stream Analytics** captures events, performs shaping, and lands data in **Azure Data Lake Storage Gen2 (ADLS)**.  
3. **Bronze Layer (Raw)** – **PySpark** adds metadata and uses **Delta Lake MERGE** for deduplicated, idempotent tables.  
4. **Silver Layer (Cleansed)** – Transforms raw data into a **single source of truth**, calculating KPIs and aggregated summaries.  
5. **Gold Layer (Business-Ready)** – Enriches Silver with **dimension tables** (drivers, trucks, locations) to produce a wide denormalized table.  
6. **Serving & Visualization** – A **Synapse Serverless SQL view** provides a high-performance endpoint for **Power BI** in **DirectQuery mode**.

---

## ⚙️ Technology Stack

| Category            | Technology                         | Purpose                                                  |
|---------------------|------------------------------------|----------------------------------------------------------|
| **Cloud Platform**  | Microsoft Azure                   | Foundational cloud provider for all services             |
| **Ingestion**       | Azure Event Hubs, Stream Analytics | Real-time data ingestion & routing                      |
| **Storage**         | Azure Data Lake Storage Gen2       | Scalable, secure storage for multi-layered data lake     |
| **Processing**      | Azure Synapse Analytics, PySpark   | ETL & data transformations at scale                      |
| **Lakehouse**       | Delta Lake                        | ACID reliability for the data lake                       |
| **Serving**         | Synapse Serverless SQL             | Logical data warehouse over the Gold layer               |
| **BI & Viz**        | Microsoft Power BI                 | Interactive dashboards & reports                         |
| **Languages**       | Python, SQL                       | Data processing & query logic                            |

---

## Setup and Execution Guide

### Prerequisites
- Active **Azure Subscription**  
- Deployed resources:  
  - Synapse Analytics Workspace  
  - ADLS Gen2  
  - Event Hubs
  - Azure Stream Analytics
- Synapse Managed Identity with **Storage Blob Data Contributor** role on ADLS

---

## 🚀 Architecture Components

### 1. Create Event Hubs Namespace & Event Hubs
- Create a **Namespace** in the Azure Portal (SKU: Standard).  
- Add Event Hubs:  
  - `esg-fleet-events` (delivery events)  
  - `esg-fleet-telemetry` (truck telemetry)  
- Configure partitions (4–8) and retention (7 days).  
- Add a **Consumer Group** for Stream Analytics (e.g., `asa_consumer`).  

---

### 2. Create ADLS Gen2 Containers & Assign Roles
- Create containers in your ADLS account:  
  - `bronze`  
  - `silver`  
  - `gold`  
  - `bronze/errors`  
- In **Access control (IAM)**, assign:
  - **ENABLE hierarchical namespace and soft delete**  
  - **Storage Blob Data Contributor** role to the **Stream Analytics Managed Identity**.  
  - **Storage Blob Data Contributor** role to the **Synapse Managed Identity**.  

---

### 3. Create Stream Analytics Job
- In Azure Portal → **Stream Analytics job** → Create `esg-shaper`.  
- **Inputs**:  
  - Event Hub stream inputs (`esg-fleet-events`, `esg-fleet-telemetry`).  
- **Outputs**:  
  - ADLS Gen2 (`bronze/processed/`) in JSON.
  - Optional: Power BI for real-time dashboards.  
- **Query**:  
  Define aggregation and joins between telemetry and delivery events (see `SQL script`).  

---

### 4. Deploy & Test
- Start the ASA job from the portal.  
- Push sample events into Event Hubs using the simulator.  
- Validate outputs in ADLS:  
  - Raw JSON in `eventhub-capture` (via Event Hub Capture).  
  - Processed/aggregated JSON/Parquet in `bronze/processed/YYYY/...`.  
- Verify Event Hub metrics and Stream Analytics diagnostics in the Azure Portal.  

---

### 5. Connect Downstream (Synapse + Power BI) (verify scripts)
- Synapse/PySpark jobs load data from `bronze/processed/` → transform into **Delta tables** in Bronze/Silver/Gold zones.  
- Build **Serverless SQL external views** for Power BI (`vw_enriched_delivery_performance`).  
- Connect Power BI via **DirectQuery** for real-time reporting.  

---

##  6. Security & Managed Identity Setup
- **Stream Analytics Managed Identity**:  
  - Enable under **Identity** → System assigned → On.  
- **Assign Permissions**:  
  - Storage account → Access control → Add **Storage Blob Data Contributor** → assign ASA managed identity.  
  - Do the same for **Synapse Managed Identity**.  
- **Event Hubs**:  
  - Use minimal **Shared access policies** (Data Sender for simulator).  
  - Store secrets in **Azure Key Vault** for secure use across services.  

---

## ✅ Checklist
- [x] Event Hubs namespace
- [x] ADLS containers provisioned (`bronze/silver/gold/errors`)  
- [x] Stream Analytics job created with Event Hub input + ADLS output  
- [x] Managed Identities enabled and assigned correct RBAC roles  
- [x] End-to-end pipeline tested with simulator → Event Hubs → ASA → ADLS → Synapse → Power BI  

---


### Execution Order

1. **Data Processing Pipeline**
   - Run **Bronze ingestion script**(sample_code_snipets)
   - Run **Silver transformation script**(sample_code_snipets)
   - Run **Gold enrichment script**(sample_code_snipets)

### Execution

![Screenshot 2025-09-20 132758-mh](https://github.com/user-attachments/assets/d10ca7ea-8786-4248-8c5c-945e18425065)

---

![Screenshot 2025-09-20 140132-mh](https://github.com/user-attachments/assets/0f6fdd90-9c4d-4171-885a-3000ac06e33d)

---

<img width="1592" height="440" alt="Screenshot 2025-09-20 123325-mh" src="https://github.com/user-attachments/assets/4f43dd88-2aed-425e-8ffa-8c77e4e76255" />

---

3. **Power BI Connection**
The live dashboard provides a **real-time view** of fleet performance & ESG KPIs, enabling **data-driven decision-making**.
   - In **Power BI Desktop** → `Get Data > Azure Synapse Analytics SQL`  
   - Enter Synapse Serverless SQL endpoint:  
     ```
     your-workspace-ondemand.sql.azuresynapse.net
     ```
   - Connect in **DirectQuery mode**  
   - Select the `vw_enriched_delivery_performance` view  
<img width="1919" height="1010" alt="Screenshot 2025-09-20 121546-mh" src="https://github.com/user-attachments/assets/0638c7e1-1818-40ea-83a5-214bbbcce493" />

---

##  Future Enhancements
- **Streaming ETL** – Convert Silver & Gold jobs to **Structured Streaming with Auto Loader** for lower latency  
- **Predictive Analytics** – Train ML models on Gold data for:  
  - Delivery time predictions  
  - Predictive truck maintenance  


