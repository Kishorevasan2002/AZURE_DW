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

### Execution Order

1. **Data Processing Pipeline**
   - Run **Bronze ingestion script**(sample_code_snipets)
 
```
def process_and_merge_data(spark: input_path: str, output_path: str, unique_keys: list, data_source_name: str):
    print(f"Starting ingestion for {data_source_name} from {input_path}")
    try:
        # Read the new data from the source path
        new_data_df = spark.read.json(input_path)

        if new_data_df.rdd.isEmpty():
            print(f"No new data found for {data_source_name}. Skipping.")
            return

        updates_df = (new_data_df
                      .withColumn("ingestion_time", current_timestamp())
                      .dropDuplicates(unique_keys)
                     )
        
        # This log now represents records read from the source for processing
        print(f"Found {updates_df.count()} unique records in source to process for {data_source_name}.")

        if DeltaTable.isDeltaTable(spark, output_path):
            print(f"Delta table found at {output_path}. Merging data...")
            delta_table = DeltaTable.forPath(spark, output_path)

            # Get the count before the merge to calculate the difference later
            count_before = delta_table.toDF().count()

            merge_condition = " AND ".join([f"target.{key} = updates.{key}" for key in unique_keys])

            # Perform the merge operation. Note: .execute() returns None.
            delta_table.alias("target").merge(updates_df.alias("updates"),condition=merge_condition).whenNotMatchedInsertAll() \
            .option("mergeSchema", "true") \
            .execute()
            
            # Get the count after the merge
            count_after = DeltaTable.forPath(spark, output_path).toDF().count()
            
            # Calculate the number of newly inserted rows
            rows_inserted = count_after - count_before
            
            # Provide the accurate log message
            print(f"Merge complete. Inserted {rows_inserted} new records into the table.")

        else:
            print(f"Creating new Delta table for {data_source_name} at {output_path}")
            updates_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(output_path)
            print("New table created.")
    except Exception as e:
        print(f"An error occurred during ingestion for {data_source_name}: {e}")


if __name__ == "__main__":
    # --- Process Data ---
    process_and_merge_data(spark, fleet_source_path, fleet_destination_path, ["delivery_id"], "Fleet")
    process_and_merge_data(spark, truck_source_path, truck_destination_path, ["delivery_id", "EventTimestamp"], "Truck Telemetry")
    
```
---
   - Run **Silver transformation script**(sample_code_snipets)
```
def main():
    """
    Main function for the  silver layer transformation.
    """
    # --- Input Paths (Bronze Layer) ---
    fleet_bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/processeddelivery/"
    truck_bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/processedtelemetry/"

    # --- Output Path (Silver Layer) ---
    silver_output_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/delivery_summary/"

    print("--- Reading Bronze Layer Tables ---")
    try:
        fleet_df = spark.read.format("delta").load(fleet_bronze_path)
        truck_df = spark.read.format("delta").load(truck_bronze_path)
        print("Successfully read fleet and truck bronze tables.")
    except AnalysisException as e:
        print(f"Error reading bronze tables: {e}. Please ensure the paths are correct and the tables exist.")
        return

    # --- 1. Clean and Transform Fleet Data ---
    print("\n--- Transforming Fleet Data ---")
    fleet_silver_df = (fleet_df
        .withColumn("pickup_ts", to_timestamp(col("pickup_time")))
        .withColumn("delivery_ts", to_timestamp(col("delivery_time")))
        .withColumn("delivery_duration_hours", 
            round((col("delivery_ts").cast("long") - col("pickup_ts").cast("long")) / 3600, 2) # Rounded
        )
        .select(
            "delivery_id", "delivery_status", "cargo_type", "driver_id", "truck_id",
            "pickup_ts", "delivery_ts", "delivery_duration_hours",
            col("trip_distance_km"), 
            col("cargo_weight_ton"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            col("location").alias("delivery_location")
        )
    )
    print("Fleet data transformation complete.")

    # --- 2. Aggregate Truck Telemetry Data ---
    print("\n--- Aggregating Truck Telemetry Data ---")
    truck_agg_df = (truck_df
        .groupBy("delivery_id")
        .agg(
            round(avg("speed"), 2).alias("avg_speed_kmh"),
            round(max("engine_temp"), 2).alias("max_engine_temp_celsius"),
            round(sum("fuel_used"), 2).alias("total_fuel_used_liters"),
            round(sum("co2_emitted"), 2).alias("total_co2_emitted_kg")
        )
    )
    print("Truck telemetry aggregation complete.")

    # --- 3. Join Fleet and Aggregated Telemetry Data ---
    print("\n--- Joining transformed data to create the delivery summary ---")
    delivery_summary_df = fleet_silver_df.join(
        truck_agg_df,
        "delivery_id",
        "left"
    )

    # --- 4. ADDED: Enrich with Efficiency KPIs ---
    print("\n--- Enriching with new efficiency KPIs (Fuel and CO2) ---")
    enriched_df = (delivery_summary_df
        .withColumn("fuel_efficiency_km_per_liter",
            # Handle potential divide-by-zero errors
            when(col("total_fuel_used_liters") > 0,
                 round(col("trip_distance_km") / col("total_fuel_used_liters"), 2)
            ).otherwise(0)
        )
        .withColumn("co2_efficiency_kg_per_km",
            # Handle potential divide-by-zero errors
            when(col("trip_distance_km") > 0,
                 round(col("total_co2_emitted_kg") / col("trip_distance_km"), 2)
            ).otherwise(0)
        )
    )
    print("Enrichment complete.")
    
    print(f"Join complete. The resulting silver table has {enriched_df.count()} records.")

    # --- 5. Write to Silver Layer ---
    print(f"\n--- Writing to Silver Layer at {silver_output_path} ---")
    (enriched_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true") 
        .save(silver_output_path)
    )
    print("Successfully wrote delivery summary to silver layer.")

    # # --- 6. Verification Step ---
    # print("\n--- Verifying Final Silver Table ---")
    # silver_df_test = spark.read.format("delta").load(silver_output_path)
    # # Using display() is often better in notebooks for interactive viewing
    # display(silver_df_test)

if __name__ == "__main__":
    main()

```
---
   - Run **Gold enrichment script**(sample_code_snipets)
```
def main():
    spark = SparkSession.builder.getOrCreate()
    print("--- Reading Silver and All Reference Tables ---")
    try:
        silver_df = spark.read.format("delta").load(silver_input_path)
        driver_df = spark.read.format("delta").load(driver_details_path)
        truck_df = spark.read.format("delta").load(truck_details_path)
        location_df = spark.read.format("delta").load(location_details_path)
        print("Successfully read all source tables.")
    except AnalysisException as e:
        print(f"Error reading tables: {e}. Please ensure all paths are correct and tables exist.")
        return

    # --- 1. Join Silver Data with All Reference Tables ---
    print("\n--- Enriching performance data with all available context ---")
    
    # The silver table needs a location column to join with location_details
    # Let's assume the silver table has 'delivery_location' from the original fleet data
    enriched_df = (silver_df
        # Join with driver details
        .join(driver_df, "driver_id", "left")
        # Join with truck details
        .join(truck_df, "truck_id", "left")
        # Join with location details using the delivery's actual location
        .join(location_df, silver_df.delivery_location == location_df.location_name, "left")
    )

    # --- 2. Select and Organize the Final Gold Table ---
    # This step creates a clean, well-ordered table for analysts.
    final_gold_df = enriched_df.select(
        # Delivery Core Info
        col("delivery_id"),
        to_date(col("pickup_ts")).alias("delivery_date"),
        col("delivery_status"),
        
        # Driver Info
        col("driver_id"),
        col("driver_name"),
        col("experience_level"),
        
        # Truck Info
        col("truck_id"),
        col("truck_model"),
        col("emission_standard"),
        col("last_maintenance_date"),
        
        # Location Info
        col("location_name").alias("delivery_location_name"), # Renamed to avoid ambiguity
        col("region"),
        col("traffic_index"),
        
        # Performance Metrics
        col("trip_distance_km"),
        col("delivery_duration_hours"),
        col("avg_speed_kmh"),
        
        # ESG Metrics
        col("fuel_efficiency_km_per_liter"),
        col("total_co2_emitted_kg"),
        col("co2_efficiency_kg_per_km")
    ).orderBy("delivery_date", "delivery_id")
    
    print("Enrichment complete. Final table is ready.")

    # --- 3. Write to Gold Layer ---
    print(f"\n--- Writing to Gold Layer at {gold_output_path} ---")
    (final_gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true") 
        .save(gold_output_path)
    )
    print("Successfully wrote enriched_delivery_performance to the gold layer.")

    # # --- 4. Verification Step ---
    # print("\n--- Verifying Final Gold Table ---")
    # gold_df_test = spark.read.format("delta").load(gold_output_path)
    # display(gold_df_test)


if __name__ == "__main__":
    main()

```

### Order of Execution

![Screenshot 2025-09-20 132758-mh](https://github.com/user-attachments/assets/d10ca7ea-8786-4248-8c5c-945e18425065)

---

3. **Power BI Connection**
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

---

## 📊 Final Power BI Dashboard
The live dashboard provides a **real-time view** of fleet performance & ESG KPIs, enabling **data-driven decision-making**.


