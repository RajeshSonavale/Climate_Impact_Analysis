🌍 Climate Impact Data Pipeline & Dashboard

📌 Project Overview

This project is an end-to-end automated climate data analytics system that collects real-time weather data, processes it through a Python pipeline, and visualizes insights using a Power BI dashboard. The system is designed to analyze temperature, humidity, and pressure trends across cities with interactive reporting and automation.

🚀 Key Features
Automated hourly data collection using Python
Task Scheduler-based pipeline automation
Cleaned and structured dataset (ETL process)
Time-based feature engineering (Date, Hour, Day, Month)
Interactive Power BI dashboard with KPIs and visuals
DAX-based calculations for dynamic insights
Map-based geographic visualization
Trend analysis and top city insights

🏗️ Project Workflow
Data Source → Python Script → Automated Scheduler → CSV Storage → Data Cleaning → Power BI Model → Dashboard Visualization

🧹 Data Processing Steps
Removed missing and duplicate values
Standardized timestamp format
Created time-based features (Hour, Day, Month)
Prepared dataset for visualization

📊 Dashboard Components
🔹 KPI Cards
Average Temperature
Average Humidity
Average Pressure

🔹 Visualizations
Temperature by City (Map View)
Temperature Trend Over Time (Line Chart)
Average Temperature by Day (Bar Chart)
Top 5 Hottest Cities

🔹 Filters
Date Range Filter
City Filter
Hour Filter

🧠 DAX Measures Used
Avg Temperature = AVERAGE(Table[Temperature])
Avg Humidity = AVERAGE(Table[Humidity])
Avg Pressure = AVERAGE(Table[Pressure])

🛠️ Tech Stack
Python (Data Pipeline)
Task Scheduler (Automation)
CSV (Data Storage)
Power BI (Visualization)
DAX (Calculations)

📈 Outcome
Fully automated data pipeline
Real-time structured dataset generation
Interactive and dynamic Power BI dashboard
Strong analytical insights for climate trends

💡 Key Learnings
End-to-end ETL pipeline design
Automation using scheduled tasks
Data modeling for BI dashboards
Advanced visualization techniques in Power BI
DAX-based analytical calculations

📌 Author

Rajesh Sonavale