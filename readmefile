🛒 Retail Sales Data ETL & Analytics Platform

📌 Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline for retail sales data and presents business insights through an interactive Streamlit dashboard.

The system processes raw sales data from files or APIs, cleans and transforms it using defined business rules, stores it in a relational database, and generates aggregated analytics.

---

⚙️ Tech Stack

- Python (Pandas) – Data processing & transformation
- Database – SQLite / PostgreSQL
- Frontend/UI – Streamlit
- Data Source – CSV / JSON / API

---

🎯 Problem Statement

Retail sales data comes from multiple stores and systems with:

- Missing values
- Incorrect formats
- Data inconsistencies

The goal is to:

- Clean and standardize the data
- Apply business rules
- Store structured data
- Generate meaningful insights for analysis

---

🔁 ETL Pipeline

1. Extract

- Read CSV / JSON files
- Fetch data from API
  Example:
  https://app.beeceptor.com/mock-server/fake-store-api
- Handle file errors and invalid formats

---

2. Transform

- Remove or flag invalid records
- Handle missing unit prices
- Remove negative quantities
- Convert dates into standard format
- Create derived columns:
  - "total_amount = quantity × unit_price"
  - "order_month"
  - "order_day"
- Apply business rules for clean data
- Generate aggregated datasets

---

3. Load

- Create database tables programmatically
- Insert cleaned data into database
- Store aggregated summaries
- Prevent duplicate inserts

---

🗄️ Database Design

Raw Sales Table

- order_id
- order_date
- store_id
- product_id
- product_category
- quantity_sold
- unit_price

Cleaned Sales Table

- order_id
- order_date (formatted)
- store_id
- product_id
- product_category
- quantity_sold
- unit_price
- total_amount
- order_month
- order_day

Aggregated Sales Table

- total_sales_by_store
- total_sales_by_category
- total_sales_by_date/month

---

📊 Analytics Generated

- Total Sales
- Sales by Category
- Sales by Store
- Sales by Date / Month

---

🖥️ Streamlit UI Features

- Upload raw sales file
- Display raw dataset
- Display cleaned/transformed dataset
- Button-triggered database load
- Visualization of sales insights
- Clear comparison: Input vs Output

---

🧪 Data Issues Handled

- Missing unit prices
- Negative quantities
- Incorrect date formats
- Corrupt or invalid records

---

📦 Project Deliverables

- Functional ETL pipeline in Python
- Database populated with cleaned data
- Aggregated sales summary tables
- Interactive Streamlit dashboard
- End-to-end demo:
  Upload → Transform → Load → Visualize

---

📈 Success Metrics

- ETL pipeline runs without errors
- Invalid data handled gracefully
- Cleaned data stored correctly
- UI clearly shows transformations
- Accurate business metrics generated

---

🧠 Evaluation Criteria

Understanding

- Clear explanation of ETL concepts
- Correct interpretation of retail data

System Design

- Proper table schema
- Logical transformation rules
- Efficient aggregations

Python & Engineering

- Proper use of Pandas
- Database connectivity
- Clean, modular code
- Error handling & validation

UI & Presentation

- Clean Streamlit layout
- Clear input vs output visibility
- Ability to explain design decisions
- Smooth live demo execution

---

🚀 How to Run

1. Clone Repository

git clone <repo-link>
cd project-folder

2. Install Dependencies

pip install -r requirements.txt

3. Run Streamlit App

streamlit run app.py

---

🔥 Optional Enhancements

- Filters (store, category, date)
- ETL logging system
- Dockerized setup
- Unit testing for transformation logic
- Role-based dashboards (Admin vs Analyst)
- Deployment on Streamlit Cloud

---

🏁 Sprint Breakdown

Sprint 1: Data Understanding & Design

- Analyze raw data
- Identify issues
- Define transformation rules
- Design database schema

Sprint 2: ETL Implementation

- Build extraction logic
- Implement transformations
- Load into database
- Generate aggregations

Sprint 3: UI & Visualization

- Build Streamlit interface
- Display datasets
- Visualize insights

---

👨‍💻 Author

Shaikshavali

---

⚠️ Final Note

If you can clearly explain:

- Why each transformation exists
- How data flows from raw → clean → aggregated
- Why you designed tables this way

Then you're not just showing a project — you're showing data engineering thinking.
