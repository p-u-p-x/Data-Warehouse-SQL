# Data Warehousing Project: Bronze-Silver-Gold Architecture

This project implements a modern data warehousing architecture using a **Bronze-Silver-Gold** layer approach on SQL Server. The pipeline processes data from multiple source systems (CRM and ERP) through cleansing, transformation, and enrichment stages to produce analytics-ready datasets.

## 🏗️ Architecture Overview

### High-Level Data Flow

**Sources → Bronze Layer → Silver Layer → Gold Layer → Consumption**

## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── Data_Architecture.png           # PNG file shows the project's architecture
│   ├── Data_Catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── Data_Flow.png                   # PNG file for the data flow diagram
│   ├── Data_Models.png                 # PNG file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
└── LICENSE                             # License information for the repository
```
---

### Layer Specifications

#### **Bronze Layer (Raw Data)**
- **Purpose**: Raw, unmodified source data ingestion
- **Load Method**: Batch processing with full load
- **Transformations**: None (as-is storage)
- **Objects**: Tables in `bronze` schema
- **Source Systems**: 
  - CRM System: Customer, Product, Sales data
  - ERP System: Location, Customer demographics, Product categories

#### **Silver Layer (Cleansed Data)**
- **Purpose**: Data cleaning, standardization, and enrichment
- **Load Method**: Batch processing with full load
- **Transformations**: 
  - Data cleaning and normalization
  - Data conversion and type casting
  - Data enrichment and business rule application
  - Duplicate removal and quality checks
- **Objects**: Tables in `silver` schema

#### **Gold Layer (Analytics Ready)**
- **Purpose**: Business-friendly dimensional models for analytics
- **Objects**: Views in `gold` schema
- **Data Model**: Star schema with facts and dimensions
- **Consumption**: BI reporting, ad-hoc queries, machine learning

---

## 📊 Data Model

### Gold Layer Dimensions & Facts

#### **Dimensions**
- `dim_customers`: Customer master data with surrogate keys
- `dim_products`: Product information with current state tracking

#### **Facts**
- `fact_sales`: Sales transactions with foreign keys to dimensions

---

## 🚀 Getting Started

### Prerequisites
- SQL Server 2016+
- Source CSV files in `/tmp/source_crm/` and `/tmp/source_erp/` directories

### Installation & Setup

1. **Create Database and Schemas**
```sql
-- Run the database creation script
-- This creates DataWarehouse database with bronze, silver, gold schemas
```

2. **Deploy Bronze Layer Tables**
```sql
-- Execute the DDL script to create bronze tables
```

3. **Deploy Silver Layer Tables**
```sql
-- Execute the DDL script to create silver tables
```

---

# 🔄 ETL Processes

## Bronze Layer Loading

```sql
-- Load raw data from CSV files into bronze layer
EXEC bronze.load_bronze;
```

---

### Features

- **Bulk insert operations** for high-performance data loading  
- **Comprehensive error handling** and detailed logging  
- **Duration tracking** for performance monitoring and optimization  
- **Table truncation before load** (full refresh pattern)

## 🪙 Silver Layer Transformation
```sql
-- Transform and cleanse bronze data into silver layer
EXEC silver.load_silver;
```
---

### 🧹 Data Quality Operations

- **Duplicate Removal**: Keep latest records using window functions  
- **Data Standardization**: Normalize values (gender, marital status, countries)  
- **Type Conversion**: Handle date formats, numeric conversions  
- **Business Rules**: Apply data validation and correction logic  
- **Referential Integrity**: Validate relationships between entities  

#### 📊 Data Standardization
- Normalize gender values (`'M'/'F'` → `'Male'/'Female'`)
- Standardize marital status codes (`'S'/'M'` → `'Single'/'Married'`)
- Convert country codes to full names (`'US'` → `'United States'`)
- Cleanse and trim unwanted spaces from text fields

#### 🔄 Type Conversion
- Handle integer date formats (`20240101` → `'2024-01-01'`)
- Convert numeric fields with proper validation
- Cast data types consistently across the layer
- Handle `NULL` and default values appropriately

#### 📐 Business Rules
- Apply data validation and correction logic
- Recalculate derived fields (`sales = quantity × price`)
- Validate date sequences (`order date ≤ ship date`)
- Handle out-of-range and invalid values

#### 🔗 Referential Integrity
- Validate relationships between entities
- Ensure foreign key consistency across tables
- Handle orphaned records with proper defaults
- Maintain data consistency across source systems

---

# 📈 Data Quality Framework

## Quality Checks Implemented

### 🔍 Duplicate Detection

```sql
-- Identify duplicate records for deduplication
ROW_NUMBER() OVER (PARTITION BY key_fields ORDER BY timestamp DESC)
```

### 🧪 Data Validation

- Trim unwanted spaces from text fields  
- Validate numeric ranges and non-negative values  
- Check date logic (end dates after start dates)  
- Ensure referential integrity across related tables  

---

### 🔄 Consistency Rules

- Standardize gender values (`'M'/'F'` → `'Male'/'Female'`)  
- Normalize country codes to full names  
- Map product line abbreviations to descriptive names  
- Handle `NULL` and default values consistently  

---

### 🗂️ Source Data Structure

#### 📁 CRM Source Files

- `cust_info.csv`: Customer demographic information  
- `prd_info.csv`: Product master data with SCD Type 2 support  
- `sales_details.csv`: Sales transaction records  

#### 📁 ERP Source Files

- `LOC_A101.csv`: Customer location and country data  
- `CUST_AZ12.csv`: Additional customer attributes  
- `PX_CAT_G1V2.csv`: Product category hierarchy  

### 🔍 Usage Examples

#### Analytical Queries
```sql
-- Sales analysis with customer and product dimensions
SELECT 
    c.first_name,
    c.country,
    p.product_name,
    SUM(f.sales_amount) as total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY c.first_name, c.country, p.product_name;
```

#### 🧭 Data Quality Monitoring
```sql
-- Check for orphaned records in fact table
SELECT COUNT(*) 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;
```
---

### 🛠️ Technical Features

- **Surrogate Key Management**: Automated key generation using `ROW_NUMBER()`  
- **Slowly Changing Dimensions**: Type 2 support for product dimension  
- **Performance Optimization**: Bulk operations, table truncation, and indexing  
- **Error Handling**: Comprehensive try-catch blocks with detailed logging  
- **Audit Trail**: `dwh_create_date` timestamps on all silver tables  

---

### 📝 Maintenance

#### 🔄 Regular Operations

- **Data Refresh**: Execute stored procedures in sequence  
- **Quality Monitoring**: Run data quality checks post-load  
- **Performance Tuning**: Monitor load durations and optimize queries  

#### 🛠️ Troubleshooting

- Check file paths and permissions for CSV sources  
- Verify data quality checks for transformation issues  
- Monitor error logs for ETL failures  

---

## 📬 Contact

- ✉️ **Email:** [i.sajeela.noor@gmail.com](mailto:i.sajeela.noor@gmail.com)  
- 💼 **LinkedIn:** [Sajeela Noor](https://www.linkedin.com/in/sajeela-noor-82b510256)
