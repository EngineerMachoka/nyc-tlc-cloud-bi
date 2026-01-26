NYC TLC Enterprise BI Analytics Platform

Overview

This repository contains an end-to-end enterprise BI analytics solution built using Azure SQL, Azure Blob Storage, Power BI, Python, and Git

The project demonstrates real-world BI engineering practices commonly used in UK commercial environments

🏗 Architecture

Data Flow:

NYC TLC Parquet Files
        ↓
Azure Blob Storage
        ↓
Azure SQL Database
        ↓
SQL Views (Fact & KPIs)
        ↓
Power BI Semantic Model
        ↓
Executive Dashboards

📐 Data Model

Fact Tables

  Daily trips & revenue

  Monthly KPIs (YoY, MoM, Risk)

Dimensions

  Date

  Month

  Borough

Design

  Star schema

  Single-direction relationships

  Optimised for performance and scalability


📊 Power BI Features

Executive KPI dashboard

Borough performance analysis

Risk-focused insights

Interactive slicers (date, month, borough)

Drill-through & tooltips

Row-Level Security (RLS)

Deployment Pipelines (Dev / Test / Prod)

🔐 Security

Dynamic Row-Level Security using user context

Designed for multi-stakeholder access

⚙ Automation

Python scripts for data ingestion

SQL views for aggregation

GitHub Actions ready for CI/CD

Refresh-safe Power BI model

🧩 Extensibility

The dataset and model are reusable:

New KPIs can be added without changing ingestion

Additional dashboards can reuse the semantic layer

Suitable for predictive analytics extensions

🛠 Tech Stack

Power BI (Desktop & Service)

Azure SQL Database

Azure Blob Storage

Python

SQL

Git & GitHub
