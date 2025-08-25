# CS3200 Incidents Database Project

## Overview

This project implements a **normalized relational database** for analyzing airline incidents.
It covers the full workflow from **schema design and creation** to **data loading, validation, and analysis reporting**.

Deliverables include:

* A **3NF database schema** with an ERD
* R scripts to create, load, test, and delete the database
* Analytical reports (PDF/HTML) generated from the incident and flight data

---

## Project Structure

### 1. **Database Design**

* **DesignNormalizedDatabase.docx / DesignNormalizedDatabase.pdf**

  * Defines **functional dependencies** in the raw incidents relation
  * Decomposes into **3NF relations** (`Airline`, `Airport`, `Aircraft`, `IncidentType`, `Severity`, `Reporter`, `Flight`, `Incident`)
  * Specifies **primary/foreign keys** and relationships
  * Includes an **ERD (Crow’s Foot notation)**

---

### 2. **Database Scripts**

All R scripts connect to the **cloud-hosted MySQL database (Aiven)** using SSL authentication.

* **configBusinessLogic.PractI.SajeevN.R**
  Centralized **database connection settings** (host, port, credentials, SSL certificate).
  Used by all other R scripts.

* **createDB.PractI.SajeevN.R**
  Creates all tables in the **normalized schema**.

* **loadDB.PractI.SajeevN.R**
  Loads **incident and flight CSV data** into the schema.

* **testDBLoading.PractI.SajeevN.R**
  Runs **sample queries** to verify data was inserted correctly.

* **deleteDB.PractI.SajeevN.R**
  Drops all project tables, allowing a clean reset.

---

### 3. **Analysis & Reports**

Generated using **RMarkdown** from the loaded database.

* **IncidentsReport.PractI.SajeevN.Rmd** – Source analysis script (RMarkdown).
* **IncidentsReport.PractI.SajeevN.pdf** – Rendered report (PDF).
* **IncidentsReport.PractI.SajeevN.nb.html** – Rendered report (HTML).

Report contents include:

* **Incident type frequencies by month**
* **Airline-level statistics** (incident counts, average delays)
* **Yearly trends** in incidents and delays

---

## Usage Workflow

1. **Set up DB config** → edit `configBusinessLogic.PractI.SajeevN.R` with credentials.
2. **Create schema** → run `createDB.PractI.SajeevN.R`.
3. **Load data** → run `loadDB.PractI.SajeevN.R`.
4. **Test data integrity** → run `testDBLoading.PractI.SajeevN.R`.
5. **Generate reports** → knit `IncidentsReport.PractI.SajeevN.Rmd` to PDF/HTML.
6. **(Optional)** Reset DB → run `deleteDB.PractI.SajeevN.R`.

---

## Tech Stack

* **R** (RMySQL, sqldf, knitr)
* **MySQL (Aiven Cloud)**
* **RMarkdown / Pandoc** (report generation)
* **LucidChart / Mermaid** (ERD design)

---

## Author

**Narayan Sajeev**
Northeastern University – CS3200 (Summer 2, 2025)