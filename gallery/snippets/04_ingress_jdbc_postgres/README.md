# JDBC Ingress Snippet

Demonstrates how to read data from a relational database (SQLite or PostgreSQL) using JDBC.

## Setup

This snippet supports both local **SQLite** (for zero-dependency testing) and **PostgreSQL**.

1. **Populate the Database**:
   ```bash
   python populate_db.py
   ```

2. **Configure Environment**:
   Create a `.env` file in this directory. 
   
   **For SQLite (Default):**
   ```env
   JDBC_URL=jdbc:sqlite:data/input/inventory.db
   JDBC_DRIVER=org.sqlite.JDBC
   ```

   **For PostgreSQL:**
   ```env
   JDBC_URL=jdbc:postgresql://localhost:5432/your_db
   JDBC_USER=postgres
   JDBC_PASSWORD=your_password
   JDBC_DRIVER=org.postgresql.Driver
   ```

## How to Run

1. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

2. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```
