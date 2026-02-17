# Data-Warehouse+ Data Modeling 
DATA WAREHOUSIN# Data Warehouse Projects

This repository demonstrates a **star-schema data warehouse design and slowly changing dimesnions type and type 2** built using SQL.  
It includes **fact and dimension tables**, along with ETL scripts to populate them.  

---

## Project Overview

**Fact Table:** `fact_sales`  
- Contains transactional sales data  
- Columns include: `OrderID`, `DimDateKey`, `CustomerID`, `ProductID`, `Quantity`, `UnitPrice`, `TotalAmount`  
- Linked to **dimension tables** using surrogate keys (`DimDateKey`, `CustomerID`, `ProductID`)  

**Dimension Tables:**  
1. **dim_date**: Stores all dates and corresponding `DimDateKey`  
2. **dim_customer**: Stores customer details (`CustomerID`, `CustomerName`, `CustomerEmail`)  
3. **dim_product**: Stores product details (`ProductID`, `ProductName`, `ProductCategory`)
   

**Views:**  
- `view_dimdate`: Generates `DimDateKey` using `ROW_NUMBER()` over distinct dates  

---


