## 1. gold.dim_customers

**Purpose:** Stores customer details enriched with demographic and geographic data.

**Columns:**

| Column Name      | Data Type     | Description |
|------------------|---------------|-------------|
| customer_key     | INT           | Surrogate key uniquely identifying each customer record |
| customer_id      | INT           | Unique numerical identifier assigned to each customer |
| customer_number  | NVARCHAR(50)  | Alphanumeric identifier used for tracking and referencing |
| first_name       | NVARCHAR(50)  | Customer's first name |
| last_name        | NVARCHAR(50)  | Customer's last name |
| country          | NVARCHAR(50)  | Country of residence |
| marital_status   | NVARCHAR(50)  | Marital status (Married, Single, etc.) |
| gender           | NVARCHAR(50)  | Gender of the customer |
| birthdate        | DATE          | Customer date of birth |
| create_date      | DATE          | Date when the record was created |
