# Test Two Mermaid Diagrams

```mermaid
flowchart LR
    A["🗂️ Online Retail II<br/>raw CSV"] --> B["🐘 PostgreSQL<br/>+ MongoDB design"]
    B --> C["⚡ Apache Spark<br/>PySpark ETL + KPIs"]
    C --> D["🌀 Apache Airflow<br/>batch orchestration"]
    D --> E["📡 Kafka + Kafdrop<br/>real-time streaming"]

    style A fill:#1e2761,stroke:#1e2761,color:#fff
    style B fill:#2563eb,stroke:#2563eb,color:#fff
    style C fill:#e25a1c,stroke:#e25a1c,color:#fff
    style D fill:#017cee,stroke:#017cee,color:#fff
    style E fill:#0f172a,stroke:#0f172a,color:#fff
```

## Middle heading

Some text between the two diagrams.

```mermaid
flowchart LR
    A["Extract<br/>retail_extract"] --> B["Validate<br/>validate_retail"]
    B --> C["Transform<br/>transform_retail"]
    C --> D["Load<br/>load_retail_to_postgres"]
    style A fill:#017cee,color:#fff
    style B fill:#017cee,color:#fff
    style C fill:#017cee,color:#fff
    style D fill:#017cee,color:#fff
```

## Test heading after both

Plain paragraph after two mermaid diagrams in the same file.
