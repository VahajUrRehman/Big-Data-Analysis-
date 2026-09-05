# Test Mermaid

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

## Test heading after mermaid

Plain paragraph to confirm markdown still parses after the mermaid block.
