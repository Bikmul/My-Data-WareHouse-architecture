graph TB
    A[Источники данных] --> B(Airflow DAGs)
    B --> C[Data Processing]
    C --> D[(ClickHouse)]
    D --> E[Analytics & Visualization]
    
    subgraph "Infrastructure"
        F[Docker]
        G[Orchestration]
    end
    
    B --> G
    F --> B
    F --> D
