flowchart TD
    %% Существующая архитектура
    A[🌐 Веб-сайты & API<br/>JSON/XML/REST] --> B[🔄 Apache Airflow<br/>DAGs & Scheduling]
    B --> C{⚡ ClickHouse Кластер 2x2<br/>Columnar Storage}
    
    subgraph C [Distributed Database]
        direction LR
        C1[🖥️ Шард 1-A<br/>ch1:8123] <--> C2[🖥️ Шард 1-B<br/>ch2:8124]
        C3[🖥️ Шард 2-C<br/>ch3:8125] <--> C4[🖥️ Шард 2-D<br/>ch4:8126]
    end
    
    C --> D[📊 Apache Superset<br/>BI Platform]
    D --> E[📈 Дашборды KPI]
    D --> F[📊 Бизнес аналитика]
    D --> G[📋 Автоматические отчеты]
    
    Z[📡 ZooKeeper Ensemble<br/>3 Nodes Coordination] --> C

    %% Блок с планами развития (Roadmap)
    subgraph ROADMAP [🚀 Планы развития]
        direction TB
        H[🐳 Контейнеризация<br/>Docker Compose → Kubernetes]
        I[📡 Мониторинг & Observability<br/>Prometheus + Grafana Stack]
        J[☁️ Вынос на арендованный сервер<br/>Dedicated / VPS / Cloud VM]
        
        H --> I --> J
    end

    %% Визуальные связи от текущей архитектуры к планам
    B -.-> H
    D -.-> I
    C -.-> J

    %% Стили
    linkStyle default stroke:#b0bec5,stroke-width:1.5px
    linkStyle 4,5,6 stroke:#ffb74d,stroke-width:2px,stroke-dasharray:5