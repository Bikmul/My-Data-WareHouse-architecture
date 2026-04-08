```mermaid
flowchart LR
    %% Блок развития (слева)
    subgraph ROADMAP [🚀 Планы развития]
        direction TB
        H[🐳 Контейнеризация<br/>Docker → K8s (Повышение отказоустойчивости и расширенные возможности управления ресурсами)]
        I[📡 Prometheus + Grafana (Мониторинг ресурсов каждой из АС)]
        J[☁️ Вынос на VPS (Для получения полной независимости от хоста (моего компа)+ возможнось выделить больше ресурсов)]
        H --> I --> J
    end

    %% Существующая архитектура (справа)
    subgraph CURRENT [📦 Текущая архитектура]
        direction TB
        A[🌐 Веб-сайты & API] --> B[🔄 Airflow]
        B --> C{⚡ ClickHouse 2x2}
        
        subgraph C [Distributed Database]
            direction LR
            C1[Шард 1-A] <--> C2[Шард 1-B]
            C3[Шард 2-C] <--> C4[Шард 2-D]
        end
        
        C --> D[📊 Superset]
        D --> E[Дашборды KPI]
        D --> F[Бизнес аналитика]
        D --> G[Автоотчеты]
        
        Z[ZooKeeper] --> C
    end

    %% Связи
    ROADMAP -.-> CURRENT

    %% Стили
    linkStyle default stroke:#b0bec5,stroke-width:1.5px
    linkStyle 0 stroke:#ffb74d,stroke-width:2px,stroke-dasharray:5
```
