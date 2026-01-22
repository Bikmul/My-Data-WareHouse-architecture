drop table car_raw_json_data ON CLUSTER cluster_2shards_2replicas;

CREATE TABLE car_raw_json_data ON CLUSTER cluster_2shards_2replicas
(
    source String,
    query_dttm DateTime,
    raw_json String
)
ENGINE = Distributed(
    'company_cluster',           -- имя кластера
     default,           -- текущая БД
    'car_raw_json_data_local',   -- локальная таблица
    rand()                       -- или xxHash64(source) для равномерного распределения
);