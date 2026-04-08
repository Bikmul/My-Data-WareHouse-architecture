-- Создаем таблицу справочник
CREATE TABLE dealers_guide ON CLUSTER cluster_2shards_2replicas
(
    dealer_name String,
    iso_code String,
    city String,
    lat Float64,
    lon Float64
) ENGINE = ReplicatedMergeTree()
ORDER BY dealer_name;

-- Заполняем данными
INSERT INTO dealers_guide VALUES
('BMW | ТТС | Казань', 'RU-TA', 'Казань', 55.796127, 49.106405),
('BROOK AUTO', 'RU-SPE', 'Санкт-Петербург', 59.934280, 30.335099),
('М Бутик Якиманка', 'RU-MOW', 'Москва', 55.730833, 37.607222),
('Million Miles', 'RU-MOW', 'Москва', 55.751244, 37.618423),
('БорисХоф BMW Юг', 'RU-MOW', 'Москва', 55.620000, 37.650000),
('БорисХоф BMW Восток', 'RU-MOW', 'Москва', 55.780000, 37.800000),
('BoutiQue Auto', 'RU-MOW', 'Москва', 55.761944, 37.618889),
('GREATS | Deluxe Auto Gallery', 'RU-MOW', 'Москва', 55.755864, 37.617698),
('Royaleks', 'RU-MOW', 'Москва', 55.751244, 37.618423),
('РОЛЬФ Премиум Вешки BMW', 'RU-MOS', 'Московская область', 55.950000, 37.550000),
('РОЛЬФ Премиум Химки BMW', 'RU-MOS', 'Химки', 55.888000, 37.445000),
('SGM Auto Group', 'RU-MOW', 'Москва', 55.751244, 37.618423),
('ImperiuMMotors', 'RU-MOW', 'Москва', 55.752222, 37.615555),
('ТрансТехСервис | Импортные автомобили', 'RU-MOW', 'Москва', 55.751244, 37.618423),
('BERG AUTO PREMIUM', 'RU-MOW', 'Москва', 55.751244, 37.618423),
('JETCAR', 'RU-MOW', 'Москва', 55.751244, 37.618423);