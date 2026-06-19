TRUNCATE TABLE device_types;
INSERT INTO device_types (id, type_name) VALUES (1, 'Термостат');
INSERT INTO device_types (id, type_name) VALUES (2, 'Гигрометр');
INSERT INTO device_types (id, type_name) VALUES (3, 'Метеостанция');
INSERT INTO device_types (id, type_name) VALUES (4, 'Датчик теплицы');
SELECT * FROM device_types ORDER BY id;