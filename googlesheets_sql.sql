CREATE DATABASE IF NOT EXISTS google_sql;

CREATE TABLE IF NOT EXISTS car_data (
    Car_ID INT PRIMARY KEY,
    Car_Name VARCHAR(255),
    Cylinders INT,
    Displacement INT,
    Horsepower INT,
    Weight INT,
    Origin VARCHAR(50)
);
-- Create a table to store changes
CREATE TABLE IF NOT EXISTS changes_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(50),
    operation VARCHAR(10),
    car_id INT,
    car_name VARCHAR(255),
    cylinders INT,
    displacement INT,
    horsepower INT,
    weight INT,
    origin VARCHAR(50),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trigger for INSERT operations
DELIMITER //
CREATE TRIGGER after_car_data_insert
AFTER INSERT ON car_data
FOR EACH ROW
BEGIN
    INSERT INTO changes_log (table_name, operation, car_id, car_name, cylinders, displacement, horsepower, weight, origin)
    VALUES ('car_data', 'INSERT', NEW.Car_ID, NEW.Car_Name, NEW.Cylinders, NEW.Displacement, NEW.Horsepower, NEW.Weight, NEW.Origin);
END//

-- Trigger for UPDATE operations
CREATE TRIGGER after_car_data_update
AFTER UPDATE ON car_data
FOR EACH ROW
BEGIN
    INSERT INTO changes_log (table_name, operation, car_id, car_name, cylinders, displacement, horsepower, weight, origin)
    VALUES ('car_data', 'UPDATE', NEW.Car_ID, NEW.Car_Name, NEW.Cylinders, NEW.Displacement, NEW.Horsepower, NEW.Weight, NEW.Origin);
END//

-- Trigger for DELETE operations
CREATE TRIGGER after_car_data_delete
AFTER DELETE ON car_data
FOR EACH ROW
BEGIN
    INSERT INTO changes_log (table_name, operation, car_id, car_name, cylinders, displacement, horsepower, weight, origin)
    VALUES ('car_data', 'DELETE', OLD.Car_ID, OLD.Car_Name, OLD.Cylinders, OLD.Displacement, OLD.Horsepower, OLD.Weight, OLD.Origin);
END//

DELIMITER ;

-- Procedure to get and clear changes
DELIMITER //
CREATE PROCEDURE get_and_clear_changes()
BEGIN
    SELECT * FROM changes_log ORDER BY changed_at;
    DELETE FROM changes_log;
END//
DELIMITER ;

