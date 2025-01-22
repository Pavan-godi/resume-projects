SHOW DATABASES;
USE MACHINE_DB;
SHOW TABLES;
-- DROP TABLE MACHINE_TABLE;
CREATE TABLE Machine_Data_Staging (
    Date VARCHAR(10),
    Machine_ID VARCHAR(50),
    Assembly_Line_No VARCHAR(50),
    Hydraulic_Pressure VARCHAR(50),
    Coolant_Pressure VARCHAR(50),
    Air_System_Pressure VARCHAR(50),
    Coolant_Temperature VARCHAR(50),
    Hydraulic_Oil_Temperature VARCHAR(50),
    Spindle_Bearing_Temperature VARCHAR(50),
    Spindle_Vibration VARCHAR(50),
    Tool_Vibration VARCHAR(50),
    Spindle_Speed VARCHAR(50),
    Voltage VARCHAR(50),
    Torque VARCHAR(50),
    Cutting VARCHAR(50),
    Downtime VARCHAR(50)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Machine_dataset.csv'
INTO TABLE Machine_Data_Staging
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE Machine_Table (
    Date DATE,
    Machine_ID VARCHAR(50),
    Assembly_Line_No VARCHAR(50),
    Hydraulic_Pressure DECIMAL(20, 2),
    Coolant_Pressure DECIMAL(20, 2),
    Air_System_Pressure DECIMAL(20, 2),
    Coolant_Temperature DECIMAL(20, 2),
    Hydraulic_Oil_Temperature DECIMAL(20, 2),
    Spindle_Bearing_Temperature DECIMAL(20, 2),
    Spindle_Vibration DECIMAL(20, 2),
    Tool_Vibration DECIMAL(20, 2),
    Spindle_Speed INT,
    Voltage DECIMAL(20, 2),
    Torque DECIMAL(20, 2),
    Cutting DECIMAL(20, 2),
    Downtime VARCHAR(50)
);

INSERT INTO Machine_Table
SELECT
    STR_TO_DATE(Date, '%Y-%m-%d'),
    Machine_ID,
    Assembly_Line_No,
    CASE WHEN Hydraulic_Pressure = '' THEN NULL ELSE CAST(Hydraulic_Pressure AS DECIMAL(20, 2)) END,
    CASE WHEN Coolant_Pressure = '' THEN NULL ELSE CAST(Coolant_Pressure AS DECIMAL(20, 2)) END,
    CASE WHEN Air_System_Pressure = '' THEN NULL ELSE CAST(Air_System_Pressure AS DECIMAL(20, 2)) END,
    CASE WHEN Coolant_Temperature = '' THEN NULL ELSE CAST(Coolant_Temperature AS DECIMAL(20, 2)) END,
    CASE WHEN Hydraulic_Oil_Temperature = '' THEN NULL ELSE CAST(Hydraulic_Oil_Temperature AS DECIMAL(20, 2)) END,
    CASE WHEN Spindle_Bearing_Temperature = '' THEN NULL ELSE CAST(Spindle_Bearing_Temperature AS DECIMAL(20, 2)) END,
    CASE WHEN Spindle_Vibration = '' THEN NULL ELSE CAST(Spindle_Vibration AS DECIMAL(20, 2)) END,
    CASE WHEN Tool_Vibration = '' THEN NULL ELSE CAST(Tool_Vibration AS DECIMAL(20, 2)) END,
    CASE WHEN Spindle_Speed = '' THEN NULL ELSE CAST(Spindle_Speed AS UNSIGNED) END,
    CASE WHEN Voltage = '' THEN NULL ELSE CAST(Voltage AS DECIMAL(20, 2)) END,
    CASE WHEN Torque = '' THEN NULL ELSE CAST(Torque AS DECIMAL(20, 2)) END,
    CASE WHEN Cutting = '' THEN NULL ELSE CAST(Cutting AS DECIMAL(20, 2)) END,
    Downtime
FROM
    Machine_Data_Staging;
-- --------------------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------- EDA EDA EDA----------------------------------------------------------------
-- --------------------------------------------------BEFORE PREPROCESSING THE DATA ------------------------------------------------------
-- --------------------------------------------------------------------------------------------------------------------------------------

SELECT 
    VARIANCE(Hydraulic_Pressure) AS var_Hydraulic_Pressure,
    VARIANCE(Coolant_Pressure) AS var_Coolant_Pressure,
    VARIANCE(Air_System_Pressure) AS var_Air_System_Pressure,
    VARIANCE(Coolant_Temperature) AS var_Coolant_Temperature,
    VARIANCE(Hydraulic_Oil_Temperature) AS var_Hydraulic_Oil_Temperature,
    VARIANCE(Spindle_Bearing_Temperature) AS var_Spindle_Bearing_Temperature,
    VARIANCE(Spindle_Vibration) AS var_Spindle_Vibration,
    VARIANCE(Tool_Vibration) AS var_Tool_Vibration,
    VARIANCE(Spindle_Speed) AS var_Spindle_Speed,
    VARIANCE(Voltage) AS var_Voltage,
    VARIANCE(Torque) AS var_Torque,
    VARIANCE(Cutting) AS var_Cutting
FROM Machine_table;

WITH stats AS (
    SELECT 
        AVG(Hydraulic_Pressure) AS mean_hydraulic_pressure,
        STDDEV(Hydraulic_Pressure) AS stddev_hydraulic_pressure,
        AVG(Coolant_Pressure) AS mean_coolant_pressure,
        STDDEV(Coolant_Pressure) AS stddev_coolant_pressure,
        AVG(Air_System_Pressure) AS mean_air_system_pressure,
        STDDEV(Air_System_Pressure) AS stddev_air_system_pressure,
        AVG(Coolant_Temperature) AS mean_coolant_temperature,
        STDDEV(Coolant_Temperature) AS stddev_coolant_temperature,
        AVG(Hydraulic_Oil_Temperature) AS mean_hydraulic_oil_temperature,
        STDDEV(Hydraulic_Oil_Temperature) AS stddev_hydraulic_oil_temperature,
        AVG(Spindle_Bearing_Temperature) AS mean_spindle_bearing_temperature,
        STDDEV(Spindle_Bearing_Temperature) AS stddev_spindle_bearing_temperature,
        AVG(Spindle_Vibration) AS mean_spindle_vibration,
        STDDEV(Spindle_Vibration) AS stddev_spindle_vibration,
        AVG(Tool_Vibration) AS mean_tool_vibration,
        STDDEV(Tool_Vibration) AS stddev_tool_vibration,
        AVG(Spindle_Speed) AS mean_spindle_speed,
        STDDEV(Spindle_Speed) AS stddev_spindle_speed,
        AVG(Voltage) AS mean_voltage,
        STDDEV(Voltage) AS stddev_voltage,
        AVG(Torque) AS mean_torque,
        STDDEV(Torque) AS stddev_torque,
        AVG(Cutting) AS mean_cutting,
        STDDEV(Cutting) AS stddev_cutting,
        COUNT(*) AS n
    FROM machine_table
),
moment_sums AS (
    SELECT
        SUM(POWER(Hydraulic_Pressure - s.mean_hydraulic_pressure, 3)) AS skewness_sum_hydraulic_pressure,
        SUM(POWER(Hydraulic_Pressure - s.mean_hydraulic_pressure, 4)) AS kurtosis_sum_hydraulic_pressure,
        SUM(POWER(Coolant_Pressure - s.mean_coolant_pressure, 3)) AS skewness_sum_coolant_pressure,
        SUM(POWER(Coolant_Pressure - s.mean_coolant_pressure, 4)) AS kurtosis_sum_coolant_pressure,
        SUM(POWER(Air_System_Pressure - s.mean_air_system_pressure, 3)) AS skewness_sum_air_system_pressure,
        SUM(POWER(Air_System_Pressure - s.mean_air_system_pressure, 4)) AS kurtosis_sum_air_system_pressure,
        SUM(POWER(Coolant_Temperature - s.mean_coolant_temperature, 3)) AS skewness_sum_coolant_temperature,
        SUM(POWER(Coolant_Temperature - s.mean_coolant_temperature, 4)) AS kurtosis_sum_coolant_temperature,
        SUM(POWER(Hydraulic_Oil_Temperature - s.mean_hydraulic_oil_temperature, 3)) AS skewness_sum_hydraulic_oil_temperature,
        SUM(POWER(Hydraulic_Oil_Temperature - s.mean_hydraulic_oil_temperature, 4)) AS kurtosis_sum_hydraulic_oil_temperature,
        SUM(POWER(Spindle_Bearing_Temperature - s.mean_spindle_bearing_temperature, 3)) AS skewness_sum_spindle_bearing_temperature,
        SUM(POWER(Spindle_Bearing_Temperature - s.mean_spindle_bearing_temperature, 4)) AS kurtosis_sum_spindle_bearing_temperature,
        SUM(POWER(Spindle_Vibration - s.mean_spindle_vibration, 3)) AS skewness_sum_spindle_vibration,
        SUM(POWER(Spindle_Vibration - s.mean_spindle_vibration, 4)) AS kurtosis_sum_spindle_vibration,
        SUM(POWER(Tool_Vibration - s.mean_tool_vibration, 3)) AS skewness_sum_tool_vibration,
        SUM(POWER(Tool_Vibration - s.mean_tool_vibration, 4)) AS kurtosis_sum_tool_vibration,
        SUM(POWER(Spindle_Speed - s.mean_spindle_speed, 3)) AS skewness_sum_spindle_speed,
        SUM(POWER(Spindle_Speed - s.mean_spindle_speed, 4)) AS kurtosis_sum_spindle_speed,
        SUM(POWER(Voltage - s.mean_voltage, 3)) AS skewness_sum_voltage,
        SUM(POWER(Voltage - s.mean_voltage, 4)) AS kurtosis_sum_voltage,
        SUM(POWER(Torque - s.mean_torque, 3)) AS skewness_sum_torque,
        SUM(POWER(Torque - s.mean_torque, 4)) AS kurtosis_sum_torque,
        SUM(POWER(Cutting - s.mean_cutting, 3)) AS skewness_sum_cutting,
        SUM(POWER(Cutting - s.mean_cutting, 4)) AS kurtosis_sum_cutting,
        s.mean_hydraulic_pressure,
        s.stddev_hydraulic_pressure,
        s.mean_coolant_pressure,
        s.stddev_coolant_pressure,
        s.mean_air_system_pressure,
        s.stddev_air_system_pressure,
        s.mean_coolant_temperature,
        s.stddev_coolant_temperature,
        s.mean_hydraulic_oil_temperature,
        s.stddev_hydraulic_oil_temperature,
        s.mean_spindle_bearing_temperature,
        s.stddev_spindle_bearing_temperature,
        s.mean_spindle_vibration,
        s.stddev_spindle_vibration,
        s.mean_tool_vibration,
        s.stddev_tool_vibration,
        s.mean_spindle_speed,
        s.stddev_spindle_speed,
        s.mean_voltage,
        s.stddev_voltage,
        s.mean_torque,
        s.stddev_torque,
        s.mean_cutting,
        s.stddev_cutting,
        s.n
    FROM machine_table, stats s
    GROUP BY s.mean_hydraulic_pressure, s.stddev_hydraulic_pressure, s.mean_coolant_pressure, s.stddev_coolant_pressure,
             s.mean_air_system_pressure, s.stddev_air_system_pressure, s.mean_coolant_temperature, s.stddev_coolant_temperature,
             s.mean_hydraulic_oil_temperature, s.stddev_hydraulic_oil_temperature, s.mean_spindle_bearing_temperature, s.stddev_spindle_bearing_temperature,
             s.mean_spindle_vibration, s.stddev_spindle_vibration, s.mean_tool_vibration, s.stddev_tool_vibration, s.mean_spindle_speed,
             s.stddev_spindle_speed, s.mean_voltage, s.stddev_voltage, s.mean_torque, s.stddev_torque, s.mean_cutting, s.stddev_cutting, s.n
)
SELECT
    n,
    mean_hydraulic_pressure,
    stddev_hydraulic_pressure,
    skewness_sum_hydraulic_pressure / (n * POWER(stddev_hydraulic_pressure, 3)) AS skewness_hydraulic_pressure,
    (kurtosis_sum_hydraulic_pressure / (n * POWER(stddev_hydraulic_pressure, 4))) - 3 AS kurtosis_hydraulic_pressure,
    mean_coolant_pressure,
    stddev_coolant_pressure,
    skewness_sum_coolant_pressure / (n * POWER(stddev_coolant_pressure, 3)) AS skewness_coolant_pressure,
    (kurtosis_sum_coolant_pressure / (n * POWER(stddev_coolant_pressure, 4))) - 3 AS kurtosis_coolant_pressure,
    mean_air_system_pressure,
    stddev_air_system_pressure,
    skewness_sum_air_system_pressure / (n * POWER(stddev_air_system_pressure, 3)) AS skewness_air_system_pressure,
    (kurtosis_sum_air_system_pressure / (n * POWER(stddev_air_system_pressure, 4))) - 3 AS kurtosis_air_system_pressure,
    mean_coolant_temperature,
    stddev_coolant_temperature,
    skewness_sum_coolant_temperature / (n * POWER(stddev_coolant_temperature, 3)) AS skewness_coolant_temperature,
    (kurtosis_sum_coolant_temperature / (n * POWER(stddev_coolant_temperature, 4))) - 3 AS kurtosis_coolant_temperature,
    mean_hydraulic_oil_temperature,
    stddev_hydraulic_oil_temperature,
    skewness_sum_hydraulic_oil_temperature / (n * POWER(stddev_hydraulic_oil_temperature, 3)) AS skewness_hydraulic_oil_temperature,
    (kurtosis_sum_hydraulic_oil_temperature / (n * POWER(stddev_hydraulic_oil_temperature, 4))) - 3 AS kurtosis_hydraulic_oil_temperature,
    mean_spindle_bearing_temperature,
    stddev_spindle_bearing_temperature,
    skewness_sum_spindle_bearing_temperature / (n * POWER(stddev_spindle_bearing_temperature, 3)) AS skewness_spindle_bearing_temperature,
    (kurtosis_sum_spindle_bearing_temperature / (n * POWER(stddev_spindle_bearing_temperature, 4))) - 3 AS kurtosis_spindle_bearing_temperature,
    mean_spindle_vibration,
    stddev_spindle_vibration,
    skewness_sum_spindle_vibration / (n * POWER(stddev_spindle_vibration, 3)) AS skewness_spindle_vibration,
    (kurtosis_sum_spindle_vibration / (n * POWER(stddev_spindle_vibration, 4))) - 3 AS kurtosis_spindle_vibration,
    mean_tool_vibration,
    stddev_tool_vibration,
    skewness_sum_tool_vibration / (n * POWER(stddev_tool_vibration, 3)) AS skewness_tool_vibration,
    (kurtosis_sum_tool_vibration / (n * POWER(stddev_tool_vibration, 4))) - 3 AS kurtosis_tool_vibration,
    mean_spindle_speed,
    stddev_spindle_speed,
    skewness_sum_spindle_speed / (n * POWER(stddev_spindle_speed, 3)) AS skewness_spindle_speed,
    (kurtosis_sum_spindle_speed / (n * POWER(stddev_spindle_speed, 4))) - 3 AS kurtosis_spindle_speed,
    mean_voltage,
    stddev_voltage,
    skewness_sum_voltage / (n * POWER(stddev_voltage, 3)) AS skewness_voltage,
    (kurtosis_sum_voltage / (n * POWER(stddev_voltage, 4))) - 3 AS kurtosis_voltage,
    mean_torque,
    stddev_torque,
    skewness_sum_torque / (n * POWER(stddev_torque, 3)) AS skewness_torque,
    (kurtosis_sum_torque / (n * POWER(stddev_torque, 4))) - 3 AS kurtosis_torque,
    mean_cutting,
    stddev_cutting,
    skewness_sum_cutting / (n * POWER(stddev_cutting, 3)) AS skewness_cutting,
    (kurtosis_sum_cutting / (n * POWER(stddev_cutting, 4))) - 3 AS kurtosis_cutting
FROM moment_sums;

SELECT COUNT(*) AS nullcount FROM machine_table WHERE Date IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Machine_ID IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Assembly_Line_No IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Hydraulic_Pressure IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Coolant_Pressure IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Air_System_Pressure IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Coolant_Temperature IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Hydraulic_Oil_Temperature IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Spindle_Bearing_Temperature IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Spindle_Vibration IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Tool_Vibration IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Spindle_Speed IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Voltage IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Torque IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Cutting IS NULL;
SELECT COUNT(*) AS nullcount FROM machine_table WHERE Downtime IS NULL;

-- =======================================================================================================================================
-- --------------------------------------------------------- DATA PREPROCESSING ----------------------------------------------------------
-- =======================================================================================================================================

CREATE TABLE MACHINE_FINAL_TABLE AS
SELECT * FROM Machine_TABLE;

UPDATE machine_final_table
SET 
    Hydraulic_Pressure = COALESCE(Hydraulic_Pressure, (SELECT AVG(Hydraulic_Pressure) FROM Machine_TABLE)),
    Coolant_Pressure = COALESCE(Coolant_Pressure, (SELECT AVG(Coolant_Pressure) FROM Machine_table)),
    Air_System_Pressure = COALESCE(Air_System_Pressure, (SELECT AVG(Air_System_Pressure) FROM Machine_table)),
    Coolant_Temperature = COALESCE(Coolant_Temperature, (SELECT AVG(Coolant_Temperature) FROM Machine_table)),
    Hydraulic_Oil_Temperature = COALESCE(Hydraulic_Oil_Temperature, (SELECT AVG(Hydraulic_Oil_Temperature) FROM Machine_table)),
    Spindle_Bearing_Temperature = COALESCE(Spindle_Bearing_Temperature, (SELECT AVG(Spindle_Bearing_Temperature) FROM Machine_table)),
    Spindle_Vibration = COALESCE(Spindle_Vibration, (SELECT AVG(Spindle_Vibration) FROM Machine_table)),
    Tool_Vibration = COALESCE(Tool_Vibration, (SELECT AVG(Tool_Vibration) FROM Machine_table)),
    Spindle_Speed = COALESCE(Spindle_Speed, (SELECT AVG(Spindle_Speed) FROM Machine_table)),
    Voltage = COALESCE(Voltage, (SELECT AVG(Voltage) FROM Machine_table)),
    Torque = COALESCE(Torque, (SELECT AVG(Torque) FROM Machine_table)),
    Cutting = COALESCE(Cutting, (SELECT AVG(Cutting) FROM Machine_table));
    
-- ====================================================================================================================================
-- ----------------------------------------------------- EDA AFTER PREPROCESSING ------------------------------------------------------
-- ====================================================================================================================================

SELECT 
    VARIANCE(Hydraulic_Pressure) AS var_Hydraulic_Pressure,
    VARIANCE(Coolant_Pressure) AS var_Coolant_Pressure,
    VARIANCE(Air_System_Pressure) AS var_Air_System_Pressure,
    VARIANCE(Coolant_Temperature) AS var_Coolant_Temperature,
    VARIANCE(Hydraulic_Oil_Temperature) AS var_Hydraulic_Oil_Temperature,
    VARIANCE(Spindle_Bearing_Temperature) AS var_Spindle_Bearing_Temperature,
    VARIANCE(Spindle_Vibration) AS var_Spindle_Vibration,
    VARIANCE(Tool_Vibration) AS var_Tool_Vibration,
    VARIANCE(Spindle_Speed) AS var_Spindle_Speed,
    VARIANCE(Voltage) AS var_Voltage,
    VARIANCE(Torque) AS var_Torque,
    VARIANCE(Cutting) AS var_Cutting
FROM Machine_table;

WITH stats AS (
    SELECT 
        AVG(Hydraulic_Pressure) AS mean_hydraulic_pressure,
        STDDEV(Hydraulic_Pressure) AS stddev_hydraulic_pressure,
        AVG(Coolant_Pressure) AS mean_coolant_pressure,
        STDDEV(Coolant_Pressure) AS stddev_coolant_pressure,
        AVG(Air_System_Pressure) AS mean_air_system_pressure,
        STDDEV(Air_System_Pressure) AS stddev_air_system_pressure,
        AVG(Coolant_Temperature) AS mean_coolant_temperature,
        STDDEV(Coolant_Temperature) AS stddev_coolant_temperature,
        AVG(Hydraulic_Oil_Temperature) AS mean_hydraulic_oil_temperature,
        STDDEV(Hydraulic_Oil_Temperature) AS stddev_hydraulic_oil_temperature,
        AVG(Spindle_Bearing_Temperature) AS mean_spindle_bearing_temperature,
        STDDEV(Spindle_Bearing_Temperature) AS stddev_spindle_bearing_temperature,
        AVG(Spindle_Vibration) AS mean_spindle_vibration,
        STDDEV(Spindle_Vibration) AS stddev_spindle_vibration,
        AVG(Tool_Vibration) AS mean_tool_vibration,
        STDDEV(Tool_Vibration) AS stddev_tool_vibration,
        AVG(Spindle_Speed) AS mean_spindle_speed,
        STDDEV(Spindle_Speed) AS stddev_spindle_speed,
        AVG(Voltage) AS mean_voltage,
        STDDEV(Voltage) AS stddev_voltage,
        AVG(Torque) AS mean_torque,
        STDDEV(Torque) AS stddev_torque,
        AVG(Cutting) AS mean_cutting,
        STDDEV(Cutting) AS stddev_cutting,
        COUNT(*) AS n
    FROM machine_final_table
),
moment_sums AS (
    SELECT
        SUM(POWER(Hydraulic_Pressure - s.mean_hydraulic_pressure, 3)) AS skewness_sum_hydraulic_pressure,
        SUM(POWER(Hydraulic_Pressure - s.mean_hydraulic_pressure, 4)) AS kurtosis_sum_hydraulic_pressure,
        SUM(POWER(Coolant_Pressure - s.mean_coolant_pressure, 3)) AS skewness_sum_coolant_pressure,
        SUM(POWER(Coolant_Pressure - s.mean_coolant_pressure, 4)) AS kurtosis_sum_coolant_pressure,
        SUM(POWER(Air_System_Pressure - s.mean_air_system_pressure, 3)) AS skewness_sum_air_system_pressure,
        SUM(POWER(Air_System_Pressure - s.mean_air_system_pressure, 4)) AS kurtosis_sum_air_system_pressure,
        SUM(POWER(Coolant_Temperature - s.mean_coolant_temperature, 3)) AS skewness_sum_coolant_temperature,
        SUM(POWER(Coolant_Temperature - s.mean_coolant_temperature, 4)) AS kurtosis_sum_coolant_temperature,
        SUM(POWER(Hydraulic_Oil_Temperature - s.mean_hydraulic_oil_temperature, 3)) AS skewness_sum_hydraulic_oil_temperature,
        SUM(POWER(Hydraulic_Oil_Temperature - s.mean_hydraulic_oil_temperature, 4)) AS kurtosis_sum_hydraulic_oil_temperature,
        SUM(POWER(Spindle_Bearing_Temperature - s.mean_spindle_bearing_temperature, 3)) AS skewness_sum_spindle_bearing_temperature,
        SUM(POWER(Spindle_Bearing_Temperature - s.mean_spindle_bearing_temperature, 4)) AS kurtosis_sum_spindle_bearing_temperature,
        SUM(POWER(Spindle_Vibration - s.mean_spindle_vibration, 3)) AS skewness_sum_spindle_vibration,
        SUM(POWER(Spindle_Vibration - s.mean_spindle_vibration, 4)) AS kurtosis_sum_spindle_vibration,
        SUM(POWER(Tool_Vibration - s.mean_tool_vibration, 3)) AS skewness_sum_tool_vibration,
        SUM(POWER(Tool_Vibration - s.mean_tool_vibration, 4)) AS kurtosis_sum_tool_vibration,
        SUM(POWER(Spindle_Speed - s.mean_spindle_speed, 3)) AS skewness_sum_spindle_speed,
        SUM(POWER(Spindle_Speed - s.mean_spindle_speed, 4)) AS kurtosis_sum_spindle_speed,
        SUM(POWER(Voltage - s.mean_voltage, 3)) AS skewness_sum_voltage,
        SUM(POWER(Voltage - s.mean_voltage, 4)) AS kurtosis_sum_voltage,
        SUM(POWER(Torque - s.mean_torque, 3)) AS skewness_sum_torque,
        SUM(POWER(Torque - s.mean_torque, 4)) AS kurtosis_sum_torque,
        SUM(POWER(Cutting - s.mean_cutting, 3)) AS skewness_sum_cutting,
        SUM(POWER(Cutting - s.mean_cutting, 4)) AS kurtosis_sum_cutting,
        s.mean_hydraulic_pressure,
        s.stddev_hydraulic_pressure,
        s.mean_coolant_pressure,
        s.stddev_coolant_pressure,
        s.mean_air_system_pressure,
        s.stddev_air_system_pressure,
        s.mean_coolant_temperature,
        s.stddev_coolant_temperature,
        s.mean_hydraulic_oil_temperature,
        s.stddev_hydraulic_oil_temperature,
        s.mean_spindle_bearing_temperature,
        s.stddev_spindle_bearing_temperature,
        s.mean_spindle_vibration,
        s.stddev_spindle_vibration,
        s.mean_tool_vibration,
        s.stddev_tool_vibration,
        s.mean_spindle_speed,
        s.stddev_spindle_speed,
        s.mean_voltage,
        s.stddev_voltage,
        s.mean_torque,
        s.stddev_torque,
        s.mean_cutting,
        s.stddev_cutting,
        s.n
    FROM machine_final_table, stats s
    GROUP BY s.mean_hydraulic_pressure, s.stddev_hydraulic_pressure, s.mean_coolant_pressure, s.stddev_coolant_pressure,
             s.mean_air_system_pressure, s.stddev_air_system_pressure, s.mean_coolant_temperature, s.stddev_coolant_temperature,
             s.mean_hydraulic_oil_temperature, s.stddev_hydraulic_oil_temperature, s.mean_spindle_bearing_temperature, s.stddev_spindle_bearing_temperature,
             s.mean_spindle_vibration, s.stddev_spindle_vibration, s.mean_tool_vibration, s.stddev_tool_vibration, s.mean_spindle_speed,
             s.stddev_spindle_speed, s.mean_voltage, s.stddev_voltage, s.mean_torque, s.stddev_torque, s.mean_cutting, s.stddev_cutting, s.n
)
SELECT
    n,
    mean_hydraulic_pressure,
    stddev_hydraulic_pressure,
    skewness_sum_hydraulic_pressure / (n * POWER(stddev_hydraulic_pressure, 3)) AS skewness_hydraulic_pressure,
    (kurtosis_sum_hydraulic_pressure / (n * POWER(stddev_hydraulic_pressure, 4))) - 3 AS kurtosis_hydraulic_pressure,
    mean_coolant_pressure,
    stddev_coolant_pressure,
    skewness_sum_coolant_pressure / (n * POWER(stddev_coolant_pressure, 3)) AS skewness_coolant_pressure,
    (kurtosis_sum_coolant_pressure / (n * POWER(stddev_coolant_pressure, 4))) - 3 AS kurtosis_coolant_pressure,
    mean_air_system_pressure,
    stddev_air_system_pressure,
    skewness_sum_air_system_pressure / (n * POWER(stddev_air_system_pressure, 3)) AS skewness_air_system_pressure,
    (kurtosis_sum_air_system_pressure / (n * POWER(stddev_air_system_pressure, 4))) - 3 AS kurtosis_air_system_pressure,
    mean_coolant_temperature,
    stddev_coolant_temperature,
    skewness_sum_coolant_temperature / (n * POWER(stddev_coolant_temperature, 3)) AS skewness_coolant_temperature,
    (kurtosis_sum_coolant_temperature / (n * POWER(stddev_coolant_temperature, 4))) - 3 AS kurtosis_coolant_temperature,
    mean_hydraulic_oil_temperature,
    stddev_hydraulic_oil_temperature,
    skewness_sum_hydraulic_oil_temperature / (n * POWER(stddev_hydraulic_oil_temperature, 3)) AS skewness_hydraulic_oil_temperature,
    (kurtosis_sum_hydraulic_oil_temperature / (n * POWER(stddev_hydraulic_oil_temperature, 4))) - 3 AS kurtosis_hydraulic_oil_temperature,
    mean_spindle_bearing_temperature,
    stddev_spindle_bearing_temperature,
    skewness_sum_spindle_bearing_temperature / (n * POWER(stddev_spindle_bearing_temperature, 3)) AS skewness_spindle_bearing_temperature,
    (kurtosis_sum_spindle_bearing_temperature / (n * POWER(stddev_spindle_bearing_temperature, 4))) - 3 AS kurtosis_spindle_bearing_temperature,
    mean_spindle_vibration,
    stddev_spindle_vibration,
    skewness_sum_spindle_vibration / (n * POWER(stddev_spindle_vibration, 3)) AS skewness_spindle_vibration,
    (kurtosis_sum_spindle_vibration / (n * POWER(stddev_spindle_vibration, 4))) - 3 AS kurtosis_spindle_vibration,
    mean_tool_vibration,
    stddev_tool_vibration,
    skewness_sum_tool_vibration / (n * POWER(stddev_tool_vibration, 3)) AS skewness_tool_vibration,
    (kurtosis_sum_tool_vibration / (n * POWER(stddev_tool_vibration, 4))) - 3 AS kurtosis_tool_vibration,
    mean_spindle_speed,
    stddev_spindle_speed,
    skewness_sum_spindle_speed / (n * POWER(stddev_spindle_speed, 3)) AS skewness_spindle_speed,
    (kurtosis_sum_spindle_speed / (n * POWER(stddev_spindle_speed, 4))) - 3 AS kurtosis_spindle_speed,
    mean_voltage,
    stddev_voltage,
    skewness_sum_voltage / (n * POWER(stddev_voltage, 3)) AS skewness_voltage,
    (kurtosis_sum_voltage / (n * POWER(stddev_voltage, 4))) - 3 AS kurtosis_voltage,
    mean_torque,
    stddev_torque,
    skewness_sum_torque / (n * POWER(stddev_torque, 3)) AS skewness_torque,
    (kurtosis_sum_torque / (n * POWER(stddev_torque, 4))) - 3 AS kurtosis_torque,
    mean_cutting,
    stddev_cutting,
    skewness_sum_cutting / (n * POWER(stddev_cutting, 3)) AS skewness_cutting,
    (kurtosis_sum_cutting / (n * POWER(stddev_cutting, 4))) - 3 AS kurtosis_cutting
FROM moment_sums;

SHOW WARNINGS;

-- ---------------------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------- STATISTICAL ANALYSIS -------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------------------
-- CALCULATING MEAN
SELECT
    AVG(Hydraulic_Pressure) AS mean_Hydraulic_Pressure,
    AVG(Coolant_Pressure) AS mean_Coolant_Pressure,
    AVG(Air_System_Pressure) AS mean_Air_System_Pressure,
    AVG(Coolant_Temperature) AS mean_Coolant_Temperature,
    AVG(Hydraulic_Oil_Temperature) AS mean_Hydraulic_Oil_Temperature,
    AVG(Spindle_Bearing_Temperature) AS mean_Spindle_Bearing_Temperature,
    AVG(Spindle_Vibration) AS mean_Spindle_Vibration,
    AVG(Tool_Vibration) AS mean_Tool_Vibration,
    AVG(Spindle_Speed) AS mean_Spindle_Speed,
    AVG(Voltage) AS mean_Voltage,
    AVG(Torque) AS mean_Torque,
    AVG(Cutting) AS mean_Cutting
FROM machine_final_table;


-- CALCULATING MEDIAN
WITH ranked_data AS (
    SELECT
        Hydraulic_Pressure,
        Coolant_Pressure,
        Air_System_Pressure,
        Coolant_Temperature,
        Hydraulic_Oil_Temperature,
        Spindle_Bearing_Temperature,
        Spindle_Vibration,
        Tool_Vibration,
        Spindle_Speed,
        Voltage,
        Torque,
        Cutting,
        ROW_NUMBER() OVER (ORDER BY Hydraulic_Pressure) AS rn_Hydraulic_Pressure,
        ROW_NUMBER() OVER (ORDER BY Coolant_Pressure) AS rn_Coolant_Pressure,
        ROW_NUMBER() OVER (ORDER BY Air_System_Pressure) AS rn_Air_System_Pressure,
        ROW_NUMBER() OVER (ORDER BY Coolant_Temperature) AS rn_Coolant_Temperature,
        ROW_NUMBER() OVER (ORDER BY Hydraulic_Oil_Temperature) AS rn_Hydraulic_Oil_Temperature,
        ROW_NUMBER() OVER (ORDER BY Spindle_Bearing_Temperature) AS rn_Spindle_Bearing_Temperature,
        ROW_NUMBER() OVER (ORDER BY Spindle_Vibration) AS rn_Spindle_Vibration,
        ROW_NUMBER() OVER (ORDER BY Tool_Vibration) AS rn_Tool_Vibration,
        ROW_NUMBER() OVER (ORDER BY Spindle_Speed) AS rn_Spindle_Speed,
        ROW_NUMBER() OVER (ORDER BY Voltage) AS rn_Voltage,
        ROW_NUMBER() OVER (ORDER BY Torque) AS rn_Torque,
        ROW_NUMBER() OVER (ORDER BY Cutting) AS rn_Cutting,
        COUNT(*) OVER () AS total_rows
    FROM machine_final_table
)
SELECT
    AVG(Hydraulic_Pressure) AS median_Hydraulic_Pressure,
    AVG(Coolant_Pressure) AS median_Coolant_Pressure,
    AVG(Air_System_Pressure) AS median_Air_System_Pressure,
    AVG(Coolant_Temperature) AS median_Coolant_Temperature,
    AVG(Hydraulic_Oil_Temperature) AS median_Hydraulic_Oil_Temperature,
    AVG(Spindle_Bearing_Temperature) AS median_Spindle_Bearing_Temperature,
    AVG(Spindle_Vibration) AS median_Spindle_Vibration,
    AVG(Tool_Vibration) AS median_Tool_Vibration,
    AVG(Spindle_Speed) AS median_Spindle_Speed,
    AVG(Voltage) AS median_Voltage,
    AVG(Torque) AS median_Torque,
    AVG(Cutting) AS median_Cutting
FROM ranked_data
WHERE
    rn_Hydraulic_Pressure IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Coolant_Pressure IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Air_System_Pressure IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Coolant_Temperature IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Hydraulic_Oil_Temperature IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Spindle_Bearing_Temperature IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Spindle_Vibration IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Tool_Vibration IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Spindle_Speed IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Voltage IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Torque IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2)) OR
    rn_Cutting IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2));

    
   -- Mode Calculation
WITH mode_table AS (
    SELECT
        Hydraulic_Pressure,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Hydraulic_Pressure
),
mode_table2 AS (
    SELECT
        Coolant_Pressure,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Coolant_Pressure
),
mode_table3 AS (
    SELECT
        Air_System_Pressure,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Air_System_Pressure
),
mode_table4 AS (
    SELECT
        Coolant_Temperature,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Coolant_Temperature
),
mode_table5 AS (
    SELECT
        Hydraulic_Oil_Temperature,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Hydraulic_Oil_Temperature
),
mode_table6 AS (
    SELECT
        Spindle_Bearing_Temperature,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Spindle_Bearing_Temperature
),
mode_table7 AS (
    SELECT
        Spindle_Vibration,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Spindle_Vibration
),
mode_table8 AS (
    SELECT
        Tool_Vibration,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Tool_Vibration
),
mode_table9 AS (
    SELECT
        Spindle_Speed,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Spindle_Speed
),
mode_table10 AS (
    SELECT
        Voltage,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Voltage
),
mode_table11 AS (
    SELECT
        Torque,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Torque
),
mode_table12 AS (
    SELECT
        Cutting,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    FROM machine_final_table
    GROUP BY Cutting
)
SELECT
    mt.Hydraulic_Pressure AS mode_Hydraulic_Pressure,
    mt2.Coolant_Pressure AS mode_Coolant_Pressure,
    mt3.Air_System_Pressure AS mode_Air_System_Pressure,
    mt4.Coolant_Temperature AS mode_Coolant_Temperature,
    mt5.Hydraulic_Oil_Temperature AS mode_Hydraulic_Oil_Temperature,
    mt6.Spindle_Bearing_Temperature AS mode_Spindle_Bearing_Temperature,
    mt7.Spindle_Vibration AS mode_Spindle_Vibration,
    mt8.Tool_Vibration AS mode_Tool_Vibration,
    mt9.Spindle_Speed AS mode_Spindle_Speed,
    mt10.Voltage AS mode_Voltage,
    mt11.Torque AS mode_Torque,
    mt12.Cutting AS mode_Cutting
FROM mode_table mt
JOIN mode_table2 mt2 ON mt.rn = 1 AND mt2.rn = 1
JOIN mode_table3 mt3 ON mt3.rn = 1
JOIN mode_table4 mt4 ON mt4.rn = 1
JOIN mode_table5 mt5 ON mt5.rn = 1
JOIN mode_table6 mt6 ON mt6.rn = 1
JOIN mode_table7 mt7 ON mt7.rn = 1
JOIN mode_table8 mt8 ON mt8.rn = 1
JOIN mode_table9 mt9 ON mt9.rn = 1
JOIN mode_table10 mt10 ON mt10.rn = 1
JOIN mode_table11 mt11 ON mt11.rn = 1
JOIN mode_table12 mt12 ON mt12.rn = 1;

SELECT Machine_ID, SUM(Downtime) AS Total_Downtime
FROM Machine_Final_Table
GROUP BY Machine_ID
ORDER BY Total_Downtime DESC;

SELECT COUNT(DISTINCT Machine_ID) AS Unique_Machine_Count
FROM Machine_Final_Table;

SELECT Machine_ID, Downtime
FROM machine_final_table;

SELECT 
    Machine_ID,
    SUM(CASE WHEN Downtime > 0 THEN 1 ELSE 0 END) AS Machine_Failure,
    SUM(CASE WHEN Downtime = 0 THEN 1 ELSE 0 END) AS No_Machine_Failure
FROM 
    Machine_Final_Table
GROUP BY 
    Machine_ID
ORDER BY 
    Machine_ID
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/downtimeanalysis.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

SELECT * FROM 
    Machine_Final_Table
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dataaftereda.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';



SELECT *
FROM Machine_Final_Table
ORDER BY RAND()
LIMIT 500 -- Adjust the sample size as needed
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sample_machine_data.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

SELECT 
    Downtime,
    COUNT(*) AS count
FROM 
    machine_final_table
GROUP BY 
    Downtime;



