USE machine_db;
show tables;

SELECT 
    'Hydraulic_Pressure' AS column_name,
    MIN(`Hydraulic_Pressure`) AS min_value,
    MAX(`Hydraulic_Pressure`) AS max_value,
    AVG(`Hydraulic_Pressure`) AS avg_value,
    STDDEV(`Hydraulic_Pressure`) AS std_dev
FROM 
    machine_final_table

UNION ALL

SELECT 
    'Coolant_Pressure' AS column_name,
    MIN(`Coolant_Pressure`) AS min_value,
    MAX(`Coolant_Pressure`) AS max_value,
    AVG(`Coolant_Pressure`) AS avg_value,
    STDDEV(`Coolant_Pressure`) AS std_dev
FROM 
    machine_final_table

UNION ALL

SELECT 
    'Air_System_Pressure' AS column_name,
    MIN(`Air_System_Pressure`) AS min_value,
    MAX(`Air_System_Pressure`) AS max_value,
    AVG(`Air_System_Pressure`) AS avg_value,
    STDDEV(`Air_System_Pressure`) AS std_dev
FROM 
    machine_final_table


UNION ALL

SELECT 
    'Coolant_Temperature' AS column_name,
    MIN(`Coolant_Temperature`) AS min_value,
    MAX(`Coolant_Temperature`) AS max_value,
    AVG(`Coolant_Temperature`) AS avg_value,
    STDDEV(`Coolant_Temperature`) AS std_dev
FROM 
    machine_final_table


UNION ALL

SELECT 
    'Hydraulic_Oil_Temperature' AS column_name,
    MIN(`Hydraulic_Oil_Temperature`) AS min_value,
    MAX(`Hydraulic_Oil_Temperature`) AS max_value,
    AVG(`Hydraulic_Oil_Temperature`) AS avg_value,
    STDDEV(`Hydraulic_Oil_Temperature`) AS std_dev
FROM 
    machine_final_table


UNION ALL

SELECT 
    'Spindle_Bearing_Temperature' AS column_name,
    MIN(`Spindle_Bearing_Temperature`) AS min_value,
    MAX(`Spindle_Bearing_Temperature`) AS max_value,
    AVG(`Spindle_Bearing_Temperature`) AS avg_value,
    STDDEV(`Spindle_Bearing_Temperature`) AS std_dev
FROM 
   machine_final_table


UNION ALL

SELECT 
    'Spindle_Vibration' AS column_name,
    MIN(`Spindle_Vibration`) AS min_value,
    MAX(`Spindle_Vibration`) AS max_value,
    AVG(`Spindle_Vibration`) AS avg_value,
    STDDEV(`Spindle_Vibration`) AS std_dev
FROM 
   machine_final_table


UNION ALL

SELECT 
    'Tool_Vibration' AS column_name,
    MIN(`Tool_Vibration`) AS min_value,
    MAX(`Tool_Vibration`) AS max_value,
    AVG(`Tool_Vibration`) AS avg_value,
    STDDEV(`Tool_Vibration`) AS std_dev
FROM 
    machine_final_table


UNION ALL

SELECT 
    'Spindle_Speed' AS column_name,
    MIN(`Spindle_Speed`) AS min_value,
    MAX(`Spindle_Speed`) AS max_value,
    AVG(`Spindle_Speed`) AS avg_value,
    STDDEV(`Spindle_Speed`) AS std_dev
FROM 
    machine_final_table


UNION ALL

SELECT 
    'Voltage' AS column_name,
    MIN(`Voltage`) AS min_value,
    MAX(`Voltage`) AS max_value,
    AVG(`Voltage`) AS avg_value,
    STDDEV(`Voltage`) AS std_dev
FROM 
    machine_final_table


UNION ALL

SELECT 
    'Torque(Nm)' AS column_name,
    MIN(`Torque`) AS min_value,
    MAX(`Torque`) AS max_value,
    AVG(`Torque`) AS avg_value,
    STDDEV(`Torque`) AS std_dev
FROM 
   machine_final_table


UNION ALL

SELECT 
    'Cutting' AS column_name,
    MIN(`Cutting`) AS min_value,
    MAX(`Cutting`) AS max_value,
    AVG(`Cutting`) AS avg_value,
    STDDEV(`Cutting`) AS std_dev
FROM 
    machine_final_table;
    
    
    
SELECT 
    Machine_ID,
    COUNT(*) AS usage_count
FROM 
    machine_final_table  
GROUP BY 
    Machine_ID
ORDER BY 
    usage_count DESC
LIMIT 1;

SELECT 
    Assembly_Line_No,
    COUNT(*) AS usage_count
FROM 
    machine_final_table
GROUP BY 
    Assembly_Line_No 
ORDER BY 
    usage_count DESC
LIMIT 1;


