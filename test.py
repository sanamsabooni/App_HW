# Equipment report Query
equipment_report = load_data_from_db("""
    WITH OrderProcessing AS (
        SELECT 
            order_id, 
            so_number, 
            merchant_number, 
            tech_setup_order_options, 
            communication_type, 
            wireless_carrier, 
            terminal_detail, 
            terminal_id, 
            outside_agents, 
            status, 
            equipment_received_date,
            
            -- Initialize fee-related counters
            0 AS g, 0 AS s, 0 AS c, 0 AS wf, 0 AS w, 0 AS v,
            
            -- Apply the conditions based on tech_setup_order_options
            CASE 
                WHEN LOWER(tech_setup_order_options) <> 'swap_replacement' THEN 
                    CASE 
                        WHEN LOWER(communication_type) = 'gateway' THEN 1 ELSE 0 END
            END AS g,
            
            CASE 
                WHEN LOWER(tech_setup_order_options) <> 'swap_replacement' THEN 
                    CASE 
                        WHEN LOWER(communication_type) = 'wireless' THEN 1 ELSE 0 END
            END AS w,
            
            CASE 
                WHEN LOWER(tech_setup_order_options) <> 'swap_replacement' THEN 
                    CASE 
                        WHEN LOWER(communication_type) = 'wifi' THEN 1 ELSE 0 END
            END AS wf,
            
            CASE 
                WHEN LOWER(tech_setup_order_options) <> 'swap_replacement' AND 
                     LOWER(communication_type) = 'wireless' AND wireless_carrier IS NOT NULL AND 
                     wireless_carrier <> 'missing_simcard' THEN 1 ELSE 0 
            END AS s,
            
            CASE 
                WHEN LOWER(tech_setup_order_options) <> 'swap_replacement' AND 
                     LOWER(terminal_detail) IN ('vp550', 'vl300', 'vl110', 'vl100 pro') THEN 1 ELSE 0
            END AS v
            
        FROM zoho_orders_table
    )
    
    SELECT 
        order_id, 
        so_number, 
        merchant_number, 
        tech_setup_order_options, 
        communication_type, 
        wireless_carrier, 
        terminal_detail, 
        terminal_id, 
        outside_agents, 
        status, 
        equipment_received_date,
        
        SUM(g) AS "Terminal/Gateway",
        SUM(v) AS "Valor Count"
        
    FROM OrderProcessing
    GROUP BY order_id, so_number, merchant_number, tech_setup_order_options, communication_type, 
             wireless_carrier, terminal_detail, terminal_id, outside_agents, status, equipment_received_date;
""")


# Load the Equipment Report Pivot Table
equipment_pivot_report = load_data_from_db("""
    WITH OrderProcessing AS (
        SELECT 
            merchant_number,
            
            SUM(
                CASE WHEN LOWER(tech_setup_order_options) = 'replacement' THEN
                    CASE 
                        WHEN LOWER(communication_type) = 'wireless' THEN -1 
                        WHEN LOWER(communication_type) = 'gateway' THEN -1 
                        WHEN LOWER(communication_type) = 'wifi' THEN -1 
                        WHEN LOWER(communication_type) = 'wireless' AND wireless_carrier IS NOT NULL AND wireless_carrier <> 'missing_simcard' THEN -1 
                        ELSE 0 
                    END
                ELSE w END
            ) AS total_wireless,
            
            SUM(
                CASE WHEN LOWER(tech_setup_order_options) = 'replacement' THEN
                    CASE 
                        WHEN LOWER(terminal_detail) IN ('vp550', 'vl300', 'vl110', 'vl100 pro') THEN -1 ELSE 0 
                    END
                ELSE v END
            ) AS total_valor,
            
            SUM(
                CASE WHEN w > 0 THEN w * 10 ELSE 0 END +
                CASE WHEN wf > 0 AND s > 0 THEN s * 10 ELSE 0 END +
                CASE WHEN v > 0 THEN 5 + CASE WHEN v > 1 THEN (v - 1) * 2 ELSE 0 END ELSE 0 END
            ) AS total_fee
    
        FROM zoho_orders_table
        WHERE LOWER(status) = 'completed'
        GROUP BY merchant_number
        HAVING 
            SUM(
                CASE 
                    WHEN LOWER(communication_type) IN ('wireless - gprs', 'wireless - cdma', 'gateway') 
                    THEN 1 ELSE 0 
                END
            ) > 0 
            OR 
            SUM(
                CASE 
                    WHEN LOWER(terminal_detail) IN ('vp550', 'vl300', 'vl110', 'vl100 pro') 
                    THEN 1 ELSE 0 
                END
            ) > 0;
""")