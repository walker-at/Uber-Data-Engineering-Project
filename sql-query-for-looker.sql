CREATE OR REPLACE TABLE "table_analytics" AS (
SELECT  
    f.VendorID,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    p.passenger_count,
    t.trip_distance,
    r.rate_code_name,
    pick.pickup_latitude,
    pick.pickup_longitude,
    dl.drop_latitude,
    dl.drop_longitude,
    pay.payment_type_name,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount
FROM walker_de_project.raw."fact_table" f
JOIN walker_de_project.raw."datetime_dim" d ON d.datetime_id = f.datetime_id
JOIN walker_de_project.raw."passenger_count_dim" p ON p.passenger_count_id = f.passenger_count_id
JOIN walker_de_project.raw."trip_distance_dim" t ON t.trip_distance_id = f.trip_distance_id
JOIN walker_de_project.raw."rate_code_dim" r ON r.rate_code_id = f.rate_code_id
JOIN walker_de_project.raw."pickup_location_dim" pick ON pick.pickup_location_id = f.pickup_location_id
JOIN walker_de_project.raw."dropoff_location_dim" dl ON dl.dropoff_location_id = f.dropoff_location_id
JOIN walker_de_project.raw."payment_type_dim" pay ON pay.payment_type_id = f.payment_type_id);
