-- ============================================================================
-- AutoQuant: Dimension Seed Data V2
-- Comprehensive seeds for dim_date, dim_oem, dim_oem_alias,
-- dim_segment, dim_vehicle_class_map, dim_fuel, dim_geo
-- ============================================================================


-- ============================================================================
-- dim_date: Calendar seed 2016-01-01 to 2027-12-31
-- ============================================================================
INSERT INTO dim_date (
    date_key, calendar_year, calendar_month, calendar_quarter,
    fy_year, fy_quarter, fy_quarter_num, month_name, day_of_week, is_weekend
)
SELECT
    d::DATE                                                      AS date_key,
    EXTRACT(YEAR FROM d)::SMALLINT                               AS calendar_year,
    EXTRACT(MONTH FROM d)::SMALLINT                              AS calendar_month,
    EXTRACT(QUARTER FROM d)::SMALLINT                            AS calendar_quarter,
    CASE WHEN EXTRACT(MONTH FROM d) >= 4
         THEN 'FY' || TO_CHAR(d + INTERVAL '1 year', 'YY')
         ELSE 'FY' || TO_CHAR(d, 'YY')
    END                                                          AS fy_year,
    CASE WHEN EXTRACT(MONTH FROM d) BETWEEN 4 AND 6   THEN 'Q1'
         WHEN EXTRACT(MONTH FROM d) BETWEEN 7 AND 9   THEN 'Q2'
         WHEN EXTRACT(MONTH FROM d) BETWEEN 10 AND 12 THEN 'Q3'
         ELSE 'Q4'
    END || CASE WHEN EXTRACT(MONTH FROM d) >= 4
                THEN 'FY' || TO_CHAR(d + INTERVAL '1 year', 'YY')
                ELSE 'FY' || TO_CHAR(d, 'YY')
           END                                                   AS fy_quarter,
    CASE WHEN EXTRACT(MONTH FROM d) BETWEEN 4 AND 6   THEN 1
         WHEN EXTRACT(MONTH FROM d) BETWEEN 7 AND 9   THEN 2
         WHEN EXTRACT(MONTH FROM d) BETWEEN 10 AND 12 THEN 3
         ELSE 4
    END::SMALLINT                                                AS fy_quarter_num,
    TO_CHAR(d, 'Month')                                          AS month_name,
    EXTRACT(DOW FROM d)::SMALLINT                                AS day_of_week,
    EXTRACT(DOW FROM d) IN (0, 6)                                AS is_weekend
FROM generate_series('2016-01-01'::DATE, '2027-12-31'::DATE, '1 day') AS gs(d)
ON CONFLICT (date_key) DO NOTHING;


-- ============================================================================
-- dim_oem: Master OEM list (listed + tracked unlisted)
-- ============================================================================
INSERT INTO dim_oem (oem_name, nse_ticker, bse_code, is_listed, is_in_scope, primary_segments) VALUES
-- Listed PV OEMs
('Maruti Suzuki',          'MARUTI',     '532500', TRUE,  TRUE,  '{PV}'),
('Hyundai Motor India',    'HYUNDAI',    '544966', TRUE,  TRUE,  '{PV}'),
('Tata Motors PV',         'TATAMOTORS', '500570', TRUE,  TRUE,  '{PV}'),
('Mahindra PV',            'M&M',        '500520', TRUE,  TRUE,  '{PV}'),
('Kia India',              NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('Toyota Kirloskar',       NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('Honda Cars India',       NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('Skoda Auto India',       NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('Volkswagen India',       NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('Renault India',          NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('Nissan India',           NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('MG Motor India',         NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('Jeep India',             NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('Citroen India',          NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('BYD India',              NULL,         NULL,     FALSE, TRUE,  '{PV}'),
('BMW India',              NULL,         NULL,     FALSE, FALSE, '{PV}'),
('Mercedes-Benz India',    NULL,         NULL,     FALSE, FALSE, '{PV}'),
('Audi India',             NULL,         NULL,     FALSE, FALSE, '{PV}'),
-- Listed CV OEMs
('Tata Motors CV',         'TATAMOTORS', '500570', TRUE,  TRUE,  '{CV}'),
('Mahindra CV',            'M&M',        '500520', TRUE,  TRUE,  '{CV}'),
('Ashok Leyland',          'ASHOKLEY',   '500477', TRUE,  TRUE,  '{CV}'),
('Eicher Motors',          'EICHERMOT',  '505200', TRUE,  TRUE,  '{CV}'),
('Force Motors',           'FORCEMOT',   '505285', TRUE,  TRUE,  '{CV}'),
('SML Isuzu',              'SMLISUZU',   '505186', TRUE,  TRUE,  '{CV}'),
('Volvo India',            NULL,         NULL,     FALSE, TRUE,  '{CV}'),
('Daimler India',          NULL,         NULL,     FALSE, TRUE,  '{CV}'),
('Isuzu Motors India',     NULL,         NULL,     FALSE, TRUE,  '{CV}'),
-- Listed 2W OEMs
('Hero MotoCorp',          'HEROMOTOCO', '500182', TRUE,  TRUE,  '{2W}'),
('Honda Motorcycle',       NULL,         NULL,     FALSE, TRUE,  '{2W}'),
('TVS Motor Company',      'TVSMOTOR',   '532343', TRUE,  TRUE,  '{2W}'),
('Bajaj Auto',             'BAJAJ-AUTO', '532977', TRUE,  TRUE,  '{2W}'),
('Royal Enfield',          'EICHERMOT',  '505200', TRUE,  TRUE,  '{2W}'),
('Suzuki Motorcycle',      NULL,         NULL,     FALSE, TRUE,  '{2W}'),
('Yamaha India',           NULL,         NULL,     FALSE, TRUE,  '{2W}'),
('Ola Electric',           'OLAELEC',    '544225', TRUE,  TRUE,  '{2W}'),
('Ather Energy',           NULL,         NULL,     FALSE, TRUE,  '{2W}'),
('Revolt Motors',          NULL,         NULL,     FALSE, TRUE,  '{2W}')
ON CONFLICT (oem_name) DO NOTHING;


-- ============================================================================
-- dim_oem_alias: Source-specific maker name mappings
-- ============================================================================
-- We use a CTE to resolve oem_id dynamically by oem_name
WITH oem_map AS (
    SELECT oem_id, oem_name FROM dim_oem
)
INSERT INTO dim_oem_alias (oem_id, source, alias_name)
SELECT o.oem_id, a.source, a.alias_name
FROM (VALUES
    -- Maruti Suzuki VAHAN names
    ('Maruti Suzuki',       'VAHAN', 'MARUTI SUZUKI INDIA LTD'),
    ('Maruti Suzuki',       'VAHAN', 'MARUTI SUZUKI'),
    ('Maruti Suzuki',       'FADA',  'Maruti Suzuki'),
    ('Maruti Suzuki',       'FADA',  'Maruti'),
    -- Hyundai VAHAN names
    ('Hyundai Motor India', 'VAHAN', 'HYUNDAI MOTOR INDIA LTD'),
    ('Hyundai Motor India', 'VAHAN', 'HYUNDAI'),
    ('Hyundai Motor India', 'FADA',  'Hyundai'),
    -- Tata Motors PV (disambiguated by vehicle_class at transform layer)
    ('Tata Motors PV',      'VAHAN', 'TATA MOTORS LTD'),
    ('Tata Motors PV',      'FADA',  'Tata Motors PV'),
    ('Tata Motors PV',      'FADA',  'Tata'),
    -- Tata Motors CV (disambiguated by vehicle_class at transform layer)
    ('Tata Motors CV',      'VAHAN', 'TATA MOTORS LTD'),
    ('Tata Motors CV',      'FADA',  'Tata Motors CV'),
    -- Mahindra PV
    ('Mahindra PV',         'VAHAN', 'MAHINDRA AND MAHINDRA LTD'),
    ('Mahindra PV',         'VAHAN', 'MAHINDRA & MAHINDRA'),
    ('Mahindra PV',         'FADA',  'Mahindra'),
    -- Mahindra CV
    ('Mahindra CV',         'VAHAN', 'MAHINDRA AND MAHINDRA LTD'),
    ('Mahindra CV',         'FADA',  'Mahindra CV'),
    -- Kia
    ('Kia India',           'VAHAN', 'KIA INDIA PRIVATE LIMITED'),
    ('Kia India',           'VAHAN', 'KIA MOTORS INDIA'),
    ('Kia India',           'FADA',  'Kia'),
    -- Toyota
    ('Toyota Kirloskar',    'VAHAN', 'TOYOTA KIRLOSKAR MOTOR PVT LTD'),
    ('Toyota Kirloskar',    'VAHAN', 'TOYOTA KIRLOSKAR MOTOR PRIVATE LIMITED'),
    ('Toyota Kirloskar',    'FADA',  'Toyota'),
    -- Honda Cars
    ('Honda Cars India',    'VAHAN', 'HONDA CARS INDIA LTD'),
    ('Honda Cars India',    'VAHAN', 'HONDA CARS INDIA LIMITED'),
    ('Honda Cars India',    'FADA',  'Honda Cars'),
    -- Skoda
    ('Skoda Auto India',    'VAHAN', 'SKODA AUTO INDIA PRIVATE LIMITED'),
    ('Skoda Auto India',    'FADA',  'Skoda'),
    -- Volkswagen
    ('Volkswagen India',    'VAHAN', 'VOLKSWAGEN INDIA PRIVATE LIMITED'),
    ('Volkswagen India',    'FADA',  'Volkswagen'),
    -- Renault
    ('Renault India',       'VAHAN', 'RENAULT INDIA PRIVATE LIMITED'),
    ('Renault India',       'FADA',  'Renault'),
    -- Nissan
    ('Nissan India',        'VAHAN', 'NISSAN MOTOR INDIA PRIVATE LIMITED'),
    ('Nissan India',        'FADA',  'Nissan'),
    -- MG Motor
    ('MG Motor India',      'VAHAN', 'MG MOTOR INDIA PRIVATE LIMITED'),
    ('MG Motor India',      'FADA',  'MG'),
    -- Jeep
    ('Jeep India',          'VAHAN', 'FCA INDIA AUTOMOBILES PRIVATE LIMITED'),
    ('Jeep India',          'VAHAN', 'STELLANTIS INDIA PRIVATE LIMITED'),
    ('Jeep India',          'FADA',  'Jeep'),
    -- Citroen
    ('Citroen India',       'VAHAN', 'CITROEN INDIA PRIVATE LIMITED'),
    ('Citroen India',       'FADA',  'Citroen'),
    -- BYD
    ('BYD India',           'VAHAN', 'BYD INDIA PRIVATE LIMITED'),
    ('BYD India',           'FADA',  'BYD'),
    -- Ashok Leyland
    ('Ashok Leyland',       'VAHAN', 'ASHOK LEYLAND LTD'),
    ('Ashok Leyland',       'VAHAN', 'ASHOK LEYLAND LIMITED'),
    ('Ashok Leyland',       'FADA',  'Ashok Leyland'),
    -- Eicher / Royal Enfield
    ('Eicher Motors',       'VAHAN', 'VE COMMERCIAL VEHICLES LIMITED'),
    ('Royal Enfield',       'VAHAN', 'ROYAL ENFIELD'),
    ('Royal Enfield',       'VAHAN', 'EICHER MOTORS LIMITED'),
    ('Royal Enfield',       'FADA',  'Royal Enfield'),
    -- Force Motors
    ('Force Motors',        'VAHAN', 'FORCE MOTORS LTD'),
    ('Force Motors',        'FADA',  'Force Motors'),
    -- SML Isuzu
    ('SML Isuzu',           'VAHAN', 'SML ISUZU LIMITED'),
    ('SML Isuzu',           'FADA',  'SML Isuzu'),
    -- Hero MotoCorp
    ('Hero MotoCorp',       'VAHAN', 'HERO MOTOCORP LTD'),
    ('Hero MotoCorp',       'VAHAN', 'HERO MOTOCORP LIMITED'),
    ('Hero MotoCorp',       'FADA',  'Hero MotoCorp'),
    -- Honda Motorcycle
    ('Honda Motorcycle',    'VAHAN', 'HONDA MOTORCYCLE & SCOOTER INDIA PVT LTD'),
    ('Honda Motorcycle',    'VAHAN', 'HONDA MOTORCYCLE AND SCOOTER INDIA PRIVATE LIMITED'),
    ('Honda Motorcycle',    'FADA',  'Honda 2Wheelers'),
    -- TVS Motor
    ('TVS Motor Company',   'VAHAN', 'T V S MOTOR COMPANY LTD'),
    ('TVS Motor Company',   'VAHAN', 'TVS MOTOR COMPANY LIMITED'),
    ('TVS Motor Company',   'FADA',  'TVS'),
    -- Bajaj Auto
    ('Bajaj Auto',          'VAHAN', 'BAJAJ AUTO LTD'),
    ('Bajaj Auto',          'VAHAN', 'BAJAJ AUTO LIMITED'),
    ('Bajaj Auto',          'FADA',  'Bajaj'),
    -- Suzuki Motorcycle
    ('Suzuki Motorcycle',   'VAHAN', 'SUZUKI MOTORCYCLE INDIA PRIVATE LIMITED'),
    ('Suzuki Motorcycle',   'FADA',  'Suzuki'),
    -- Yamaha
    ('Yamaha India',        'VAHAN', 'INDIA YAMAHA MOTOR PRIVATE LIMITED'),
    ('Yamaha India',        'FADA',  'Yamaha'),
    -- Ola Electric
    ('Ola Electric',        'VAHAN', 'OLA ELECTRIC TECHNOLOGIES PRIVATE LIMITED'),
    ('Ola Electric',        'VAHAN', 'OLA ELECTRIC'),
    ('Ola Electric',        'FADA',  'Ola Electric'),
    -- Ather Energy
    ('Ather Energy',        'VAHAN', 'ATHER ENERGY PRIVATE LIMITED'),
    ('Ather Energy',        'FADA',  'Ather')
) AS a(oem_name, source, alias_name)
JOIN oem_map o ON o.oem_name = a.oem_name
ON CONFLICT (source, alias_name) DO NOTHING;


-- ============================================================================
-- dim_segment: Segment and sub-segment codes
-- ============================================================================
INSERT INTO dim_segment (segment_code, segment_name, sub_segment) VALUES
('PV',  'Passenger Vehicles', NULL),
('PV',  'Passenger Vehicles', 'Sedan'),
('PV',  'Passenger Vehicles', 'Hatchback'),
('PV',  'Passenger Vehicles', 'SUV'),
('PV',  'Passenger Vehicles', 'MPV'),
('PV',  'Passenger Vehicles', 'Van'),
('CV',  'Commercial Vehicles', NULL),
('CV',  'Commercial Vehicles', 'LCV'),
('CV',  'Commercial Vehicles', 'MHCV'),
('CV',  'Commercial Vehicles', 'SCV'),
('CV',  'Commercial Vehicles', 'Bus'),
('2W',  'Two Wheelers', NULL),
('2W',  'Two Wheelers', 'Motorcycle'),
('2W',  'Two Wheelers', 'Scooter'),
('2W',  'Two Wheelers', 'Moped')
ON CONFLICT (segment_code, sub_segment) DO NOTHING;


-- ============================================================================
-- dim_vehicle_class_map: VAHAN vehicle class names → segment
-- ============================================================================
-- PV classes
WITH seg AS (SELECT segment_id, segment_code FROM dim_segment WHERE sub_segment IS NULL)
INSERT INTO dim_vehicle_class_map (vahan_class_name, segment_id, is_excluded, notes)
SELECT v.class_name, seg.segment_id, v.is_excluded, v.notes
FROM (VALUES
    ('Motor Car',                       'PV',  FALSE, NULL),
    ('Motor Car (Invalid Registration)','PV',  FALSE, 'Includes revised entries'),
    ('Jeep',                            'PV',  FALSE, NULL),
    ('Maxi Cab',                        'PV',  FALSE, 'Shared mobility PV'),
    ('Motor Cab',                       'PV',  FALSE, 'Taxi PV'),
    ('Omnibus',                         'CV',  FALSE, 'Bus/minibus'),
    ('Light Motor Vehicle',             'PV',  FALSE, 'LMV catch-all'),
    ('Light Goods Vehicle',             'CV',  FALSE, NULL),
    ('Medium Goods Vehicle',            'CV',  FALSE, NULL),
    ('Heavy Goods Vehicle',             'CV',  FALSE, NULL),
    ('Medium Passenger Vehicle',        'CV',  FALSE, 'Bus'),
    ('Heavy Passenger Vehicle',         'CV',  FALSE, 'Bus'),
    ('Light Passenger Vehicle',         'CV',  FALSE, 'Mini bus/van'),
    ('Motor Cycle/Scooter',             '2W',  FALSE, 'Combined 2W class'),
    ('Motor Cycle',                     '2W',  FALSE, NULL),
    ('Scooter',                         '2W',  FALSE, NULL),
    ('Moped',                           '2W',  FALSE, NULL),
    ('Electric Motor Cycle',            '2W',  FALSE, 'EV 2W'),
    ('Three Wheeler(Passenger)',        NULL,  TRUE,  '3W excluded from scope'),
    ('Three Wheeler(Goods)',            NULL,  TRUE,  '3W excluded from scope'),
    ('Tractor',                         NULL,  TRUE,  'Agri excluded'),
    ('Power Tiller',                    NULL,  TRUE,  'Agri excluded'),
    ('Agricultural Vehicle',            NULL,  TRUE,  'Agri excluded'),
    ('E-Rickshaw',                      NULL,  TRUE,  '3W EV excluded'),
    ('E-Rickshaw (Cart)',                NULL,  TRUE,  '3W EV excluded'),
    ('Ambulance',                       NULL,  TRUE,  'Special purpose'),
    ('Fire Brigade',                    NULL,  TRUE,  'Special purpose'),
    ('Crane',                           NULL,  TRUE,  'Special purpose'),
    ('Construction Equipment Vehicle',  NULL,  TRUE,  'CE excluded')
) AS v(class_name, segment_code, is_excluded, notes)
JOIN seg ON seg.segment_code = v.segment_code OR (v.segment_code IS NULL AND v.is_excluded)
ON CONFLICT (vahan_class_name) DO NOTHING;


-- ============================================================================
-- dim_fuel: Fuel / powertrain codes
-- ============================================================================
INSERT INTO dim_fuel (fuel_code, powertrain, dashboard_bucket, fuel_group) VALUES
('PETROL',              'ICE',    'ICE', 'Petrol'),
('DIESEL',              'ICE',    'ICE', 'Diesel'),
('CNG',                 'ICE',    'ICE', 'CNG'),
('LPG',                 'ICE',    'ICE', 'CNG/LPG'),
('CNG + PETROL',        'ICE',    'ICE', 'CNG'),
('LPG + PETROL',        'ICE',    'ICE', 'CNG/LPG'),
('STRONG HYBRID(EV)',   'HYBRID', 'ICE', 'Hybrid'),
('MILD HYBRID',         'HYBRID', 'ICE', 'Hybrid'),
('PLUG-IN HYBRID(PHEV)','HYBRID', 'ICE', 'Hybrid'),
('ELECTRIC(BOV)',       'EV',     'EV',  'Electric'),
('ELECTRIC',            'EV',     'EV',  'Electric'),
('HYDROGEN FUEL CELL',  'EV',     'EV',  'Electric'),
('OTHERS',              'ICE',    'ICE', 'Other'),
('NOT APPLICABLE',      'ICE',    'ICE', 'Other')
ON CONFLICT (fuel_code) DO NOTHING;


-- ============================================================================
-- dim_geo: Geography dimension
-- ============================================================================
INSERT INTO dim_geo (geo_id, level, state_name, vahan4_active) OVERRIDING SYSTEM VALUE VALUES
(1, 'NATIONAL', 'All India', FALSE)
ON CONFLICT (geo_id) DO NOTHING;

-- States (future use, vahan4_active = states live on VAHAN4)
INSERT INTO dim_geo (level, state_name, vahan4_active) VALUES
('STATE', 'Andhra Pradesh',    TRUE),
('STATE', 'Arunachal Pradesh', FALSE),
('STATE', 'Assam',             FALSE),
('STATE', 'Bihar',             TRUE),
('STATE', 'Chhattisgarh',      FALSE),
('STATE', 'Goa',               FALSE),
('STATE', 'Gujarat',           TRUE),
('STATE', 'Haryana',           TRUE),
('STATE', 'Himachal Pradesh',  FALSE),
('STATE', 'Jharkhand',         FALSE),
('STATE', 'Karnataka',         TRUE),
('STATE', 'Kerala',            TRUE),
('STATE', 'Madhya Pradesh',    TRUE),
('STATE', 'Maharashtra',       TRUE),
('STATE', 'Manipur',           FALSE),
('STATE', 'Meghalaya',         FALSE),
('STATE', 'Mizoram',           FALSE),
('STATE', 'Nagaland',          FALSE),
('STATE', 'Odisha',            FALSE),
('STATE', 'Punjab',            TRUE),
('STATE', 'Rajasthan',         TRUE),
('STATE', 'Sikkim',            FALSE),
('STATE', 'Tamil Nadu',        TRUE),
('STATE', 'Telangana',         TRUE),
('STATE', 'Tripura',           FALSE),
('STATE', 'Uttar Pradesh',     TRUE),
('STATE', 'Uttarakhand',       FALSE),
('STATE', 'West Bengal',       TRUE),
('STATE', 'Delhi',             TRUE),
('STATE', 'Jammu & Kashmir',   FALSE),
('STATE', 'Ladakh',            FALSE),
('STATE', 'Chandigarh',        FALSE),
('STATE', 'Puducherry',        FALSE)
ON CONFLICT DO NOTHING;
