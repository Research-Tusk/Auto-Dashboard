-- ============================================================================
-- AutoQuant: ASP Seed Data
-- Initial Average Selling Price assumptions for revenue proxy calculation
-- All prices in INR Lakhs (100,000 INR = 1 Lakh)
-- Source: Analyst estimates based on published price lists and mix assumptions
-- These will be calibrated from earnings data as it becomes available
-- ============================================================================

WITH oem_seg AS (
    SELECT o.oem_id, o.oem_name, s.segment_id, s.segment_code
    FROM dim_oem o
    CROSS JOIN dim_segment s
    WHERE s.sub_segment IS NULL  -- top-level segments only
)
INSERT INTO fact_asp_master (oem_id, segment_id, fuel_id, effective_from, asp_inr_lakhs, asp_source, notes)
SELECT
    os.oem_id,
    os.segment_id,
    0 AS fuel_id,
    '2024-04-01'::DATE AS effective_from,
    asp.asp_val::DECIMAL(10,4),
    'ANALYST_ESTIMATE' AS asp_source,
    asp.notes
FROM (
    VALUES
    -- PV ASPs (INR Lakhs): Blended mix estimate
    ('Maruti Suzuki',        'PV', 8.5,  'Blended mix incl. entry hatchbacks + SUVs'),
    ('Hyundai Motor India',  'PV', 12.5, 'Blended mix; Creta/Venue heavy'),
    ('Tata Motors PV',       'PV', 13.0, 'Blended; Nexon/Punch/Harrier mix'),
    ('Mahindra PV',          'PV', 16.0, 'Blended; XUV700/Scorpio/BE6 mix'),
    ('Kia India',            'PV', 15.0, 'Blended; Sonet/Seltos mix'),
    ('Toyota Kirloskar',     'PV', 22.0, 'Blended; Innova/Hyryder/Fortuner mix'),
    ('Honda Cars India',     'PV', 13.0, 'City/Elevate blended'),
    ('Skoda Auto India',     'PV', 20.0, 'Slavia/Kushaq/Kodiaq blended'),
    ('Volkswagen India',     'PV', 18.0, 'Virtus/Taigun blended'),
    ('Renault India',        'PV', 9.0,  'Kiger/Triber blended'),
    ('Nissan India',         'PV', 10.0, 'Magnite only'),
    ('MG Motor India',       'PV', 18.0, 'Hector/Windsor/Comet blended'),
    ('Jeep India',           'PV', 32.0, 'Compass/Meridian blended'),
    ('Citroen India',        'PV', 12.0, 'C3/C3 Aircross blended'),
    ('BYD India',            'PV', 35.0, 'Atto3/Seal blended'),
    -- CV ASPs (INR Lakhs): Blended mix
    ('Tata Motors CV',       'CV', 28.0, 'Blended LCV+MHCV mix'),
    ('Mahindra CV',          'CV', 15.0, 'Blended; SCV heavy'),
    ('Ashok Leyland',        'CV', 32.0, 'MHCV focused'),
    ('Eicher Motors',        'CV', 28.0, 'Pro series blended'),
    ('Force Motors',         'CV', 18.0, 'Traveller/Trax blended'),
    ('SML Isuzu',            'CV', 22.0, 'MCV blended'),
    -- 2W ASPs (INR Lakhs): Blended mix
    ('Hero MotoCorp',        '2W', 0.95, 'Mass segment; Splendor heavy'),
    ('Honda Motorcycle',     '2W', 0.90, 'Activa/Shine blended'),
    ('TVS Motor Company',    '2W', 1.10, 'Jupiter/Apache/Ntorq blended'),
    ('Bajaj Auto',           '2W', 1.20, 'Pulsar/CT series blended'),
    ('Royal Enfield',        '2W', 2.20, 'Bullet/Classic/Meteor blended'),
    ('Suzuki Motorcycle',    '2W', 1.10, 'Access/Gixxer blended'),
    ('Yamaha India',         '2W', 1.15, 'FZS/R15/Fascino blended'),
    ('Ola Electric',         '2W', 1.30, 'S1 series blended'),
    ('Ather Energy',         '2W', 1.45, '450X/Rizta blended')
) AS asp(oem_name, segment_code, asp_val, notes)
JOIN oem_seg os ON os.oem_name = asp.oem_name AND os.segment_code = asp.segment_code
ON CONFLICT (oem_id, segment_id, fuel_id, effective_from) DO NOTHING;
