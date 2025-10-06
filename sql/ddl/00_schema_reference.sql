-- ================================================================
-- NYC TLC Trip Data - Schema Reference
-- ================================================================
-- This file documents the source schemas from TLC data dictionaries
-- Used as reference for mapping to our unified data model

-- ================================================================
-- YELLOW TAXI SCHEMA (tpep = Taxi & For-Hire Vehicle Electronic Payment)
-- ================================================================
-- Source: https://nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

/*
VendorID                    INTEGER     - Technology provider (1=Creative Mobile, 2=VeriFone Inc.)
tpep_pickup_datetime        TIMESTAMP   - Meter engaged timestamp
tpep_dropoff_datetime       TIMESTAMP   - Meter disengaged timestamp
passenger_count             DOUBLE      - Number of passengers (driver entered)
trip_distance               DOUBLE      - Miles reported by taximeter
RatecodeID                  DOUBLE      - Final rate code (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride)
store_and_fwd_flag          VARCHAR     - Y=stored before sending, N=not stored
PULocationID                INTEGER     - Pickup location zone ID (1-263)
DOLocationID                INTEGER     - Dropoff location zone ID (1-263)
payment_type                INTEGER     - Payment method (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided)
fare_amount                 DOUBLE      - Time-and-distance fare
extra                       DOUBLE      - Miscellaneous extras (rush hour, overnight surcharges)
mta_tax                     DOUBLE      - $0.50 MTA tax (auto-triggered)
tip_amount                  DOUBLE      - Tips (credit card only, cash tips not included)
tolls_amount                DOUBLE      - Total tolls paid
improvement_surcharge       DOUBLE      - $0.30 improvement surcharge (since 2015)
total_amount                DOUBLE      - Total charged (excludes cash tips)
congestion_surcharge        DOUBLE      - NYS congestion surcharge ($2.50 for non-shared, $0.75 for shared)
airport_fee                 DOUBLE      - $1.25 for pickups at LGA/JFK (since 2015)
cbd_congestion_fee          DOUBLE      - MTA Congestion Relief Zone fee (started Jan 5, 2025: $1.50 day, $0.75 night)
*/

-- ================================================================
-- GREEN TAXI SCHEMA (lpep = Livery Passenger Enhancement Program)
-- ================================================================
-- Source: https://nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf

/*
VendorID                    INTEGER     - Technology provider (1=Creative Mobile, 2=VeriFone Inc.)
lpep_pickup_datetime        TIMESTAMP   - Meter engaged timestamp
lpep_dropoff_datetime       TIMESTAMP   - Meter disengaged timestamp
store_and_fwd_flag          VARCHAR     - Y=stored before sending, N=not stored
RatecodeID                  DOUBLE      - Final rate code
PULocationID                INTEGER     - Pickup location zone ID (1-263)
DOLocationID                INTEGER     - Dropoff location zone ID (1-263)
passenger_count             DOUBLE      - Number of passengers
trip_distance               DOUBLE      - Miles reported by taximeter
fare_amount                 DOUBLE      - Time-and-distance fare
extra                       DOUBLE      - Miscellaneous extras
mta_tax                     DOUBLE      - $0.50 MTA tax
tip_amount                  DOUBLE      - Tips (credit card only)
tolls_amount                DOUBLE      - Total tolls paid
ehail_fee                   DOUBLE      - E-hail fee (discontinued)
improvement_surcharge       DOUBLE      - $0.30 improvement surcharge
total_amount                DOUBLE      - Total charged (excludes cash tips)
payment_type                INTEGER     - Payment method (1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided)
trip_type                   DOUBLE      - 1=Street-hail, 2=Dispatch
congestion_surcharge        DOUBLE      - NYS congestion surcharge
cbd_congestion_fee          DOUBLE      - MTA Congestion Relief Zone fee (started Jan 5, 2025)
*/

-- ================================================================
-- HVFHV SCHEMA (High Volume For-Hire Vehicle - Uber/Lyft/Via/Juno)
-- ================================================================
-- Source: https://nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf

/*
hvfhs_license_num           VARCHAR     - HVFHS base license (HV0002=Juno, HV0003=Uber, HV0004=Via, HV0005=Lyft)
dispatching_base_num        VARCHAR     - Base that dispatched the trip
originating_base_num        VARCHAR     - Base that received the original request
request_datetime            TIMESTAMP   - When passenger requested pickup
on_scene_datetime           TIMESTAMP   - When driver arrived (accessible vehicles only)
pickup_datetime             TIMESTAMP   - Trip pickup time
dropoff_datetime            TIMESTAMP   - Trip dropoff time
PULocationID                BIGINT      - Pickup location zone ID
DOLocationID                BIGINT      - Dropoff location zone ID
trip_miles                  DOUBLE      - Total trip miles
trip_time                   BIGINT      - Total trip time in SECONDS
base_passenger_fare         DOUBLE      - Base fare (before tolls, tips, taxes, fees)
tolls                       DOUBLE      - Total tolls paid
bcf                         DOUBLE      - Black Car Fund fee
sales_tax                   DOUBLE      - NYS sales tax
congestion_surcharge        DOUBLE      - NYS congestion surcharge
airport_fee                 DOUBLE      - $2.50 for pickup/dropoff at LGA/JFK/Newark
tips                        DOUBLE      - Total tips received
driver_pay                  DOUBLE      - Driver pay (net of commission, surcharges, taxes; excludes tolls/tips)
shared_request_flag         VARCHAR     - Y=passenger agreed to share, N=no
shared_match_flag           VARCHAR     - Y=shared with another passenger, N=no
access_a_ride_flag          VARCHAR     - Y=MTA Access-A-Ride trip, N/blank=no
wav_request_flag            VARCHAR     - Y=wheelchair accessible vehicle requested, N=no
wav_match_flag              VARCHAR     - Y=trip in wheelchair accessible vehicle, N=no
cbd_congestion_fee          DOUBLE      - MTA Congestion Relief Zone fee (started Jan 5, 2025: $1.50 day, $0.75 night)
*/

-- ================================================================
-- TAXI ZONE LOOKUP
-- ================================================================
-- Source: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

/*
LocationID                  INTEGER     - Primary key (1-263)
Borough                     VARCHAR     - Manhattan, Queens, Bronx, Brooklyn, Staten Island, EWR
Zone                        VARCHAR     - Zone name (e.g., "Upper East Side North")
service_zone                VARCHAR     - Boro Zone, Yellow Zone, Airports, etc.
*/

-- ================================================================
-- KEY DIFFERENCES & MAPPINGS
-- ================================================================

/*
FIELD MAPPING TO UNIFIED SCHEMA:

Pickup DateTime:
  - Yellow: tpep_pickup_datetime
  - Green:  lpep_pickup_datetime
  - HVFHV:  pickup_datetime

Dropoff DateTime:
  - Yellow: tpep_dropoff_datetime
  - Green:  lpep_dropoff_datetime
  - HVFHV:  dropoff_datetime

Trip Distance:
  - Yellow: trip_distance (MILES)
  - Green:  trip_distance (MILES)
  - HVFHV:  trip_miles (MILES) *** DIFFERENT FIELD NAME ***

Trip Duration:
  - Yellow: Calculate from timestamps
  - Green:  Calculate from timestamps
  - HVFHV:  trip_time (SECONDS) *** PROVIDED DIRECTLY ***

Total Fare:
  - Yellow: total_amount
  - Green:  total_amount
  - HVFHV:  base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + airport_fee + tips + cbd_congestion_fee

Driver Pay:
  - Yellow: N/A
  - Green:  N/A
  - HVFHV:  driver_pay *** UNIQUE TO HVFHV ***

Take Rate Calculation (HVFHV only):
  take_rate = (total_fare - driver_pay) / total_fare
  Represents platform commission rate

Congestion Fee (Jan 5, 2025+):
  - cbd_congestion_fee field added to all three datasets
  - Day rate (6am-9pm): $1.50 for Yellow/Green, $1.50 for HVFHV
  - Night rate (9pm-6am): $0.75 for Yellow/Green, $0.75 for HVFHV
  - Key for before/after analysis!

Payment Type:
  - Yellow: payment_type (INTEGER codes)
  - Green:  payment_type (INTEGER codes)
  - HVFHV:  N/A (all electronic)

Passenger Count:
  - Yellow: passenger_count (driver entered, may be inaccurate)
  - Green:  passenger_count (driver entered, may be inaccurate)
  - HVFHV:  N/A (not provided)
*/

-- ================================================================
-- DATA QUALITY CONSIDERATIONS
-- ================================================================

/*
KNOWN ISSUES FROM TLC DATA:

1. Passenger Count:
   - Often defaults to 0 or 1 even for multi-passenger trips
   - Driver manually enters, prone to error
   - HVFHV doesn't report at all

2. Cash Tips:
   - Only credit card tips are recorded
   - Cash tips are missing (underreporting)
   - Affects Yellow/Green more than HVFHV

3. Zero/Negative Values:
   - trip_distance=0 (short trips, GPS errors)
   - fare_amount < 0 (refunds, disputes)
   - Need filtering/flagging

4. Timestamp Issues:
   - dropoff < pickup (clock errors)
   - Future dates (system errors)
   - Need validation

5. Location IDs:
   - Some trips have LocationID = 264, 265 (not in lookup table)
   - Unknown/unassigned zones
   - Need to handle gracefully

6. Speed Outliers:
   - Unrealistic speeds (>100 mph) indicate data errors
   - GPS drift, tunnel issues

7. HVFHV Shared Rides:
   - shared_request_flag vs shared_match_flag
   - Passenger requested share but may not have matched
   - Affects per-passenger economics

8. Schema Evolution:
   - cbd_congestion_fee added Jan 2025
   - Earlier data won't have this field
   - Need NULL handling
*/

