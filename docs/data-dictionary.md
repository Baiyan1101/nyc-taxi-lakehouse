# Data Dictionary

## Yellow Taxi Raw Trip Schema

The initial ingestion job targets NYC TLC Yellow Taxi trip records.

| Column | Type | Meaning |
| --- | --- | --- |
| `VendorID` | integer | Taxi technology provider identifier |
| `tpep_pickup_datetime` | timestamp | Pickup timestamp |
| `tpep_dropoff_datetime` | timestamp | Dropoff timestamp |
| `passenger_count` | long | Driver-reported passenger count |
| `trip_distance` | double | Trip distance in miles |
| `RatecodeID` | long | TLC rate code |
| `store_and_fwd_flag` | string | Whether the record was stored before forwarding |
| `PULocationID` | integer | Pickup taxi zone ID |
| `DOLocationID` | integer | Dropoff taxi zone ID |
| `payment_type` | long | Payment type code |
| `fare_amount` | double | Metered fare amount |
| `extra` | double | Extra charges |
| `mta_tax` | double | MTA tax |
| `tip_amount` | double | Tip amount |
| `tolls_amount` | double | Toll amount |
| `improvement_surcharge` | double | Improvement surcharge |
| `total_amount` | double | Total charged amount |
| `congestion_surcharge` | double | Congestion surcharge |
| `Airport_fee` | double | Airport fee |

Schema drift note: newer files can add columns, such as congestion-pricing-related fields. Raw ingestion currently selects the columns needed by the first project milestones; schema drift handling will be revisited when quality checks and curated modeling are added.

## Cleaned Trip Derived Columns

| Column | Type | Meaning |
| --- | --- | --- |
| `pickup_date` | date | Date extracted from pickup timestamp |
| `pickup_hour` | integer | Hour extracted from pickup timestamp |
| `trip_duration_minutes` | double | Difference between dropoff and pickup timestamps in minutes |
| `total_amount_per_mile` | double | `total_amount / trip_distance` |
| `tip_percentage` | double | `tip_amount / fare_amount * 100`; zero when fare is zero |

## Curated Tables

### fact_trips

| Column | Meaning |
| --- | --- |
| `trip_id` | Deterministic hash ID generated from stable trip attributes |
| `pickup_datetime` | Pickup timestamp |
| `dropoff_datetime` | Dropoff timestamp |
| `pickup_date` | Pickup date, also used as partition column |
| `pickup_hour` | Pickup hour |
| `pickup_location_id` | Foreign key to `dim_zone.location_id` |
| `dropoff_location_id` | Foreign key to `dim_zone.location_id` |
| `passenger_count` | Passenger count |
| `trip_distance` | Trip distance in miles |
| `fare_amount` | Fare amount |
| `tip_amount` | Tip amount |
| `total_amount` | Total amount |
| `payment_type_id` | Foreign key to `dim_payment_type.payment_type_id` |
| `trip_duration_minutes` | Trip duration in minutes |
| `total_amount_per_mile` | Total amount divided by trip distance |
| `tip_percentage` | Tip amount divided by fare amount |

### dim_date

| Column | Meaning |
| --- | --- |
| `date` | Calendar date |
| `year` | Calendar year |
| `month` | Calendar month |
| `day` | Day of month |
| `day_of_week` | Short weekday label |
| `day_of_week_number` | Spark weekday number, 1 = Sunday and 7 = Saturday |
| `is_weekend` | Weekend flag |

### dim_zone

| Column | Meaning |
| --- | --- |
| `location_id` | TLC taxi zone location ID |
| `borough` | NYC borough |
| `zone` | Taxi zone name |
| `service_zone` | TLC service zone |

### dim_payment_type

| Column | Meaning |
| --- | --- |
| `payment_type_id` | TLC payment code |
| `payment_type_description` | Human-readable payment type |
