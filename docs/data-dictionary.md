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
