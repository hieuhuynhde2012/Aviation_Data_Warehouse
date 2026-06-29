# Data Sources

## BTS Airline On-Time Performance

- Purpose: real flight operations, delays, cancellations, carrier, airport, and route activity.
- Config: `BTS_YEAR`, `BTS_MONTH`, `BTS_URL_TEMPLATE`.
- Default file: April 2026 BTS On-Time Performance ZIP.
- Local path: `data/input/bts/bts_on_time_2026_04.csv`.

## OurAirports

- Purpose: airport, country, and region reference dimensions.
- Files: `airports.csv`, `countries.csv`, `regions.csv`.
- Local path: `data/input/ourairports/`.

## Synthetic Booking And Payment Events

- Purpose: business data that is usually private in real airlines.
- Generator: `ingestion/generate_booking_events.py`.
- Quality profile: intentionally dirty enough to exercise cleaning, standardization, deduplication, and quality reporting.
- Controls:
  - `SYNTHETIC_SEED`
  - `BOOKINGS_PER_FLIGHT_MIN`
  - `BOOKINGS_PER_FLIGHT_MAX`
  - `MAX_FLIGHTS_FOR_SYNTHETIC`
  - `SYNTHETIC_TARGET_ROWS`
  - `DIRTY_DATA_RATE`
  - `DUPLICATE_EVENT_RATE`
  - `MISSING_OPTIONAL_RATE`
  - `PAYMENT_MISMATCH_RATE`

The generator samples real BTS flights, creates bookings per flight, prices tickets from route distance, assigns channel/customer segment/status, and creates linked payment events.

Dirty data is generated on purpose:

- Case and whitespace drift in status, channel, payment method, currency, airline, and airport codes.
- Status synonyms such as `CONF`, `Booked`, `VOID`, `SUCCESS`, `DECLINED`, and `CHARGEBACK`.
- Duplicate events with newer `updated_at` timestamps to simulate CDC/event replay.
- Missing optional `customer_id` values.
- Occasional negative ticket values and out-of-order timestamps.
- Payment amount mismatches and refund/status inconsistencies.
