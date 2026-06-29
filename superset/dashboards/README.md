# Superset Dashboard Plan

Create one dashboard named `Aviation Data Warehouse Overview` with three tabs:

1. Executive Overview
   - Total revenue
   - Total bookings
   - Average ticket value
   - Refund amount
   - Top routes by revenue

2. Booking Trend
   - Bookings by day and hour
   - Booking channel performance
   - Booking/payment status distribution
   - Payment failure rate

3. Flight And Route Performance
   - Average delay minutes
   - Delayed flight percentage
   - Cancellation by route
   - Airport delay ranking
   - Carrier delay comparison

Use the seven `mart.*` tables as Superset datasets.

## Bootstrap Assets

After the pipeline/dbt run succeeds, bootstrap Superset assets from the host:

```powershell
Get-Content superset/setup_assets.py | docker compose exec -T superset python -
```

This creates/updates:

- Database connection: `Aviation Warehouse`
- Datasets for `mart.*`, `metadata.*`, and `quarantine.*`
- Dashboard: `Aviation Data Warehouse Operations`
- Charts covering revenue, booking trends, route performance, data quality errors, quarantine, and source-to-raw reconciliation

Open Superset at <http://localhost:8088> and log in with `admin` / `admin`.
