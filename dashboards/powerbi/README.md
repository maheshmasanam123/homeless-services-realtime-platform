# Power BI Dashboard

Open `hmis_dashboard.pbids` in Power BI Desktop, point the Snowflake connection
at your account, and pull the three pages:

1. **Coordinator** — live bed availability, wait times, outreach hot zones
2. **Funder / HUD APR** — Annual Performance Report metrics (entries, exits,
   destinations, length of stay) per HUD CoC reporting spec
3. **Equity Lens** — demographic distribution (veteran, race, gender) of
   services delivered, with disparity flags

DAX measures (paste into the model):

```dax
Active Clients = DISTINCTCOUNT(fact_service_event[client_id_hash])
Length of Stay (days) = AVERAGEX(
    SUMMARIZE(fact_service_event, fact_service_event[enrollment_id]),
    DATEDIFF(MIN(fact_service_event[service_date]),
             MAX(fact_service_event[service_date]), DAY)
)
Returns to Homelessness 6m = CALCULATE([Active Clients],
    FILTER(fact_service_event, fact_service_event[service_date] >
                                EARLIER(fact_service_event[service_date]) + 180))
```
