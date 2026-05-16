# Tableau Workbook

`hmis_workbook.twb` connects to the Redshift gold layer (read-only role
`R_HMIS_ANALYST`). Sheets:

- **Daily service volume** by project type
- **Bed utilization heatmap** by shelter × hour
- **Outreach geo** with chronic-encounter highlight
- **HUD APR exits to destination** stacked bar

LOD calc for "first service ever per client":

```
{ FIXED [client_id_hash] : MIN([service_date]) }
```
