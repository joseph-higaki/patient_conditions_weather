gcs project <>

```
patient-weather-data (bucket)
├── emr-data-raw (folder)
│   ├── conditions.csv
│   ├── encounters.csv
│   └── organizations.csv
├── weather-data-raw (folder)
│   ├── event_years_coordinates.csv
│   └── weather-data
│       └── daily/  year/lat/long: code, description, temp (+- year)
│
├── patient-insights-stg (folder)
    ├── weather-data
    │   └── daily/  year/lat/long: code, description, temp, lagging    │
    └── emr
        └── diagnoses/ (from encounters, conditions), partition by year  (parquet) /
```
