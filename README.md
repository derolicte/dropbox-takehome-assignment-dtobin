# Dropbox Takehome Project

This project demonstrates SQL modeling and ETL processing for exploratory sales analytics using Docker containers. It includes:
- A Postgres database preloaded with cleaned data
- An ETL service to load and process CSVs


## 🧰 Requirements
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## 📁 Project Structure
```
├── app/                         # ETL
│   ├── seeds/                   # Raw CSVs to load
│   ├── output/                  # Cleaned CSVs for use in Tableau Public Views
│   ├── load_csvs.py             # ETL logic
├── db/
│   └── init.sql                 # Optional: schema or seed SQL
│   └── pipeline_coverage.sql    # Pipeline coverage by product and region
│   └── forecast_attainment.sql  # Forecast Attainment by product and region
│   └── daily_pacing.sql         # Optional: SQL for calculating bookings daily pacing vs target and forecast
├── .env                         # Postgres credentials
├── docker-compose.yml
├── dockerfile.app
├── README.md                    # You're here
```

## ⚙️ Setup Instructions

1. **Clone the repo**
   ```bash
   git clone ttps://github.com/derolicte/dropbox-takehome-assignment-dtobin.git
   cd dropbox_takehome_project
   ```

2. **Ensure `.env` file exists** (edit if needed):
   ```
   POSTGRES_USER=dbx
   POSTGRES_PASSWORD=dbxpassword
   POSTGRES_DB=dbx
   POSTGRES_HOST=db
   POSTGRES_PORT=5432
   ```

3. **Run the stack**
   ```bash
   docker-compose up --build
   ```

   This will:
   - Start the Postgres DB and initialize it with `init.sql`
   - Run the ETL pipeline to load and clean the CSVs

## 🐘 Connecting to the Database (optional)
From within any container (or your host if `psql` is installed):
```bash
psql -h localhost -U dbx -d dbx -p 5432
```

Password is `dbxpassword` (or whatever is set in `.env`).

## 📊 Tableau Public Views
- [Forecast attainment by region/product](https://public.tableau.com/app/profile/derek.tobin5334/viz/1_ForecastAttainment/1_ForecastAttainment)
- [Pipeline Coverage (Pt.1)](https://public.tableau.com/app/profile/derek.tobin5334/viz/2_PipelineCoveragePt_1/2_PipelineCoveragePt_1)
- [Pipeline Coverage (Pt.2)](https://public.tableau.com/app/profile/derek.tobin5334/viz/3_PipelineCoveragePt_2/3_PipelineCoveragePt_2)


## 🧪 Development Tips
- Modify `load_csvs.py` if seed structure changes
- Restart with:
  ```bash
  docker-compose down -v  # wipe volumes (e.g., Postgres data)
  docker-compose up --build
  ```

## ❓Troubleshooting

- **"could not translate host name 'db'"**: make sure you're using the service name `db` inside Docker; from your host, use `localhost`.
