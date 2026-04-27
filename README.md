# RailwayScrape: Train Delay Risk Prediction & Passenger Reliability Analytics

This repository contains a data collection and generation script designed to provide realistic, comprehensive train delay data for Power BI dashboards. It scrapes schedule data for various Indian Railways trains and simulates realistic delay scenarios to generate risk metrics and passenger reliability indices.

## Features

- **Train Schedule Scraping**: Automatically fetches train timetables and stops.
- **Realistic Delay Generation**: Simulates realistic delays based on time of day (peak vs. off-peak, night trains) and train types.
- **Power BI Ready**: Generates a consolidated Fact and Dimension star schema (CSV and Excel formats).
- **Risk Metrics**: Calculates `Delay_Risk_Score`, `Passenger_Reliability_Index`, and `Journey_Predictability_Pct`.

## Setup and Installation

1. Clone the repository.
2. Ensure you have Python installed.
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   *(Requires `selenium`, `webdriver-manager`, `pandas`, `beautifulsoup4`)*

## Usage

Run the scraper script:

```bash
python delay_analytics_scraper.py
```

The script will launch a Chrome browser, scrape the data, process it, and output the final consolidated datasets in the `train_delay_data` directory.

## Output

The output includes comprehensive datasets combining:
- Train Information
- Route Information
- Station Information
- Schedule Times (Scheduled vs Actual)
- Delay Metrics
- Time Context (Hour, Day, Month, Peak/Off-peak)
- Calculated Risk & Reliability Scores
