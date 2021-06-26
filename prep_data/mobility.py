"""
Compare the variation of the air quality with the variation in mobility
"""
import pandas as pd

mobility = pd.read_csv("data/Global_Mobility_Report.csv")
mobility = mobility[mobility['country_region_code'] == 'HK'].copy()
mobility_columns = ['retail_and_recreation_percent_change_from_baseline',
                    'grocery_and_pharmacy_percent_change_from_baseline',
                    'parks_percent_change_from_baseline',
                    'transit_stations_percent_change_from_baseline',
                    'workplaces_percent_change_from_baseline',
                    'residential_percent_change_from_baseline']


mobility.index = pd.DatetimeIndex(pd.to_datetime(mobility['date']))
mobility.sort_index()

# Rolling, 2 week window
mobility[mobility_columns] = mobility[mobility_columns].rolling('28D').mean()

for c in mobility_columns:
    mobility[c] = mobility[c] / 100  # For some reason, Google distributes the percentage data * 100. Annoying.

mobility.to_csv("data/clean/mobility_hk_only.csv")
