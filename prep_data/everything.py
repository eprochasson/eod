"""
Compare the variation of the air quality with the variation in mobility
"""
import pandas as pd
import datetime
import seaborn as sns
import numpy as np
sns.set_theme(style="darkgrid")

indices = ['pm25', 'pm10', 'o3', 'no2', 'so2']  # Some stations also have "co", but not all.
stations = ['CWB', 'Shenzhen', 'Guangzhou', 'Central', 'Macau', 'Zhuhai', 'Shatin']
columns = [f"{station}_{index}" for index in indices for station in stations]


mobility = pd.read_csv("../data/clean/mobility_hk_only.csv")
mobility_columns = ['retail_and_recreation_percent_change_from_baseline',
                    'grocery_and_pharmacy_percent_change_from_baseline',
                    'parks_percent_change_from_baseline',
                    'transit_stations_percent_change_from_baseline',
                    'workplaces_percent_change_from_baseline',
                    'residential_percent_change_from_baseline']


mobility.index = pd.DatetimeIndex(pd.to_datetime(mobility['date']))
mobility.sort_index()

# covid data
cv = pd.read_csv("../data/covid.csv")
cv.index = pd.DatetimeIndex(cv['date'])
cv['new_cases_rolling'] = cv['new_cases'].rolling("28D").mean()
del cv['date']

air = pd.read_csv("../data/clean/air_pollution_gd_area.csv")
air.index = pd.DatetimeIndex(pd.to_datetime(air['date']))
air = air[air.index.year >= 2020]  # First date available in the mobility dataset

air[columns] = air[columns].rolling("28D").mean()
air['year'] = air.index.map(lambda d: d.year)
air['week'] = air.index.map(lambda d: d.isocalendar().week)
air['doy'] = air.index.dayofyear
air['month'] = air.index.month
air['quarter'] = air.index.quarter

air_baseline = pd.read_csv("../data/clean/air_pollution_baseline.csv")
air_baseline.index = air_baseline.doy
air_baseline.rename(columns={c: c.replace("from_baseline", "seasonal_variation") for c in air_baseline.columns}, inplace=True)
del air_baseline['doy']

yearly_mean = air.groupby('year')[columns].mean()


def from_baseline(year, column, value):
    baseline = yearly_mean.loc[year, column]
    return (value - baseline) / baseline


def adjust_baseline(doy, column, value):
    return (value - air_baseline.loc[doy, column]) / air_baseline.loc[doy, column]


air = air.merge(air_baseline, how='left', left_on='doy', right_index=True)
air = air.merge(yearly_mean.rename(columns={c: c+"_mean" for c in yearly_mean.columns}), how='left', left_on='year', right_on='year')

# The index gets destroyed here. Don't know why. No time to investigate
air.index = pd.DatetimeIndex(pd.to_datetime(air['date']))
air = air.merge(mobility[mobility_columns], left_index=True, right_index=True)

# for c in columns:
#     air[f"{c}_adjusted"] = air[c] + (air[f"{c}_seasonal_variation"] * air[f"{c}_mean"])


for ix, row in air[columns].iterrows():
    for col, val in row.iteritems():
        bl = from_baseline(ix.year, col, val)
        air.loc[ix, f"{col}_from_baseline"] = bl

for c in columns:
    air[f'{c}_from_baseline_adjusted'] = air[f'{c}_from_baseline'] - air[f'{c}_seasonal_variation']


air = air.merge(cv, how='left', left_index=True, right_index=True)

air.to_csv("data/clean/everything.csv", index=False)