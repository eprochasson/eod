"""
Prepare the Air Quality datasets
"""
import pandas as pd
import seaborn as sns
sns.set_theme(style="darkgrid")


########################################################################################################################
# First, pick up a few station and mash all their data in one large table.
cwb = pd.read_csv("data/causeway-bay, hongkong-air-quality.csv")
sz = pd.read_csv("data/shenzhen-air-quality.csv")
gz = pd.read_csv("data/guangzhou-air-quality.csv")
ct = pd.read_csv("data/central_western-air-quality.csv")
zh = pd.read_csv("data/zhuhai-air-quality.csv")
mc = pd.read_csv("data/coloane,-macau-air-quality.csv")
st = pd.read_csv("data/sha-tin, hongkong-air-quality.csv")

cwb.rename(columns={c: f'CWB_{c.strip()}' for c in cwb.columns if c != 'date'}, inplace=True)
sz.rename(columns={c: f'Shenzhen_{c.strip()}' for c in sz.columns if c != 'date'}, inplace=True)
gz.rename(columns={c: f'Guangzhou_{c.strip()}' for c in gz.columns if c != 'date'}, inplace=True)
ct.rename(columns={c: f'Central_{c.strip()}' for c in ct.columns if c != 'date'}, inplace=True)
zh.rename(columns={c: f'Zhuhai_{c.strip()}' for c in zh.columns if c != 'date'}, inplace=True)
mc.rename(columns={c: f'Macau_{c.strip()}' for c in mc.columns if c != 'date'}, inplace=True)
st.rename(columns={c: f'Shatin_{c.strip()}' for c in st.columns if c != 'date'}, inplace=True)


df = cwb.merge(sz, how='left', on='date')\
        .merge(gz, how='left', on='date')\
        .merge(ct, how='left', on='date')\
        .merge(mc, how='left', on='date')\
        .merge(zh, how='left', on='date')\
        .merge(st, how='left', on='date')

df.index = pd.DatetimeIndex(pd.to_datetime(df['date']))
df['year'] = df.index.map(lambda d: d.year)
df['week'] = df.index.map(lambda d: d.isocalendar().week)
df['doy'] = df.index.dayofyear
df['month'] = df.index.month
df['quarter'] = df.index.quarter

del df['date']
df.index.name = 'date'

df = df.sort_index(ascending=True)
df.to_csv("data/clean/air_pollution_gd_area.csv")



########################################################################################################################
# Rest of this script is about variation from baseline.

indices = ['pm25', 'pm10', 'o3', 'no2', 'so2']  # Some stations also have "co", but not all.
stations = ['CWB', 'Shenzhen', 'Guangzhou', 'Central', 'Macau', 'Zhuhai', 'Shatin']
columns = [f"{station}_{index}" for index in indices for station in stations]


# Remove pre-COVID, pre-protest years
df = df[df['year'] < 2019]

# For every year, every index, every station, we look at the yearly average, and how a measurement differs from that
# baseline. Assumption being that we'll be able to absorb seasonality with that.
yearly_mean = df.groupby('year')[columns].mean()

# Weekly rolling average for the pollution values, to smooth outliers
df[columns] = df[columns].rolling('7D', center=True).mean()

def from_baseline(year, column, value):
    baseline = yearly_mean.loc[year, column]
    return (value - baseline) / baseline


# There is a probably a way to do that with vectorized functions but who cares.
for ix, row in df[columns].iterrows():
    for col, val in row.iteritems():
        df.loc[ix, f"{col}_from_baseline"] = from_baseline(ix.year, col, val)


# Verify that the variation from baseline are somewhat consistent.
ts = df[['year', 'week', 'CWB_no2_from_baseline']].resample('2W').mean().rename(columns={'CWB_no2_from_baseline': 'no2_from_baseline'})
ts['station'] = 'CWB'
for s in ['Central', 'Shatin', 'Shenzhen']:
    if s != 'CWB':
        tmp = df[['year', 'week', f'{s}_no2_from_baseline']].resample('2W').mean().rename(columns={f'{s}_no2_from_baseline': 'no2_from_baseline'})
        tmp['station'] = s
        ts = pd.concat([ts, tmp])

# Need to reset the week value, because it's been mean-ed by the resampling
ts['week'] = ts.index.map(lambda x: x.isocalendar().week)
# sns.lineplot(x='week', y='no2_from_baseline', hue='station', data=ts)  # Uncomment for pretty plot
# somewhat...


res = df.groupby("doy").mean().reset_index()
res = res[['doy']+[f"{c}_from_baseline" for c in columns]]
res.index = res['doy']
del res['doy']

WINDOW = 28

# Need to make the dataframe cyclical for rolling average
before = res.loc[list(range(366-WINDOW, 366)), :].copy()
before.index = list(range(-WINDOW+1, 1))
after = res.loc[list(range(1, WINDOW+1)), :].copy()
after.index = list(range(367, 367+WINDOW))

res = pd.concat([before, res, after])
res = res.rolling(WINDOW, center=True).mean()

res.index.name = 'doy'

res.loc[1:366].to_csv("data/clean/air_pollution_baseline.csv", index=True)
