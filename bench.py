import pandas as pd
import seaborn as sns
sns.set_theme(style="darkgrid")


cwb = pd.read_csv("data/causeway-bay, hongkong-air-quality.csv")
sz = pd.read_csv("data/shenzhen-air-quality.csv")
gz = pd.read_csv("data/guangzhou-air-quality.csv")
ct = pd.read_csv("data/central_western-air-quality.csv")
zh = pd.read_csv("data/zhuhai-air-quality.csv")
mc = pd.read_csv("data/coloane,-macau-air-quality.csv")
st = pd.read_csv("data/sha-tin, hongkong-air-quality.csv")

mobility = pd.read_csv("data/Global_Mobility_Report.csv")
mobility = mobility[mobility['country_region_code'] == 'HK'].copy()

mobility.index = pd.DatetimeIndex(pd.to_datetime(mobility['date']))

# cwb['date_fixed'] = pd.to_datetime(cwb['date'])
# sz['date_fixed'] = pd.to_datetime(sz['date'])
# gz['date_fixed'] = pd.to_datetime(gz['date'])
# ct['date_fixed'] = pd.to_datetime(gz['date'])


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


season = df.groupby(['year', 'week', 'doy']).mean()

# measure = 'no2'
# cms = [f'CWB_{measure}', f'Central_{measure}', f'Shenzhen_{measure}', f'Guangzhou_{measure}', f'Macau_{measure}', f'Zhuhai_{measure}']
# df.resample('2W').mean()[cms].plot()
# df[cms].corr()

# measure = 'no2'
# cms = [f'CWB_{measure}', f'Central_{measure}', f'Shatin_{measure}']
# df.resample('2W').mean()[cms].plot()
# df[cms].corr()


# gb = df.groupby(['year', 'quarter'])[['CWB_no2', 'Central_no2', 'Shatin_no2']].mean().reset_index()
# piv = pd.pivot_table(gb, index=['quarter'], columns=['year'], values=['Shatin_no2'])


cwb_no2 = df[['CWB_no2']].copy().rename(columns={'CWB_no2': 'no2'})
mb = mobility.merge(cwb_no2, how='left', left_index=True, right_index=True)
m = mb['no2'].mean()
mb['no2_percent_change_from_baseline'] = mb['no2'].map(lambda x: (m - x) / m * 100)


import xmltodict
import lxml

res = xmltodict.parse(open("data/shipping/SC_Masterdata1.xml").read())