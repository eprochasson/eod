"""
Rebuild and denormalize the shipping data from that awful XML datasets.

Original dataset can be obtained here:
https://www.censtatd.gov.hk/en/data/stat_report/product/X1020008/att/X10200082021QQ01B0100.zip

Unzip into data/shipping/ to run the script.

Some useful column description:

   THEME  CLASS_VAR               CLASS_DESC_ENG CLASS_DESC_CHI
0     SC   CAR_TYPE                   Cargo Type           貨物種類
1     SC       CCYY  Century and Year (calendar)              年
2     SC        CHL      Cargo handling location         貨物裝卸地點
3     SC        COM      Commodity section/group        貨品類別／組別
4     SC        CTP   Country/territory and port       國家／地區及港口
5     SC       Flag                         Flag             船旗
6     SC         MM                        Month              月
7     SC       Mode            Mode of Transport           運輸方式
8     SC          Q                      Quarter              季
9     SC     S_type                Shipment type           裝運種類
10    SC  Ship_type                    Ship type           船舶類型
11    SC     V_type      Type of vessel arrivals           船次種類
12    SC        YTQ                   CumQuarter           累積季度
"""
import pandas as pd


# Metadata
main = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[1]/item')
schema = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[2]/item')
statuses = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[3]/item')
reference = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[4]/item')
stat_display = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[5]/item')
stat_multiplier = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[6]/item')
stat_precision  = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[7]/item')
stat_type = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[8]/item')
stat_var = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[9]/item')
column_description = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[10]/item')
class_var = pd.read_xml("../data/shipping/SC_Metadata.xml", '/SC/data/table[11]/item')

t = None
for i in range(1, 5):
    if t is None:
        t = pd.read_xml(f"data/shipping/SC_Masterdata{i}.xml", xpath="/SC/data/table/item")
    else:
        t = pd.concat([t, pd.read_xml(f"data/shipping/SC_Masterdata{i}.xml", xpath="/SC/data/table/item")])


t = t.merge(statuses[['STATUS', 'STATUS_DESC_ENG']], how='left', left_on='STATUS', right_on='STATUS')
t = t.merge(reference[['REF_PER_COLLECT', 'REF_PER_COLLECT_DESC_ENG', 'REF_PER_COLLECT_RMK_ENG']], how='left',
            left_on='REF_PER_COLLECT',
            right_on='REF_PER_COLLECT'
            )

t = t.merge(stat_display[['SD_VALUE', 'SD_DESC_ENG']], how='left', left_on='SD_VALUE', right_on='SD_VALUE')
t = t.merge(stat_var[['STAT_VAR', 'STAT_PRES', 'STAT_PRES_DESC_ENG', 'STAT_TYPE', 'STAT_UNIT',
                  'STAT_UNIT_DESC_ENG', 'STAT_MULTIPLIER', 'STAT_PRECISION']],
        how='left', left_on=['STAT_VAR', 'STAT_PRES'], right_on=['STAT_VAR', 'STAT_PRES'])

t = t.merge(stat_multiplier[['STAT_MULTIPLIER', 'STAT_MULTIPLIER_DESC_ENG']], how='left', left_on='STAT_MULTIPLIER', right_on='STAT_MULTIPLIER')
t = t.merge(stat_precision[['STAT_PRECISION', 'STAT_PRECISION_DESC_ENG']], how='left', left_on='STAT_PRECISION', right_on='STAT_PRECISION')
t = t.merge(stat_type[['STAT_TYPE', 'STAT_TYPE_DESC_ENG']], how='left', left_on='STAT_TYPE', right_on='STAT_TYPE')

# The description of the possible values for possible columns is given in a single table.
# Fun time to unnest.

# Remove some clutter
del class_var['CLASS_CODE_DESC_CHI']
del class_var['THEME']

# The master data mixes lower and upper case for the S_TYPE value.
# Additionally, the metadata calls it S_Type (same with some others)
# Fixing that here.
t.rename(columns={'S_TYPE': 'S_type', 'FLAG': 'Flag', 'MODE': 'Mode', 'SHIP_TYPE': 'Ship_type', 'V_TYPE': 'V_type'}, inplace=True)
t['S_type'] = t['S_type'].map(lambda x: x.lower() if x is not None else None)

class_ = 'S_type'
j = class_var[class_var['CLASS_VAR'] == class_].copy()
del j['CLASS_VAR']
j.rename(columns={'CLASS_CODE': class_, 'CLASS_CODE_DESC_ENG': f"{class_}_DESC"}, inplace=True)
j[class_] = j[class_].map(lambda x: x.lower())
t = t.merge(j, how='left', right_on='S_type', left_on='S_type')

for class_ in [c for c in class_var['CLASS_VAR'].unique() if c not in ['CCYY', 'S_type']]:  # There is an entire table that maps each years to itself. This is stupid and we're going to skip it.
    j = class_var[class_var['CLASS_VAR'] == class_].copy()
    del j['CLASS_VAR']
    j.rename(columns={'CLASS_CODE': class_, 'CLASS_CODE_DESC_ENG': f"{class_}_DESC"}, inplace=True)
    # Some fields are parsed as float in the master table, and as text in the metadata. They won't `merge`.
    if class_ in ['MM', 'Q', 'Ship_type', 'YTQ']:
        j[class_] = j[class_].astype(float)

    t = t.merge(j, how='left', left_on=class_, right_on=class_)


del t['THEME']

t.to_csv("data/clean/shipping.csv")