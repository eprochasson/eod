"""Rebuild the shipping data from that awful XML datasets."""
import pandas as pd
pd.options.display.max_rows = 999

# Metadata
main = pd.read_xml("data/shipping/SC_Metadata.xml", '/SC/data/table[1]/item')
schema = pd.read_xml("data/shipping/SC_Metadata.xml", '/SC/data/table[2]/item')
statuses = pd.read_xml("data/shipping/SC_Metadata.xml", '/SC/data/table[3]/item')
reference = pd.read_xml("data/shipping/SC_Metadata.xml", '/SC/data/table[4]/item')
stat_display = pd.read_xml("data/shipping/SC_Metadata.xml", '/SC/data/table[5]/item')
stat_type = pd.read_xml("data/shipping/SC_Metadata.xml", '/SC/data/table[8]/item')
stat_var = pd.read_xml("data/shipping/SC_Metadata.xml", '/SC/data/table[9]/item')
class_var = pd.read_xml("data/shipping/SC_Metadata.xml", '/SC/data/table[11]/item')

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

