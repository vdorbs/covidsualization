from pandas import Series, read_excel
from us import states

url = 'https://www2.census.gov/programs-surveys/popest/geographies/2016/all-geocodes-v2016.xlsx'
fips_table = read_excel(url, header=4)

state_label = 'State Code (FIPS)'
county_label = 'County Code (FIPS)'
area_label = 'Area Name (including legal/statistical area description)'

state_code_to_abbr = { int(state.fips):state.abbr for state in states.STATES }

fips_table = fips_table[[state_label, county_label, area_label]]
fips_table = fips_table.loc[fips_table[county_label] != 0]
fips_table = fips_table.loc[fips_table[state_label].apply(lambda state_code: state_code in state_code_to_abbr)]

def process(row):
    state_code, county_code, area_name = row
    abbr = state_code_to_abbr[state_code]
    fips = str(state_code * 1000 + county_code).zfill(5)
    return Series([ abbr, area_name, fips ], index=[ 'State', 'County', 'FIPS' ])

fips_table = fips_table.apply(process, axis='columns')

fips_table.to_csv('fips.csv', index=False)
