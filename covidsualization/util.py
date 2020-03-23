from numpy import array, isfinite, isnan, where
from pandas import concat, merge, Series, read_csv
from us import states

def get_fips_data():
    url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQn-7EE3BeHW1uJMuGBM83cGfHfWGLMMEX65l4FDlvGzoHhzHiadO_8zPOzuifwxj1xYjnL0qVaf2oX/pub?gid=1665765766&single=true&output=csv'
    fips_table = read_csv(url)

    return fips_table

def get_timeseries_data():
    url = 'https://coronadatascraper.com/timeseries-jhu.csv'
    timeseries_table = read_csv(url)

    state_abbrs = [ state.abbr for state in states.STATES ]

    def filter(row):
      county, state, country, lat, long = row[1:6]
      return (country == 'USA') and (state in state_abbrs) and (not isinstance(county, float)) and (not isnan([ lat, long ]).all())

    def get_timeseries(row):
      county, state = row[1:3]
      timeseries = array(row[9:]).astype(float)
      timeseries = where(isfinite(timeseries), timeseries, 0)
      return Series([ state, county, timeseries.astype(int) ], index=['State', 'County', 'Timeseries'])

    timeseries_table = timeseries_table.loc[timeseries_table.apply(filter, axis='columns')]
    timeseries_table = timeseries_table.apply(get_timeseries, axis='columns')

    return timeseries_table

def get_data():
    fips_table = get_fips_data()
    timeseries_table = get_timeseries_data()
    data_table = merge(fips_table, timeseries_table, on=['State', 'County'])

    return data_table

def restrict(table, scope):
    restricted_table = concat([table.loc[table['State'] == state] for state in scope])
    
    return restricted_table
