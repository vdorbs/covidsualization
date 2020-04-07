from pandas import concat, DataFrame, merge, Series, read_csv

ALL_COUNTIES = slice(None)

def select_by_dict(table, state_dict):
    return concat([table.loc[(state, counties), :] for state, counties in state_dict.items()])

def select_type(table, type):
    return table.loc[(slice(None), slice(None), type), :].droplevel(-1)

def get_data():
    nyt_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    census_url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv'

    nyt_data = read_csv(nyt_url)
    census_data = read_csv(census_url, encoding='latin-1')

    # Labels
    STATE = 'State'
    COUNTY = 'County'
    FIPS = 'FIPS'
    TYPE = 'Type'
    CASES = 'Cases'
    DEATHS = 'Deaths'
    DATE = 'Date'
    POPULATION = 'Population'

    # Rename columns
    nyt_data = nyt_data.rename(columns={'state':STATE, 'county':COUNTY, 'fips':FIPS, 'cases':CASES, 'deaths':DEATHS, 'date':DATE})

    # Assign NYC data to NYC FIPS
    nyc_idx = nyt_data[COUNTY] == 'New York City'
    nyt_data.loc[nyc_idx, [COUNTY, FIPS]] = DataFrame({COUNTY:'New York', FIPS:36000}, index=nyt_data[nyc_idx].index)

    nyt_data = nyt_data.dropna()                                                # Drop rows missing county data

    nyt_data[FIPS] = nyt_data[FIPS].apply(lambda fips: str(int(fips)).zfill(5)) # Convert FIPS floats to strings

    timeseries_data = (nyt_data[[STATE, COUNTY, DATE, CASES, DEATHS]]           # Select all columns except FIPS
      .set_index([STATE, COUNTY, DATE]).sort_index()                            # Remap (STATE, COUNTY, DATE) -> (CASES, DEATHS)
      .stack().to_frame())                                                      # Stack CASES, DEATHS in same column

    timeseries_data = timeseries_data[~timeseries_data.index.duplicated()]      # Drop duplicated New York, New York, 2020-04-06 row

    timeseries_data = (timeseries_data.unstack(DATE)                            # Remap (STATE, COUNTY, TYPE) -> (DATE, ..., DATE)
      .fillna(0)                                                                # Replace NaN with 0
      .astype(int))                                                             # Convert floats to integers

    timeseries_data.columns = timeseries_data.columns.droplevel()               # Flatten column multiindex

    timeseries_data.index.names = [STATE, COUNTY, TYPE]                         # Rename row multiindex

    def census_process(row):
      state, county, pop = row
      fips = str(state).zfill(2) + str(county).zfill(3)
      return Series([fips, pop], index=[FIPS, POPULATION])

    pop_data = (census_data[['STATE', 'COUNTY', 'POPESTIMATE2019']]             # Select STATE, COUNTY, POPULATION columns
      .apply(census_process, axis='columns'))                                   # Convert FIPS integers to strings

    fips_data = (nyt_data[[STATE, COUNTY, FIPS]]                                # Select STATE, COUNTY, FIPS columns
      .drop_duplicates())                                                       # Remove duplicate rows

    county_data = (merge(fips_data, pop_data, on=FIPS)                          # Join on FIPS column
      .set_index([STATE, COUNTY]).sort_index())                                 # Remap (STATE, COUNTY) -> (FIPS, POPULATION)

    county_data = county_data.loc[county_data['FIPS'] != '36061']               # Drop New York county row

    return timeseries_data, county_data
