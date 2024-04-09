import urllib3
import json
import pandas as pd
import datetime

class WundergroundFetcher:
    # you need a config.json file that contains your api_key
    api_key = json.load(open("config.json"))["api_key"]

    params = ["obsTimeLocal", "obsTimeUtc", "humidityAvg", "solarRadiationHigh"]
    metricparams = ["tempAvg", "windspeedAvg", "dewptAvg", "pressureMax", "precipRate"]


    def FetchHistoricalDay(date: datetime.date, station_id: str = "IBERLI673"):
        '''Fetches one day of historical weather data (hourly data points) from the wunderground api'''

        datestring = str(date).replace("-", "")

        url = f"https://api.weather.com/v2/pws/history/hourly?stationId={station_id}&format=json&units=m&date={datestring}&apiKey={WundergroundFetcher.api_key}"
        
        f = urllib3.request('GET', url)

        return json.loads(f.data)


    def FetchHistoricalPeriod(start_date: datetime.date, end_date: datetime.date, station_id: str = "IBERLI673") -> pd.DataFrame:
        '''Fetches the Historical Weather data between start_date and end_data and returns a pandas dataframe'''

        current_date = start_date

        # the entries of this list will be dataframe rows
        df_data = []


        while (current_date < end_date):
            print(f'Fetching data for {current_date}')

            # get the data from wunderground
            data = WundergroundFetcher.FetchHistoricalDay(current_date, station_id)

            for observation in data["observations"]:
                obs_data = {} # obs_data is one row in the data frame
                
                for param in WundergroundFetcher.params:
                    obs_data[param] = observation[param]
                for metricparam in WundergroundFetcher.metricparams:
                    obs_data[metricparam] = observation["metric"][metricparam]
                
                df_data.append(obs_data)


            current_date = current_date + datetime.timedelta(days= 1)
        
        
        df = pd.DataFrame(df_data)   
        print( df.head)
        return df




#########################################################
if __name__ == "__main__":
    # Variables setup
    start = datetime.date(2023, 1, 1)
    end = datetime.date(2023, 1, 2)

    #"IBADDR78" Bad Dürrheim
    #"IVILLI15" Schwenningen
    station_id = "IBADDR78" 

    # where should the csv be saved?
    downloads_path = "downloaded_csvs/"
    filename = f"{station_id}_{start}_to_{end}.csv"

    # Fetch the data
    weather = WundergroundFetcher.FetchHistoricalPeriod(start, end, station_id)

    # save the CSV file
    weather.to_csv(downloads_path + filename)