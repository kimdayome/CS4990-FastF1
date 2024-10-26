import fastf1
import pandas as pd
import csv
import argparse


def get_dataset(filename, rows=None):
    # get the event schedule for the 2024 F1 season
    output_data = [] # store race info

    # 반복문 추가 // throw another loop to extract 2020-2024
    for year in [2020,2021,2022,2023,2024]:
        events = fastf1.get_event_schedule(year) # by the year instead of 2024
        #previousEvents = fastf1.ergast.fetch_season(year)

        for evnt in events['OfficialEventName']:

            race_name = evnt
            # found directly from API to extra event data by name
            race_event = events.get_event_by_name(evnt)
            race_country = race_event['Country']
            race_loc = race_event['Location']
            race_format = race_event['EventFormat']
            race_date = race_event['EventDate']

            # skip test events to only aggregate real race data
            is_test_event = race_event.is_testing()
            if is_test_event:
                continue

            #load up data for race stat
            race_stats = race_event.get_race()
            race_stats.load(laps=False, telemetry=False, messages=False)
            # this is the race start TIME (different from event date, when race started)
            race_ts = race_stats.date

            # get the results for all drivers
            race_results = race_stats.results

            # race results for EACH driver
            for idx, driver_info in race_results.iterrows():
                race_info = {} # dictionary 

                # set race information into race_info
                race_info['Race Name'] = race_name
                race_info['Race Location'] = "{},{}".format(race_loc, race_country)
                race_info['Race Date'] = race_date
                race_info['Race Format'] = race_format

                # add race start timestamp
                race_info['Race Start Time'] = race_ts

                # add results and per driver info
                driver_number = driver_info['DriverNumber']
                driver_name = driver_info['DriverId']
                driver_team = driver_info['TeamName']
                driver_nationality = driver_info['CountryCode']
                driver_race_pos = driver_info['Position']
                driver_race_time = driver_info['Time']
                driver_race_points = driver_info['Points']
                driver_grid_pos = driver_info['GridPosition']

                # add driver data to the dictionary
                race_info['Driver ID'] = driver_number
                race_info['Driver Name'] = driver_name
                race_info['Driver Number and Race Name'] = "{} : {}".format(driver_number, race_name)
                race_info['Driver Team'] = driver_team
                race_info['Driver Country'] = driver_nationality
                race_info['Position'] = driver_race_pos
                race_info['Race Time'] = driver_race_time
                race_info['Race Point'] = driver_race_points
                race_info['Race Grid Position'] = driver_grid_pos # what number they were at when starting
                print("processing racer {} for {}".format(driver_name, race_name))

                # Add final race_info data into output_data
                output_data.append(race_info)
                # if rows EXISTS (is not None) and matches number of rows, exit.
                if rows and len(output_data) == rows:

                    _write_csv(output_data, filename)
                    return # EXIT IF the function reaches the specific number of rows
    _write_csv(output_data, filename) # write data to csv after all races


# csv file
def _write_csv(output_data, filename):
    fieldnames = output_data[0].keys()
    # Open the file in write mode
    with open(filename, mode='w', newline='') as file:
        # Create a DictWriter object
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        # Write the header
        writer.writeheader()
        # Write the rows
        writer.writerows(output_data)


# entry point

if __name__ == '__main__':
    # set up the argparse to handel command line arguments
    aparser = argparse.ArgumentParser(
        description='Generate csv file for 2024 F1 season results')

    # filename handling, default filename will be f1_2024.csv
    aparser.add_argument(
        '--filename',
        default='f1_2024.csv',
        required=False,
        help='filename to produce')

    # rows handling
    aparser.add_argument(
        '--rows',
        default=None,
        type=int,
        required=False,
        help='only generate data for number of rows')
    args = aparser.parse_args()
    get_dataset(args.filename, rows=args.rows)
    

# How to run the program

# pip or pip3 install pandas
# pip or pip3 install fastf1  
# python3 data_collect.py --filename yourfilename.csv --rows 10(or any number) (Also can be executed without filename or rows too)