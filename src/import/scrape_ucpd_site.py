import pandas as pd 
import requests
from bs4 import BeautifulSoup
import sys

TRAFFIC_URL = 'https://incidentreports.uchicago.edu/trafficStopsArchive.php?startDate=1451628000&endDate=1597899600&offset='
TRAFFIC_OUTPATH = "../../data/raw/ucpd_traffic_stops.csv"
INTERVIEWS_URL = 'https://incidentreports.uchicago.edu/fieldInterviewsArchive.php?startDate=1451628000&endDate=1597899600&offset='
INTERVIEWS_OUTPATH = "../../data/raw/ucpd_interviews.csv"

def download_traffic_stops():
    traffic_stops = []
    # Range manually listed from UCPD site (5 complaints listed per page)
    for i in range(0, 4720, 5):
        if i % 100 == 0:
            print("Parsing index", i, "of 4715")
        scrape = TRAFFIC_URL + str(i)
        r = requests.get(scrape)
        soup = BeautifulSoup(r.text, 'html.parser')
        rows = soup.find('tbody').find_all('tr')
        # Only include entries with more than 3 elements (two are <tr> and </tr>,
        # so if an entry has 3 elements, it only has one <td> element noting that
        # there were no stops that day)
        data = list(filter(lambda x: len(x) > 3, rows))
        parsed_data = [[x.text.strip() for x in y.find_all('td')] for y in data]
        traffic_stops.extend(parsed_data)

    traffic_stops_df = pd.DataFrame(
        traffic_stops, columns=['datetime', 'location', 'race', 'gender', 'idot',
                                'reason', 'citation', 'disposition', 'search'])

    return traffic_stops_df


def download_interviews():
    interviews = []
    for i in range(0, 1520, 5):
        if i % 100 == 0:
            print("Parsing index", i, "of 1520")
        scrape = INTERVIEWS_URL + str(i)
        r = requests.get(scrape)
        soup = BeautifulSoup(r.text, 'html.parser')
        rows = soup.find('tbody').find_all('tr')
        data = list(filter(lambda x: len(x) > 3, rows))
        parsed_data = [[x.text.strip() for x in y.find_all('td')] for y in data]
        interviews.extend(parsed_data)

    interviews_df = pd.DataFrame(
        interviews, columns=['datetime', 'location', 'initiated_by', 'race', 'gender',
                             'reason', 'disposition', 'search'])

    return interviews_df


def go(args):
    usage = "usage: python3 scrape_ucpd.py"
    if len(args) != 1:
        print(usage)
        sys.exit(1)

    traffic_stops = download_traffic_stops()
    interviews = download_interviews()

    traffic_stops.to_csv(TRAFFIC_OUTPATH, index=False)
    interviews.to_csv(INTERVIEWS_OUTPATH, index=False)


if __name__ == "__main__":
    go(sys.argv)