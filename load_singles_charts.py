from datetime import date
from time import sleep

from bs4 import BeautifulSoup
from retry import retry
import dateutil
import pandas as pd
import requests


BASE_URL = 'https://www.officialcharts.com'
BATCH_SIZE = 2000
TABLE_NAME = 'charts.singles'
PROJECT_ID = 'motida2'
TABLE_SCHEMA = [{'name': 'id', 'type': 'INTEGER'},
                {'name': 'from_date', 'type': 'DATE'},
                {'name': 'to_date', 'type': 'DATE'},
                {'name': 'position', 'type': 'INTEGER'},
                {'name': 'title', 'type': 'STRING'},
                {'name': 'artist', 'type': 'STRING'},
                {'name': 'label', 'type': 'STRING'}]


def main(first_chart_date_str):
    next_chart_url = '{}/charts/singles-chart/{}/7501/'.format(BASE_URL, first_chart_date_str)
    chart_id = 448
    batch_data = pd.DataFrame()
    while next_chart_url:
        data, next_chart_url = get_chart_data(chart_id, next_chart_url)
        batch_data = batch_data.append(data, ignore_index=True)
        if len(batch_data) >= BATCH_SIZE:
            batch_data.to_gbq(TABLE_NAME, project_id=PROJECT_ID, table_schema=TABLE_SCHEMA, if_exists='append')
            batch_data = pd.DataFrame()
            sleep(10)
        chart_id += 1
        sleep(10)
        #print(next_chart_url)
    if len(batch_data) > 0:
        batch_data.to_gbq(TABLE_NAME, project_id=PROJECT_ID, table_schema=TABLE_SCHEMA, if_exists='append')


@retry(tries=100, backoff=2)
def get_chart_data(chart_id, curr_chart_url):
    page = requests.get(curr_chart_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    next_chart_tag = soup.find("a", {"class": "next chart-date-directions"})
    if next_chart_tag:
        next_chart_url = '{}{}'.format(BASE_URL, next_chart_tag.attrs['href'])
    else:
        next_chart_url = None

    # Chart Week
    dt_element = soup.find("p", {"class": "article-date"})
    from_dt_str, to_dt_str = dt_element.get_text().strip().split('-')
    chart_start_date = dateutil.parser.parse(from_dt_str).date()
    chart_end_date = dateutil.parser.parse(to_dt_str).date()
    if chart_end_date == date(2001, 1, 1):
        print('Wrong date received')
        raise Exception('Date received indicates an error: {}')
    print(chart_id, chart_start_date, chart_end_date)
    # Chart Data
    positions = [x.text for x in soup.find_all("span", {"class": "position"})]
    titles = [list(x.children)[1].text for x in soup.findAll("div", {"class": "title"})]
    artists = [list(x.children)[1].text for x in soup.findAll("div", {"class": "artist"})]
    labels = [list(x.children)[0].text for x in soup.findAll("div", {"class": "label-cat"})]
    # Chart
    df = pd.DataFrame(zip(positions, titles, artists, labels), columns=['position', 'title', 'artist', 'label'])
    df['from_date'] = chart_start_date
    df['to_date'] = chart_end_date
    df['id'] = chart_id
    df = df[['id', 'from_date', 'to_date', 'position', 'title', 'artist', 'label']]

    return df, next_chart_url


if __name__ == '__main__':
    first_chart_date = date(1961, 6, 8)
    #first_chart_date = date(2019, 12, 13)
    first_chart_date_str = first_chart_date.strftime('%Y%m%d')

    main(first_chart_date_str)
