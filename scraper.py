import re
import json
import datetime
import requests
import html
import psycopg2  # 导入 psycopg2 库进行PostgreSQL数据库操作

# 尝试导入zoneinfo，如果失败则从backports导入
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from db import get_db_conn

URL = 'https://visitseattle.org/events/page/'
URL_LIST_FILE = './data/links.json'
URL_DETAIL_FILE = './data/data.json'


def get_location(query):
    """Get latitude and longitude for a query using Nominatim API."""
    base_url = "https://nominatim.openstreetmap.org/search"
    params = {'q': query, 'format': 'jsonv2'}
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data:
            return data[0]['lat'], data[0]['lon']
    return None, None

def list_links():
    res = requests.get(URL + '1/')
    last_page_no = int(re.findall(r'bpn-last-page-link"><a href=".+?/page/(\d+?)/.+" title="Navigate to last page">', res.text)[0])

    links = []
    for page_no in range(1, last_page_no + 1):
        res = requests.get(URL + str(page_no) + '/')
        links.extend(re.findall(r'<h3 class="event-title"><a href="(https://visitseattle.org/events/.+?/)" title=".+?">.+?</a></h3>', res.text))

    json.dump(links, open(URL_LIST_FILE, 'w'))

def get_detail_page():
    links = json.load(open(URL_LIST_FILE, 'r'))
    data = []
    for link in links:
        try:
            row = {}
            res = requests.get(link)
            row['title'] = html.unescape(re.findall(r'<h1 class="page-title" itemprop="headline">(.+?)</h1>', res.text)[0])
            datetime_venue = re.findall(r'<h4><span>.*?(\d{1,2}/\d{1,2}/\d{4})</span> \| <span>(.+?)</span></h4>', res.text)[0]
            row['date'] = datetime.datetime.strptime(datetime_venue[0], '%m/%d/%Y').replace(tzinfo=ZoneInfo('America/Los_Angeles')).isoformat()
            row['venue'] = datetime_venue[1].strip()
            metas = re.findall(r'<a href=".+?" class="button big medium black category">(.+?)</a>', res.text)
            row['category'] = html.unescape(metas[0])
            row['location'] = metas[1]
            lat, lon = get_location(row['location'] + ", Seattle")
            if lat and lon:
                row['geolocation'] = f"{lon}, {lat}"  # 以"经度, 纬度"的格式保存经纬度信息
            data.append(row)
        except IndexError as e:
            print(f'Error: {e}')
            print(f'Link: {link}')
    json.dump(data, open(URL_DETAIL_FILE, 'w'), indent=4)

def insert_to_pg():
    data = json.load(open(URL_DETAIL_FILE, 'r'))
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            title TEXT,
            date TIMESTAMP WITH TIME ZONE,
            venue TEXT,
            category TEXT,
            location TEXT,
            geolocation TEXT
        );
    ''')
    # try:
    #    cur.execute('ALTER TABLE events ADD COLUMN geolocation TEXT;')
    #    conn.commit()  # 应用更改
    #except Exception as e:
    #    print(f"An error occurred: {e}")
    #    conn.rollback()  # 其他错误，撤销操作

    urls = json.load(open(URL_LIST_FILE, 'r'))
    data = json.load(open(URL_DETAIL_FILE, 'r'))
    for url, row in zip(urls, data):
        q = '''
            INSERT INTO events (url, title, date, venue, category, location, geolocation)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE 
            SET geolocation = EXCLUDED.geolocation;
    '''
        cur.execute(q, (url, row['title'], row['date'], row['venue'], row['category'], row['location'], row.get('geolocation')))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    list_links()
    get_detail_page()
    insert_to_pg()
