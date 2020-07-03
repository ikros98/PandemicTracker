from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup

url = 'https://www.google.com/covid19/mobility/'
u = urlopen(url)

try:
    html = u.read().decode('utf-8')
finally:
    u.close()

soup = BeautifulSoup(html, features="html.parser")

link = soup.select_one('.icon-link')
href = link.get('href')

try:
    urlretrieve(href, "GoogleMobilityTrends.csv")
except:
    print('failed to download')
