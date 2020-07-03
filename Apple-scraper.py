from urllib.request import urlretrieve
from selenium import webdriver

op = webdriver.ChromeOptions()
op.add_argument('headless')
driver = webdriver.Chrome('/usr/lib/chromium-browser/chromedriver', options=op) 

driver.get('https://www.apple.com/covid19/mobility')

driver.implicitly_wait(5)
link = driver.find_element_by_xpath('//*[@id="download-card"]/div[2]/a')
href = link.get_attribute('href')

try:
    urlretrieve(href, "AppleMobilityTrends.csv")
except:
    print('failed to download')

driver.quit()