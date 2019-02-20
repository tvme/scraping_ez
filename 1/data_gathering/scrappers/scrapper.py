import time
import logging
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


logger = logging.getLogger(__name__)
TIMEOUT = 20  # selenium timeout for scraping
DRIVER_PATH = 'C:\\Users\\ersan\\PycharmProjects\\DS_OTUS\\1\\data_gathering\\scrappers\\chromedriver.exe'

class Scrapper(object):
    def __init__(self, skip_objects=None):
        self.skip_objects = skip_objects

    def scrap_process(self, url):
        '''
        :param url for scrapping:
        :return html text
        '''
        s = requests.Session()
        s.headers.update(
            {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'})
        response = s.get(url)


        if not response.ok:
            logger.error(response.text + 'status_code: ' + str(response.status_code))
            # then continue process, or retry, or fix your code
        else:
            return response.text


    def scrap_slenium(self, url_list, class_dict):
        '''
        :param  urls list for scrapping
                class_nm - dict of paths for locator, предполагается, что path единственный
        :return list of dict data texts, keys = class_dict kyes
        '''
        s = requests.Session()
        s.headers.update(
            {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'})

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        driver_active = False
        text_sources = []
        for url in url_list:
            response = s.get(url)
            if not response.ok:
                logger.error(url + '  status_code: ' + str(response.status_code))
                text_sources.append({p_loc: None for p_loc in class_dict.keys()})
                # then continue process, or retry, or fix your code
            else:
                if not driver_active:  # scraping begin
                    driver = webdriver.Chrome(executable_path=DRIVER_PATH, chrome_options=chrome_options)
                    driver_active = True
                t_start = time.time()
                driver.get(url)
                try:
                    text_sources.append({p_loc: WebDriverWait(driver, TIMEOUT).
                                                until(EC.element_to_be_clickable((By.CLASS_NAME, c_nm))).text
                                         for p_loc, c_nm in class_dict.items()})
                    # locator is a tuple of (by, path)
                    print('URL successfully load in {t_s} sec'.format(t_s=time.time() - t_start))
                except TimeoutException as e:
                    print("Page load Timeout Occured. Quiting !!!")
                finally:
                    if url == url_list[-1]:  # scraping end
                        driver.quit()
        return text_sources

