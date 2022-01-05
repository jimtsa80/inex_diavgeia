from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import bs4
from datetime import date
import time
import gspread
from pandas import DataFrame
from zipfile import ZipFile
import shutil
import os
from os.path import basename
import pathlib
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import webbrowser


class InexPrometheus:

    def __init__(self):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('secret_key/client_secret.json', self.scope)
        self.client = gspread.authorize(self.creds)
        self.query_url = "http://www.eprocurement.gov.gr/kimds2/unprotected/searchPayments.htm?execution=e3s1"

        # self.download_location = "C:\\Users\\jimtsa\\Desktop\\inex_diavgeia"
        # chromedriver_location = 'C:\\Users\\jimtsa\\.wdm\\drivers\\chromedriver\\win32\\89.0.4389.23\\chromedriver.exe'

        chrome_options = webdriver.ChromeOptions()

        chrome_options.add_experimental_option('prefs', {
            "download.default_directory": "C:\\Users\\jimtsa\\Desktop\\inex_diavgeia", #Change default directory for downloads
            "download.prompt_for_download": False, #To auto download the file
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True#It will not show PDF directly in chrome
            # "download_restrictions": 0,
            # "prompt_for_download": False,
            # "directory_upgrade": True,
            # 'safebrowsing.enabled': False,
            # 'safebrowsing.disable_download_protection': True,
            # "safebrowsing_for_trusted_sources_enabled": False
        })
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument('--verbose')
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # chrome_options.add_argument("--disable-web-security")
        # chrome_options.add_argument('--disable-gpu')
        # chrome_options.add_argument('--disable-software-rasterizer')
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

        # self.driver = webdriver.Chrome(chromedriver_location, options=chrome_options)
        # self.enable_download_in_headless_chrome(self.driver, self.download_location)
        self.today = date.today().strftime("%d/%m/%Y")
        # self.today = "04/01/2022"
        self.afm = "094356041"
        self.data = []


    def enable_download_in_headless_chrome(self, driver, download_dir):
        """
        there is currently a "feature" in chrome where
        headless does not allow file download: https://bugs.chromium.org/p/chromium/issues/detail?id=696481
        This method is a hacky work-around until the official chromedriver support for this.
        Requires chrome version 62.0.3196.0 or above.
        """

        # add missing support for chrome "send_command"  to selenium webdriver
        self.driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')

        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
        command_result = driver.execute("send_command", params)
        print("response from browser:")
        for key in command_result:
            print("result:" + key + ":" + str(command_result[key]))

    def upload_zip_files(self):

        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        gdrive = GoogleDrive(gauth)

        for folderName, subfolders, filenames in os.walk('.'):
            for filename in filenames:

                if filename.endswith('.zip'):
                    gfile = gdrive.CreateFile({'title': filename, "parents":  [{"id": "1inCj1Iiv22SpbLk8v1atFF7183HwEpGf"}]})
                    gfile.SetContentFile(filename)
                    gfile.Upload()

    def zip_pdfs(self):

        fn_mtime = {}
        counter = 0

        for folderName, subfolders, filenames in os.walk('.'):
            for filename in filenames:
                if filename.endswith('.pdf'):
                    fname = pathlib.Path(filename)
                    fn_mtime[filename] = fname.stat().st_mtime

        sorted_fn_mtime = {k: v for k, v in sorted(fn_mtime.items(), key=lambda item: item[1])}

        for filename, mtime in sorted_fn_mtime.items():

            filepath = os.path.join(os.getcwd(), filename)
            os.rename(filepath, os.path.join(os.getcwd(), self.data[counter][2]+'_'+date.today().strftime("%Y%m%d")+'_'+self.data[counter][1]+'.pdf'))
            counter += 1

        with ZipFile(date.today().strftime("%Y%m%d")+'.zip', 'w') as zip:
            for folderName, subfolders, filenames in os.walk('.'):
                for filename in filenames:
                    if filename.endswith('.pdf'):
                        filepath = os.path.join(folderName, filename)
                        zip.write(filepath, basename(filepath))

    def get_query(self):

        self.driver.get(self.query_url)
        self.driver.find_element_by_name("pageForm:dateFromInputDate").send_keys(self.today)
        self.driver.find_element_by_name("pageForm:dateToInputDate").send_keys(self.today)
        self.driver.find_element_by_name("pageForm:j_id247").send_keys(self.afm)
        self.driver.find_element_by_name("pageForm:j_id255").click()

        time.sleep(20)

    def get_results_for_gs(self):

        self.get_query()

        data = self.driver.page_source
        soup = bs4.BeautifulSoup(data, 'lxml')

        for node in soup.findAll('td', class_="rich-table-cell"):
            id = node.text.split("Στοιχεία Εντολής")[0].split('21PAY')[0].strip()
            pay = '21PAY'+node.text.split("Στοιχεία Εντολής")[0].split('21PAY')[1].strip()
            foreas = node.text.split("Φορέας")[1].replace("/","").strip()
            price = node.text.split("ΦΠΑ")[1].split("Φορέας")[0].strip()
            dt = node.text.split("Αναρτήθηκε")[1].split("Τελευταία")[0].strip()
            self.data.append([id, pay, foreas, price, dt])
            # print(id, pay, foreas, price, dt)
        return self.data

    def get_pdf(self):

        self.get_query()

        WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.ID, 'pageForm:j_id44')))

        for i in range(int(len(self.get_results_for_gs()) / 2)):
            time.sleep(20)
            self.driver.find_element_by_id('pageForm:j_id290:'+str(i)+':j_id306').click()
            time.sleep(20)
            self.driver.find_element_by_link_text('Κατεβάστε το αρχείο').click()
            time.sleep(10)
            self.driver.back()

        self.driver.close()
        self.zip_pdfs()

    def write_to_gs(self):

        prom_tracker_current = self.client.open('inex_diavgeia').worksheet('current_prom')
        prom_tracker_history = self.client.open('inex_diavgeia').worksheet('history_prom')

        today_tracker = DataFrame(self.data, columns=['id', 'pay', 'foreas', 'price', 'dt'])

        today_df = get_as_dataframe(prom_tracker_current)
        today_df = today_df.dropna(how='all')
        today_df = today_df[["id",	"pay", "foreas", "price", "dt"]]

        history_df = get_as_dataframe(prom_tracker_history)
        history_df = history_df.dropna(how='all')
        history_df = history_df[["id",	"pay", "foreas", "price", "dt"]]

        from_current_to_history = history_df.append(today_df)
        from_current_to_history = from_current_to_history.sort_values('dt').drop_duplicates('pay')
        from_current_to_history = from_current_to_history.sort_values(by=['dt'], ascending=False)

        prom_tracker_current.delete_rows(2, len(today_df) + 1)
        set_with_dataframe(prom_tracker_history, from_current_to_history)

        # today_tracker = today_tracker.sort_values('dt').drop_duplicates('id')
        today_tracker = today_tracker.sort_values(by=['dt'], ascending=False)
        set_with_dataframe(prom_tracker_current, today_tracker)

        print("{} new entries".format(len(today_tracker)))


        # elems = self.driver.find_elements_by_link_text("Στοιχεία Εντολής")
        # for elem in elems:
        #     print(elem.text)

        # i = 0
        # while True:
        #     self.driver.find_element_by_name("pageForm:j_id290:{}:j_id306".format(i)).click()
        #     i += 1
        # data = self.driver.page_source
        # soup = bs4.BeautifulSoup(data)
        # for node in soup.findAll('a'):
        #     print(node['href'])

# if __name__ == '__main__':
#
#     inexScrapper = InexPrometheus()
#     inexScrapper.get_query()
#     inexScrapper.write_to_gs()


