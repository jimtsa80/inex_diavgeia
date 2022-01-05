from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import date
import gspread
from pandas import DataFrame
import bs4
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
from pathlib import Path
from itertools import chain


class InexGenn:

    def __init__(self):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('secret_key/client_secret.json', self.scope)
        self.client = gspread.authorize(self.creds)
        self.query_url = "https://promitheftes.gennimatas-thess.gr/#/Web.Platform/LoginForm"
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        self.today = date.today().strftime("%d/%m/%Y")
        # self.today = "26/7/2021"
        self.old = "1/1/2022"
        self.afm = "094356041inex"
        self.username = "accounting@inexmedical.gr"
        self.data = []
        self.data_expand = []
        self.comb_data = []
        self.current = []

    def upload_xlsx_files(self):

        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        gdrive = GoogleDrive(gauth)

        fileList = gdrive.ListFile({'q': "'1inCj1Iiv22SpbLk8v1atFF7183HwEpGf' in parents and trashed=false"}).GetList()

        for file in fileList:

            if file['title'].startswith("ΓΕΝΝΗΜΑΤΑ"):
                print('Title: %s, ID: %s' % (file['title'], file['id']))
                fileID = file['id']

                file2 = gdrive.CreateFile({'id': fileID})
                file2.Delete()

        for folderName, subfolders, filenames in os.walk(os.path.join(Path.home(), 'Downloads')):
            for filename in filenames:

                if filename.endswith('.xlsx'):
                    new_filename = os.path.join('ΓΕΝΝΗΜΑΤΑΣ_'+os.path.splitext(filename)[0]+"_εως_"+date.today().strftime("%d_%m_%y")+".xlsx")
                    os.rename(os.path.join(Path.home(), 'Downloads', filename), new_filename)

                    gfile = gdrive.CreateFile({'title': new_filename, "parents":  [{"id": "1inCj1Iiv22SpbLk8v1atFF7183HwEpGf"}]})
                    gfile.SetContentFile(new_filename)
                    gfile.Upload()

    def get_query(self):

        self.driver.get(self.query_url)
        time.sleep(10)
        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//button[@class='btn btn-default'][text()='Αποδοχή']"))).click()
        self.driver.find_element_by_id("edtLoginName").send_keys(self.username)
        self.driver.find_element_by_id("edtLoginPassword").send_keys(self.afm)
        self.driver.find_element_by_id("btnLogin").click()

        time.sleep(5)
        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//span[@class='ctaf-menu-item'][text()='Εντάλματα']"))).click()

        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//input[@data-bind='value: dateFrom']"))).send_keys(self.old)
        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//input[@data-bind='value: dateTo']"))).send_keys(self.today)
        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//button[@class='btn btn-primary search'][text()=' Εμφάνιση']"))).click()

        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//button[@class='btn btn-default excel'][text()=' Εξαγωγή σε Αρχείο']"))).click()

        time.sleep(10)
        data = self.driver.page_source

        soup = bs4.BeautifulSoup(data, 'lxml')

        results = []
        for node in soup.findAll('td', _class=""):
            for key, value in node.attrs.items():
                if value == "AANUM":
                    id = node.text
                    results.append(id)
                if value == "AXIA":
                    axia = node.text
                    results.append(axia)
                if value == "KRATISEIS":
                    kratiseis = node.text
                    results.append(kratiseis)
                if value == "FOROS":
                    foros = node.text
                    results.append(foros)
                if value == "PLHROTEO":
                    pliroteo = node.text
                    results.append(pliroteo)
                if value == "HMER_PLIROMIS":
                    dt = node.text
                    results.append(dt)

        self.data = [results[x:x+6] for x in range(0, len(results), 6)]

        for i in self.data:
            if i[5] == self.today:
                self.current.append(i)

        time.sleep(10)

        rows = self.driver.find_elements_by_xpath("//a[@class='k-icon k-i-expand']")
        counter = 2
        for row in rows:
            row.click()
            time.sleep(10)

            table = row.find_element_by_xpath('//*[@id="grdEntalmata"]/div[2]/table/tbody/tr[{}]/td[2]/div[2]/div[2]/table/tbody'.format(counter))

            self.data_expand.append(table.text.split('\n'))
            counter += 2

        a_list = []
        for d, de in zip(self.data, self.data_expand):
            a_list.append([d, de])
            d.extend([de])
            self.comb_data.append(d)

        return a_list

    def write_to_gs(self):

        prom_tracker_current = self.client.open('inex_diavgeia').worksheet('current_genn')
        prom_tracker_history = self.client.open('inex_diavgeia').worksheet('history_genn')

        today_tracker = DataFrame(self.comb_data, columns=['id', 'axia', 'kratiseis', 'foros', 'pliroteo', 'dt', 'expand'])
        today_tracker['id'] = today_tracker['id'].astype(int)

        today_df = get_as_dataframe(prom_tracker_current)
        today_df = today_df.dropna(how='all')
        today_df = today_df[['id', 'axia', 'kratiseis', 'foros', 'pliroteo', 'dt', 'expand']]

        history_df = get_as_dataframe(prom_tracker_history)
        history_df = history_df.dropna(how='all')
        history_df = history_df[['id', 'axia', 'kratiseis', 'foros', 'pliroteo', 'dt', 'expand']]

        from_current_to_history = history_df.append(today_df)
        from_current_to_history = from_current_to_history.sort_values('dt').drop_duplicates('id')
        from_current_to_history = from_current_to_history.sort_values(by=['dt'], ascending=False)

        prom_tracker_current.delete_rows(2, len(today_df) + 1)
        set_with_dataframe(prom_tracker_history, from_current_to_history)

        # today_tracker = today_tracker.sort_values('dt').drop_duplicates('id')
        today_tracker = today_tracker.sort_values(by=['id'], ascending=True)
        set_with_dataframe(prom_tracker_current, today_tracker)


# if __name__ == '__main__':
#
#     inexScrapper = InexScrapper()
#     inexScrapper.get_query()
#     inexScrapper.write_to_gs()

    # if nothing_found == len(keywords):
    #     print("Nothing found today!")
    #     inexScrapper.clean_folder()
    # else:
    #     inexScrapper.write_to_gs()
    #     inexScrapper.clean_folder()

    # promScrapper = InexPrometheus()
    # promScrapper.get_query()
    # promScrapper.write_to_gs()
