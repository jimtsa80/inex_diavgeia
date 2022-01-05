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


class InexDrama:

    def __init__(self):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('secret_key/client_secret.json', self.scope)
        self.client = gspread.authorize(self.creds)
        self.query_url = "http://84.205.238.203:8888/login"
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        self.today = date.today().strftime("%d/%m/%Y")
        # self.today = "26/7/2021"
        self.old = "1/1/2022"
        self.afm = "094356041"
        self.username = "INEX"

    def upload_xlsx_files(self):

        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        gdrive = GoogleDrive(gauth)

        fileList = gdrive.ListFile({'q': "'1inCj1Iiv22SpbLk8v1atFF7183HwEpGf' in parents and trashed=false"}).GetList()

        for file in fileList:

            if file['title'].startswith("ΔΡΑΜΑ"):
                print('Title: %s, ID: %s' % (file['title'], file['id']))
                fileID = file['id']

                file2 = gdrive.CreateFile({'id': fileID})
                file2.Delete()

        for folderName, subfolders, filenames in os.walk(os.path.join(Path.home(), 'Downloads')):
            for filename in filenames:

                if filename.endswith('.xlsx'):
                    new_filename = os.path.join('ΔΡΑΜΑ_'+os.path.splitext(filename)[0]+"_εως_"+date.today().strftime("%d_%m_%y")+".xlsx")
                    os.rename(os.path.join(Path.home(), 'Downloads', filename), new_filename)

                    gfile = gdrive.CreateFile({'title': new_filename, "parents":  [{"id": "1inCj1Iiv22SpbLk8v1atFF7183HwEpGf"}]})
                    gfile.SetContentFile(new_filename)
                    gfile.Upload()

    def get_query(self):

        self.driver.get(self.query_url)
        self.driver.find_element_by_name("username").send_keys(self.username)
        self.driver.find_element_by_name("password").send_keys(self.afm)
        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//span[@class='bigger-110'][text()='Είσοδος']"))).click()

        self.driver.find_element_by_name("start_date").send_keys(self.old)
        self.driver.find_element_by_name("end_date").send_keys(self.today)
        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//button[@class='btn btn-primary btn-sm pull-right'][text()='Αναζήτηση']"))).click()

        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'dt-buttons')))

        WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH,"//a[@class='dt-button buttons-excel buttons-html5']"))).click()

    def write_to_gs(self):

        prom_tracker_current = self.client.open('inex_diavgeia').worksheet('current_pap')
        prom_tracker_history = self.client.open('inex_diavgeia').worksheet('history_pap')

        today_tracker = DataFrame(self.data, columns=['id', 'axia', 'kratiseis', 'foros', 'pliroteo', 'dt'])
        today_tracker['id'] = today_tracker['id'].astype(int)

        today_df = get_as_dataframe(prom_tracker_current)
        today_df = today_df.dropna(how='all')
        today_df = today_df[['id', 'axia', 'kratiseis', 'foros', 'pliroteo', 'dt']]

        history_df = get_as_dataframe(prom_tracker_history)
        history_df = history_df.dropna(how='all')
        history_df = history_df[['id', 'axia', 'kratiseis', 'foros', 'pliroteo', 'dt']]

        from_current_to_history = history_df.append(today_df)
        from_current_to_history = from_current_to_history.sort_values('dt').drop_duplicates('id')
        from_current_to_history = from_current_to_history.sort_values(by=['dt'], ascending=False)

        prom_tracker_current.delete_rows(2, len(today_df) + 1)
        set_with_dataframe(prom_tracker_history, from_current_to_history)

        # today_tracker = today_tracker.sort_values('dt').drop_duplicates('id')
        today_tracker = today_tracker.sort_values(by=['id'], ascending=True)
        set_with_dataframe(prom_tracker_current, today_tracker)