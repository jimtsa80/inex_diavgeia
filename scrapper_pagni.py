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


class InexPagni:

    def __init__(self):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('secret_key/client_secret.json', self.scope)
        self.client = gspread.authorize(self.creds)
        self.query_url = "https://www.pagni.gr/index.php/prom-eservices/timprom"
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        self.today = date.today().strftime("%d/%m/%Y")
        # self.today = "26/7/2021"
        self.old = "1/1/2022"
        self.afm = "944U612J"
        self.username = "094356041"
        self.data = []


    def get_query(self):

        self.driver.get(self.query_url)

        self.driver.find_element_by_xpath("//input[@name='afm']").send_keys(self.username)
        self.driver.find_element_by_xpath("//input[@name='kvdikos']").send_keys(self.afm)
        self.driver.find_element_by_xpath("//input[@type='submit']").click()

        time.sleep(5)

        self.driver.find_element_by_xpath('//*[@id="form1"]/table/tbody/tr[2]/td[3]/input').click()
        self.driver.find_element_by_xpath('//*[@id="form1"]/p[7]/input').click()

        time.sleep(5)

        table = self.driver.find_element_by_xpath('//*[@id="sp-component"]/div/font/font/div[1]/div[2]/table')

        rows = table.find_element_by_tag_name('tr').text
        results = rows.split("\n")[2:]

        self.data = [i.split(" ") for i in results]

    def write_to_gs(self):

        prom_tracker_current = self.client.open('inex_diavgeia').worksheet('current_pagni')
        prom_tracker_history = self.client.open('inex_diavgeia').worksheet('history_pagni')

        today_tracker = DataFrame(self.data, columns=['Αρ.Τιμ.', 'Ημ.Έκδ.', 'Α/Α Χ.Ε.(*)', 'Ημ. Εξόφλ.', 'Ολ. Ποσό', 'Κρ/σεις', 'Κατ/ση'])
        # today_tracker['Αρ.Τιμ'] = today_tracker['Αρ.Τιμ'].astype(int)

        today_df = get_as_dataframe(prom_tracker_current)
        today_df = today_df.dropna(how='all')
        today_df = today_df[['Αρ.Τιμ.', 'Ημ.Έκδ.', 'Α/Α Χ.Ε.(*)', 'Ημ. Εξόφλ.', 'Ολ. Ποσό', 'Κρ/σεις', 'Κατ/ση']]

        history_df = get_as_dataframe(prom_tracker_history)
        history_df = history_df.dropna(how='all')
        history_df = history_df[['Αρ.Τιμ.', 'Ημ.Έκδ.', 'Α/Α Χ.Ε.(*)', 'Ημ. Εξόφλ.', 'Ολ. Ποσό', 'Κρ/σεις', 'Κατ/ση']]

        from_current_to_history = history_df.append(today_df)
        from_current_to_history = from_current_to_history.sort_values('Ημ. Εξόφλ.').drop_duplicates('Αρ.Τιμ.')
        from_current_to_history = from_current_to_history.sort_values(by=['Ημ. Εξόφλ.'], ascending=False)

        prom_tracker_current.delete_rows(2, len(today_df) + 1)
        set_with_dataframe(prom_tracker_history, from_current_to_history)

        # today_tracker = today_tracker.sort_values('dt').drop_duplicates('id')
        today_tracker = today_tracker.sort_values(by=['Αρ.Τιμ.'], ascending=True)
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
