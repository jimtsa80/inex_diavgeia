# -*- coding: utf-8 -*-
import urllib
from urllib.request import urlopen
from urllib.parse import quote
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date
import gspread
import json
import re
import os
import pandas as pd
from prom_scrapper import InexPrometheus
from scrapper_pap import InexPap


class InexScrapper:

    def __init__(self, query, query_date):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('secret_key/client_secret.json', self.scope)
        self.client = gspread.authorize(self.creds)
        self.counter = 0
        self.query = query
        # self.query_date = query_date
        self.query_date = query_date
        self.today = date.today().strftime("%Y%m%d")
        # self.today = "20201208"
        self.query_url = "https://diavgeia.gov.gr/luminapi/api/search/export?q=q:[%22{}%22]&sort=recent&wt=json".format(quote(self.query))
        # self.query_url = "https://diavgeia.gov.gr/luminapi/api/search/export?q=q:[%22{}%22]&sort=recent&wt=json".format(self.query)
        # chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--headless")
        # self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        self.get_query()
        # self.read_json_to_df()
        self.filter_by_date_get_df()
        self.concat_results()

    def cleanhtml(self, raw_html):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', raw_html)
        return cleantext

    def get_query(self):
        # self.driver.get(self.query_url)
        jsonurl = urllib.request.urlopen(self.query_url).read().decode('utf-8')

        text = json.loads(jsonurl)
        for key, value in text.items():
            df = pd.DataFrame(value)

        return df

    # with open('{}.json'.format(self.query), 'w', encoding='utf-8') as f:
        #     clean_data = self.cleanhtml(self.driver.page_source)
        #     f.write(clean_data)

    # def read_json_to_df(self):
    #     for fl in os.scandir('.'):
    #         if fl.path.endswith(".json"):
    #             with open(fl, 'r', encoding='utf-8') as json_file:
    #
    #                 self.data = json.loads(json_file.read())
    #
    #                 for key, values in self.data.items():
    #                     df = pd.DataFrame(values)
    #
    #     return df

    def filter_by_date_get_df(self):
        data = self.get_query()
        data['submissionTimestamp'] = pd.to_datetime(data['submissionTimestamp'])
        entries = data[data['submissionTimestamp'].dt.date.astype(str) == self.query_date]
        print(self.query_date)
        print(data['submissionTimestamp'].dt.date.astype(str))
        print(entries)
        if not entries.empty:
            entries.to_excel('{}_{}.xlsx'.format(self.query, self.today), sheet_name='sheet1', index=False)
        else:
            self.counter += 1
            print('{} ---> Nothing Found'.format(self.query))

    def concat_results(self):
        df = pd.DataFrame()
        for fl in os.scandir('.'):
            if fl.path.endswith(".xlsx"):
                data = pd.read_excel(fl, 'sheet1')
                df = df.append(data)

        df.to_excel('{}_all_results.xlsx'.format(self.today), sheet_name='sheet1', index=False)

    def write_to_gs(self):

        today_tracker = pd.read_excel('{}_all_results.xlsx'.format(self.today), 'sheet1')
        diavgeia_tracker_current = self.client.open('inex_diavgeia').worksheet('current_diav')
        diavgeia_tracker_history = self.client.open('inex_diavgeia').worksheet('history_diav')

        today_df = get_as_dataframe(diavgeia_tracker_current)
        today_df = today_df.dropna(how='all')
        today_df = today_df[
            ["ada", "protocolNumber", "issueDate", "submissionTimestamp", "documentUrl", "subject", "decisionTypeUid",
             "decisionTypeLabel", "organizationUid", "organizationLabel"]]

        history_df = get_as_dataframe(diavgeia_tracker_history)
        history_df = history_df.dropna(how='all')
        history_df = history_df[
            ["ada", "protocolNumber", "issueDate", "submissionTimestamp", "documentUrl", "subject", "decisionTypeUid",
             "decisionTypeLabel", "organizationUid", "organizationLabel"]]

        from_current_to_history = history_df.append(today_df)
        from_current_to_history = from_current_to_history.sort_values('submissionTimestamp').drop_duplicates('ada')
        from_current_to_history = from_current_to_history.sort_values(by=['submissionTimestamp'], ascending=False)

        diavgeia_tracker_current.delete_rows(2, len(today_df) + 1)
        set_with_dataframe(diavgeia_tracker_history, from_current_to_history)

        # today_data = today_df.append(today_tracker)
        today_tracker = today_tracker.sort_values('submissionTimestamp').drop_duplicates('ada')
        today_tracker = today_tracker.sort_values(by=['submissionTimestamp'], ascending=False)
        set_with_dataframe(diavgeia_tracker_current, today_tracker)

    def clean_folder(self):
        for fl in os.scandir('.'):
            if fl.path.endswith(".xlsx") or fl.path.endswith(".json"):
                os.remove(fl)


if __name__ == '__main__':
    # # today = "2021-02-15"
    # # if date.today().month % 2 == 0:
    # today = date.today().strftime("%Y-%m-%d")
    # # else:
    # #     today = date.today().strftime("%Y-%d-%m")
    # # today = date.today().strftime("%Y-%m-%d")
    # # today = date.today().strftime("%d/%m/%Y")
    #
    # nothing_found = 0
    # keywords = ["094356041", "inex", "ινεξ"]
    #
    # for kw in keywords:
    #     inexScrapper = InexScrapper(kw, today)
    #     nothing_found += inexScrapper.counter
    #
    # if nothing_found == len(keywords):
    #     for kw in keywords:
    #         today = date.today().strftime("%Y-%d-%m")
    #         inexScrapper = InexScrapper(kw, today)
    #         nothing_found += inexScrapper.counter
    #
    # if nothing_found == len(keywords):
    #     print("Nothing found today!")
    #     inexScrapper.clean_folder()
    # else:
    #     inexScrapper.write_to_gs()
    #     inexScrapper.clean_folder()

    promScrapper = InexPrometheus()
    # promScrapper.get_results_for_gs()
    # promScrapper.write_to_gs()
    # promScrapper.get_pdf()
    promScrapper.upload_zip_files()

    # papScrapper = InexPap()
    # papScrapper.get_query()
    # papScrapper.write_to_gs()
