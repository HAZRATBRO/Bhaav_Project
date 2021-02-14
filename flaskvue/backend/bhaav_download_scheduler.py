import requests
from bs4 import BeautifulSoup
import zipfile
import json
import pandas as pd
import redis
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import sys
from datetime import date , timedelta


logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
sh.setFormatter(formatter)
logger.addHandler(sh)



class BhaavScheduler:

	def __init__(self):
		self.header_data = {
		"__VIEWSTATEENCRYPTED": "",
		"ctl00$ContentPlaceHolder1$Debt": "rbteqty",
		"ctl00$ContentPlaceHolder1$btnSubmit": "Submit"
		}
			
	def set_form_data(self , url):
		"""This function basically gets the BhavCopy Hidden Form fields
		to be sent to the post form"""
		header = {
			'User-Agent':'Mozilla/5.0'
		}   
		response = requests.get(url , headers=header)
		doc = BeautifulSoup(response.text , 'html.parser')
		self.header_data['__VIEWSTATE'] = doc.find('input',attrs={'id':'__VIEWSTATE'}).get('value')
		self.header_data['__VIEWSTATEGENERATOR'] = doc.find('input',attrs={'id':'__VIEWSTATEGENERATOR'}).get('value')
		self.header_data['__EVENTVALIDATION'] = doc.find('input',attrs={'id':'__EVENTVALIDATION'}).get('value')
		#print(header_data['__EVENTVALIDATION'])

	def get_bhav_data(self ,date , url):
		""" This function gets the actual data by making the scheduled calls 
			to the BSE url and fetches and parses the CSV File for data"""
		self.set_form_data(url)
		date_arr = date.split('-')
		self.header_data['ctl00$ContentPlaceHolder1$fdate1'] = date_arr[1]
		self.header_data['ctl00$ContentPlaceHolder1$fyear1'] = date_arr[0]
		self.header_data['ctl00$ContentPlaceHolder1$fmonth1'] = date_arr[2]
		self.header_data['ctl00$ContentPlaceHolder1$DDate'] = date
		##### make the POST Call #####
		header = {
			'User-Agent':'Mozilla/5.0'
		}
		response = requests.post(url , data=self.header_data , headers=header)
		doc = BeautifulSoup(response.text , 'html.parser')
		download_url = doc.find('a' , attrs={'id': 'ContentPlaceHolder1_btnHylSearBhav'})
		if (download_url == None) or (download_url == ''):
			logger.debug('Zip file Unavailable , exiting download sequence ...')
			return
		download_url = download_url.get('href')	
		logger.info(download_url)
		self.download_zip(download_url ,r'tmp.zip' , 128)
		json_data = self.read_zip_content('tmp.zip' , date)
		self.store_to_redis(json_data , date)

	def download_zip(self , download_url , save_path , chunk_size=128):
		"""Save the zip file in a temp location for later use"""
		header = {
			'User-Agent':'Mozilla/5.0'
		}
		r = requests.get(download_url , stream=True , headers=header)
		with open(save_path , 'wb') as fd:
			for chunk in r.iter_content(chunk_size=chunk_size):
				fd.write(chunk)

	def read_zip_content(self , file_path , date):
		"""Read the zip file contents , and write them to a redis Cache"""
		archive = zipfile.ZipFile(r'tmp.zip' , 'r')
		f_name  = archive.namelist()[0]
		fields = ['SC_CODE', 'SC_NAME' , 'OPEN', 'HIGH', 'LOW', 'CLOSE']
		fp = archive.open(f_name , 'r')
		csv_data = pd.read_csv(fp)
		df_json = csv_data[fields]
		df_json['SC_DATE'] = date
		df_json['SC_NAME'] = df_json['SC_NAME'].str.replace(' +$', '')
		df_json = df_json.set_index('SC_NAME')
		json_data = df_json.to_json(orient='index')	
		json_data = json.loads(json_data)
		return json_data

	def store_to_redis(self , json_data , date):
		"""Store json data with date as the key"""
		logger.info('Storing data to cache ...')
		r_cache = redis.StrictRedis()
		pipe = r_cache.pipeline()
		for key in json_data.keys():
			obj = json.dumps(json_data[key])
			#logger.info(obj)
			if r_cache.exists(key) != 0:
				#### Append the data to existing key ####
				#logger.info("Appending data ...")
				pipe.append(key , obj)
			else:
				#logger.info("Setting data ...")
				pipe.set(key , obj)
		pipe.execute()
		logger.info("Pipeline execution complete ...")

	def get_date(self , date):
		""" Returns a date in Bhav Website compatible format """
		date_list = date.split('-')
		date_list[1] = str(int(date_list[1]))
		date_list[2] = str(int(date_list[2]))
		
		return date_list[0] + '-' + date_list[1] + '-' + date_list[2]

	def exec_job(self):
		""" Wrapper function to trigger the Job Function """
		date = datetime.date.today().strftime("%Y-%d-%m")
		curr_date = self.get_date(date)
		url = 'https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx'
		self.get_bhav_data(curr_date , url)

	def push_bhav_data(self):
		url = 'https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx'
		sdate = date(2020 , 1 , 1)
		edate = date(2020 , 2 , 12)
		date_arr = [self.get_date(x) for x in (pd.date_range(sdate,edate-timedelta(days=1),freq='d').strftime("%Y-%d-%m").array)]
		[self.get_bhav_data(d, url) for d in date_arr]

# push_bhav_data()
# sched_buddy = BlockingScheduler()
# sched_buddy.add_job(exec_job , 'cron' , day='*' , hour='18' , minute='0' , month='*')
# sched_buddy.start()