import requests
from bs4 import BeautifulSoup
import zipfile
import json
from io import TextIOWrapper
import pandas as pd
import redis
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import sys

logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
sh.setFormatter(formatter)
logger.addHandler(sh)




header_data = {
  	"__VIEWSTATEENCRYPTED": "",
 	"ctl00$ContentPlaceHolder1$Debt": "rbteqty",
	"ctl00$ContentPlaceHolder1$btnSubmit": "Submit"
	}

# url = "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"
# response = requests.post(url , data={
# 	"__VIEWSTATE": "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",
# 	"__VIEWSTATEGENERATOR": "80305CC0",
# 	"__VIEWSTATEENCRYPTED": "",
# 	"__EVENTVALIDATION": "Egw+d6CsDhR0M0/VO13f1asdb1ZxRRGvaTIKq749TRlbWZbOGXdgjvdFW/x+qVTL0zrVcJBWfVLzsHBLEiQ/oSW4F3GvVfTMstNSh7mdnW37Wo7eRm77O9VJQD7dSRvjiMIvM4O2i6SZOHCm+4GXvXAFzPFEfBlUlOrM+BL87PFHkmvGui+A9S21dDlgCQAr85iFm424kM0cNmhgIZCqNsq4vIA9WMB1ow3LcfWOM8+25Wy58KZ9pyifjZZTEwmrM2aA/vN2A0eKtyR9nDj1fElQjXPS1qx6ovQfpt+tauwPh1f1T9JDSnYW3umyq0vkgR7FSJ6v2m85mpc/tCvqUNlOKqacQxP7Zrri7rTyssfksMOQXx7nHF7DVTwjhmBnyU4KXmVKM8BmISA1cZ+i178sO9l+WOCS+C6+NPEzakTgvlB+h2ST2wsaNDuBcCyaVip3XbKnlyInVJD5V9tusM0ua/J8QWLlkn4z1FyBYmrSEczXherHTf1TfVlaMaiw2g+vNz5a4L1A3GzJ8PZQtwJTg6EMkh13Ud4x8UudbqZCpPQoJe+7eBtk7Wo7JTVC98eNXmu2njkwVLumvZAXsx+4r0q5AiuEAtq2s1kN4p7huVXow5UomtFnuE/fuvHeb3l7E5+LYGTSRp+iZfwFXZwuLw4AUFibcZuOeQGFZ+bj8bnAq1ANmi6vtruMGKElPI1/UU5RjbT2BmmWJdJl4QfP/dWtxXWp+cVMR96pQBsyuUF0Xc6gELDiJroyLcqY6onWKADbDxrqGWAXjLI9CyLfELmfqL4LquiPs1FDSjvij0ZF97DlyfcixUkC6sfTBFhnOL1J1JifyJEiS4vMhnxAYppsGlFnrGesDJVsnhJZMaWfgqgbYo84CYcYYfZF3WPKVCwx3jeBxCnv8XzXfDa20oFCh3xhVYZbVrjb35qPl4lvldwJ9icMKY2pKQ2iY9SgIOiwtyeUkEFDt4ZSvsUSZ/bA/Y2Rl8CpKGAE/kKnzByrqXKiln7eNCDmnznDXuxW3EfZEDLdQ9tsxO24keflyDw3hD+DJvwOtJ3QPwtwdKmMbMvVyp2nMZmyF/LKnfFv6OUpWw9kPujiao0YSRyJLrtjpkGjQ3AQI/VZFKF+PIrxFHwroAELYL6cOetjpDW9CGAm01AzfLxQGfwORI5bkUnIB9FKLatkZYXSiAy/vCG0H95g0IyDC5cRY0AffyfKwDM50EqEF0SvdQNBLDyn0BJV74p26XCordb7H/s4tH+ZKk3AhnpWApsguAwEXHW6S/KQaN493QSXBBiW2tA8VGcAGag5k7O3v5URHxUf6obOvOrThjRID4FD633mWi8eRTJiEz+hmaC5/vcfoms6GhC0lXCO1DmRPv7gtBb3N3FwNnioiFDXkzRfpgJXJsqDAD7RBj2PX9Lj1MW4cmykQu6GMcxrX1ZEnmivgsFrM6f+YjtkDOv2NUgBCd4prX6Ps4PlHWF/R5GHreqlufDm5+bVnmFFZeMuRhbN/H8oZcUb66Pc6jIJ3tSTFMw+4uUCxug8p8dsSWFKaL3N3MDgDBlEy2UpYruBX6p+xdi5q6c35qFGAsCPsDXNKBxT8pB2X+mnn3TEH9qnLe6hQijmPvBmHi1gNORegZOn4fc=",
# 	"ctl00$ContentPlaceHolder1$Debt": "rbteqty",
# 	"ctl00$ContentPlaceHolder1$fdate1": "1",
# 	"ctl00$ContentPlaceHolder1$fmonth1": "1",
# 	"ctl00$ContentPlaceHolder1$fyear1": "2020",
# 	"ctl00$ContentPlaceHolder1$btnSubmit": "Submit",
# 	"ctl00$ContentPlaceHolder1$DDate": "2020-1-1"
# } ,headers={'User-Agent': 'Mozilla/5.0'})
# doc = BeautifulSoup(response.text , 'html.parser')
# print(doc.find_all('a' , attrs={'id':'ContentPlaceHolder1_btnHylSearBhav'})[0].get('href'))

#print(data)

def set_form_data(url):
	"""This function basically gets the BhavCopy Hidden Form fields
	   to be sent to the post form"""
	header = {
		'User-Agent':'Mozilla/5.0'
	}   
	response = requests.get(url , headers=header)
	doc = BeautifulSoup(response.text , 'html.parser')
	header_data['__VIEWSTATE'] = doc.find('input',attrs={'id':'__VIEWSTATE'}).get('value')
	header_data['__VIEWSTATEGENERATOR'] = doc.find('input',attrs={'id':'__VIEWSTATEGENERATOR'}).get('value')
	header_data['__EVENTVALIDATION'] = doc.find('input',attrs={'id':'__EVENTVALIDATION'}).get('value')
	#print(header_data['__EVENTVALIDATION'])

def get_bhav_data(date , url):
	""" This function gets the actual data by making the scheduled calls 
	    to the BSE url and fetches and parses the CSV File for data"""
	set_form_data(url)
	date_arr = date.split('-')
	header_data['ctl00$ContentPlaceHolder1$fdate1'] = date_arr[1]
	header_data['ctl00$ContentPlaceHolder1$fyear1'] = date_arr[0]
	header_data['ctl00$ContentPlaceHolder1$fmonth1'] = date_arr[2]
	header_data['ctl00$ContentPlaceHolder1$DDate'] = date
	print(header_data['ctl00$ContentPlaceHolder1$fdate1'] , header_data['ctl00$ContentPlaceHolder1$DDate'])
	##### make the POST Call #####
	header = {
		'User-Agent':'Mozilla/5.0'
	}
	response = requests.post(url , data=header_data , headers=header)
	doc = BeautifulSoup(response.text , 'html.parser')
	download_url = doc.find('a' , attrs={'id': 'ContentPlaceHolder1_btnHylSearBhav'}).get('href')
	if (download_url == None) or (download_url == ''):
		logger.debug('Zip file Unavailable , exiting download sequence ...')
		return
	download_zip(download_url ,r'tmp.zip' , 128)
	json_data = read_zip_content('tmp.zip')
	store_to_redis(json.dumps(json_data) , date)

def download_zip(download_url , save_path , chunk_size=128):
	"""Save the zip file in a temp location for later use"""
	header = {
		'User-Agent':'Mozilla/5.0'
	}
	r = requests.get(download_url , stream=True , headers=header)
	with open(save_path , 'wb') as fd:
		for chunk in r.iter_content(chunk_size=chunk_size):
			fd.write(chunk)


def read_zip_content(file_path):
	"""Read the zip file contents , and write them to a redis Cache"""
	archive = zipfile.ZipFile(r'tmp.zip' , 'r')
	f_name  = archive.namelist()[0]
	fields = ['SC_CODE', 'SC_NAME' , 'OPEN', 'HIGH', 'LOW', 'CLOSE']
	fp = archive.open(f_name , 'r')
	csv_data = pd.read_csv(fp)
	df_json = csv_data[fields]
	json_data = df_json.to_json(orient='records')	
	json_data = json.loads(json_data)
	return json_data

def store_to_redis(json_data , date):
	"""Store json data with date as the key"""
	logger.info('Storing data to cache ...')
	r_cache = redis.StrictRedis()
	r_cache.set(date , json_data)

def get_date():
	""" Returns a date in Bhav Website compatible format"""
	date = datetime.date.today().strftime("%Y-%d-%m")
	date_list = date.split('-')
	date_list[1] = str(int(date_list[1]))
	date_list[2] = str(int(date_list[2]))

	return date_list[0] + '-' + date_list[1] + '-' + date_list[2]

def exec_job():
	""" Wrapper function to trigger the Job Function """
	curr_date = get_date()
	url = 'https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx'
	get_bhav_data(curr_date , url)

#get_bhav_data('2020-1-1' , 'https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx')



sched_buddy = BlockingScheduler()
sched_buddy.add_job(exec_job , 'cron' , day='*' , hour='18' , minute='0' , month='*')
sched_buddy.start()

