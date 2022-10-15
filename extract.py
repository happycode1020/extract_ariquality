# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 10:20:00 2022

@author: schao
"""
import json
import requests
import pandas as pd
from datetime import datetime, timedelta

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}

def extract_site(city):
	'''
	purpose:提取气象和空气质量站点
	'''
	url = 'https://q-weather.info/api/city/?q='
	city_site = url+city
	ret = requests.get(city_site,headers=headers,verify=False).content
	data = json.loads(ret)
	weather_stations = data.get('weather_stations')
	airquality_stations = data.get('airquality_stations')
	name = data.get('name')
	return weather_stations,airquality_stations,name

def weather_site_csv(w_station,city_name,outdir):
	'''
	purpose:城市气象站点数据写出到csv格式文件
	args:
		w_station:气象数据的站点信息；
		city_name:城市名称
		outdir:文件写出路径
	'''
	lat,lon,wmo,name = [],[],[],[]
	for dict_s in w_station:
		lat.append(dict_s.get('lat'))
		lon.append(dict_s.get('lon'))
		wmo.append(dict_s.get('wmo'))
		name.append(dict_s.get('name'))
	data = {'lat':lat,'lon':lon,'wmo':wmo,'name':name}
	df = pd.DataFrame(data,index_col=None)
	df.to_csv(outdir+'/'+city_name+'-气象站点.csv',encoding='utf-8-sig')

def airquaity_site_csv(a_station,city_name,outdir):
	'''
	purpose:空气质量站点写出到csv文件
	'''
	lat,lon,code,name = [],[],[],[]
	for dict_s in a_station:
		lat.append(dict_s.get('lat'))
		lon.append(dict_s.get('lon'))
		code.append(dict_s.get('code'))
		name.append(dict_s.get('name'))
	data = {'lat':lat,'lon':lon,'code':code,'name':name}
	df = pd.DataFrame(data)
	df.to_csv(outdir+'/'+city_name+'-空气质量站点.csv',encoding='utf-8-sig')

def airquality_history_dat(air_site,dtime):
	'''
	purpose:提取特定站点某一天的历史监测数据
	args:
		air_site:待提取的站点编号
		dtime:提取数据时间 格式：2022-07-10
	'''
	url = '''https://q-weather.info/api/airquality/{air_site}/history/?date={dtime}'''
	down_url = url.format(air_site=air_site,dtime=dtime)
	ret = requests.get(down_url,headers=headers,verify=False).content
	data = json.loads(ret)
	obs_data = data.get('res')
	pm2_5_dict,pm10_dict,so2_dict,no2_dict,co_dict,o3_dict = {},{},{},{},{},{}
	for d_obs in obs_data[:-1]:
		ttime = d_obs['time']
		thistime_p = datetime.strptime(ttime, '%Y-%m-%dT%H:00:00')
		thistime_f = thistime_p+timedelta(hours=8)
		thistime = thistime_f.strftime('%Y-%m-%d %H:00:00')
		ttime = thistime
		print('>>>>>> hour ',ttime)
		try:
			pm2_5_dict[ttime] = d_obs['PM2_5']
		except Exception as e:
			pm2_5_dict[ttime] = None
		try:
			pm10_dict[ttime] = d_obs['PM10']
		except Exception as e:
			pm10_dict[ttime] = None
		try:
			so2_dict[ttime] = d_obs['SO2']
		except Exception as e:
			so2_dict[ttime] = None
		try:
			no2_dict[ttime] = d_obs['NO2']
		except Exception as e:
			no2_dict[ttime] = None
		try:
			co_dict[ttime] = d_obs['CO']
		except Exception as e:
			co_dict[ttime] = None
		try:
			o3_dict[ttime] = d_obs['O3']
		except Exception as e:
			o3_dict[ttime] = None
			print(e)
	return pm2_5_dict,pm10_dict,so2_dict,no2_dict,co_dict,o3_dict

def data_conc(starttime,endtime,air_site):
	'''
	purpose:爬取日期内的某个站点的所有监测数据
	args:
		starttime:开始时间
		endtime:结束时间
		air_site:站点编号
	'''
	
	Startday = datetime.strptime(starttime, "%Y%m%d")
	Endday = datetime.strptime(endtime, "%Y%m%d")
	data_conc = []
	while Startday <= Endday:
		dtime = Startday.strftime("%Y-%m-%d")
		print('>>> day ',dtime)
		pm2_5_dict,pm10_dict,so2_dict,no2_dict,co_dict,o3_dict = airquality_history_dat(air_site, dtime)
		for hs in pm10_dict.keys():
			value = [hs,pm2_5_dict[hs],pm10_dict[hs],so2_dict[hs],no2_dict[hs],co_dict[hs],o3_dict[hs]]
			data_conc.append(value)
		Startday += timedelta(days=1)
	return data_conc

def pbtach_write(city_name,starttime,endtime,outdir):
	'''批量写出城市的所有空气质量监测站点'''
	w_stations,air_stations,name = extract_site(city_name)
	print(air_stations)
	writer = pd.ExcelWriter(outdir+'/'+name+'.xlsx',mode='w')
	columns = ['dtime','pm2_5','pm10','so2','no2','co','o3']
	for air in air_stations:
		air_site = air['code']
		d_conc = data_conc(starttime,endtime,air_site)
		df = pd.DataFrame(d_conc,columns=columns)
		df.set_index('dtime',inplace=True)
		df.to_excel(writer,sheet_name=air_site)
	writer.save()
	writer.close()


from concurrent.futures import ThreadPoolExecutor

def thread_write():
	df = pd.read_table('C:/Users/schao/Desktop/city.dat',names=['city'])
	city_list = df['city'][:]
	city_list = list(set([c.replace('市','') for c in city_list]))
	print(len(city_list))
	starttime_list = len(city_list)*[starttime]
	endtime_list = len(city_list)*[endtime]
	print(starttime_list)
	outdir_list = len(city_list)*[outdir]
	with ThreadPoolExecutor(max_workers=12) as executer:
		try:
			all_work = executer.map(pbtach_write,city_list,starttime_list,endtime_list,outdir_list)
		except Exception as e:
			print(e)
		
if __name__ == '__main__':
	city_name='厦门'
	outdir = 'D:/jobs/聚光科技/谱育科技/202209/数据爬取'
	air_site = '3270A'
	starttime = '20220902'
	endtime = '20220909'
	extract_site(city_name)
# 	pbtach_write(city_name,starttime,endtime,outdir)
	thread_write()
	