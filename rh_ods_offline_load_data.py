#!/usr/bin/python
# -*- coding: utf-8 -*-
#author:ytliu 
#date:2019-01-03
#**
# ////////////////////////////////////////////////////////////////////
# //                          _ooOoo_                               //
# //                         o8888888o                              //
# //                         88" . "88                              //
# //                         (| ^_^ |)                              //
# //                         O\  =  /O                              //
# //                      ____/`---'\____                           //
# //                    .'  \\|     |//  `.                         //
# //                   /  \\|||  :  |||//  \                        //
# //                  /  _||||| -:- |||||-  \                       //
# //                  |   | \\\  -  /// |   |                       //
# //                  | \_|  ''\---/''  |   |                       //
# //                  \  .-\__  `-`  ___/-. /                       //
# //                ___`. .'  /--.--\  `. . ___                     //
# //              ."" '<  `.___\_<|>_/___.'  >'"".                  //
# //            | | :  `- \`.;`\ _ /`;.`/ - ` : | |                 //
# //            \  \ `-.   \_ __\ /__ _/   .-` /  /                 //
# //      ========`-.____`-.___\_____/___.-`____.-'========         //
# //                           `=---='                              //
# //      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^        //
# //         佛祖保佑         再无Bug                               //
# ////////////////////////////////////////////////////////////////////
# User:ytliu
# Date:2019-01-03 
#/
#cd /data/etlscript/CMRH_ODS/SCRIPT

import os
import re

import json
import requests 

import sys
import xlrd 
import socket

import datetime
import loadsJson
import rh_hive_conn 
import ods_excel_extract 

##################
#default_encoding = 'utf-8'
#if sys.getdefaultencoding() != default_encoding:
#    reload(sys)
#    sys.setdefaultencoding(default_encoding)
   
##################
def python_curl(error,task_name,run_serial_no):
  url ='https://datahub.cmrh.com/datahub/post/taskStatus'
  headers = {'Content-Type':'application/json','Accept':'application/json'}

  nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  message="脚本耗时:%s秒"%(nowTime)

  if error == 0: 
    error='0'
  else:
    error='1'


  data = {'code':error, 'message': message,'runSerialNo':run_serial_no,'taskName':task_name}
  data = json.dumps(data)

  res = requests.post(url, data=data, headers=headers)
  print data,res

def getHost():
  hostname = socket.gethostname()
  ip = socket.gethostbyname(hostname)
  port = 10001
  hive_user=""
  hive_password=""

  if ip[4:6] == '69' :
    print "现在在开发环境 ip : %s " %(ip)
    ip = '100.69.216.40'
    hive_user = 'sys_hadoop_srv'
    hive_password = '8UDrC#66'

  elif  ip[4:6] == '64' :
    print "现在在生产环境 ip : %s " %(ip)
    ip = '100.64.164.11'
    hive_user = 'sys_dw_adm'
    hive_password = 'rZZnjQ$-'

  return ip , hive_user, hive_password



if __name__ == '__main__':

	#审计离线数据库cmf_audm
	#sys.argv[1] : excel文件名
	#sys.argv[2] : target 数据库名
	#python  ods_offline_create_tbl.py '/data/etlscript/CMRH_ODS/SCRIPT/' 'contract' 'cmf_audm'
	param = sys.argv[1]
	oracle_host,oracle_port,oracle_servicename,oracle_user,oracle_password,oracle_database,oracle_table,hive_host,hive_port,hive_user,hive_password,hive_database,dest_table,add_param,day_interval,p1,p2,p3,task_name,run_serial_no,inc_start,inc_end,hive_queue=loadsJson.JsonPythonInstance(param)

	day_interval = int(day_interval)
	hive_syn_table = dest_table
	oracle_where = add_param

	print  "============"
	print oracle_host,oracle_port,oracle_servicename,oracle_user,oracle_password,oracle_database,oracle_table

	print  "============"
	print hive_host,hive_port,hive_user,hive_password,hive_database,dest_table,add_param,day_interval,p1,p2

	print  "============"
	print task_name,run_serial_no,inc_start,inc_end,hive_queue


	begin_date=(datetime.date.today()-datetime.timedelta(days=day_interval)).strftime("%Y-%m-%d %H:%M:%S")
	end_date=(datetime.date.today()-datetime.timedelta(days=day_interval-1)).strftime("%Y-%m-%d %H:%M:%S")
	etl_date=(datetime.date.today()).strftime("%Y%m%d")
	print begin_date,end_date,etl_date


	excel_name ='%s.xls'%(oracle_table)
	txt_name = '%s.txt'%(oracle_table)

	dst_db = hive_database

	###上传到HDFS上
	local_source_pth   = '/data/etlscript/CMRH_ODS/SCRIPT/'
	hdfs_src_csv_path  = '/data/audit/%s.xls'%(oracle_table)
	hdfs_dst_path      = '/user/hive/warehouse/%s.db/%s/'%(dst_db, excel_name[:-4])
	

	download_hdfs_comad    = 'hadoop fs -copyToLocal  %s %s'%(hdfs_src_csv_path,local_source_pth)  ###将hdfs上的文件拷贝到本地
	load_to_hdfs_comad     = 'hadoop fs -copyFromLocal %s%s  %s'%(local_source_pth,txt_name,hdfs_dst_path) ###将本地的文件拷贝到hdfs上
	del_hdfs_comad         = 'hadood fs -rm -f %s%s '%(local_source_pth,txt_name)  ###删除hdfs上的文件

	print "======local_source_pth:%s======="%(local_source_pth)
	print "======hdfs_src_csv_path:%s======="%(hdfs_src_csv_path)
	print "======hdfs_dst_path:%s======="%(hdfs_dst_path)
	print "======download_hdfs_comad:%s======="%(download_hdfs_comad)


	###先下载
	try:
		rm_local_file = 'rm -f %s%s '%(local_source_pth,excel_name)
		os.system(rm_local_file)
		print rm_local_file
	except:
		pass


	try:
		rm_local_file = 'rm -f %s%s '%(local_source_pth,txt_name)
		os.system(rm_local_file)
		print rm_local_file
	except:
		pass


	try:
		os.system(download_hdfs_comad)
		print download_hdfs_comad
	except:
		pass


	##生成workbook的instance	
	workbook_instance = ods_excel_extract.ExcelInstance(local_source_pth+excel_name)
	print excel_name,txt_name,workbook_instance

	all_sheet_name = workbook_instance.getAllSheetsName()
	print all_sheet_name

	authMechanism = 'PLAIN'

	try:
		hive_client = rh_hive_conn.HiveClient(hive_host,hive_port,authMechanism,hive_user,hive_password,hive_database)
	except:
		print  "hive connection 连接失败"

	print "hive client is here!!"
	print hive_client 
	

	###获取每个sheet页面的名字
	for s_index in range(0, len(all_sheet_name)):

		create_sql = 'create table  `%s`.'%(dst_db)
		create_sql = create_sql + '`%s`('%(excel_name[:-4])

		workbook_instance.getOneSheetInstance(all_sheet_name[s_index])
		sheet_nrows = workbook_instance.getExcelRows()
		print  "sheet_nrows %s " %(str(sheet_nrows))


		ncols = workbook_instance.getExcelCols()
		nrows = workbook_instance.getExcelRows()

		print  "ncols %s " %(str(ncols))

		for c in range(0, ncols):
			one_content = workbook_instance.getOneContent(0, c)

			#create_sql = create_sql + 's'+str(c)+'  string  comment \''+one_content+'\' '
			#if one_content.find('日期') != -1 or one_content.find('时间')!= -1 or one_content.find('date')!= -1 or one_content.find('time')!= -1:
			#	create_sql = create_sql + ' prop%s  date  COMMENT \'%s\''%(str(c),str(one_content)) #COMMENT \"%s\""%('string',str(one_content))
			#else:
			create_sql = create_sql + ' prop%s  string COMMENT \'%s\''%(str(c),str(one_content)) #COMMENT \"%s\""%('string',str(one_content))
			
			if c < ncols - 1:
				create_sql = create_sql + ','
			elif c ==  ncols - 1:
				create_sql = create_sql + ')comment "'+all_sheet_name[s_index]+'" ROW FORMAT DELIMITED FIELDS TERMINATED BY \'\\001\'  LINES TERMINATED BY \'\\n\''

		print str(create_sql), type(create_sql)
		print  '建表完成'

		txt_name = excel_name[:-3]+'txt'

		try:
			if  os.path.exists(local_source_pth+txt_name) == True:
				os.remove(local_source_pth+txt_name)
		except:
			pass 

		f = open(txt_name, 'w+')

		for i in range(1, nrows):
			tmp_str = ""
			for j in range(0, ncols):
				tmp_str = tmp_str+'%s'%(str(workbook_instance.getOneContent(i, j)))
				#print str(workbook_instance.getOneContent(i, j))

				if j < ncols - 1 :
					tmp_str = tmp_str+"\001"

			tmp_str=tmp_str+'\n'
			f.write(tmp_str)

		try:
			f.close()
		except:
			pass 

		try:
			hive_client.execute_hive_sql_no_return(str(create_sql))
		except:
			pass


		try:
			os.system(load_to_hdfs_comad)
			print "将本地txt文件上传到hdfs"
			print  load_to_hdfs_comad
			
		except:
			os.system(del_hdfs_comad)
			os.system(load_to_hdfs_comad)
			print  del_hdfs_comad
			print  load_to_hdfs_comad

		
		#hive_src_file = txt_name
		#hive_src_table = 'tmp_%s '%(excel_name[:-4])
		#load_hdfs_into_hive_sql =" load data inpath '%s%s' overwrite into table %s.%s"%(hive_src_path,hive_src_file, dst_db,hive_src_table)
		#print load_hdfs_into_hive_sql

		#hive_client.execute_hive_sql_no_return(load_hdfs_into_hive_sql)
		
		repair_sql = 'MSCK REPAIR TABLE  %s.%s'%(dst_db,excel_name[:-4])
		error = 0

		python_curl(error,task_name,run_serial_no)

	hive_client.close() 
