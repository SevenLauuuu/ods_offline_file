#!/usr/bin/python
# -*- coding: utf-8 -*-
#author:
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

import sys
import xlrd 
import socket

import rh_hive_conn 
import ods_excel_extract 

##################
#default_encoding = 'utf-8'
#if sys.getdefaultencoding() != default_encoding:
#    reload(sys)
#    sys.setdefaultencoding(default_encoding)
   
##################
def getHost():
  hostname = socket.gethostname()
  ip = socket.gethostbyname(hostname)
  port = 10001
  hive_user=""
  hive_password=""

  if ip[4:6] == '69' :
    print "现在在开发环境 ip : %s " %(ip)
    ip = 'xxxx.xxx.216.40'
    hive_user = 'xxxx'
    hive_password = 'xxx'

  elif  ip[4:6] == '64' :
    print "现在在生产环境 ip : %s " %(ip)
    ip = '100.64.164.xx'
    hive_user = 'xxxx'
    hive_password = 'xxxx'

  return ip , hive_user, hive_password



if __name__ == '__main__':

	#审计离线数据库cmf_audm
	#sys.argv[1] : excel文件名
	#sys.argv[2] : target 数据库名
	#python  ods_offline_create_tbl.py '/data/etlscript/CMRH_ODS/SCRIPT/' 'contract' 'cmf_audm'

	excel_name ='%s.xls'%(sys.argv[2])
	txt_name = '%s.txt'%(sys.argv[2])

	dst_db = sys.argv[3]

	###上传到HDFS上
	local_source_pth   = sys.argv[1]
	hdfs_src_csv_path  = '/data/audit/%s.xls'%(sys.argv[2])
	hdfs_dst_path      = '/user/hive/warehouse/%s.db/%s/'%(dst_db, excel_name[:-4])
	

	download_hdfs_comad    = 'hadoop fs -copyToLocal  %s %s'%(hdfs_src_csv_path,local_source_pth)  ###将hdfs上的文件拷贝到本地
	load_to_hdfs_comad     = 'hadoop fs -copyFromLocal %s%s  %s'%(local_source_pth,txt_name,hdfs_dst_path) ###将本地的文件拷贝到hdfs上
	del_hdfs_comad         = 'hadood fs -rm -f %s%s '%(local_source_pth,txt_name)  ###删除hdfs上的文件

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
	workbook_instance = ods_excel_extract.ExcelInstance(excel_name)
	print excel_name,txt_name,workbook_instance

	all_sheet_name = workbook_instance.getAllSheetsName()
	print all_sheet_name



	hive_user      = ''
	hive_password  = ''
	hive_client    = ''
	hive_database  = 'default' 

	hive_host, hive_user, hive_password = getHost()
	hive_port = 10001
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



		

	hive_client.close() 
