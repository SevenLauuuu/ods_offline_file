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
#rm -f rh_task_rel.py
#python rh_task_rel.py  "ODS_CMRH_SLIS_ODS_SLIS_SYN_OCCUPATION_GRADE_I"
#python rh_task_rel.py  "ODS_CMRH_SLIS_ODS_SLIS_BAS_OCCUPATION_GRADE"
#python rh_task_rel.py  "HIVE_DWD_OCS_CLM_INFO_D" "HIVE_DWS_OCS_CLM_INFO_D"

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


def Search_Children_Node(task_nodes_list,task_name):
	cn = list()
	#al_cn_set = set()
		
	if len(task_nodes_list) == 0:
		return cn

	for tn in task_nodes_list:

		if tn.task_name == task_name:
			continue                                                                                                  
			
		if task_name in tn.father_node_list:
			cn.append(tn.task_name)


	return cn

def Update_Node_Message(task_nodes_list):

	for u in task_nodes_list:
		u.children_node_list = Search_Children_Node(task_nodes_list, u.task_name)

	return task_nodes_list

def Get_Father_Node(task_nodes_list,task_name):
	for i in task_nodes_list:
		if i.task_name == task_name:
			return i.father_node_list


def Get_Children_Node(task_nodes_list,task_name):

	children_set = set()
	child_tmp_set = set()
	child_tmp_tmp_set = set()

	for i in task_nodes_list:

		if task_name in i.father_node_list:

			children_set.add((i.task_name,i.task_n))
			child_tmp_set.add(i.task_name)

	if len(child_tmp_set) == 0:
		return children_set
	else:

		for a in child_tmp_set:
			tmp = Get_Children_Node(task_nodes_list,a)
			children_set.union(tmp)
			child_tmp_set.add(a)


	return  children_set



def Cal_Node_N(task_nodes_list,task_name):
	task_n = None
	task_max = 0
	task_list = set()
	
	father_node_list = Get_Father_Node(task_nodes_list,task_name)
	
	if  len(father_node_list) == 0 :
		task_list.add((task_name,0))
		return 0, task_list


	for f in father_node_list:
		task_tmp, task_tmp_list = Cal_Node_N(task_nodes_list,f)

		task_list = task_list | task_tmp_list 
		if task_tmp > task_max:
			task_max = task_tmp

	task_n = task_max + 1
	task_list.add((task_name,task_n))

	return task_n,task_list


def Find_Same_List(set1, set2):
	same_set = set()

	for s1 in set1:
		for s2 in set2:
			if s1 == s2:
				same_set.add(s1)
				break

	return same_set

def get_One_children(all_task_nodes_list,task_name_fir):

	print "====Get_Children_Node task_name_fir====="
	children_list = Get_Children_Node(all_task_nodes_list,task_name_fir)
	print children_list


	children_set = set()

	for c in children_list:
		task_n,task_nodes_sec_list = Cal_Node_N(all_task_nodes_list, c[0])
		children_set.add((c[0],task_n))

	print "======children_set======="
	for c in children_set:
		print c[0],c[1] 

	return children_set

class Task_Node:
	def __init__(self,task_nodes_list,task_name,father_node_list):
		self.task_name = task_name 
		self.task_n = 0 
		self.father_node_list = father_node_list
		self.children_node_list = Search_Children_Node(task_nodes_list,task_name)


#cd /data/etlscript/CMRH_ODS/SCRIPT/ 
#rm -f rh_task_rel.py
#python rh_task_rel.py "SQOOP_EXP_NODS_ADS_LEP_DAY_REPORT"
#python rh_task_rel.py "HIVE_DWD_PRE_PREM_DAILY"
#python rh_task_rel.py  "SQOOP_EXP_NODS_ADS_LEP_DAY_REPORT" "HIVE_DWD_PRE_PREM_DAILY" 

if __name__ == '__main__':
	local_path ="/data/etlscript/CMRH_ODS/SCRIPT/datahub_task.xls"
	sheet_name = "task"

	task_name_fir = None
	task_name_sec = None

	if    len(sys.argv)==2:
		task_name_fir = sys.argv[1]
	elif  len(sys.argv)==3:
		task_name_fir = sys.argv[1]
		task_name_sec = sys.argv[2]

	print sys.argv
	print  task_name_fir, task_name_sec

	f=open("data_task_anl.txt","w")

	workbook = xlrd.open_workbook(local_path)
	sheet2 = workbook.sheet_by_name(sheet_name)
	
	rows = sheet2.nrows
	cols = sheet2.ncols
	print rows,cols

	all_task_nodes_list = list()

	for i in range(0, rows):
		task_name = sheet2.row_values(i)[0]
		father_node_list = sheet2.row_values(i)[7]


		if father_node_list != "[]":
			task_father_list = father_node_list[1:-1].replace("\"","").split(",")
		else:
			task_father_list=list()

		task_node = Task_Node(all_task_nodes_list,task_name,task_father_list)
		all_task_nodes_list.append(task_node)

	#从第一个开始更新所有结点的信息
	all_task_nodes_list = Update_Node_Message(all_task_nodes_list)

	task_nodes_fir_list = list()
	task_nodes_sec_list = list()
	task_n = 0
	
	for a in all_task_nodes_list:
		if a.task_name == task_name_fir and task_name_fir != None:
			#print a, a.task_name, a.father_node_list 
			task_n,task_nodes_fir_list = Cal_Node_N(all_task_nodes_list, a.task_name)
			break


	for a in all_task_nodes_list:		
		if  a.task_name == task_name_sec and task_name_sec != None:
			#print a, a.task_name, a.father_node_list 
			task_n,task_nodes_sec_list = Cal_Node_N(all_task_nodes_list, a.task_name)
			break

	print "====task_nodes_fir_list====="
	print task_n, task_name_fir
	for t in task_nodes_fir_list:
		print t[0], t[1]
	#print task_n,task_nodes_fir_list
	
	all_rt_set = set()
	
	rt_set = get_One_children(all_task_nodes_list,task_name_fir)
	if len(rt_set) != 0:
		for r in rt_set:
			print r 
			get_One_children(all_task_nodes_list,r[0])


	if task_name_sec != None :
		print "====task_nodes_sec_list====="
		print task_nodes_sec_list 


