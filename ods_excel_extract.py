#!/usr/bin/python
# -*- coding: utf-8 -*-
#author:
#date:2018-08-22
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
# Date:2018-12-18  
#/
import os
import sys
import xlrd 
import re
##########################################################初始化开始#######################################################################################
#reload(sys)
#sys.setdefaultencoding('utf8')
#os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8' 

##########################################################初始化开始#######################################################################################
class ExcelInstance:
  def __init__(self,excel_name):
    print excel_name
    self.excel_name = excel_name


    if len(self.excel_name) == 0:
      print "传入的excel_name 为空"
      return ''

    if self.excel_name[-3::] != 'xls' and self.excel_name[-3::] != 'csv':
      self.excel_name = self.excel_name + '.xls'


    if os.path.exists(self.excel_name) == False : 
      print '文件不存在'

    print self.excel_name
    self.workbook = xlrd.open_workbook(self.excel_name)

  def getAllSheetsName(self):
    #print  self.workbook.sheet_names()
    return self.workbook.sheet_names()

  def getOneSheetInstance(self,sheet_name):
    self.sheet = self.workbook.sheet_by_name(sheet_name)

  def getExcelRows(self):
    return self.sheet.nrows

  def getExcelCols(self):
    return self.sheet.ncols 

  def getOneRow(self, iRow ):
    return self.sheet.row_values(iRow)


  def getOneContent(self, iRow, iCol):
    if iRow >= 0 and iCol >= 0 :
      return self.sheet.row_values(iRow)[iCol]



