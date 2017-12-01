#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# 在python3下运行
import sys
import os

PATHNAME=''

'''
获取当前执行路径
'''
def cur_file_dir():
    global PATHNAME
    path= sys.path[0]
    #如果是脚本文件，返回脚本所在目录
    #如果是py2exe编译后的文件，返回便以后的文件路径
    if os.path.isdir(path):
        PATHNAME = path
    elif os.path.isfile(path):
        PATHNAME = os.path.dirname(path)

'''
获取路径下的文件名
return list
'''
def listfiles(path):
    files=[]
    filenum=0
    #获取路径下的所有文件和文件夹 的名字
    list=os.listdir(path)
    for line in list:
        filepath=os.path.join(path,line)
        #print filepath
        if os.path.isfile(filepath):
            #print line
            #print filepath
            files.append(line)
    return files


'''
增量
'''
def incremental(strPk,str,filename):
    end_str=")PARTITION BY RANGE COLUMNS(ext_date_time)(PARTITION p201711 VALUES LESS THAN ('2017-11-01 00:00:00'),PARTITION p201712 VALUES LESS THAN ('2017-12-01 00:00:00'),PARTITION p201801 VALUES LESS THAN ('2018-01-01 00:00:00'),PARTITION p201802 VALUES LESS THAN ('2018-02-01 00:00:00')"

    keyseq='KEY `KEY_SEQ` (`EXT_OGG_SEQ`)'
    keyid=' KEY `key_id` (`EXT_rowid`)'
    result_ogg_filename=filename[:-4]+'_ogg.sql'
    print ('处理增量数据:{}'.format(filename))

    sFile=open(r''+PATHNAME+'/source/'+filename,'r')
    rFile=open(r''+PATHNAME+'/result/'+result_ogg_filename,'w')
    sFileList=sFile.readlines()
    flag=False
    for sFileLine in sFileList :
        if not sFileLine:
            break
        #if 'DROP TABLE IF EXISTS' in sFileLine :
        #    rFile.write(sFileLine[:-4]+'_ogg`;')
        #    continue
        #if 'drop table if exists' in sFileLine :
        #    rFile.write(sFileLine[:-3]+'_ogg;')
        #    continue
        #if 'create table' in sFileLine :
        #    rFile.write(sFileLine[:-2]+'_ogg')
        #    continue
        #if  'CREATE TABLE' in sFileLine:
        #    #用于sgrdb导出格式的sql文件
        #    rFile.write(sFileLine[:-5]+'_ogg'+sFileLine[-5:])
        #    continue
        if 'primary key' in sFileLine:
            rFile.write(strPk+','+sFileLine[:16]+'EXT_Date_Time,'+'EXT_ogg_seq,'+sFileLine[16:]+','+keyseq)
            flag=True
            continue
        if 'PRIMARY KEY' in sFileLine:
            rFile.write(strPk+','+sFileLine[:15]+'EXT_Date_Time,'+'EXT_ogg_seq,'+sFileLine[15:]+','+keyseq)
            flag=True
            continue
        elif flag==True:
            rFile.write(end_str+sFileLine)
            flag=False
            continue
        elif ');' in sFileLine:
            rFile.write(str+end_str+sFileLine)
            continue
        elif 'CHARSET=utf8;' in sFileLine:
            rFile.write(str+end_str+sFileLine)
            continue
        else:
            rFile.write(sFileLine)

    log='处理完成'
    print(log)
    sFile.close()
    rFile.close


'''
#全量
'''

def total(strPk,str,filename):
    keyseq='KEY `key_seq` (`EXT_ogg_seq`)'
    keyid=' KEY `key_id` (`EXT_rowid`)'
    keytime=' KEY `key_time` (`EXT_Date_Time`)'
    
    print('处理全量数据:{}'.format(filename))

    sFile=open(r''+PATHNAME+'/source/'+filename,'r')
    rFile=open(r''+PATHNAME+'/result/'+filename,'w')

    sFileList=sFile.readlines()
    flag=False
    for sFileLine in sFileList:
        if not sFileLine:
            break
        if 'primary key' in sFileLine:
            #rFile.write(strPk+','+sFileLine+','+keyseq+','+keytime+'\n')
            #rFile.write(strPk+','+sFileLine+','+keyseq+'\n')
            rFile.write(strPk+','+sFileLine+','+keyseq+','+keytime+'\n')
            flag=True
            continue
        if 'PRIMARY KEY' in sFileLine:
            #rFile.write(strPk+','+sFileLine+','+keyseq+','+keytime+'\n')
            rFile.write(strPk+','+sFileLine+','+keyseq+','+keytime+'\n')
            flag=True
            continue
        elif flag==True:
            rFile.write(sFileLine)
            flag=False
            continue
        elif ');' in sFileLine:
            rFile.write(str+sFileLine)
            continue
        elif 'CHARSET=utf8;' in sFileLine:
            rFile.write(str+sFileLine)
            continue
        else:
            rFile.write(sFileLine)
        
    log='处理完成'
    print(log)
    sFile.close()
    rFile.close


if __name__ == '__main__':
    
    #strPk='EXT_Src_System VARCHAR(128),EXT_Valid_Flag VARCHAR(128),EXT_Provincial_Flag VARCHAR(128),EXT_Reserve1 TEXT,EXT_Reserve2 TEXT,EXT_Reserve3 TEXT,informatica_row_id VARCHAR(20),informatica_flag VARCHAR(10),informatica_date_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,informatica_ogg_seq BIGINT(20) NOT NULL AUTO_INCREMENT'
    '''
    strPk='\
    informatica_row_id VARCHAR(20),\
    informatica_flag VARCHAR(10),\
    informatica_date_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
    EXT_flag varchar(20),\
    EXT_Src_System VARCHAR(128),\
    EXT_Valid_Flag VARCHAR(128),\
    EXT_Provincial_Flag VARCHAR(128),\
    EXT_Reserve1 VARCHAR(128),\
    EXT_Reserve2 VARCHAR(128),\
    EXT_Reserve3 VARCHAR(128),\
    EXT_oper_count int,\
    EXT_rowid varchar(20),\
    informatica_ogg_seq BIGINT(20) NOT NULL AUTO_INCREMENT\' 
    '''

    strPk='\
        `INFORMATICA_row_id` varchar(20) DEFAULT NULL,\
        `INFORMATICA_flag` varchar(10) DEFAULT NULL,\
        `INFORMATICA_date_time` datetime NOT NULL,\
        `EXT_Date_Time` datetime NOT NULL,\
        `EXT_FLAG` varchar(20) DEFAULT NULL,\
        `EXT_SRC_SYSTEM` varchar(20) DEFAULT NULL,\
        `EXT_VALID_FLAG` varchar(20) DEFAULT NULL,\
        `EXT_PROVINCIAL_FLAG` varchar(5) DEFAULT NULL,\
        `EXT_RESERVE1` text,\
        `EXT_RESERVE2` text,\
        `EXT_RESERVE3` text,\
        `EXT_oper_count` int,\
        `EXT_rowid` varchar(20),\
        `EXT_ogg_seq` bigint(20) NOT NULL AUTO_INCREMENT'

    q_strPk='`INFORMATICA_row_id` varchar(20) DEFAULT NULL,\
        `INFORMATICA_flag` varchar(10) DEFAULT NULL,\
        `INFORMATICA_date_time` datetime NOT NULL,\
        `EXT_Date_Time` datetime NOT NULL,\
        `EXT_flag` varchar(20) DEFAULT NULL,\
        `EXT_Src_System` varchar(20) DEFAULT NULL,\
        `EXT_Valid_Flag` varchar(20) DEFAULT NULL,\
        `EXT_Provincial_Flag` varchar(5) DEFAULT NULL,\
        `EXT_Reserve1` text,\
        `EXT_Reserve2` text,\
        `EXT_Reserve3` text,\
        `EXT_oper_count` int, \
        `EXT_rowid` varchar(20), \
        `EXT_ogg_seq` bigint(20) NOT NULL AUTO_INCREMENT'

    #str=',EXT_Src_System VARCHAR(128),EXT_Valid_Flag VARCHAR(128),EXT_Provincial_Flag VARCHAR(128),EXT_oper_count INT NOT NULL,EXT_rowid VARCHAR(20),EXT_Reserve3 TEXT,informatica_row_id VARCHAR(20),informatica_flag VARCHAR(10),informatica_date_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,informatica_ogg_seq BIGINT(20) NOT NULL AUTO_INCREMENT, PRIMARY KEY `informatica_ogg_seq` (`informatica_ogg_seq`), KEY `informatica_ogg_seq` (`informatica_ogg_seq`), KEY `EXT_rowid` (`EXT_rowid`)'
    '''
    str=',\
    informatica_row_id VARCHAR(20),\
    informatica_flag VARCHAR(10),\
    informatica_date_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
    EXT_flag varchar(20),\
    EXT_Src_System VARCHAR(128),\
    EXT_Valid_Flag VARCHAR(128),\
    EXT_Provincial_Flag VARCHAR(128),\
    EXT_Reserve1 VARCHAR(128),\
    EXT_Reserve2 VARCHAR(128),\
    EXT_Reserve3 VARCHAR(128),\
    EXT_oper_count int,\
    EXT_rowid varchar(20),\
    informatica_ogg_seq BIGINT(20) NOT NULL AUTO_INCREMENT,\
    PRIMARY KEY (`informatica_ogg_seq`,`informatica_date_time`),\
    KEY `key_seq` (`informatica_ogg_seq`),\
    KEY `key_id` (`EXT_rowid`)'
    '''
    str='\
        ,`INFORMATICA_row_id` varchar(20) DEFAULT NULL,\
        `INFORMATICA_flag` varchar(10) DEFAULT NULL,\
        `INFORMATICA_date_time` datetime NOT NULL,\
        `EXT_Date_Time` datetime NOT NULL,\
        `EXT_flag` varchar(20) DEFAULT NULL,\
        `EXT_Src_System` varchar(20) DEFAULT NULL,\
        `EXT_Valid_Flag` varchar(20) DEFAULT NULL,\
        `EXT_Provincial_Flag` varchar(5) DEFAULT NULL,\
        `EXT_Reserve1` text,\
        `EXT_Reserve2` text,\
        `EXT_Reserve3` text,\
        `EXT_oper_count` int NOT NULL, \
        `EXT_rowid` varchar(20),\
        `EXT_ogg_seq` bigint(20) NOT NULL AUTO_INCREMENT,\
        KEY `key_seq` (`EXT_ogg_seq`),\
        PRIMARY KEY (`EXT_Date_Time`,`EXT_ogg_seq`),\
        KEY `key_id` (`EXT_rowid`)'

    q_str=',`INFORMATICA_row_id` varchar(20) DEFAULT NULL,\
        `INFORMATICA_flag` varchar(10) DEFAULT NULL,\
        `INFORMATICA_date_time` datetime NOT NULL,\
        `EXT_Date_Time` datetime NOT NULL,\
        `EXT_flag` varchar(20) DEFAULT NULL,\
        `EXT_Src_System` varchar(20) DEFAULT NULL,\
        `EXT_Valid_Flag` varchar(20) DEFAULT NULL,\
        `EXT_Provincial_Flag` varchar(5) DEFAULT NULL,\
        `EXT_Reserve1` text,\
        `EXT_Reserve2` text,\
        `EXT_Reserve3` text,\
        `EXT_oper_count` int NOT NULL,\
        `EXT_rowid` varchar(20),\
        `EXT_ogg_seq` bigint(20) NOT NULL AUTO_INCREMENT,\
        PRIMARY KEY (`EXT_Date_Time`, `EXT_ogg_seq`),\
        KEY `key_seq` (`EXT_ogg_seq`),\
        KEY `key_id` (`EXT_rowid`)'


    cur_file_dir()
    
    files_name=listfiles(PATHNAME+'/source')
    for filename in files_name:
        if not filename:
            break
        print ('正在处理:{}'.format(filename))
        #增量
        incremental(strPk,str,filename)
        #全量
        #total(q_strPk,q_str,filename)

