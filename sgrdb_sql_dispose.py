import sys

import re


def dispose(file_name):

    #new_file_name='new_'+file_name.split('\\')[-1]
    new_file_name=file_name.replace('.sql','_new')+'.sql'
    p = re.compile("ENGINE(\s?)=(\s?)TokuDB")
    count=0
    with open(file_name,'r') as f:
        with open(new_file_name,'w') as w:
            sql=f.readlines()
            for line in sql:
                count+=1
                if 'use' in line or 'USE' in line:
                    for sql_line in sql[count:]:
                        w.write(p.sub('ENGINE=InnoDB',sql_line))
                    return
        #print(sql[5:])

def dispose_run():
    '''
    更改建表语句
    删除use 行及上面的内容
    更改引擎类型
    :return:
    '''
    print('输入Q/q结束')
    while(True):
        file=input('请输入文件名（全路径）：')
        if file.lower() =='q':
            print('程序退出。。。。')
            break
        dispose(file)


def get_ctsqls(file_name):
    ctsqls_dic=dict()
    p=re.compile("^(\s*`)")
    with open(file_name, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if 'create table' in line.lower():
                table_name=line.split('`')[1]
                continue
            if p.match(line):
                field=line.lstrip().split(' ')[0]
                f_type=line.lstrip().split(' ')[1]
                lst=[]
                lst.append(field)
                lst.append(f_type)
                ctsqls_dic.setdefault(table_name,[]).append(lst)
            continue
    return ctsqls_dic

from xlrd import open_workbook
from xlutils.copy import copy

def write_excel(model_file,out_file,dic):
    with open_workbook(model_file) as workbook:
        worksheet=workbook.sheet_by_name('Sheet1')
        wk=copy(workbook)
        st=wk.get_sheet(0)
        row_index=1
        for v,k in dic.items():
            for field_content in k:
                if field_content !=None:
                    st.write(row_index,0,label=v)
                    st.write(row_index,1,label=field_content[0].replace('`',''))
                    st.write(row_index,2,label=field_content[1].split('(')[0].replace(',\n',''))
                    st.write(row_index,3,label=field_content[1].replace(',\n',''))

                    row_index+=1
    wk.save(out_file)


def sgrdb_sql_excel():
    file_name=r'apsr3db.sql'
    mf=r'model.xlsx'
    of=r'test_model.xlsx'
    dic=get_ctsqls(file_name)
    if dic !=None:
        write_excel(mf,of,dic)








