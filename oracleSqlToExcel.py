import re

#从sql建表语句中提取字段信息

def get_ctsqls(file_name):
    ctsqls_dic=dict()
    p=re.compile("^(\s*\")")
    with open(file_name, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if 'create table' in line.lower():
                table_name=line.split('"')[1]
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
                    st.write(row_index,1,label=field_content[0].replace('"',''))
                    st.write(row_index,2,label=field_content[1].split('(')[0].replace(',\n',''))
                    st.write(row_index,3,label=field_content[1].replace(',\n',''))

                    row_index+=1
    wk.save(out_file)


def sgrdb_sql_excel():
    file_name=input("请输入sql文件全路径名：")
    mf='model.xlsx'
    of=input("保存的文件名：")
    dic=get_ctsqls(file_name)
    if dic !=None:
        write_excel(mf,of,dic)
if __name__ == '__main__':
    sgrdb_sql_excel()








