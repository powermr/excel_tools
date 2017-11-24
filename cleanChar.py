from xlrd import open_workbook
from xlutils.copy import copy

def clean_LF(xls_file,sheet_name,save_name):
    clean_char=input('请输入要去除的字符：')
    c=input('请输入要处理的列（字母）：')
    list_num=ord(c.lower())-97
    with open_workbook(xls_file) as workbook:
        worksheet=workbook.sheet_by_name(sheet_name)
        wk=copy(workbook)
        ws=wk.get_sheet(0)
        for row_index in range(1,worksheet.nrows):
            row=worksheet.row_values(row_index)
            ws.write(row_index,list_num,label=row[list_num].replace(clean_char,''))
        wk.save(save_name)

if __name__ == '__main__':
    xls=input('请输入要处理的excel文件名（全路径）：')
    sheet=input('请输入sheet名：')
    save_file_name=input('请输入要保存的文件名：')
    clean_LF(xls,sheet,save_file_name)
