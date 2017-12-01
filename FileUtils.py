import os

def get_all_filePath(path):
    '''

    :param path: 要递归查询的路径
    :return: 返回文件绝对路径名的list
    '''
    pf_name=list()
    for i in os.walk(path):
        for filename in i[2]:
            pf_name.append(i[0]+'\\'+filename)
    return  pf_name


if __name__ == '__main__':
    import time
    start_time=time.time()

    #fn_list=get_all_filePath('d:\\')
    fn_list=get_all_filePath(input('请输入要查询的路径：'))

    print(len(fn_list))

    print(str(time.time()-start_time))
