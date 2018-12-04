import requests#引入网络请求库
import pymysql#隐去数据库操作库
import time#引入系统时间库
import bs4#引入字符处理库
from bs4 import BeautifulSoup

#定义主函数
def main():
    #模拟谷歌浏览器
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3537.111 Safari/538.36'}
    #数据库连接字符串
    mysqldb = pymysql.connect("127.0.0.1","root","mysqlroot","house",3306)
    #开始连接
    cursor = mysqldb.cursor()
    #总页数
    page_max = 70
    #获取系统当前时间，用来记录爬取数据的日期
    DatetimeNow = time.strftime('%Y.%m.%d',time.localtime(time.time()))
    #开始循环访问徐州赶集二手房页面
    for i in range(1, int(page_max) + 1):
        house = 'http://xuzhou.ganji.com/wblist/ershoufang/pn'+str(i)
        #house = 'http://nj.ganji.com/wblist/ershoufang/pn'+str(i)
        #输出当前访问的页面地址
        print(house)
        try:
            #发送访问请求
            res = requests.get(house, headers=headers)
            #格式化请求到的页面
            soup = BeautifulSoup(res.text, 'html.parser')
            #根据手动分析页面数据得出：列表在标签为div class名称为f-list js-tips-list的节点内的标签名称为dl class名称为f-list-item-wrap f-clear的节点内
            #拿到当前页面中的所有二手房节点
            List_All_House = soup.find("div",class_='f-list js-tips-list').find_all("dl",class_='f-list-item-wrap f-clear')
        except Exception as e:
            print(e)
        #循环每一个节点
        for One in List_All_House:
            try:
                #提取数据对象
                T_header = One.find("dd",class_='dd-item title').find("a",class_='js-title value title-font')
                T_url = T_header.attrs['href']
                T_size = One.find("dd",class_='dd-item size').find_all('span')
                T_address = One.find("dd",class_='dd-item address').find("span",class_='area')
                T_info = One.find("dd",class_='dd-item info')

                #提取具体数据值
                D_header = T_header.text
                D_price_all = T_info.find("div",class_='price').text
                D_price_sin = T_info.find("div",class_='time').text
                D_address_City = T_address.find("a",class_='address-eara').text
                D_address_Community = T_address.find("span",class_='address-eara').text
                for s in T_size:
                    T_size_temp = s.text
                    if T_size_temp.strip():
                        if '室' in T_size_temp:
                            D_size_HX = T_size_temp
                        if '㎡' in T_size_temp:
                            D_size_MJ = T_size_temp
                        if '层' in T_size_temp:
                            D_size_CS = T_size_temp
                        if '东' in T_size_temp or '西' in T_size_temp or '南' in T_size_temp or '北' in T_size_temp:
                            D_size_CX = T_size_temp
                #数据格式整理
                D_size_MJ = D_size_MJ.replace('㎡','')
                D_price_all = D_price_all.replace('万','')
                D_price_sin = D_price_sin.replace('元/㎡','')
                D_url = T_url
                #数据入库
                try:
                    #拼接入库脚本
                    sql = "INSERT INTO house.HouseDay (Nian,Yue,Ri,Zhou,Header,Address_Country,Address_City,Address_Communtiy,Size_Floor,Size_Direct,Size_Room,Size_Size,Price_All,Price_Sin,CreateDate,DataSource) "
                    sql = sql+ "VALUES("
                    sql = sql + "'2018' ,"
                    sql = sql + "'12' ,"
                    sql = sql + "'3' ,"
                    sql = sql + "'49' ,"
                    sql = sql + "'"+D_header+ "' ,"
                    sql = sql + "'徐州' ,"
                    sql = sql + "'"+D_address_City+ "' ,"
                    sql = sql + "'"+D_address_Community+ "' ,"
                    sql = sql + "'"+D_size_CS+ "' ,"
                    sql = sql + "'"+D_size_CX+ "' ,"
                    sql = sql + "'"+D_size_HX+ "' ,"
                    sql = sql + "'"+D_size_MJ+ "' ,"
                    sql = sql + "'"+D_price_all+ "' ,"
                    sql = sql + "'"+D_price_sin+ "' ,"
                    sql = sql + "'"+DatetimeNow+ "' ,"
                    sql = sql + " 'GanJi' "
                    sql = sql+ ")"
                    #执行入库脚本
                    cursor.execute(sql)
                    #脚本执行正常，提交数据库变更
                    mysqldb.commit()
                    #输出当前入库的数据标题
                    print(D_header)
                except Exception as e:
                    #抛出异常
                    print(e)
            except Exception as e:
                #抛出异常
                print(e)
    #断开数据库连接
    mysqldb.close()

#程序入口
if __name__ == '__main__':
    #调用主函数
    main()