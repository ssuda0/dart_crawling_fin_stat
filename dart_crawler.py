import requests
from io import BytesIO

import pandas
import re

import os
import pymysql

import time
from random import randint

from web import test_requests_session

api_key = "2ba2bdab49db9c04ababf27b31ce1f8386599255"

################################################################################
#데이터 베이스 쿼리 실행
def execute_sql(sql):
    print(sql)
    connection  = pymysql.connect(host='192.168.56.101', user='dba', password='mysql', db='test', autocommit=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
    finally:
        connection.close()


################################################################################
#사업보고서 저장하기
def download_excel(period,rcp_no,dcm_no,company):
    url = "http://dart.fss.or.kr/pdf/download/excel.do?rcp_no={}&dcm_no={}&lang=ko".format(rcp_no,dcm_no)
    resp = test_requests_session(url)
        
    path = './data/'+company +'/'
    if not os.path.exists(path):
        os.mkdir(path)

    f = open(path + str(period) + company + '.xlsx', 'w')
    f.close()

    writer = pandas.ExcelWriter(path + str(period) + company + '.xlsx', engine = 'openpyxl')   
    for sheet in ["재무상태표","손익계산서", "포괄손익계산서","자본변동표", "현금흐름표"]:
        table = BytesIO(resp.content)
        try:
            data = pandas.read_excel(table, sheet_name=sheet, skiprows=5)
            data.to_excel(writer, sheet, encoding="euc-kr")
        except:
            print("error downloading " + str(period) + company+ sheet)
            continue
    try : 
        writer.save()
        writer.close()
    except :
        print("there is no visible sheet")

#한 회사의 사업보고서 저장하기.
def download_corp_excel(corpname, fin_state_list):
    for period, rcp_no, dcm_no in fin_state_list:
        print(period, rcp_no, dcm_no)
        download_excel(period, rcp_no, dcm_no, corpname)
        rand_value = randint(1, 10)
        time.sleep(rand_value)
        


################################################################################
#kospi200 회사 이름, 종목 코드 db에 저
def save_corpcode_to_db(corpinfo):
    for corpcode, corpname in corpinfo.items() :    
        sql = "INSERT INTO " + "company_info(company_id, company_name)" + " VALUES (\"" + str(corpcode) + "\", \"" + corpname + "\")"
        execute_sql(sql)


#kospi200 회사 이름, 종목 코드 가져오기
def get_kospi200_corpcode():
    data = pandas.read_excel('상장법인목록.xlsx', dtype = {'회사명':str, '종목코드':str})
    corpname = data['회사명']
    corpcode = data['종목코드']
    return dict(zip(corpcode, corpname))
        
###################################################################################

sheet_name_list = ['재무상태표','손익계산서','현금흐름표']

table_name_list = {'재무상태표':['자산', '부채', '자본'],
            '손익계산서':['영업','금융','기타','순이익'],
            '현금흐름표':['개별 현금 흐름', '통합 현금 흐름']}

attribute_name_list = {'자산' :['유동자산', '비유동자산', '자산총계'],
            '부채':['유동부채', '비유동부채','부채총계'],
            '자본' :['자본금', '주식발행초과금', '이익잉여금', '기타자본항목', '자본총계', '자본과부채총계'],
            '영업':['영업이익','매출액', '매출원가', '매출총이익', '판매비와관리비'],
            '금융':['금융수익','금융비용'],
            '기타':['기타수익','기타비용'],
            '순이익':['법인세비용차감전순이익', '법인세비용', '당기순이익', '주당이익'],
            '개별 현금 흐름':['영업활동 현금흐름', '투자활동 현금흐름', '재무활동 현금흐름', '외화환산으로 인한 현금의 변동'],
            '통합 현금 흐름' : ['현금및현금성자산의 순증감', '기초의 현금및현금성자산', '기말의 현금및현금성자산']
            }

attribute_name_list_english = {
    '자산' :'assets', '부채':'liabilities', '자본':'capital',
    '영업' : 'operating', '금융' :'financial','기타' : 'other', '순이익' :'net_profit',
    '개별 현금 흐름' : 'individual_cash_flow', '통합 현금 흐름':'integrated_cash_flow'
    }
         

def save_extracted_fin_state_to_db(extracted_data):
    for key, value_list in extracted_data.items():
        values = ', '.join(str(val) for val in value_list)
        sql = "INSERT INTO " + key + " VALUES ( " + values + ")"
        execute_sql(sql)

def extract_fin_state(pk, file_name, dcm_no):
    def find_attribute_idx(attribute_name):
        val = ""
        for i in data.index:
            if attribute_name in i:
                val = i
        return val
    
    data_extracted = {}
    for sheet_name in sheet_name_list :
        data = pandas.read_excel(file_name, header = 1, sheet_name = sheet_name, index_col=1)
        
        for table_name in table_name_list[sheet_name] :    
            table_data_list = [pk, "\""+str(dcm_no)+"\""]
            for attribute_name in attribute_name_list[table_name]:
                idx = find_attribute_idx(attribute_name)
                if idx == "" :
                    table_data_list.append(0)
                elif data[idx:idx][data.columns[1]][0] == " ":
                    table_data_list.append(0)
                else:
                    table_data_list.append(data[idx:idx][data.columns[1]][0])
            data_extracted[attribute_name_list_english[table_name]] = table_data_list
    return data_extracted    
    
def find_file_path_dcm_no(report_nm, file_list):
    for file_name in file_list:
        if file_name.find(report_nm)!= -1:
            return file_name
    return " "

def get_corp_file_list(corpname):
    file_path = "./data_five_corp/" + corpname + "/"
    file_list = os.listdir(file_path)
    return file_list

################################################################################
#엑셀로부터 가져온 rcp_no, period, dum_no정보를 db에 저장하기
def save_fin_tate_info_to_db(data):
    for rcept_no, company_id, report_nm, dcm_no in data:
        value = "(\"" + rcept_no + "\",\"" + company_id + "\",\"" + report_nm + "\")"
        sql = "INSERT INTO fin_state_rcept_info VALUES " + value
        execute_sql(sql)
        
        value = "(\"" + dcm_no + "\",\"" + rcept_no + "\")"
        sql = "INSERT INTO fin_state_dcm_info VALUES " + value
        execute_sql(sql)

#rcp_no, period, dum_no정보를 엑셀파일로부터 가져오기
def get_fin_state_info(corpname):
    file_name = './data_five_corp/' + corpname + '/' + corpname + '.xlsx'
    data = pandas.read_excel(file_name,  dtype = {'rcept_no':str, 'company_id':str, 'report_nm':str,'dcm_no':str})
    return zip(data['rcept_no'], data['company_id'],data['report_nm'], data['dcm_no'])

#회사의 rcp_no, period, dum_no를 dart 홈페이지로부터 가져오기
def get_fin_state_list(corpcode):
    corp_code = corpcode
    url = 'https://opendart.fss.or.kr/api/list.xml?crtfc_key={}&corp_code={}&bgn_de=19990101&pblntf_detail_ty=A001&pblntf_detail_ty=A002&pblntf_detail_ty=A003&page_count=100'.format(api_key,corp_code)    
    
    resp = requests.get(url)
    webpage = resp.content.decode('UTF-8')
    rcp_no_list = re.findall(r'<rcept_no>(.*?)</rcept_no>',webpage)
    period_list = re.findall(r'<report_nm>(.*?)</report_nm>',webpage)


    year_rcp_no_list = []
    year_period_list = []
    for period, rcp_no in zip(period_list,rcp_no_list):
        if '사업보고서' in period:
            year_rcp_no_list.append(rcp_no)
            word_idx = period.find('사업보고서')
            year_period_list.append(period[word_idx:word_idx+16])
    
    dcm_no_list = []
    for rcp_no in rcp_no_list:
        url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={}'.format(rcp_no)
        webpage = test_requests_session(url).text
        dcm_no = re.findall(r"{}', '(.*?)',".format(rcp_no),webpage)[0]
        dcm_no_list.append(dcm_no)

        
    fin_state_list = zip(year_period_list,year_rcp_no_list,dcm_no_list)
    return fin_state_list

def relatinoship_dcm_no_to_db(dcm_no):
    value = "(\"" + dcm_no + "\")"
    r_table_name_list = [
        "state_of_financial_position",
        "income_statement",
        "cash_flow_statement"
        ]
    for table_name in r_table_name_list :
        sql = "INSERT INTO " + table_name + "(dcm_no) VALUES " + value
        execute_sql(sql)

def relatinoship_pk_to_db(pk,dcm_no):
    table_name_list = [
        "state_of_financial_position",
        "income_statement",
        "cash_flow_statement"
        ]
    attribute_name_list = {
        "state_of_financial_position" : ["assets_id", "liabilities_id", "capital_id"],
        "income_statement" : ["operating_id", "other_id", "financial_id", "net_profit_id"],
        "cash_flow_statement" : ["individual_cash_flow_id", "integrated_cash_flow_id"]
        }
    
    for t_name in table_name_list :
        for a_name in attribute_name_list[t_name] : 
            sql = "UPDATE " + t_name + " SET " + a_name + "=" + str(pk) + " WHERE dcm_no=\"" + str(dcm_no) + "\""
            execute_sql(sql)
        

################################################################################
if __name__== "__main__":           
    # 전체 회사의 고유번호 받아오기
    corpinfo = get_kospi200_corpcode()
    
    #고유 번호 데이터베이스에 저장하기
    save_corpcode_to_db(corpinfo)
    
    
    
    #재무제표 리스트 엑셀파일로 받아오고 저장하기
    for corpcode, corpname in corpinfo.items() :        
        time.sleep(randint(1,6))
        fin_state_list = get_fin_state_list(corpcode)
    
    # 엑셀파일로부터 재무제표 리스트 가져와서 데이터베이스에 저장하기
    for corpcode, corpname in corpinfo.items() :
        data = get_fin_state_info(corpname)
        save_fin_tate_info_to_db(data)

    

    #전체 회사의 재무제표 데이터 엑셀파일로 받아오기
    for corpcode, corpname in corpinfo.items() :
        print("\n\nstart download excel file of ", corpname, corpcode)
        fin_state_list = get_fin_state_list(corpcode)         
        download_corp_excel(fin_state_list, corpname)
        print('successfully download excel file of ', corpname, corpcode)
        break

        
    #dcm_no 가져와서 relationship의 dcm_no에 저장하기
    for corpcode, corpname in corpinfo.items() :
        fin_state_info = get_fin_state_info(corpname)
        for rcept_no, company_id, report_nm, dcm_no in fin_state_info:
            relatinoship_dcm_no_to_db(dcm_no)
    
    idx = 0
    # 재무제표 db에 올리기
    for corpcode, corpname in corpinfo.items() :
        file_list = get_corp_file_list(corpname)
        fin_state_info = get_fin_state_info(corpname)
        for rcept_no, company_id, report_nm, dcm_no in fin_state_info:
            file_name = find_file_path_dcm_no(report_nm, file_list)
            if file_name == " " :
                print("there is no file\n\n" + report_nm + corpname)
            else :
                print("\n\n save file " + report_nm + " " +corpname + " to db")
                idx = idx+1
                file =  "./data_five_corp/" + corpname + "/" + file_name
                extracted_data = extract_fin_state(idx, file, dcm_no)
                save_extracted_fin_state_to_db(extracted_data)
                print("successfully save file " + report_nm +  " " + corpname + " to db\n\n")
                relatinoship_pk_to_db(idx, dcm_no)

            
