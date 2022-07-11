#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
from datetime import datetime

engine = create_engine("mysql+pymysql://yusupov_av:password@10.167.128.42/yusupov_av")

readFileExcel = r"C:\Users\yav\Desktop\report - 2022-07-08T090812.271\exportFnsRegisterAndActs.xlsx"

excelFile = pd.ExcelFile(readFileExcel)
_Xls1 = excelFile.parse(sheet_name = "Данные", header=1, dtype=str)
_Xls2 = excelFile.parse(sheet_name = "Данные_1", header=1, dtype=str)
Xls = pd.concat([_Xls1, _Xls2], ignore_index=True)
Xls['C_CHARGE_RATE_SQUARE_METERS'] = Xls['C_CHARGE_RATE_SQUARE_METERS'].astype(float)
Xls['C_SHOPPING_ROOM_AREA'] = Xls['C_SHOPPING_ROOM_AREA'].astype(float)
Xls['C_QUARTER_FEE'] = Xls['C_QUARTER_FEE'].astype(int)
Xls['C_QUARTER_FEE_CALCULATING'] = Xls['C_QUARTER_FEE_CALCULATING'].astype(int)

Xls.to_sql('tc_1', if_exists='replace', con=engine)


# In[5]:


import pandas as pd
import sqlite3
from sqlite3 import OperationalError
from datetime import datetime as dt
from sqlalchemy import create_engine

print("====== [START EXPORT TC1] ======")

exportDir = 'Z:/ТС/Аналитика/Сотрудники/Медведев Р.А/EXPORT_DATA/'


conn = create_engine("mysql+pymysql://yusupov_av:password@10.167.128.42/yusupov_av")

actualTC1TableName = f"TC_1-{dt.now().strftime('%Y-%m-%d')}"
tc1Df = pd.read_sql("SELECT * FROM `tc_1`", con=conn)

tc1shortDf = pd.DataFrame()

tc1shortDf['C_IGNORING_TYPE'] = tc1Df['C_IGNORING_TYPE']
tc1shortDf['C_DOC_DATE'] = tc1Df['C_DOC_DATE']
tc1shortDf['C_MARK_NOTICE'] = tc1Df['C_MARK_NOTICE']
tc1shortDf['C_TAX_AUTORITY_CODE'] = tc1Df['C_TAX_AUTORITY_CODE']
tc1shortDf['C_BUILDING_CADASTRAL_NUMBER'] = tc1Df['C_BUILDING_CADASTRAL_NUMBER']
tc1shortDf['C_CHARGE_RATE_SQUARE_METERS'] = tc1Df['C_CHARGE_RATE_SQUARE_METERS']
tc1shortDf['C_OBJECT_ID'] = tc1Df['C_OBJECT_ID']
tc1shortDf['C_OBJECT_NAME'] = tc1Df['C_OBJECT_NAME']

tc1shortDf['C_OBJECT_TYPE'] = tc1Df['C_OBJECT_TYPE']
tc1shortDf['C_QUARTER_FEE'] = tc1Df['C_QUARTER_FEE']
tc1shortDf['C_QUARTER_FEE_CALCULATING'] = tc1Df['C_QUARTER_FEE_CALCULATING']
tc1shortDf['C_SHOPPING_ROOM_AREA'] = tc1Df['C_SHOPPING_ROOM_AREA']
tc1shortDf['C_TRADE_KIND'] = tc1Df['C_TRADE_KIND']
tc1shortDf['C_USE_OBJECT_EMERGENCE_DATE'] = tc1Df['C_USE_OBJECT_EMERGENCE_DATE']
tc1shortDf['C_STOP_USING_DATE'] = tc1Df['C_STOP_USING_DATE']
tc1shortDf['C_ACTIVITY_TERMINATION_DATE'] = tc1Df['C_ACTIVITY_TERMINATION_DATE']

tc1shortDf['C_STOP_USING_REASON'] = tc1Df['C_STOP_USING_REASON']
tc1shortDf['C_NOTIFICATION_ANNULMENT'] = tc1Df['C_NOTIFICATION_ANNULMENT']
tc1shortDf['C_ANNULMENT_CAUSE'] = tc1Df['C_ANNULMENT_CAUSE']
tc1shortDf['C_UNIQUE_TRADE_OBJECT_ID'] = tc1Df['C_UNIQUE_TRADE_OBJECT_ID']
tc1shortDf['C_PAYER_FEES_FAMILY'] = tc1Df['C_PAYER_FEES_FAMILY']
tc1shortDf['ACT_FIELDS'] = tc1Df['ACT_FIELDS']
tc1shortDf['ACT_NUMBER'] = tc1Df['ACT_NUMBER']

tc1shortDf['ACT_CANCELED'] = tc1Df['ACT_CANCELED']
tc1shortDf['C_COMPANY_NAME'] = tc1Df['C_COMPANY_NAME']
tc1shortDf['C_INN'] = tc1Df['C_INN']
tc1shortDf['ADDRESS_FIELDS'] = tc1Df['ADDRESS_FIELDS']
tc1shortDf['C_BUILDING'] = tc1Df['C_BUILDING']
tc1shortDf['C_CITY'] = tc1Df['C_CITY']
tc1shortDf['C_HOUSE'] = tc1Df['C_HOUSE']

tc1shortDf['C_LOCALITY'] = tc1Df['C_LOCALITY']
tc1shortDf['C_REGION'] = tc1Df['C_REGION']
tc1shortDf['C_ROOM'] = tc1Df['C_ROOM']
tc1shortDf['C_STREET'] = tc1Df['C_STREET']
tc1shortDf['C_ADMINISTRATIVE_DISTRICT'] = tc1Df['C_ADMINISTRATIVE_DISTRICT']
tc1shortDf['C_OKTMO'] = tc1Df['C_OKTMO']
tc1shortDf['C_REGISTRATION_DATE'] = tc1Df['C_REGISTRATION_DATE']
tc1shortDf['C_LOAD_DATE'] = tc1Df['C_LOAD_DATE']
tc1shortDf['C_SYNCHRONIZATION_DATE'] = tc1Df['C_SYNCHRONIZATION_DATE']
tc1shortDf['C_PLACEMENT_NTO_NUMBER_PERMITS'] = tc1Df['C_PLACEMENT_NTO_NUMBER_PERMITS']
tc1shortDf['(ИНФО) Дата Выгрузки'] = actualTC1TableName

tc1shortDf.to_excel(exportDir + "TC1_BACKUP/" + actualTC1TableName + '.xlsx', sheet_name='ТС1')
"""
with pd.ExcelWriter(exportDir + "TC1_BACKUP/" + actualTC1TableName + '.xlsx', engine='openpyxl', mode='a') as writer:
    infoPd = pd.DataFrame({"Признак": ['Дата Выгрузки'], "Значение": [actualTC1TableName]})
    infoPd.to_excel(writer, sheet_name='Информация')

    writer.save()

writer.close()
"""


print("====== [END EXPORT TC1] ======")


# # Для уведомлений

# In[ ]:


import pandas as pd
import sqlite3
from sqlite3 import OperationalError
from datetime import datetime as dt
from sqlalchemy import create_engine

with open('Z:\ТС\Сбор информации\Сотрудники\Андрей Ю\python\lists\login_pass_ip.txt', encoding='utf-8') as file:
    a = file.readlines()

conn = create_engine(f"mysql+pymysql://{a[0].strip()}:{a[1].strip()}@{a[2].strip()}/yusupov_av")

s = pd.read_sql("SELECT `INN`, `STREET`, `DATE_STOP_PATENT`, `DATE_START_PATENT`, `DATE_LOSS_PATENT`, `DATE_CESSATION_PATENT`, `DATE_STOP_USE_PATENT`, `HOUSE`, `KORP` FROM PSN WHERE DATE_START_PATENT > '2021-12-31'", con=conn)


# # Для патентов

# In[3]:


import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
engine = create_engine("mysql+pymysql://yusupov_av:password@10.167.128.42/yusupov_av")

readFileExcel = r"Z:\ТС\Аналитика\Текущая выгрузка\ПСН\Excel\20220630_PSN.xlsx"

excelFile = pd.ExcelFile(readFileExcel)
XLs = excelFile.parse(sheet_name = "Лист1", header=0, dtype=str)
XLs['DATE_START_PATENT'] = pd.to_datetime(XLs['ДатаНачПт']).dt.date
XLs['INN'] = XLs['ИННЮЛ']
XLs['STREET'] = XLs['Улица1']
XLs['HOUSE'] = XLs['Дом1']
XLs['KORP'] = XLs['Корпус1']
XLs['district'] = XLs['Район1']
XLs['city'] = XLs['Город1']
XLs['locality'] = XLs['НаселПункт1']

XLs['STREET1'] = XLs['Улица2']
XLs['HOUSE1'] = XLs['Дом2']
XLs['KORP1'] = XLs['Корпус2']
XLs['district1'] = XLs['Район2']
XLs['city1'] = XLs['Город2']
XLs['locality1'] = XLs['НаселПункт2']

XLs['DATE_STOP_PATENT'] = pd.to_datetime(XLs['ДатаКонПт']).dt.date
XLs['DATE_LOSS_PATENT'] = pd.to_datetime(XLs['ДатаУтрПСН']).dt.date
XLs['DATE_CESSATION_PATENT'] = pd.to_datetime(XLs['ДатаПрекрПСН']).dt.date
XLs['DATE_STOP_USE_PATENT'] = pd.to_datetime(XLs['ДатаПрекрПримПСН']).dt.date
XLs.to_sql('PSN', if_exists='replace', con=engine)


# # Для ЕСХН

# In[ ]:


engine = create_engine("mysql+pymysql://yusupov_av:password@10.167.128.42/yusupov_av")

readFileExcel = r"Z:\ТС\Аналитика\Текущая выгрузка\ЕСХН\20220324 ЕСХН.xlsx"

excelFile = pd.ExcelFile(readFileExcel)
XLs = excelFile.parse(sheet_name = "Список", header=0, dtype=str)
XLs['ДатаНачЕСХН'] = pd.to_datetime(XLs['ДатаНачЕСХН']).dt.date
XLs['ДатаКонЕСХН'] = pd.to_datetime(XLs['ДатаКонЕСХН']).dt.date
XLs.to_sql('ESHN', if_exists='replace', con=engine)


# In[ ]:


ecxn = pd.read_sql("SELECT `ИНН`, `ДатаНачЕСХН`, `ДатаКонЕСХН` FROM ESHN", con=conn)


# In[ ]:


ecxn


# In[ ]:




