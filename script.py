# In[1]:

#BAJAR VARIOS CSVS Y HACER MERGE EN UN MASTER (UNO POR DIA)

import requests
import csv
from datetime import date, timedelta
from collections import OrderedDict
import glob
import pandas as pd
import pymysql
from sqlalchemy import create_engine


# In[2]:

yesterday = date.today() - timedelta(1)

api_token=''
fromdate=yesterday.strftime('%Y-%m-%d')
todate=yesterday.strftime('%Y-%m-%d')
params={'api_token':api_token,'from':fromdate,'to':todate}

iOS = ''
Android = ''

url_appevents_Android='https://hq.appsflyer.com/export/'+Android+'/in_app_events_report/v5'
url_installs_Android='https://hq.appsflyer.com/export/'+Android+'/installs_report/v5'
#no hay datos url_appevents_iOS='https://hq.appsflyer.com/export/'+iOS+'/in_app_events_report/v5'
url_installs_iOS='https://hq.appsflyer.com/export/'+iOS+'/installs_report/v5'


# In[3]:

def reporte(tiporeporte,nombre):
    data = requests.get(tiporeporte,params=params).content
    with open(nombre+'.csv', 'wb') as f:
        f.write(data)


# In[4]:

def mergecsv():
    filenames = glob.glob(fromdate+'_reporte*.csv')  #testeando dia de ayer
    df = pd.DataFrame()
    for filename in filenames:
        df = df.append(pd.read_csv(filename))
    # df is now a dataframe of all the csv's in filenames appended together   
    df.to_csv(fromdate+'_master.csv')
    


# In[5]:

def subirmysql():
    nombre_csv = fromdate+'_master.csv'
    separador = ','
    nombre_tabla = ''

    #si se toman sólo algunas columnas, especificar en forma de lista. 
    #Si se toman todas las columnas poner ''.
    columnas = ['Attributed Touch Type', 'Event Time', 'Event Name', 'Partner', 'Media Source', 'Channel', 'City', 'Platform', 'Device Type'] 

    if columnas == '':
       data = pd.read_csv(nombre_csv, sep = separador, error_bad_lines=False, low_memory=False, encoding = "ISO-8859-1")
    else:
       data = pd.read_csv(nombre_csv, sep = separador, error_bad_lines=False, low_memory=False, encoding = "ISO-8859-1")[columnas]

    cnx = create_engine('mysql+pymysql://user:pass@direccion:3306/db', echo=False)
    
    
    data.to_sql(name= nombre_tabla, con=cnx, if_exists = 'append', index=False)
    
    

    


# In[6]:

def main():    
    reporte(url_appevents_Android,fromdate+'_reporte_events-android')
    reporte(url_installs_Android,fromdate+'_reporte_installs-android')
   #no hay datos reporte(url_appevents_iOS,fromdate+'_reporte_events-ios')
    reporte(url_installs_iOS,fromdate+'_reporte_installs-ios')
    mergecsv()
    subirmysql()


# In[7]:

if __name__ == '__main__':
    main()   

