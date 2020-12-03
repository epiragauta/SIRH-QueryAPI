##################################################################
#   Descarga de datos de SIRH/api/v1/
#    
#   Prerequisitos: Python 2.7 o superior
#    
#  Este procedimiento toma la URL de los servicios REST de SIRH
#  Realiza la autenticacion para obtener el Token   
#  
#
##################################################################
import requests
import csv
import os
import calendar
import time
import shutil
import sys
import unicodedata

reload(sys)
sys.setdefaultencoding('utf8')

from osgeo import ogr
from requests.exceptions import Timeout


#url = "http://wssirh-qa.ideam.gov.co/API-SIRH/api/v1/"
#url = "http://186.155.208.228/API-SIRH/api/v1/"
url = "http://129.146.75.74:9073/API-SIRH/api/v1/"
base_path = "C:/ws/MADS/SIRH/pull_data/"
ts = str(calendar.timegm(time.gmtime()))
schema_gdb = "sirh_services_data_schema.gdb"
path_schema_gdb = base_path + schema_gdb
path_gdb = base_path + "sirh_data_" + ts + ".gdb"

shutil.copytree(path_schema_gdb, path_gdb)

services = ["usuarios", "predios", "concesiones", "captaciones", "usos", "muestras", "mediciones","permisosvertimiento","puntosvertimiento","puntosmonitoreo","funias","pueas","fuentes"]
csv_ext = ".csv"
GRADOS_LONG = 'GRADOS_LONG'
MINUTOS_LONG = 'MINUTOS_LONG'
SEGUNDOS_LONG = 'SEGUNDOS_LONG'

GRADOS_LAT = 'GRADOS_LAT'
MINUTOS_LAT = 'MINUTOS_LAT'
SEGUNDOS_LAT = 'SEGUNDOS_LAT'
CAUDAL_CONCESIONADO = 'CAUDAL_CONCESIONADO'
CAUDAL_ASIGNADO = 'CAUDAL_ASIGNADO'
CAUDALDISENO = 'CAUDALDISENO'
OFERTA_DISPONIBLE = 'OFERTA_DISPONIBLE'
OFERTA_HIDRICA_TOTAL = 'OFERTA_HIDRICA_TOTAL'
ALTITUD = 'ALTITUD'
fields_double = [SEGUNDOS_LONG,SEGUNDOS_LAT,CAUDAL_ASIGNADO, CAUDAL_CONCESIONADO,OFERTA_DISPONIBLE,CAUDALDISENO,OFERTA_HIDRICA_TOTAL,ALTITUD]

payload = "{\r\n    \"userName\":\"admin\",\r\n    \"password\":\"admin\"\r\n}\r\n"
headers = {
  'Content-Type': 'application/json'
}

driver = ogr.GetDriverByName("FileGDB")
ds = driver.Open(path_gdb,1)


urlAuth = url + "auth"
response = requests.request("POST", urlAuth, headers=headers, data = payload)
r = response.json();

token =  r.get('token')

urlAutoridades = url + "autoridades"

payload = {}
headers = {
  'Content-Type': 'application/json',
  'Sirh-Token': token
}

response = requests.request("GET", urlAutoridades, headers=headers, data = payload)
autoridades = response.json().get('registros')
#autoridades = filter(lambda s: s["SIGLA"] == "BARR_VERDE", autoridades)
#print (autoridades)
#services = ["captaciones"]

for autoridad in autoridades:
    print ("Autoridad : " , autoridad.get('SIGLA'))
    for s in services:
        urlService = url + s + "/" + autoridad.get('SIGLA')
        # write_file = base_path + s + "_" + autoridad.get('SIGLA') +  csv_ext
        #if not os.path.exists(write_file):
        print ("Executing URL : " , urlService)
        lyr_name = "sirh_" + s
        lyr = ds.GetLayerByName(lyr_name)
        try:
            response = requests.request("GET", urlService, headers=headers, data = payload, timeout=30)
        except Timeout:
            print("Response time too long")
        else:
            print("Request executed")
        r = response.json()
        rows = r.get('registros')
        
        if len(rows) > 0:
            title_cols = rows[0].keys()                
            featureDefn = lyr.GetLayerDefn()
            #lyr.StartTransaction()
            #with open(write_file,'w') as f:
                #writer = csv.writer(f, lineterminator = '\n')
                #writer.writerow(title_cols)
            
            i = 1
            for row in rows:
                #print ("inserting row : " , i)
                feature = ogr.Feature(featureDefn)
                if row.has_key(GRADOS_LAT) and row.has_key(GRADOS_LONG):
                    degrees = abs(row[GRADOS_LAT])
                    minutes = row[MINUTOS_LAT]
                    seconds = row[SEGUNDOS_LAT]
                    direction = ""
                    if row[GRADOS_LAT] < 0:
                        direction = "S"
                    ddy = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);
                    if direction == 'W' or direction == 'S':
                        ddy *= -1
                    
                    #print("field: " , GRADOS_LONG , " :: value: " , row[GRADOS_LONG])
                    degrees = abs(row[GRADOS_LONG])
                    minutes = row[MINUTOS_LONG]
                    seconds = row[SEGUNDOS_LONG]
                    direction = ""
                    if row[GRADOS_LONG]< 0:
                        direction = "W"
                    ddx = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);
                    if direction == 'W':
                        ddx *= -1
                    
                    point = ogr.Geometry(ogr.wkbPoint)
                    point.AddPoint(ddx, ddy)
                    feature.SetGeometry(point)
                    feature.SetField(SEGUNDOS_LAT, 1.1)
                    feature.SetField(SEGUNDOS_LONG, 1.1)
                i = i+1
                
                for f in title_cols:                            
                    #print ("validating field : " , f , "typeof f", type(f))
                    if (row.has_key(f)):
                        val = row[f]
                        str_field = unicodedata.normalize('NFKD', f).encode('ascii','ignore')
                        #print("insert in field: " , str_field , " :: value: " , val)
                        #try:
                        feature.SetField(str_field,val)
                                                   
                        #except:
                        #    print ("error inserting in field : " + f +" :: value: " + str(val).encode())
                    #else:
                    #    print ("field does not exist: " + f)
                #print ("creating feature")
                lyr.CreateFeature(feature)
                #print(feature.ExportToJson())
                feature = None
                    #print ("write row")
                    #writer.writerow([unicode(s).encode("utf-8") for s in row.values()])
            
            #ds = None
            #print ("saving data in: " + path_gdb)
        else:
            print ("No Data in service ")
        #else:
        #    print("File exist dta downloaded previously")
            
                    
            #print (response.text.encode('utf8'))


def dms2dd(degrees, minutes, seconds, direction):
    dd = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);
    if direction == 'W' or direction == 'S':
        dd *= -1
    return dd;
    