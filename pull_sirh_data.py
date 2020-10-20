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
import time
from requests.exceptions import Timeout

url = "http://wssirh-qa.ideam.gov.co/API-SIRH/api/v1/"
#url = "http://186.155.208.228/API-SIRH/api/v1/"
base_path = "C:/ws/MADS/SIRH/pull_data/"

services = ["usuarios", "predios", "concesiones", "captaciones", "usos", "muestras", "mediciones","permisosvertimiento","puntosvertimiento","puntosmonitoreo","funias","pueas","fuentes"]
csv_ext = ".csv"

payload = "{\r\n    \"userName\":\"admin\",\r\n    \"password\":\"admin\"\r\n}\r\n"
headers = {
  'Content-Type': 'application/json'
}

urlAuth = url + "auth"
response = requests.request("POST", urlAuth, headers=headers, data = payload)
r = response.json();

token =  r.get('token')

urlAutoridades = url + "autoridades"

payload = {}
headers = {
  'Content-Type': 'application/json',
  'Authorization': token
}

response = requests.request("GET", urlAutoridades, headers=headers, data = payload)
autoridades = response.json().get('registros')

for autoridad in autoridades:
    print ("Autoridad : " + autoridad.get('SIGLA'))
    for s in services:
        urlService = url + s + "/" + autoridad.get('SIGLA')
        write_file = base_path + s + "_" + autoridad.get('SIGLA') +  csv_ext
        if not os.path.exists(write_file):
            print ("Executing URL : " + urlService)
            #time.sleep(5)
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
                
                
                with open(write_file,'w') as f:
                    writer = csv.writer(f, lineterminator = '\n')
                    writer.writerow(title_cols)
                    for row in rows:
                        #print(row.values())
                        writer.writerow([unicode(s).encode("utf-8") for s in row.values()])
                        #writer.writerow(row.values().encode('utf-8').strip())
            else:
                print ("No Data in service ")
        else:
            print("File exist dta downloaded previously")
            
                    
            #print (response.text.encode('utf8'))

