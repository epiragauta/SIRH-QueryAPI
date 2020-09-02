##################################################################
#   Descarga de datos de SIRH/api/v1/
#    
#   Prerequisitos: Python 2.7 o superior
#    
#  Este procedimiento toma la URL de los servicios REST de SIRH
#  Realiza la autenticación para obtener el Token   
#  
#
##################################################################
import requests
import csv
import os

url = "http://wssirh-qa.ideam.gov.co/API-SIRH/api/v1/"
base_path = "C:/ws/MADS/SIRH/pull_data/"

services = ["usuarios", "predios", "concesiones", "captaciones", "usos", "muestras", "mediciones","permisosvertimiento","puntosvertimiento","puntosmonitoreo","funias","pueas","fuentes"]
csv_ext = ".csv"

payload = "{\r\n    \"userName\":\"******\",\r\n    \"password\":\"******\"\r\n}\r\n"
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
            response = requests.request("GET", urlService, headers=headers, data = payload)
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

