# Procedimiento para la descarga de datos de SIRH (Sistema de Información del Recurso Hídrico) #

Fecha: Agosto de 2020

Procedimiento desarrollado en Python para consultar y descargar el API REST expuesto por el Ideam para la información del SIRH.
Los datos son descargados en formatos CSV (comma separated values). (Depreciado)
Los datos son descargados en una File GDB conforme la estructura de todos los servicios expuestos, realizando el proceso de espacialización de aquellas entidades espaciales (feature classes)

1. usuarios
1. predios
1. concesiones (feature class)
1. captaciones (feature class)
1. usos
1. muestras
1. mediciones
1. permisosvertimiento 
1. puntosvertimiento (feature class)
1. puntosmonitoreo (feature class)
1. funias
1. pueas
1. fuentes