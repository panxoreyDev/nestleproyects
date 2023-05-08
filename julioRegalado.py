# -*- coding: utf-8 -*-
"""
Created on Wed May  3 15:42:58 2023

@author: MXRodrigEl1
"""

import pandas as pd
import os
import tkinter as tk
import numpy as np
from tkinter import filedialog, messagebox
import datetime
import time
from datetime import date

#TRICKS
root = tk.Tk()
root.withdraw()



#RUTAS
Master_upc_path = r"C:\Users\MXRodrigEl1\OneDrive - NESTLE\Bases Resurtido Soriana\Master_UPC.csv"
Catalogo_resurtible_path = r"C:\Users\MXRodrigEl1\Desktop\Nestlé\Catalogos\Catalogos SAP Soriana\Consolidado_Cat.csv"
Transitos_path = r"C:\Users\MXRodrigEl1\Desktop\Nestlé\Nivel de Servicio\OSIRIS\Tiendas.txt"

#VARIABLES GLOBALES
desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') 
vigencia = date(2023, 5, 30)
my_date = datetime.date.today()
print(vigencia)
print(my_date)



#DATA SETS IMPORTS
####################################################################################################################################################
def get_status(row):
    if row['Estat Mat en Centro'] == 'A' and row['Estatus Material Centro'] == 'A' and row['Caract.planif.nec.'] == 'Z1':
        return 'Resurtible'
    else:
        return 'No Resurtible'   

####################################################################################################################################################
def Importar_MasterUpc():
    global Master_UPC 
    Master_UPC = pd.read_csv(Master_upc_path,usecols=['Codigo',
                                                      'Cat_Vtas_Nes',
                                                      'MG_Nes',
                                                      'Desc'],encoding="latin1")
    #CONVIERTE A STRING LOS DATOS OBJECT PARA MERGE CON CAT
    Master_UPC = Master_UPC.astype({'Codigo':'string'})
    tipo_dato_MASTER = Master_UPC.dtypes
    print(tipo_dato_MASTER)
####################################################################################################################################################    
def Importar_catalogo():
    global Catalogo 
    Catalogo = pd.read_csv(Catalogo_resurtible_path,usecols=['EAN/UPC',
                                                      'Material',
                                                      'Estat Mat en Centro',
                                                      'Estatus Material Centro',
                                                      'Caract.planif.nec.',
                                                      'Centro'],encoding="latin1",low_memory=False)
    #CONVIERTE A STRING LOS DATOS OBJECT PARA MERGE CON CAT
    Catalogo['Centro'] = pd.to_numeric(Catalogo['Centro'], errors='coerce')
    
    #CONVERTIR A 0 LOS NAN Y LOS -INF & INF QUE SALIERON DE LA CONVERSION DEL CENTRO
    Catalogo = Catalogo.fillna(0)
    Catalogo = Catalogo.replace([np.inf, -np.inf], 0)
    
    #CONVIERTE LOS TIPOS DE DATO PARA HACERLOS COMPATIBLES CON OTROS
    Catalogo = Catalogo.astype({'EAN/UPC':'string','Estat Mat en Centro':'string','Estatus Material Centro':'string','Caract.planif.nec.':'string','Centro':'int64'})
    Dtype_Cat = Catalogo.dtypes
    print(Dtype_Cat)


    #LE AGREGA COLUMNA CON ID "CONCAT"
    Catalogo["ID_CAT"] = Catalogo["Centro"].map(str) + "-" + Catalogo["Material"].map(str)
    
    #CAMBIA NOMBRE DE CARACTPLANIFICACIÓN PARA QUITAR LOS PUNTOS Y EVITAR ERRORES TAMBIEN QUITA LOS ESPACIOS EN LOS OTROS CAMPOS
    Catalogo["CaracPlanNec"] = Catalogo["Caract.planif.nec."].map(str)
    
    #LE AGREGA COLUMNA STATUS A TRAVES DE UNA FUNCION MAMALONA ANTES HECHA "get_status"
    Catalogo['Status'] = Catalogo.apply(get_status, axis=1)
    
    #BORRA LAS COLMNAS INNECESARIAS DESPUES DE FILTRARLAS
    Catalogo = Catalogo.drop(['Estat Mat en Centro',
    'Estatus Material Centro',
    'Caract.planif.nec.',
    'CaracPlanNec'], axis=1)
    
    List_res = len(Catalogo.index)
    print(List_res )  
####################################################################################################################################################

def Consolidacion_35D():
    global Estructura_35
    global Ventas35D
    global  Venta_35D_Week
    #CREA LA CONSOLIDACION MEDIANTE UN BATCH
    
    path = filedialog.askdirectory()
    bat_path = os.path.join(path, path, "Concatenate Script.bat")
    ventas_path = os.path.join(path, path, "ventas.txt")
    myBat = open(bat_path,'w+')
    myBat.write('copy /b *.txt ventas.txt')
    myBat.close()
    path = path.replace("/", '\\')
    print(path)
    os.chdir(path)
    os.startfile("Concatenate Script.bat")
    time.sleep(5) #sleep for 1 sec
    #DECLARA VARIABLES
    #------------------------------------------------------------------------------------------------------------
    DIAS = 35
    Estructura_35 = 0
    #------------------------------------------------------------------------------------------------------------
    #IMPORTA EL ARCHIVO CONSOLIDADO DE VENTAS
    Ventas35D =  pd.read_csv(ventas_path, 
                             sep='!', 
                             encoding='utf-16',
                             header=0,
                             index_col=False,
                             low_memory=False
                             )
    #ASIGNA NOMBRE DE COLUMNAS
    Ventas35D.columns =['TDA', 'Departamento', 'UPC', 'FECHA','VTA','CPA','INV','VTA $','CPA $']
    #Crea ID
    Ventas35D["concat"] = Ventas35D["TDA"].map(str) + "-"+ Ventas35D["UPC"].map(str)
    #------------------------------------------------------------------------------------------------------------
    #CAMBIA EL TIPO DE DATO PARA CRUCES CON OTROS DF
    Ventas35D = Ventas35D.astype({'UPC':'string','FECHA':'int64','concat':'string'})
    #REVISA TIPO DE DATOS
    print(Ventas35D.dtypes)
    #SE CREA VARIABLE PARA CONEVRTIR Y AGREGAR SEMANA SELL OUT EN OTRA FUNCION
    Venta_35D_Week = Ventas35D
    hoy = date.today()
    #FILTRA SOLO LOS 35 DIAS
    Vta_Filtrada = Ventas35D[(Ventas35D.FECHA < int(hoy.strftime('%Y%m%d'))-35)]
    print(Vta_Filtrada)
    Ventas35D = Vta_Filtrada
    #CREA PIVOT DE LA SUMA DE VENTAS
    Piv_35d = pd.pivot_table(Ventas35D, 
                        values=['VTA','CPA','VTA $','CPA $'], 
                        index=['TDA', 'Departamento', 'UPC', 'concat'], 
                        aggfunc=np.sum)#Con sum, sumamos los valores de un campo
    #CONVIERTE EL OBJETO PIVOT A DF
    Piv_35d = Piv_35d.reset_index()

    #CREA PIVOT DE LOS NUMEROS DE FECHAS
    PIV_DIAS = pd.pivot_table(Ventas35D, 
                        values=['VTA'], 
                        index=['FECHA'], 
                        aggfunc=np.sum)
    print(PIV_DIAS)
    #CREA PIVOT DE INVENTARIOS CON BASE EN LAS FECHAS MAXIMAS
    PIV_FECHAS = pd.pivot_table(Ventas35D, 
                        values=['FECHA','INV'], 
                        index=['concat'], 
                        aggfunc=np.amax)#Con amax, sacamos el valor maximo de un campo
    #CONVIERTE EL OBJETO PIVOT A DF
    PIV_FECHAS = PIV_FECHAS.reset_index() 
    #------------------------------------------------------------------------------------------------------------
    #Se cruza Estructura vs ultimos inventarioa
    Estructura_35 = pd.merge(Piv_35d,
                 PIV_FECHAS,
                 left_on = 'concat',
                 right_on= 'concat',
                 how = 'left')
    #CONVIERTE EL OBJETO PIVOT A DF
    Estructura_35 = Estructura_35.reset_index() 
    #------------------------------------------------------------------------------------------------------------
    #CREA CAMPOS CALCULADOS
    Estructura_35["VTA_DIA"] = Estructura_35["VTA"]/DIAS
    Estructura_35["DOH"] = Estructura_35["INV"]/Estructura_35["VTA_DIA"]
    #------------------------------------------------------------------------------------------------------------
    #REEMPLAZA DATOS ERRONEOS INF Y -INF POR NAN
    Estructura_35.replace([np.inf, -np.inf], np.nan, inplace=True)
    #------------------------------------------------------------------------------------------------------------
    #REEMPLAZA LOS NAN POR 0 EN LA COLUMNA VTA DIA
    Estructura_35.fillna("0",inplace=True)
    print(Estructura_35)
    #ELIMINA BAT Y VENTAS TXT
    os.remove(bat_path)    
    os.remove(ventas_path) 
    del Venta_35D_Week
    del Ventas35D
####################################################################################################################################################

def Importar_Transitos():
    global Transitos
    Transitos = pd.read_csv(Transitos_path,sep='\t',encoding="latin1", low_memory=False)
    
    #ELIMINA FILA DE VALOR "====" PARA PODER CONVERTIR A NUMERO
    Transitos = Transitos.drop(Transitos[Transitos['Codigo de Barr'] == '=============='].index)
    
    #CONVIERTE LOS TIPOS DE DATO PARA HACERLOS COMPATIBLES CON OTROS
    Transitos = Transitos.astype({'Pedido    ':'string',
                                  'Fecha de Pedido     ':'string',
                                  'Codigo de Barr':'string',
                                  'UniPedidas                                           ':'float',
                                  'IDTie':'int64',
                                  'Cantidad Recibida en Tienda                          ':'float'})    
    #cambia el nombre para quitar espacios y lo asigna string
    Transitos["Pedido"] = Transitos["Pedido    "].map(str)
    Transitos["Fecha_Pedido"] = Transitos["Fecha de Pedido     "].map(str)
    Transitos["UniPedidas"] = Transitos["UniPedidas                                           "]
    Transitos["Cantidad_Rec_Tda"] = Transitos["Cantidad Recibida en Tienda                          "]
    
    #FILTRA SOLO LAS COLUMNAS A UTILIAR
    Transitos = Transitos[['Pedido', 'Fecha_Pedido','UniPedidas','Cantidad_Rec_Tda']]

    #REVISIÓN DE TIPO DE DATOS
    Dtype_Tran = Transitos.dtypes
    print(Dtype_Tran)





#AGREGAR TRANSITOS_CONFIRMADOS
#AGREGAR VENTA REFERENCIA
#AGREAGR NUEVOS_TRANSITOS
#TABLAS DE PEDIDOS JULIO REGALADO
#AGREGAR CALCULOS DE DDI FINALES CON VENTA NORMAL Y CON VENTA JULIO REGALADO
    
    
if (my_date < vigencia):
    print("letsgo")
    Importar_Transitos()
    #Importar_catalogo()
    #Importar_MasterUpc()
    #Consolidacion_35D()
    
    
else: 
    messagebox.showinfo(message="Vigencia del programa vencida", title="No se puede iniciar")
    print("Vigencia del programa vencida")
     
    

