# -*- coding: utf-8 -*-
"""
Created on Fri Jun 24 14:22:29 2022

@author: MXRodrigEl1
"""



import pandas as pd
import os
import tkinter as tk
import datetime
from datetime import date
from tkinter import filedialog, messagebox
import time
import numpy as np

#ASIGNA VARIBLE PATH Y EL NOMRBE DEL ICONO DE LA APLICACIÓN
vigencia = date(2023, 5, 30)
my_date = datetime.date.today()
print(vigencia)
print(my_date)
#ELIMINA LA VENTANITA CAGAPALOS
root = tk.Tk()
root.withdraw()
desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') 



Master_upc_path = r"C:\Users\MXRodrigEl1\OneDrive - NESTLE\Bases Resurtido Soriana\Master_UPC.csv"

def Importar_MasterUpc():
    global Master_UPC 
    #messagebox.showinfo(message="Ingrese Master UPC", title="Master Upc")
    #IMPORTA CSV DESDE EL DESKTOP
    #Path_Master_UPC = filedialog.askopenfile(initialdir = desktop,
                                          #title = "Importar Master UPC",
                                          #filetypes = (("Csv files",
                                           #             "*.csv*"),
                                            #           ("all files",
                                             #           "*.*")))
    Master_UPC = pd.read_csv(Master_upc_path,usecols=['Codigo',
                                                      'Cat_Vtas_Nes',
                                                      'MG_Nes',
                                                      'Desc'],encoding="latin1")
    #CONVIERTE A STRING LOS DATOS OBJECT PARA MERGE CON CAT
    Master_UPC = Master_UPC.astype({'Codigo':'string'})
    #tipo_dato_MASTER = Master_UPC.dtypes
    #print(tipo_dato_MASTER)
   
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
    time.sleep(1) #sleep for 1 sec
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
    #ASIGNA COLUMNAS
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
    

    
    

def SO_calweek():    
    global Piv_35dWeek
    # Convierte la columna 'FECHA' a datetime y obtén la semana del año calendario    
    Venta_35D_Week['FECHA'] = pd.to_datetime(Venta_35D_Week['FECHA'], format='%Y%m%d')
    Venta_35D_Week['SEMANA'] = Venta_35D_Week['FECHA'].dt.week    
    Piv_35dWeek = pd.pivot_table(Venta_35D_Week, 
                        values=['VTA','CPA','VTA $','CPA $'], 
                        index=['UPC','SEMANA'], 
                        aggfunc=np.sum)#Con sum, sumamos los valores de un campo
    #CONVIERTE EL OBJETO PIVOT A DF
    Piv_35dWeek = Piv_35dWeek.reset_index()



def Merge_Detalle():
    global SOWeekDetalle
    Merge_SO_Master = pd.merge(Piv_35dWeek,
                               Master_UPC,
                     left_on = 'UPC',
                     right_on= 'Codigo',
                     how = 'left')  
    Piv_Merge = pd.pivot_table(Merge_SO_Master, 
                            values=['VTA'], 
                            index=['SEMANA','UPC','Desc','MG_Nes','Cat_Vtas_Nes'], 
                            aggfunc=np.sum)    
    SOWeekDetalle = Piv_Merge.reset_index() #Convierte a DF el pivot






def messagebox1():        
    messagebox.showinfo(title="Dispersión creada", message='Se han realizado calculo de inventarios' '\n''Puede exportar el archico .csv.')

      

def exportar_data():
    data = [('csv', '*.csv')]
    Exp_Path = filedialog.asksaveasfile(filetypes=data, defaultextension=data, initialfile = "Dispersión")
    Estructura_35.to_csv(Exp_Path, line_terminator='\n',index=False,encoding="utf-16")
    
    



    
    
if (my_date < vigencia):
    print("letsgo")
    Consolidacion_35D()
    SO_calweek()
    Importar_MasterUpc()
    Merge_Detalle()
    messagebox1()
else: 
    messagebox.showinfo(message="Vigencia del programa vencida", title="No se puede iniciar")
    print("Vigencia del programa vencida")
    
    
del Estructura_35
del Master_UPC
del Piv_35dWeek
del Venta_35D_Week
del Ventas35D




#FALTA REVISAR PRO QUE SOLO PUEDO TOMAR EN CUENTA A PARTIR DE LOS 35 DIAS HACIA ATRAS
