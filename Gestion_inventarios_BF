# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 20:42:06 2023

@author: MXRodrigEl1
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 13:41:13 2023

@author: Isaias Rodriguez"""


import glob
import pandas as pd
import os
from tkinter import Tk, filedialog, messagebox
import numpy as np
import tkinter as tk
import datetime
import re
import time

start_time = time.time()



desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') 
root = tk.Tk()
root.withdraw()




# Lista para almacenar cada DataFrame de los archivos CSV
frames = []
    
Master_upc_path = r"C:\Users\MXRodrigEl1\OneDrive - NESTLE\Bases Resurtido Soriana\Master_UPC.csv"
BASEFFolder = r'C:\Users\MXRodrigEl1\Desktop\Nestlé\Dispersión Soriana\Fría\2023\CSV'

# Directorio que contiene los archivos CSV a consolidar
directory = BASEFFolder


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
    Master_UPC = pd.read_csv(Master_upc_path,usecols=['Codigo','Cat_Vtas_Nes','MG_Nes','Desc','Costo','UM_1','Resurtidor'],encoding="latin1")
    #CONVIERTE A STRING LOS DATOS OBJECT PARA MERGE CON CAT
    Master_UPC = Master_UPC.astype({'Codigo':'string'})
    tipo_dato_MASTER = Master_UPC.dtypes
    print(tipo_dato_MASTER)


def Import_BASEF():
    #messagebox.showinfo(message="Ingrese carpeta con archivos FCST ASL", title="Carpeta FCST ASL")
    #folders = filedialog.askdirectory() # Returns opened path as str    
    global BF_data
    path = BASEFFolder
    csv_files = glob.glob(path + "/*.csv")
    # Read each CSV file into DataFrame
    # This creates a list of dataframes
    df_list = (pd.read_csv(file, encoding="latin1", low_memory=False).assign(filename=file) for file in csv_files)
    # Concatenate all DataFrames
    BASEF_data_cons = pd.concat(df_list, ignore_index=True)


    Pivot_BF = pd.pivot_table(BASEF_data_cons,
                              values=['Venta35D','Inv Uds Res','Vta Dia','Tránsito Pz','Inventario BI','Faltante bi'],
                              index=['upc','filename','Estatus Final','Tienda Num'],
                              aggfunc=np.sum)
    

    
    #Convierte a DF el pivot
    BF_data = Pivot_BF.reset_index() 
    
    #CAMBIA EL NOMBRE DE COLUMNA tienda num A TIPO_RES PARA PODER FILTRAR
    BF_data['Tienda_Num'] = BF_data['Tienda Num']
    
    #CAMBIA EL NOMBRE DE COLUMNA "ESTATUS FINAL A TIPO_RES PARA PODER FILTRAR
    BF_data['Tipo_Res'] = BF_data['Estatus Final']
    #Cambia la columna de inventario para usar el data o el bi
    BF_data['Inv'] = BF_data['Inv Uds Res']
    #AGRGA COLUMNA UNIVERSO
    BF_data['Universo'] = 1
    
    #FILTRA SOLO TIENDAS SORIANA PARA QUITAR CITY 
    BF_data = BF_data[(BF_data.Tienda_Num < 1000) & (BF_data.Universo == 1)]
    
    #FILTRA SOLO RESURTIBLE
    BF_data = BF_data[(BF_data.Tipo_Res == "RESURTIBLE") & (BF_data.Universo == 1)]
    #EXTRAE NOMBRE DE ARCHIVO DE LA RUTA DESPUES DEL -
    BF_data['Semana'] = BF_data['filename'].str.split('-').str[-1]
    #EXTRAE SOLO LOS NUMEROS DEL NOMBRE DE ARCHIVO
    BF_data['Semana'] = BF_data['Semana'].apply(lambda x: float(re.findall('\d+\.*\d*', x)[0]) if re.findall('\d+\.*\d*', x) else None)
    #CONVIERTE A STRING upc PARA CRUCE CON MASTER
    BF_data = BF_data.astype({'upc':'string'})


    #Devuelve EL NOMBRE DE COLUMNA tienda 
    BF_data['Tienda Num'] = BF_data['Tienda_Num']

def Calculos():
    
    global BF_basic
    #CREAR DOH
    BF_data["DOH"] = BF_data["Inv"]/BF_data["Vta Dia"]
    #Crea calculo de inventarios negativos
    BF_data["INV_NEG"] = BF_data.apply(lambda x: 1 if x['Inv'] < 0
                                      else 0, axis=1)
    
    #Crea calculo de excedentes
    BF_data['>35'] = BF_data['DOH'].apply(lambda x: 1 if x > 35 else 0)
    
    #Crea calculo de exc
    BF_data['ExCrit'] = BF_data.apply(lambda x: 1 if x['DOH'] > 35 
                                      and x['Tránsito Pz'] > 0
                                      else 0, axis=1)
    
    #Crea calculo de Inv_SV
    BF_data['Inv_SV'] = BF_data.apply(lambda x: 1 if x['Vta Dia'] == 0                                       
                                      and x['Inv'] > 0
                                      else 0, axis=1)
    
    #Crea calculo de 0-3
    #BF_data['0-3'] = BF_data.apply(lambda x: 1 if x['DOH'] >= 0 
     #                                 and x['DOH'] < 3
      #                                and x['Inv'] >= 0
       #                               else 0, axis=1)
    
    BF_data['0-3'] = BF_data.apply(lambda x: 1 if (np.isinf(x['DOH'] and x['Inv'] <= 0) 
                                                   or np.isnan(x['DOH'] and x['Inv'] <= 0)
                                                   or (x['DOH'] == 0 and x['Inv'] <= 0) 
                                                   or (x['DOH'] > 0  and x['DOH'] < 3))
                                                   else 0, axis=1)
   
    
    
    
    #Crea calculo de 0-7
    BF_data['0-7'] = BF_data.apply(lambda x: 1 if x['DOH'] > 0 
                                      and x['DOH'] < 7
                                      else 0, axis=1)

    #Crea calculo de TripleCero
    BF_data['TripleCero'] = BF_data.apply(lambda x: 1 if x['Inv'] == 0 
                                      and x['Vta Dia'] == 0
                                      and x['Tránsito Pz'] == 0 else 0, axis=1)
    

    BF_basic = BF_data[['Tipo_Res','Vta Dia','Inv','Semana','upc',
                        'Tienda Num','Tránsito Pz','DOH','TripleCero','>35','0-3','0-7','Inv_SV','ExCrit','Faltante bi',
                        'INV_NEG','Universo']]    


    
    #REEMPLAZA -inf a 0 del data frame
    BF_basic = BF_basic.replace(-np.inf, 0)
    #REEMPLAZA inf a 0 del data frame
    BF_basic = BF_basic.replace(np.inf, 0)
    
    tipo_dato_bf = BF_basic.dtypes
    print(tipo_dato_bf)
    
    
def BF_Detalle():
    global BF_Detalle
    Merge = pd.merge(BF_basic,
                     Master_UPC,
                     left_on = 'upc',
                     right_on= 'Codigo',
                     how = 'left')
    Piv_Merge = pd.pivot_table(Merge, 
                            values=['INV_NEG','Inv','Vta Dia','Tránsito Pz','DOH','TripleCero','>35','Universo','0-3','0-7','Inv_SV','ExCrit'], 
                            index=['upc','Semana','Tipo_Res','Tienda Num','Desc','MG_Nes','Cat_Vtas_Nes','Costo','UM_1','Resurtidor','Faltante bi'], 
                            aggfunc=np.sum)
    BF_Detalle = Piv_Merge.reset_index() #Convierte a DF el pivot


def Calculos_2nd():
    global BF_Detalle
    BF_Detalle = BF_Detalle.astype({'Costo':'int64'})
    #Crea calculo de inv $
    BF_Detalle["Inv $"] = BF_Detalle["Inv"] * BF_Detalle["Costo"]
    

def pivotita():
    global BF_DetalleResumen
    BF_DetalleResumen = pd.pivot_table(BF_Detalle, 
                            values=['Faltante bi'], 
                            index=['Semana','Tipo_Res','Cat_Vtas_Nes'], 
                            aggfunc=np.sum)


Importar_MasterUpc()
Import_BASEF()
Calculos()   
BF_Detalle()
pivotita()
#Calculos_2nd()
del BF_data
del BF_basic

end_time = time.time()
duration = end_time - start_time
print("Tmepo de ejecución:", duration / 60, " minutos")

#MELT la base de forecast Soriana (como la de ASL)

#Cruzar vs forecats Semanal y agregar semana 5 de base fria en csv
#Cruzar vs Master tiendas Soriana
# Cruzas vs pedidos Geerdos en 30 días

