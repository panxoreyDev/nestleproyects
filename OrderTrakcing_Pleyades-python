# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 13:22:55 2023

@author: MXRodrigEl1
"""
import pandas as pd
from tkinter import filedialog
import os
import tkinter as tk


#ELIMINA LA VENTANITA CAGAPALOS
root = tk.Tk()
root.withdraw()

#DECLARA VARIABLE PARA INICIAR EN EL DESKTOP
desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') 


#ABRE VENTANA EXPLORADOR PARA ENCONTRAR EL PATTH DE UNA CARPETA
csv_file_path = filedialog.askopenfile(initialdir = desktop,
                                          title = "Importar CSV File",
                                          filetypes = (("Csv files",
                                                        "*.csv*"),
                                                       ("all files",
                                                        "*.*")))
#LEE EL FILE CSV DE LA CARPETA ANTES PROPORCIONADA EN FILEPATH                                      
csv_file =  pd.read_csv(csv_file_path, 
                             sep=',', 
                             encoding='utf-16',
                             header=0,
                             index_col=False,
                             low_memory=False
                             )

print(csv_file)
