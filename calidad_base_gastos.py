# Importe de librerias a utilizar
import pandas as pd
import numpy as np
from pandas import ExcelWriter


# Se solicita la fecha de ejecución y con la ayuda de un diccionario se crea el path que leera el archivo

# Input data 
months={'01':'Enero', '02':'Febrero', '03':'Marzo', '04':'Abril', '05':'Mayo', '06':'Junio', '07':'Julio', '08':'Agosto', '09':'Septiembre',
        '10':'Octubre', '11':'Nomviembre', '12':'Diciembre'}
        
data_date='20221231'
month_date=months

# Create path
file = 'C:\\Users\\laura.mariana.jimen1\\Documents\\Calidad_Datos_MIS_CR\\Fuentes_iniciales\\Base de Gastos ' + month_date[data_date[4:6]] + ' ' + data_date[2:4] + ' MS.xlsx'

# Raed file
df = pd.read_excel(file)

# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

# Create output file
f = open("C:\\Users\\laura.mariana.jimen1\\Documents\\Calidad_Datos_MIS_CR\\Informes\\Informe_base_gastos.txt","w+")
f.write(file)

# Print columns and rows
f.write("\nFilas: %d" % len(df))
f.write("\nColumnas: %d" % len(df.columns))

# Validate empty cells
for column in df:
    text = column + ": " + str(df[column].isnull().sum())
    f.write("\n")
    f.write(text)

f.close()

#print(df['Cia'])
'''
print(df['Detalle'].iloc[21999])
print("")
print(df['Detalle'].iloc[22000])
print("")
print(df['Detalle'].iloc[22001])
print("")
print(df['PUC'].iloc[22001])
'''


# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
# especiales que puedan afectar la converisión a csv

# Remove entirely empty row
df = df.dropna(how='all')

# Changes for all columns

#Remove carring return
df = df.replace({r'\\r': ' '}, regex=True)
#Remove line breaks
df = df.replace(r'\s+|\\n', ' ', regex=True)
#Remove pipelines, single quote, semicolon
df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)
#Remove comma
#df = df.replace(r',', ' ', regex=True)

'''
print("")
print(df['Detalle'].iloc[21999]) 
print("")
print(df['Detalle'].iloc[22000])
print("")
print(df['Detalle'].iloc[22001])
print("")
print(df['PUC'].iloc[22001])
'''

# Tratamientos especificos para campos puntuales del MIS según reglas de negocio definidas.

i = 0
for column in df:

	# Cia cod_entity
	if i == 3:
		df[column] = df[column].astype(int)
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace(' ', '')
		df[column] = df[column].str.replace('[^A-Za-z0-9\\s]+', '', regex=True)
		print(df[column])

		#df[column] = df[column].str.replace('[^0-9\\s]+', '')

	#CC cod_acco_cent
	elif i == 5:
		df[column] = df[column].astype(int)
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace(' ', '')
		df[column] = df[column].str.replace('[^A-Za-z0-9\\s]+', '', regex=True)
		#df[column] = df[column].str.replace('[^0-9\\s]+', '')

	#Cod cod_expense cod_nar
	elif i == 6:
		df[column] = df[column].astype(int)
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace(' ', '')
		df[column] = df[column].str.replace('[^A-Za-z0-9\\s]+', '', regex=True)
		#df[column] = df[column].str.replace('[^0-9\\s]+', '')

	#Cuenta cod_gl
	elif i==17:		
		df[column] = df[column].astype(int)
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace(' ', '')
		df[column] = df[column].str.replace('[^A-Za-z0-9\\s]+', '', regex=True)
		#df[column] = df[column].str.replace('[^0-9\\s]+', '')

	#Nombre cuenta des_gl
	elif i==18:
		df[column] = df[column].astype(str)

	#Monto pl
	
	elif i == 22:
		df[column] = df[column].astype(str)
		print(df['Monto'].iloc[21999])
		print(df['Monto'].iloc[22000])
		print(df['Monto'].iloc[22001])
		print(column)
		#df[column] = pd.to_numeric(df[column])
		#df[column] = df[column].str.replace(' ', '')
		df[column] = df[column].str.replace(r',', '.', regex=True)
		df[column] = df[column].str.replace('[^0-9.\\s]+', '', regex=True)
		df[column] = df[column].astype(float)
	i=i+1

file = 'C:\\Users\\laura.mariana.jimen1\\Documents\\Calidad_Datos_MIS_CR\\Fuentes_procesadas\\Base de Gastos ' + month_date[data_date[4:6]] + ' ' + data_date[2:4] + ' MS_ok.xlsx'

writer = ExcelWriter(file)
df.to_excel(writer, 'Hoja de datos', index=False)
writer.save()
#print (df)