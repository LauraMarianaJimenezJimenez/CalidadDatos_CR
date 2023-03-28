# Importe de librerias a utilizar
import pandas as pd
import numpy as np
from pandas import ExcelWriter
import datetime
import time
import warnings

warnings.filterwarnings("ignore")

# Se solicita la fecha del archivo para la creación del path que leera el archivo

# Input
print("Inserte la fecha de la fuente que desea procesar")
data_date = input()
#20230117
#print(data_date)

# Create path
file = 'C:\\Users\\laura.mariana.jimen1\\Documents\\Calidad_Datos_MIS_CR\\Fuentes_iniciales\\Curva_Colocacion_Dol_' + data_date + '.xls'

# Raed file
df = pd.read_excel(file, header=None)

# Se toman el dataframe desde la fila 7 de la fuente para descartar textos informativos

# Take data frame
df = df.iloc[5: , :]
# Defien headers
df.columns = df.iloc[1]
df = df[2:]

#print(df.columns.values.tolist())

# Remove entirely empty row
df = df.dropna(how='all')
# Remove entirely empty column
df = df.dropna(how='all', axis=1)
# Take specific columns
df = df[["Días", "Curva Colocación", "Prima Liquidez", "Rend + Prima Liquidez"]]
# Remove duplicate records
df = df.drop_duplicates()

# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

# Create output file
f = open('C:\\Users\\laura.mariana.jimen1\\Documents\\Calidad_Datos_MIS_CR\\Informes\\Informe_curva_colocacion_dol' + data_date + '.txt',"w+")
f.write(file)

# Print columns and rows
f.write("\nCantidad de filas: %d" % len(df))
f.write("\nCantidad de Columnas: %d" % len(df.columns))

f.write("\nCantidad de datos vacios por cada columna del archivo")

# Validate empty cells
for column in df:
    text = column + ": " + str(df[column].isnull().sum())
    f.write("\n")
    f.write(text)

# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
# especiales que puedan afectar la converisión a csv

# Changes for all columns

# Remove carring return
df = df.replace({r'\\r': ' '}, regex=True)
# Remove line breaks
df = df.replace(r'\s+|\\n', ' ', regex=True)
# Remove pipelines, single quote, semicolon
df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

# Tratamientos especificos para campos puntuales del MIS según reglas de negocio definidas.

i = 0
for column in df:
	# Dias 
	# Se valida que la cantidad de filas, excluyendo las completamente vacias, sean 15000, si no se informa cuantas columnas adicionales o faltantes hay
	if i == 0:
		# Replace NaN values with zeros
		df[column] = df[column].fillna(0)
		if len(df) != 15000:
			X = 15000 - len(df)
			if X > 0:
				text = "Hay " + str(X) + " filas menos"
				f.write("\n")
				f.write(text)
			else:
				X = X * - 1
				text = "Hay " + str(X) + " filas adicionales"
				f.write("\n")
				f.write(text)
	
	# Curva colocación
	# Se valida que no hayan porcentajes mayores que 1, en caso de que hayan se informa	
	if i == 1:
		# Replace NaN values with zeros
		df[column] = df[column].fillna(0)
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace('[^0-9.,\\s]+', '', regex=True)
		df[column] = df[column].str.replace(',', '.', regex=False)
		df[column] = df[column].astype(float)

		j = 0
		for row in df[column]:
			if(df[column].iloc[j] > 1):
				final = row / 100
				df[column].iloc[j] = final
				j = j + 1

		if (df[column] >= 1).all():
			text = "Hay curvas con porcentaje mayor que 1"

	# Prima liquidez
	# Se valida que no hayan porcentajes mayores que 1, en caso de que hayan se informa	
	if i == 2:
		# Replace NaN values with zeros
		df[column] = df[column].fillna(0)
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace('[^0-9.,\\s]+', '', regex=True)
		df[column] = df[column].str.replace(',', '.', regex=False)
		df[column] = df[column].astype(float)

		j = 0
		for row in df[column]:
			if(df[column].iloc[j] > 1):
				final = row / 100
				df[column].iloc[j] = final
				j = j + 1

		if (df[column] >= 1).all():
			text = "Hay curvas con porcentaje mayor que 1"

	# Rend + prima liquidez
	# Se valida que no hayan porcentajes mayores que 1, en caso de que hayan se informa	
	if i == 3:
		# Se calcula el campo 'Rend +  prima de liquedez' (columna 4) sumando las datos de curva de colocación + prima liquidez (columna 2 + columna 3) del marco de datos
		sum_column = df.iloc[:, 1] + df.iloc[:, 2]
		df.iloc[:, 3] = sum_column
		
		# Replace NaN values with zeros
		df[column] = df[column].fillna(0)
		df[column] = df[column].astype(float)
		
	i=i + 1

i = 0
for column in df:
	if (i > 0 and i < 3):
		df[column] = df[column] * 100
		df[column] = df[column].astype(str).str[:4]
		df[column] = df[column] + '%'
		df[column] = df[column].str.replace('.', ',', regex=False)

	if i == 3:
		if (df[column] >= 1).all():
			print('entro')
			f.write("\nEn la columna 'Rend +  prima de liquidez' hay porcentajes mayores a 1")

		df[column] = df[column] * 100
		df[column] = df[column].astype(str).str[:4]
		df[column] = df[column] + '%'
		df[column] = df[column].str.replace('.', ',', regex=False)

	i = i + 1

print("Fuentes procesada con exito")
f.close()

# Generación del flag de validación, marcación de tiempo unix
date_time = datetime.datetime.now()      
unix_time = time.mktime(date_time.timetuple())
unix_time = str(unix_time)

# Se escribe un nuevo archivo con la fuente procesada 

file = 'C:\\Users\\laura.mariana.jimen1\\Documents\\Calidad_Datos_MIS_CR\\Fuentes_procesadas\\Curva_Colocacion_Dol_' + data_date + '_' + unix_time + '.xls'
writer = ExcelWriter(file)
df.to_excel(writer, 'Hoja de datos', index=False)
writer.save()
