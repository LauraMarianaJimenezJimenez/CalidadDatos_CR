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
file = 'C:\\Users\\laura.mariana.jimen1\\Documents\\Calidad_Datos_MIS_CR\\Fuentes_iniciales\\Curva_PIPCA_Dol_' + data_date + '.xls'

# Raed file
df = pd.read_excel(file, header=None)

# Se define la fila 1 como el header y el se toma el data frame desde la fila 3

# Define headers
df.columns = df.iloc[0]
# Take data frame
df = df[2:]

#print(df.columns.values.tolist())

# Remove entirely empty row
df = df.dropna(how='all')
# Remove entirely empty column
df = df.dropna(how='all', axis=1)
# Take specific columns
df = df[["Plazo", "Yield Lineal en Dólares"]]
# Remove duplicate records
df = df.drop_duplicates()

# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

# Create output file
f = open('C:\\Users\\laura.mariana.jimen1\\Documents\\Calidad_Datos_MIS_CR\\Informes\\Informe_curva_PIPCA_dol' + data_date + '.txt',"w+")
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
	# Plazo
	# Se valida que la cantidad de filas, excluyendo las completamente vacias, sean 7450, si no se informa cuantas columnas adicionales o faltantes hay
	if i == 0:
		# Replace NaN values with zeros
		df[column] = df[column].fillna(0)
		if len(df) != 7450:
			X = 7450 - len(df)
			if X > 0:
				text = "Hay " + str(X) + " filas menos"
				f.write("\n")
				f.write(text)
			else:
				X = X * - 1
				text = "Hay " + str(X) + " filas adicionales"
				f.write("\n")
				f.write(text)

	# Yield Lineal en Dólares
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

		if (df[column] >= 1).any():
			text = "Hay curvas con porcentaje mayor que 1"
		
	i=i + 1

f.close()

print("Fuentes procesada con exito")
f.close()

# Generación del flag de validación, marcación de tiempo unix
date_time = datetime.datetime.now()      
unix_time = time.mktime(date_time.timetuple())
unix_time = str(unix_time)

# Se escribe un nuevo archivo con la fuente procesada 


file = 'C:\\Users\\laura.mariana.jimen1\\Documents\\Calidad_Datos_MIS_CR\\Fuentes_procesadas\\Curva_PIPCA_Dol_' + data_date + "_" + unix_time + '.xls'
writer = ExcelWriter(file)
df.to_excel(writer, 'Hoja de datos', index=False)
writer.save()


