# Scrip de calidad de datos, curvas captación, colocación y PIPCA

# Importe de librerias a utilizar
import pandas as pd
import numpy as np
from pandas import ExcelWriter
import datetime
import time
import warnings
import os

# Ignore warnings
warnings.filterwarnings("ignore")

# Function curva captación colones
def captacion_col():

	print('Procesando...')

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/Curva_Captacion_Col_' + data_date + '.xls')
		# Raed file
		df = pd.read_excel(file, header=None)

		try:
			# Se toman el dataframe desde la fila 8 de la fuente para descartar textos informativos

			# Take dataframe
			df = df.iloc[6: ,]
			# Define headers
			df.columns = df.iloc[1]
			df = df[2:]

			# Remove entirely empty row
			df = df.dropna(how='all')
			# Remove entirely empty column
			df = df.dropna(how='all', axis=1)

			try:
				# Delete withespace in headers
				df = df.rename(columns=lambda x: x.strip())
				# Take specific columns
				df = df[["Días", "Curva Captación"]]
				# Remove duplicate records
				df = df.drop_duplicates()

				try:	
					# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

					# Create output file
					path = os.path.abspath('../Informes/Informe_curva_captacion_col_' + data_date + '.txt')
					f = open(path,"w+")
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

					#Remove carring return
					df = df.replace({r'\\r': ' '}, regex=True)
					#Remove line breaks
					df = df.replace(r'\s+|\\n', ' ', regex=True)
					#Remove pipelines, single quote, semicolon
					df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

					# Tratamientos especificos para campos puntuales del MIS según reglas de negocio definidas.

					i = 0
					for column in df:
						# Dias 
						# Se valida que la cantidad de filas, excluyendo las completamente vacias, sean 3600, si no se informa cuantas columnas adicionales o faltantes hay
						if i == 0:
							# Replace NaN values with zeros
							df[column] = df[column].fillna(0)
							if len(df) != 3600:
								X = 3600 - len(df)
								if X > 0:
									text = "Hay " + str(X) + " filas menos"
									f.write("\n")
									f.write(text)
								else:
									X = X * - 1
									text = "Hay " + str(X) + " filas adicionales"
									f.write("\n")
									f.write(text)

						# Curva captacion
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
							
							df[column] = df[column] * 100
							df[column] = df[column].astype(str).str[:12]
							df[column] = df[column] + '%'
							df[column] = df[column].str.replace('.', ',', regex=False)
							
						i=i + 1

					f.close()

					# Generación del flag de validación, marcación de tiempo unix
					date_time = datetime.datetime.now()      
					unix_time = time.mktime(date_time.timetuple())
					unix_time = str(unix_time)
					unix_time = unix_time.split('.')[0]


					# Se escribe un nuevo archivo con la fuente procesada 

					file = os.path.abspath('../Fuentes_procesadas/Curva_Captacion_Col_' + data_date + '_' + unix_time + '.xls')
					writer = ExcelWriter(file)
					df.to_excel(writer, 'Hoja de datos', index=False)
					writer.save()

					print("Fuente Curva_Captacion_Col_" + data_date + ".xls procesada con exito")

				except:
					print(' Ha ocurrido un error, por favor verifique su fuente Curva_Captacion_Col')
			except:
				print(' Hay un error en los nombres de las columnas, valide que sean [Días, Curva Captación], teniendo en cuenta el orden, las mayusculas y minusculas para la fuente Curva_Captacion_Col')
		except:
			 print(" Ha ocurrido un error, revise el formato de su fuente Curva_Captacion_Col")			
	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo Curva_Captacion_Col")

def captacion_dol():

	print('Procesando...')

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/Curva_Captacion_Dol_' + data_date +'.xls')

		# Raed file
		df = pd.read_excel(file, header=None)

		try:
			# Se toman el dataframe desde la fila 8 de la fuente para descartar textos informativos

			# Take dataframe
			df = df.iloc[6: , :]
			# Define headers
			df.columns = df.iloc[1]
			df = df[2:]

			#print(df.columns.values.tolist())

			# Remove entirely empty row
			df = df.dropna(how='all')
			# Remove entirely empty column
			df = df.dropna(how='all', axis=1)

			try:
				# Delete withespace in headers
				df = df.rename(columns=lambda x: x.strip())
				# Take specific columns
				df = df[["Días", "Curva Captación"]]
				# Remove duplicate records
				df = df.drop_duplicates()

				try:
					# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

					# Create output file
					path = os.path.abspath('../Informes/Informe_curva_captacion_dol_' + data_date + '.txt')
					f = open(path,"w+")
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
						# Se valida que la cantidad de filas, excluyendo las completamente vacias, sean 3600, si no se informa cuantas columnas adicionales o faltantes hay
						if i == 0:
							# Replace NaN values with zeros
							df[column] = df[column].fillna(0)
							if len(df) != 3600:
								X = 3600 - len(df)
								if X > 0:
									text = "Hay " + str(X) + " filas menos"
									f.write("\n")
									f.write(text)
								else:
									X = X * - 1
									text = "Hay " + str(X) + " filas adicionales"
									f.write("\n")
									f.write(text)

						# Curva captacion
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
							
							df[column] = df[column] * 100
							df[column] = df[column].astype(str).str[:12]
							df[column] = df[column] + '%'
							df[column] = df[column].str.replace('.', ',', regex=False)
							
						i=i + 1

					f.close()

					# Generación del flag de validación, marcación de tiempo unix
					date_time = datetime.datetime.now()      
					unix_time = time.mktime(date_time.timetuple())
					unix_time = str(unix_time)
					unix_time = unix_time.split('.')[0]

					# Se escribe un nuevo archivo con la fuente procesada 

					file = os.path.abspath('../Fuentes_procesadas/Curva_Captacion_Dol_' + data_date + '_' + unix_time + '.xls')
					writer = ExcelWriter(file)
					df.to_excel(writer, 'Hoja de datos', index=False)
					writer.save()
					print("Fuente Curva_Captacion_Dol_" + data_date +".xls procesada con exito")

				except:
					print(' Ha ocurrido un error, por favor verifique su fuente Curva_Captacion_Dol')
			except:
				print(' Hay un error en los nombres de las columnas, valide que sean [Días, Curva Captación], teniendo en cuenta el orden, las mayusculas y minusculas para la fuente Curva_Captacion_Col')
		except:
			 print(" Ha ocurrido un error, revise el formato de su fuente Curva_Captacion_Dol")			
	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo Curva_Captacion_Dol")



def colocacion_col():

	print('Procesando...')

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/Curva_Colocacion_Col_' + data_date + '.xls')

		# Raed file
		df = pd.read_excel(file, header=None)

		try:
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

			try:
				# Delete withespace in headers
				df = df.rename(columns=lambda x: x.strip())
				# Take specific columnas
				df = df[["Días", "Curva Colocación", "Prima Liquidez", "Rend + Prima Liquidez"]]
				# Remove duplicate records
				df = df.drop_duplicates()

				try:
					# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

					# Create output file

					path = os.path.abspath('../Informes/Informe_curva_colocacion_col_' + data_date + '.txt')
					f = open(path,"w+")
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

							if (df[column] >= 1).any():
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

							if (df[column] >= 1).any():
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
							df[column] = df[column].astype(str).str[:12]
							df[column] = df[column] + '%'
							df[column] = df[column].str.replace('.', ',', regex=False)

						if i == 3:
							if (df[column] >= 1).any():
								f.write("\nEn la columna 'Rend +  prima de liquidez' hay porcentajes mayores a 1")

							df[column] = df[column] * 100
							df[column] = df[column].astype(str).str[:12]
							df[column] = df[column] + '%'
							df[column] = df[column].str.replace('.', ',', regex=False)

						i = i + 1

					f.close()

					# Generación del flag de validación, marcación de tiempo unix
					date_time = datetime.datetime.now()      
					unix_time = time.mktime(date_time.timetuple())
					unix_time = str(unix_time)
					unix_time = unix_time.split('.')[0]

					# Se escribe un nuevo archivo con la fuente procesada 

					file = os.path.abspath('../Fuentes_procesadas/Curva_Colocacion_Col_' + data_date + '_' + unix_time + '.xls')
					writer = ExcelWriter(file)
					df.to_excel(writer, 'Hoja de datos', index=False)
					writer.save()
					print("Fuente Curva_Colocacion_Col_" + data_date + ".xls procesada con exito")

				except:
					print(' Ha ocurrido un error, por favor verifique su fuente Curva_Colocacion_Col_')
			except:
				print(' Hay un error en los nombres de las columnas, valide que sean [Días, Curva Colocación, Prima Liquidez, Rend + Prima Liquidez], teniendo en cuenta el orden, las mayusculas y minusculas de su fuente Curva_Colocacion_Col_')
		except:
			print(' Ha ocurrido un error, por favor verifique su fuente Curva_Colocacion_Col_')
	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo Curva_Colocacion_Col_")

	
def colocacion_dol():

	print('Procesando...')

	try: 
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/Curva_Colocacion_Dol_' + data_date + '.xls')

		# Raed file
		df = pd.read_excel(file, header=None)

		try:

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

			try:
				# Delete withespace in headers
				df = df.rename(columns=lambda x: x.strip())
				# Take specific columns
				df = df[["Días", "Curva Colocación", "Prima Liquidez", "Rend + Prima Liquidez"]]
				# Remove duplicate records
				df = df.drop_duplicates()

				try:
					# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

					# Create output file

					path = os.path.abspath('../Informes/Informe_curva_colocacion_dol_' + data_date + '.txt')
					f = open(path,"w+")
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

							if (df[column] >= 1).any():
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

							if (df[column] >= 1).any():
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
							df[column] = df[column].astype(str).str[:12]
							df[column] = df[column] + '%'
							df[column] = df[column].str.replace('.', ',', regex=False)

						if i == 3:
							if (df[column] >= 1).any():
								f.write("\nEn la columna 'Rend +  prima de liquidez' hay porcentajes mayores a 1")

							df[column] = df[column] * 100
							df[column] = df[column].astype(str).str[:12]
							df[column] = df[column] + '%'
							df[column] = df[column].str.replace('.', ',', regex=False)

						i = i + 1

					f.close()

					# Generación del flag de validación, marcación de tiempo unix
					date_time = datetime.datetime.now()      
					unix_time = time.mktime(date_time.timetuple())
					unix_time = str(unix_time)
					unix_time = unix_time.split('.')[0]


					# Se escribe un nuevo archivo con la fuente procesada 

					file = os.path.abspath('../Fuentes_procesadas/Curva_Colocacion_Dol_' + data_date + '_' + unix_time + '.xls')
					writer = ExcelWriter(file)
					df.to_excel(writer, 'Hoja de datos', index=False)
					writer.save()
					print("Fuente Curva_Colocacion_Dol_" + data_date + ".xls procesada con exito")

				except:
					print(' Ha ocurrido un error, por favor verifique su fuente Curva_Colocacion_Dol')
			except:
				print(' Hay un error en los nombres de las columnas, valide que sean [Días, Curva Colocación, Prima Liquidez, Rend + Prima Liquidez], teniendo en cuenta el orden, las mayusculas y minusculas de su fuente Curva_Colocacion_Dol')
		except:
			print(' Ha ocurrido un error, por favor verifique su fuente Curva_Colocacion_Dol')
	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo Curva_Colocacion_Dol")

	
	


def PIPCA_col():

	print('Procesando...')

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/Curva_PIPCA_Col_' + data_date + '.xls')

		# Raed file
		df = pd.read_excel(file, header=None)

		try:

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

			try:
				# Delete withespace in headers
				df = df.rename(columns=lambda x: x.strip())
				# Take specific columns
				df = df[["Plazo", "Yield Lineal en Colones"]]
				# Remove duplicate records
				df = df.drop_duplicates()

				try:
					# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

					# Create output file
					path = os.path.abspath('../Informes/Informe_curva_PIPCA_col_' + data_date + '.txt')
					f = open(path,"w+")
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

						#Yield Lineal en Colones
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

					# Generación del flag de validación, marcación de tiempo unix
					date_time = datetime.datetime.now()      
					unix_time = time.mktime(date_time.timetuple())
					unix_time = str(unix_time)
					unix_time = unix_time.split('.')[0]


					# Se escribe un nuevo archivo con la fuente procesada 

					file = os.path.abspath('../Fuentes_procesadas/Curva_PIPCA_Col_' + data_date + '_' + unix_time + '.xls')
					writer = ExcelWriter(file)
					df.to_excel(writer, 'Hoja de datos', index=False)
					writer.save()
					print("Fuente Curva_PIPCA_Col_" + data_date + ".xls procesada con exito")

				except:
					print(' Ha ocurrido un error, por favor verifique su fuente Curva_PIPCA_Col')
			except:
				print(' Hay un error en los nombres de las columnas, valide que sean [Plazo, Yield Lineal en Colones], teniendo en cuenta el orden, las mayusculas y minusculas de su fuente Crva_PIPCA_Col')
		except:
			print(' Ha ocurrido un error, por favor verifique su fuente Curva_PIPCA_Col')
	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo Curva_PIPCA_Col")


def PIPCA_dol():

	print('Procesando...')

	try: 
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/Curva_PIPCA_Dol_' + data_date + '.xls')

		# Raed file
		df = pd.read_excel(file, header=None)

		try:	

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

			try:
				# Delete withespace in headers
				df = df.rename(columns=lambda x: x.strip())
				# Take specific columns
				df = df[["Plazo", "Yield Lineal en Dólares"]]
				# Remove duplicate records
				df = df.drop_duplicates()

				try:
					# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

					# Create output file
					path = os.path.abspath('../Informes/Informe_curva_PIPCA_dol_' + data_date + '.txt')
					f = open(path,"w+")
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

					# Generación del flag de validación, marcación de tiempo unix
					date_time = datetime.datetime.now()      
					unix_time = time.mktime(date_time.timetuple())
					unix_time = str(unix_time)
					unix_time = unix_time.split('.')[0]


					# Se escribe un nuevo archivo con la fuente procesada 

					file = os.path.abspath('../Fuentes_procesadas/Curva_PIPCA_Dol_' + data_date + '_' + unix_time + '.xls')
					writer = ExcelWriter(file)
					df.to_excel(writer, 'Hoja de datos', index=False)
					writer.save()
					print("Fuente Curva_PIPCA_Dol_" + data_date + ".xls procesada con exito")

				except:
					print(' Ha ocurrido un error, por favor verifique su fuente Curva_PIPCA_Dol')
			except:
				print(' Hay un error en los nombres de las columnas, valide que sean [Plazo, Yield Lineal en Colones], teniendo en cuenta el orden, las mayusculas y minusculas de su fuente Crva_PIPCA_Dol')
		except:
			print(' Ha ocurrido un error, por favor verifique su fuente Curva_PIPCA_Dol')
	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo Curva_PIPCA_Dol")


# Se solicita la fecha del archivo para la creación del path que leera el archivo
# Input
print("Inserte la fecha de la fuente que desea procesar")
data_date = input()

captacion_col()
captacion_dol()
colocacion_col()
colocacion_dol()
PIPCA_col()
PIPCA_dol()