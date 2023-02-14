# Importe de librerias a utilizar
import pandas as pd
import numpy as np
from pandas import ExcelWriter
import datetime
import time
import warnings
import os
import re

warnings.filterwarnings("ignore")

months={'01':31, '02':28, '03':31, '04':30, '05':31, '06':30, '07':31, '08':31, '09':30, '10':31, '11':30, '12':31}

# Se solicita la fecha del archivo para la creación del path que leera el archivo
try:
	# Input
	print("Inserte la fecha de la fuente que desea procesar")
	data_date = input()

	print('Procesando...')

	# file = os.path.abspath("../Fuentes_iniciales/ManualTransactions_"+ data_date "_.xls")
	file = os.path.abspath("../Fuentes_iniciales/TipoCambio_" + data_date + ".xlsx")

	# Raed file
	df = pd.read_excel(file, header=None)

	# Se toman el dataframe desde la fila 8 de la fuente para descartar textos informativos
	try: 
		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')
		# Remove entirely empty column
		df = df.dropna(how='all', axis=1)

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['Contenido', 'Fecha', 'Moneda', 'Tipo Cambio']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try: 
				path = os.path.abspath('../Informes/Informe_TipoCambio_' + data_date + '.txt')

				f = open(path,"w+")
				f.write(file)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df))
				f.write("\nCantidad de Columnas: %d" % len(df.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				for column in df:		
					text = str(column)
					f.write("\n")
					f.write(text)
					f.write(": ")
					text = str(df[column].isnull().sum())
					f.write(text)
					df[column] = df[column].astype(str)

				#Remove carring return
				df = df.replace({r'\\r': ' '}, regex=True)
				#Remove line breaks
				df = df.replace(r'\s+|\\n', ' ', regex=True)
				#Remove pipelines, single quote, semicolon
				df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

				i = 0
				for column in df:
					#Pregunta porque solo TC_FOTO
					if i == 0:
						contenido = ['TC_FOTO','TC_PROMEDIO', 'REG_FOTO', 'REG_PROMEDIO']
						if (~df[column].isin(contenido).all()):
							f.write("\nHay valores que no corresponden con el conetenido 'TC_FOTO','TC_PROMEDIO', 'REG_FOTO', 'REG_PROMEDIO'")

					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)
						if (df[column].str.slice(4, 6) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden con el mes del archivo")

					if i == 2:
						monedas = ['USD','EUR']
						if (~df[column].isin(monedas).all()):
							f.write("\nHay monedas que no corresponden 'USD' o 'EUR'")

					if i == 3:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].astype(float)

					i = i + 1

				f.close()

				# Replace nan to empty
				df = df.fillna('')
				df = df.replace('nan', '', regex=False)

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/TipoCambio_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'TC', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except:
				print(' Ha ocurrido un error, por favor verifique su fuente')
		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [Contenido, Fecha, Moneda, Tipo Cambio], teniendo en cuenta el orden, las mayusculas y minusculas')
	except:
		print(' Ha ocurrido un error, por favor verifique su fuente')
except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")

