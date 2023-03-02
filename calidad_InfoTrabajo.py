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
try:
	# Se solicita la fecha del archivo para la creación del path que leera el archivo

	# Input
	print("Inserte la fecha de la fuente que desea procesar (yyyymm)")
	data_date = input()
	#data_date = '202201'

	print('Procesando...')

	# file = os.path.abspath("../Fuentes_iniciales/ManualTransactions_"+ data_date "_.xls")
	file = os.path.abspath("../Fuentes_iniciales/InfoTrabajo_" + data_date + ".xlsx")

	# Raed file
	df = pd.read_excel(file, 'Hoja1', header=None)

	try:
		# Se toman el dataframe desde la fila 8 de la fuente para descartar textos informativos

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
			df = df[['ID', 'BIN', 'NOMBRE_BIN', 'PRODUCTO', 'TIPO', 'activo', 'IdProducto', 'PRODUCTO2']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try:

				path = os.path.abspath('../Informes/Informe_InfoTrabajo_' + data_date + '.txt')

				f = open(path,"w+")
				f.write(file)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df))
				f.write("\nCantidad de Columnas: %d" % len(df.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				if ((df['ID'].iloc[0]) == -1):
					df = df[df.ID != -1]

				# Validate empty cells
				for column in df:
					text = str(column)
					f.write("\n")
					f.write(text)
					f.write(": ")
					text = str(df[column].isnull().sum())
					f.write(text)

				#Remove carring return
				df = df.replace({r'\\r': ' '}, regex=True)
				#Remove line breaks
				df = df.replace(r'\s+|\\n', ' ', regex=True)
				#Remove pipelines, single quote, semicolon
				df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

				i = 0
				for column in df:
					# ID
					if i == 0:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# BIN
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

						for items in df['BIN'].iteritems():
							if(len(items[1]) > 6):
								f.write("\nhay bines con longitud mayor de 6")

							elif (len(items[1]) < 6):
								f.write("\nhay bines con longitud menor de 6")
								break

					# Nombre BIN
					if i == 2:
						df[column] = df[column].astype(str)
						BIN = ['Visa', 'MC']
						if (~df[column].isin(BIN).all()):
							f.write("\nHay nombres de BIN que no corresponden a Visa o MC")

					# Producto
					if i == 3:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z-\\s]+', '', regex=True)

						if (df[column] == 'ND').any():
							f.write("\nHay BINES no activos")

					if i == 4:
						df[column] = df[column].astype(str)
						tipo = ['Credito', 'Debito']
						if (~df[column].isin(tipo).all()):
							f.write("\nHay tipos de tarjeta que no corresponden a Debito o Credito")

					if i == 5:
						df[column] = '1'

					if i == 6:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9-\\s]+', '', regex=True)

					if i == 7:
						df[column] = df[column].astype(str)
						tipo = ['Platino', 'Dorado', 'Clásico', 'Business', 'Premier', 'Infinity']
						if (~df[column].isin(tipo).all()):
							f.write("\nHay tipos de tarjeta que no corresponden a 'Platino', 'Dorado', 'Clásico', 'Business', 'Premier', 'Infinity'")

					i = i + 1

				if ((df['ID'].iloc[0]) != '-1'):
					new_row = pd.DataFrame({'ID':'-1', 'BIN':'NA','NOMBRE_BIN':'NO DISPONIBLE', 'PRODUCTO':'NO DISPONIBLE', 'TIPO':'NO DISPONIBLE', 'activo':'1', 'IdProducto':'-20', 'PRODUCTO2':'NULL'}, index=[0])
					df = pd.concat([new_row,df.loc[:]]).reset_index(drop=True)

				f.close()

				df = df.fillna('')
				df = df.replace('nan', '', regex=False)


				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/InfoTrabajo_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su fuente')
				print(e)

		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [ID, BIN, NOMBRE_BIN, PRODUCTO, TIPO, activo, IdProducto, PRODUCTO2], teniendo en cuenta el orden, las mayusculas y minusculas')
	

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su fuente')
		print(e)
		
except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")

	