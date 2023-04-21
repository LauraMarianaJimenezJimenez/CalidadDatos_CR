# Importe de librerias a utilizar
import pandas as pd
import numpy as np
from pandas import ExcelWriter
import datetime
import time
from pathlib import Path
import warnings
import os

warnings.filterwarnings("ignore")

# Se solicita la fecha del archivo para la creación del path que leera el archivo
try:
	# Input
	print("Inserte la fecha de la fuente que desea procesar")
	data_date = input()
	#data_date = '20230101'

	print('Procesando...')

	file = os.path.abspath("../Fuentes_iniciales/Base_Reservas_" + data_date + ".txt")

	# Raed file
	df = pd.read_csv(file, header=None, sep='|', encoding='latin-1')

	try:
		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')
		# Remove entirely empty column
		# df = df.dropna(how='all', axis=1)
		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['no_operacion','rsv_deterioro','rsv_no_generador','rsv_csd','rsv_generica','rsv_contraciclica','moneda','op_producto','sector_macro','sector_economico','ind_linea','ind_contable','empresa','categoria']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try:
				# Create output file
				path = os.path.abspath('../Informes/Informe_Base_Reservas_' + data_date + '.txt')

				f = open(path,"w+")
				f.write(file)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df))
				f.write("\nCantidad de Columnas: %d" % len(df.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				i = 1
				for column in df:
					#print(column)
					#print(df[column])
					text = str(column)
					f.write("\n")
					#f.write(str(i) + ". ")
					f.write(text)
					f.write(": ")
					text = str(df[column].isnull().sum())
					f.write(text)
					i = i + 1

				# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
				# especiales que puedan afectar la converisión a csv

				# Changes for all columns

				#Remove carring return
				df = df.replace({r'\\r': ' '}, regex=True)
				#Remove line breaks
				df = df.replace(r'\s+|\\n', ' ', regex=True)
				#Remove pipelines, single quote, semicolon
				df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

				#print(df)
				
				i = 0
				for column in df:
					

					# no_operacion
					if i == 0:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z0-9-\\s]+', '', regex=True)				

					# rsv_deterioro, rsv_no_generador, rsv_csd, rsv_generica, rsv_contraciclica
					montos =[1, 2, 3, 4, 5]
					if i in montos:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace(',', '.', regex=False)				

					# moneda
					if i == 6:
						df[column] = df[column].astype(str)
						monedas = ['1','2']
						if (~df[column].isin(monedas).all()):
								f.write("\nHay monedas que no corresponden a 1 o 2 ")						
									
					# op_producto, sector_macro, sector_economico, ind_linea, ind_contable, empresa, categoria
					otros = [7, 8 , 9, 10, 11, 12]
					if i in otros:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^.0-9a-zA-Z_ÑñÁáÉéÍíÓóÚú \\s]+', '', regex=True)
						

					i = i + 1 

				df = df.fillna('')
				df = df.replace('nan', '', regex=False)

				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				#print(df)
				
				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/Base_Reservas_' + data_date + '_' + unix_time + '.txt')
				df.to_csv(file, index=None, sep='|', mode='a')
				'''
				file = os.path.abspath('../Fuentes_procesadas/Operacion_' + unix_time +'.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja de datos', index=False)
				writer.save()
				'''

				print("Fuentes procesada con exito")


			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su fuente')
				print(e)

		except Exception as e:
			print(' Hay un error en los nombres de las columnas, valide que sean [no_operacion, rsv_deterioro, rsv_no_generador, rsv_csd, rsv_generica, rsv_contraciclica, moneda, op_producto, sector_macro, sector_economico, ind_linea, ind_contable, empresa, categoria], teniendo en cuenta el orden, las mayusculas y minusculas')
			print(e)

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su fuente')
		print(e)

except Exception as e:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")
	print(e)

