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
	print("Inserte la fecha de la fuente que desea procesar(yyyymm) ")
	data_date = input()


	print('Procesando...')

	# file = os.path.abspath("../Fuentes_iniciales/ManualTransactions_"+ data_date "_.xls")
	file = os.path.abspath("../Fuentes_iniciales/ListaMov_" + data_date + ".xlsx")

	# Raed file
	df = pd.read_excel(file, header=None)

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
			df = df[['COD', 'DESCRIP', 'AFECTACION', 'Signo', 'Desc_IncCom_CBO']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try:
				path = os.path.abspath('../Informes/Informe_ListaMov_' + data_date + '.txt')

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
					# COD
					if i == 0:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)

					# DESCRIP
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z-\\s]+', '', regex=True)

					# AFECTACION
					if i == 2:
						afectacion = ['Credito', 'Debito']
						if (~df[column].isin(afectacion).all()):
							f.write("\nHay tipos de tarjeta que no corresponden a Credito o Debito")

					# Signo
					if i == 3:
						for items in df['AFECTACION'].iteritems():
							if(items[1] == 'Credito'):
								valor = '-1'
								df.loc[items[0]][3] = valor
							if(items[1] == 'Debito'):
								valor = '1'
								df.loc[items[0]][3] = valor
							if(items[1] != 'Debito' and items[1] != 'Credito'):
								valor = '0'
								df.loc[items[0]][3] = valor
					
					if i == 4: 
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)

					i = i + 1

				f.close()

				df = df.fillna('')
				df = df.replace('nan', '', regex=False)

				df.insert(0,"", "")

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/ListaMov_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja1', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except:
				print(' Ha ocurrido un error, por favor verifique su fuente')
		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [COD, DESCRIP, AFECTACION, Signo, Desc_IncCom_CBO], teniendo en cuenta el orden, las mayusculas y minusculas')
	except:
		print(' Ha ocurrido un error, por favor verifique su fuente')
except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")

	