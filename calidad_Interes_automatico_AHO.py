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

# Se solicita la fecha del archivo para la creación del path que leera el archivo

try:
	# Input
	print("Inserte la fecha de la fuente que desea procesar (yyyymmdd)")
	data_date = input()

	print('Procesando...')

	# file = os.path.abspath("../Fuentes_iniciales/ManualTransactions_"+ data_date "_.xls")
	file = os.path.abspath("../Fuentes_iniciales/Interes_automatico_AHO_" + data_date + ".xlsx")

	# Raed file
	df = pd.read_excel(file, header=None)

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
			df = df[['en_ente', 'en_linea_neg_cv', 'en_subsegmento', 'ah_cta_banco', 'hm_moneda', 'hm_fecha', 'hm_valor', 'hm_referencia', 'hm_signo']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try: 
				path = os.path.abspath('../Informes/Interes_automatico_AHO_' + data_date + '.txt')

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
					#en_ente
					if i == 0:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# en_linea_neg_cv
					if i == 1:
						linea_neg = ['PFS','GBM','CMB']
						if (~df[column].isin(linea_neg).any()):
							f.write("\nHay valores que no corresponden a 'PFS','GBM','CMB' ")

					# en_subsegmento
					if i == 2:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z-\\s]+', '', regex=True)

					# ah_cta_banco
					if i == 3:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# hm_moneda		
					if i == 4:
						monedas = ['0', '20', '6']
						if (~df[column].isin(monedas).all()):
							f.write("\nHay monedas que no corresponden a 0, 6 o 20")

					# hm_fecha
					if i == 5:
						#df[column] = pd.to_datetime(df[column])
						#df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)
						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden para el mes de ejecución")

					# hm_valor
					if i == 6:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].astype(float)

					# hm_referencia
					if i == 7:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z-\\s]+', '', regex=True)

					# hm_signo
					if i == 8:
						signo = ['C']
						if (~df[column].isin(linea_neg).all()):
							f.write("\nHay signos que no corresponden a 'C' ")

					i = i + 1 

				f.close()

				# Replace nan to empty
				df = df.fillna('')
				df = df.replace('nan', '', regex=False)

				# Validation flag, unix time
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/Interes_automatico_AHO_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'AHO', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except:
				print(' Ha ocurrido un error, por favor verifique su fuente')
		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [en_ente, en_linea_neg_cv, en_subsegmento, ah_cta_banco, hm_moneda, hm_fecha, hm_valor, hm_referencia, hm_signo], teniendo en cuenta el orden, las mayusculas y minusculas')
	except:
		print(' Ha ocurrido un error, por favor verifique su fuente')
except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")