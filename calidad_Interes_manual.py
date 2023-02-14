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
	print("Inserte la fecha de la fuente que desea procesar (yyyymmdd)")
	data_date = input()

	print('Procesando...')

	file = os.path.abspath("../Fuentes_iniciales/Interes_manual_" + data_date + ".xlsx")

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
			# Add empty column 
			df["*MO = Moneda Origen"] = np.nan
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['Fecha', 'ENTE', 'Nombre de Cuenta', 'Moneda', 'Cuenta', 'Intereses Bruto MO', '*MO = Moneda Origen']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try: 
				path = os.path.abspath('../Informes/Informe_Interes_manual_' + data_date + '.txt')

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
					# Fecha
					if i == 0:
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)
						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden para el mes de ejecución")
					# Ente
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# Nombre de Cuenta
					if i == 2: 
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z-\\s]+', '', regex=True)

					# Moneda
					if i == 3:
						df[column] = df[column].astype(str)
						monedas = ['0', '20', '6']
						if (~df[column].isin(monedas).all()):
							f.write("\nHay monedas que no corresponden a 0, 6 o 20")

					# Cuenta
					if i == 4:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# Intereses Bruto MO
					if i == 5:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].astype(float)

					if i == 6:
						df[column] = df[column].fillna('')
						df[column] = df[column].replace('nan', '', regex=False)


					i = i + 1

				f.close()

				df = df.fillna('')
				df = df.replace('nan', '', regex=False)

				# Validation flag, unix time
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 

				file = os.path.abspath('../Fuentes_procesadas/Interes_manual_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Interes Manual', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except:
				print(' Ha ocurrido un error, por favor verifique su fuente')
		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [Fecha, ENTE, Nombre de Cuenta, Moneda, Cuenta, Intereses Bruto MO, *MO = Moneda Origen], teniendo en cuenta el orden, las mayusculas y minusculas')
	except:
		print(' Ha ocurrido un error, por favor verifique su fuente')
except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")

