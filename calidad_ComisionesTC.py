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

	print('Procesando...')

	file = os.path.abspath("../Fuentes_iniciales/ComisionesTC_" + data_date + ".xlsx")

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
			df = df[['Fecha', 'Tarjeta', 'Monto', 'Moneda', 'Tipo Tarjeta', 'Tipo Cuenta']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try:
				path = os.path.abspath('../Informes/Informe_ComisionesTC_' + data_date + '.txt')

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
						df[column] = df[column].astype(str)
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)
						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden para el mes de ejecución")
					# Tarjeta
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

						for items in df['Tarjeta'].iteritems():
							if(len(items[1]) > 16):
								f.write("\nHay tarjetas con longitud mayor de 16")
								break

						for items in df['Tarjeta'].iteritems():
							if(len(items[1]) < 16):
								f.write("\nHay tarjetas con longitud menor de 16")
								break

					# Monto
					if i == 2:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

					#  Moneda
					if i == 3:
						df[column] = df[column].astype(str)
						monedas = ['0']
						if (~df[column].isin(monedas).all()):
							f.write("\nHay monedas que no corresponden a 0")

					# Tipo Tarejta
					if i == 4:
						df[column] = df[column].astype(str)
						tipo_t = ['Credito', 'Debito']
						if (~df[column].isin(tipo_t).all()):
							f.write("\nHay tipos de tarjeta que no corresponden a Debito o Credito")

				 	# Tipo Cuenta
					if i == 5:
						df[column] = df[column].astype(str)
						tipo_C = ['Gasto MC', 'Ingreso MC',	'Gasto Visa', 'Ingreso Visa', 'Ingreso Visa/MC', 'Ingreso Express Digital']
						if (~df[column].isin(tipo_C).all()):
							f.write("\nHay tipos de tarjeta que no corresponden a Gasto MC, Ingreso MC, Gasto Visa, Ingreso Visa, Ingreso Visa/MC, Ingreso Express Digital")

					i = i + 1


				f.close()

				df = df.fillna('')
				df = df.replace('nan', '', regex=False)


				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/ComisionesTC_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja1', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su fuente')
				print(e)

		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [Fecha, Tarjeta, Monto, Moneda, Tipo Tarjeta, Tipo Cuenta], teniendo en cuenta el orden, las mayusculas y minusculas')

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su fuente')
		print(e)

except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")

	