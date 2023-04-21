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
	#data_date = input()
	data_date = '20230414'
	print('Procesando...')

	file = os.path.abspath("../Fuentes_iniciales/TarjetasEntregadas_" + data_date + ".xlsx")

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
			df = df[['Cuenta TC', 'TC', 'Nombre', 'Cédula', 'Tipo', 'Gestión', 'Fecha', 'Límite', 'Cod Ejec', 'Nombre Ejec', 'Puesto', 'Ubicación', 'Canal', 'OFICIAL', 'BIN', 'Producto', 'Color', 'Ente Tarjeta']]
				
			# Remove duplicate records
			df = df.drop_duplicates()
			try:
				path = os.path.abspath('../Informes/Informe_TarjetasEntregadas_' + data_date + '.txt')

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
					# Cuenta TC
					if i == 0:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)
					
						for items in df['Cuenta TC'].iteritems():
							if(len(items[1]) > 13):
								f.write("\nhay cuentas de tarjetas con longitud mayor de 13")
								
							if (len(items[1]) < 13):
								f.write("\nhay cuentas de tarjetas con longitud menor de 13")
								break
					# TC
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)
					
						for items in df['TC'].iteritems():
							if(len(items[1]) > 16):
								f.write("\nhay números de tarjetas con longitud mayor de 16")
								break

							if (len(items[1]) < 16):
								print(items[1])
								f.write("\nhay números de tarjeta con longitud menor de 16")
								break
					# Nombre
					if i == 2:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z\\s]+', '', regex=True)

					# Cedula 
					if i == 3:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z0-9\\s]+', '', regex=True)

					# Tipo
					if i == 4:
						df[column] = df[column].astype(str)
						tipo = ['T']
						if (~df[column].isin(tipo).all()):
							f.write("\nHay tipos de tarjetas que no son titulares, son diferentes a 'T'")


					# Gestión
					if i == 5:
						df[column] = df[column].astype(str)
						gestion = ['NU']
						if (~df[column].isin(gestion).all()):
							f.write("\nHay tarjetas que no son de nueva produccion, son diferentes a 'NU'")

					# Fecha 
					if i == 6:
						df[column] = df[column].astype(str)
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)
						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden para el mes de ejecución")
					# Limite
					if i == 7:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

					# Cod Ejec
					if i == 8:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z0-9\\s]+', '', regex=True)

					# Nombre Ejec
					if i == 9:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z\\s]+', '', regex=True)
						df[column] = df[column].str.replace('nan', '#N/A', regex=False)

					# Puesto
					if i == 10:			
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z0-9#\\s]+', '', regex=True)
						df[column] = df[column].str.replace('nan', '#N/A', regex=False)

					# Ubicación
					if i == 11:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z0-9\\s]+', '', regex=True)
						df[column] = df[column].str.replace('nan', '#N/A', regex=False)

					# Canal
					if i == 12:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z0-9\\s]+', '', regex=True)
						df[column] = df[column].str.replace('nan', '#N/A', regex=False)

					# OFICIAL
					if i == 13:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)
						df[column] = df[column].str.replace('nan', '#N/A', regex=False)

					# BIN
					if i == 14:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

						for items in df['BIN'].iteritems():
							if(len(items[1]) > 6):
								f.write("\nhay bines con longitud mayor de 6")
								
							if (len(items[1]) < 6):
								f.write("\nhay bines con longitud menor de 6")
								break

					# Producto
					if i == 15:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z0-9-\\s]+', '', regex=True)

					# Color
					if i == 16:
						df[column] = df[column].astype(str)
						color = ['Black', 'Platino', 'Clásica', 'Bussines', 'Dorada', 'Infinite']
						if (~df[column].isin(color).any()):
							f.write("\nHay colores que no corresponden a 'Black', 'Platino', 'Clásica', 'Bussines', 'Dorada', 'Infinite'")
					
					# Ente Tarjeta
					if i == 17:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					i = i + 1

				f.close()

				df = df.fillna('')
				df = df.replace('nan', '', regex=False)

				df.insert(18,"TC VC CEE", "")
				df.insert(19,"TC Digital", "")

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 

				file = os.path.abspath('../Fuentes_procesadas/TarjetasEntregadas_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'TC', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su fuente')
				print(e)

		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [Fecha, Tarjeta, Monto, Moneda, Tipo Tarjeta, Tipo Cuenta], teniendo en cuenta el orden, las mayusculas y minusculas')
			print(e)
	
	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su fuente')
		print(e)

except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")
	print(e)

	