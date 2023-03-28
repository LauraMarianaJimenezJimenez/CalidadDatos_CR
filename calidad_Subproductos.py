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
	print("Inserte la fecha de la fuente que desea procesar")
	data_date = input()

	print('Procesando...')

	# file = os.path.abspath("../Fuentes_iniciales/ManualTransactions_"+ data_date "_.xls")
	file = os.path.abspath("../Fuentes_iniciales/Subproductos_" + data_date + ".xlsx")

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
			df = df[['Tarjeta', 'Cuenta TCR', 'Cliente', 'Ente', 'Monto', 'Moneda', 'Dolariza', 'Fecha', 'Vendedor', 'Usuario', 'Puesto', 'Canal de Venta', 'Tipo de Plan', 'PLAN2', 'VENDEDOR']]
				
			# Remove duplicate records
			#df = df.drop_duplicates()

			try:

				path = os.path.abspath('../Informes/Informe_Subproductos_' + data_date + '.txt')

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
					# Tarjeta
					if i == 0:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)
					
						for items in df['Tarjeta'].iteritems():
							if(len(items[1]) > 16):
								f.write("\nhay tarjetas con longitud mayor de 16")

							elif (len(items[1]) < 16):
								f.write("\nhay tarjetas con longitud menor de 16")
								break

					# Cuenta TCR
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

						for items in df['Cuenta TCR'].iteritems():
							if(len(items[1]) > 13):
								f.write("\nhay cuentas con longitud mayor de 13")

							elif (len(items[1]) < 13):
								f.write("\nhay cuentas con longitud menor de 13")
								break


						# Cliente
					if i == 2:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z\\s]+', '', regex=True)

					# Ente
					if i == 3:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)			

					# Monto
					if i == 4:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

					# Moneda
					if i == 5:
						df[column] = df[column].astype(str)
						moneda = ['colones', 'dolares']
						if (~df[column].isin(moneda).all()):
							f.write("\nHay tipos de moneda que no corresponden a colones o dolares")

					# Dolariza
					if i == 6:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

					# Fecha
					if i == 7:
						df[column] = df[column].astype(str)
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)
						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden para el mes de ejecución")

					# Vendedor
					if i == 8:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)

					# Usuario
					if i == 9:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)
						
					# Puesto 
					if i == 10:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z\\s]+', '', regex=True)
						df[column] = df[column].str.replace('', '-', regex=False)


						for items in df['Usuario'].iteritems():
							if(items[1] == 'Digital'):
								valor = '-'
								df.loc[items[0]][10] = valor

								if (str(df.loc[items[0]][10]) != '-'):
									f.write("\nPara usuario Digital hay puesto diferente de '-'")

					# Canal de Ventas
					if i == 11:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)	

					# Tipo de Plan
					if i == 12:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z-\\s]+', '', regex=True)	

					# PLAN2
					if i == 13:
						df[column] = df[column].astype(str)
						plan = ['Intra', 'Extra']
						if (~df[column].isin(plan).all()):
							f.write("\nHay tipos de plan que no corresponden a Intra o Extra")

					# VENDEDOR
					if i == 14:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)	

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
				file = os.path.abspath('../Fuentes_procesadas/Subproductos_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Subproductos', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su fuente')
				print(e)

		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [COD, DESCRIP, AFECTACION, Signo, Desc_IncCom_CBO], teniendo en cuenta el orden, las mayusculas y minusculas')

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su fuente')
		print(e)

except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")

	