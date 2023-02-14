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
	print("Inserte la fecha de la fuente que desea procesar (yyyymm)")
	data_date = input()
	#data_date = '202212'

	# file = os.path.abspath("../Fuentes_iniciales/ManualTransactions_"+ data_date "_.xls")
	file = os.path.abspath("../Fuentes_iniciales/Trade_" + data_date + ".xlsx")

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
			df = df[['Mes', 'Operación', 'Amortización del mes', 'Producto', 'Ente', 'Moneda']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try: 
				path = os.path.abspath('../Informes/Informe_Trade_' + data_date + '.txt')

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
					# Mes 
					# Formato dd/mm/yyyy y se valida que todos los meses correspondan 
					if i == 0:
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)
						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden para el mes de ejecución")

					# Operación
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z0-9\\s]+', '', regex=True)

					# Amortización del mes
					if i == 2:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].astype(float)

					# Producto
					if i == 3:
						productos = ['Garantias Colones', 'Garantias Colones', 'Garantias Dólares', 'Cartas de Crédito']
						if (~df[column].isin(productos).all()):
							f.write("\nHay productos que no corresponden a 'Garantias Colones', 'Garantias Colones', 'Garantias Dólares', 'Cartas de Crédito' ")
						

					if i == 4:
						for items in df['Producto'].iteritems():
							if(items[1] == 'Contragarantias'):

								valor = 'No son clientes del banco'
								df.loc[items[0]][4] = valor

								if (str(df.loc[items[0]][4]) != 'No son clientes del banco'):
									f.write("\nPara productos de contragarantias hay clientes con valor difetente a 'No son clientes del banco'")



							if(items[1] != 'Contragarantias'):
								text = df.loc[items[0]][4]
								df.loc[items[0]][4] = re.search('[^0-9\\s]+', text)

					if i == 5:
						monedas = ['0', '20', '6']
						if (~df[column].isin(monedas).all()):
							f.write("\nHay monedas que no corresponden a 0, 6 o 20")

					i = i + 1

				f.close()

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 

				file = os.path.abspath('../Fuentes_procesadas/Trade_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja1', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except:
				print(' Ha ocurrido un error, por favor verifique su fuente')
		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [Mes, Operación, Amortización del mes, Producto, Ente, Moneda], teniendo en cuenta el orden, las mayusculas y minusculas')
	except:
		print(' Ha ocurrido un error, por favor verifique su fuente')
except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")