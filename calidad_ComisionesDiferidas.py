# Importe de librerias a utilizar
import pandas as pd
import numpy as np
from pandas import ExcelWriter
import datetime
import time
import warnings
import os

warnings.filterwarnings("ignore")

# Se solicita la fecha del archivo para la creación del path que leera el archivo
try: 
	# Input
	print("Inserte la fecha de la fuente que desea procesar (yyyymm)")
	data_date = input()

	print('Procesando....')

	file = os.path.abspath("../Fuentes_iniciales/ComisionesDiferidas_" + data_date + ".xlsx")
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
			df = df[['FECHA', 'OPERACION', 'MONEDA_OPE', 'IngresoRealizado', 'Tipo de Operación']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try:
				path = os.path.abspath('../Informes/Informe_ComisionesDiferidas_' + data_date + '.txt')

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
					# Formato dd/mm/yyyy y se valida que todos los meses correspondan a 
					if i == 0:
						df[column] = df[column].astype(str)
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)

						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden para el mes de ejecución")

					# Operacion
					# Datos númericos
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# Moneda
					# Solo valor 0, valor 20 y valor 6
					if i == 2:
						df[column] = df[column].astype(str)
						monedas = ['0', '20', '6']
						if (~df[column].isin(monedas).all()):
							f.write("\nHay monedas que no corresponden a 0, 6 o 20")

					# Ingreso realizado
					# Solo datos numéricos, decimal separado por punto
					if i == 3:
							df[column] = df[column].astype(str)
							df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
							df[column] = df[column].str.replace(',', '.', regex=False)
							df[column] = df[column].fillna('0')
							df[column] = df[column].replace('nan', '0', regex=False)
							df[column] = df[column].replace('', '0', regex=False)
							df[column] = df[column].astype(float)

					# Tipo de Operación
					# Letras y guion al medio
					if i == 4:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z-\\s]+', '', regex=True)

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
				file = os.path.abspath('../Fuentes_procesadas/ComisionesDiferidas_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'CarteraPlazosYCuotasInteres', index=False)
				writer.save()

				print("Fuente procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su fuente')
				print(e)

		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [FECHA, OPERACION, MONEDA_OPE, IngresoRealizado, Tipo de Operación], teniendo en cuenta el orden, las mayusculas y minusculas')

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su fuente')
		print(e)

except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")


