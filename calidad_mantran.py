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
	print("Inserte la fecha de la fuente que desea procesar")
	data_date = input()

	print('Procesando...')

	file = os.path.abspath("../Fuentes_iniciales/ManualTransactions_" + data_date + ".xls")

	# Raed file
	df = pd.read_excel(file, header=None)

	try:

		# Se toman el dataframe desde la fila 8 de la fuente para descartar textos informativos

		# Take dataframe
		df = df.iloc[9: ,]

		# Define headers
		df.columns = df.iloc[0]
		df = df[1:]
		df.columns = df.columns.astype(str)

		# Remove entirely empty column
		df = df.dropna(how="all", axis=1)
		# Remove entirely empty row
		df = df.dropna(how="all")

		try:

			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['Trade Date', 'Time', 'CCY1', 'Notional1', 'Client Price', 'Close Price', 'Client Type', 'Reference Price', 'PL CM', 'PL GBM', 'Total PL', 'PL CM2', 'PL GBM2', 'Total PL2', 'PL COL CB', 'Blank', 'Client Type 2', 'Criterio', 'Subsegmento', 'Ente', 'CCY', 'Type']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try:

				path = os.path.abspath('../Informes/Informe_ManualTransactions_' + data_date + '.txt')

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

				# Changes for all columns

				#Remove carring return
				df = df.replace({r'\\r': ' '}, regex=True)
				#Remove line breaks
				df = df.replace(r'\s+|\\n', ' ', regex=True)
				#Remove pipelines, single quote, semicolon
				df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

				i = 0
				for column in df:
					# Fecha as date_origin or date_disb
					# Formato DD/MM/AAAA
					if i == 0:
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)

						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden para el mes de ejecución")
						
					# Client Type as cod_subproduct
					# Alfabético. Las opciones son "CMB", "PFS", "GBM".
					if i == 6:
						subproductos = ['CMB', 'PFS', 'GBM']
						if (~df[column].isin(subproductos).all()):
							f.write("\nHay subproductos que no corresponden en la columna client type")

					if (i > 2 and i < 6 or i > 6 and i < 15):
						df[column] = df[column].fillna(0)
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace('.', ',', regex=False)

					# Cliente as idf_cli
					# Solo datos numéricos
					if i == 19:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					i = i + 1

				df_merged = df 

				df_merged.rename(columns={"PL CM2": "PL CM"}, inplace=True)
				df_merged.rename(columns={"PL GBM2": "PL GBM"}, inplace=True)
				df_merged.rename(columns={"Total PL2": "Total PL"}, inplace=True)
				df_merged.rename(columns={"Blank": " "}, inplace=True)
				df.insert(16,"", "")

				df_merged = df_merged.fillna('')
				df_merged = df_merged.replace('nan', '', regex=False)
				df_merged = df_merged.replace(np.nan, '', regex=False)

				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				file = os.path.abspath('../Fuentes_procesadas/ManualTransactions_' + data_date + '_' + unix_time +'.xls')
				writer = ExcelWriter(file)
				df_merged.to_excel(writer, 'Hoja de datos', index=False)
				writer.save()

				print("Fuente procesada con exito")

			except:
				print(' Ha ocurrido un error, por favor verifique su fuente')
		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [Trade Date, Time, CCY1, Notional1, Client Price, Close Price, Client Type, Reference Price, PL CM, PL GBM, Total PL, PL CM2, PL GBM2, Total PL2, PL COL CB, Blank, Client Type 2, Criterio, Subsegmento, Ente, CCY, Type], teniendo en cuenta el orden, las mayusculas y minusculas')
	except:
		print(' Ha ocurrido un error, por favor verifique su fuente')
except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")
