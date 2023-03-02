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

	file = os.path.abspath("../Fuentes_iniciales/Brokerage_" + data_date + ".xls")

	# Raed file
	df = pd.read_excel(file, header=None)

	try:
		# Se toman el dataframe desde la fila 8 de la fuente para descartar textos informativos

		# Take dataframe
		df = df.iloc[8: ,]
		# Define headers
		df.columns = df.iloc[1]
		df = df[2:]
		df.columns = df.columns.astype(str)

		# Remove entirely empty row
		df = df.dropna(how="all")
		# Remove entirely empty column
		#df = df.dropna(how="all", axis=1)

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['Empresa', 'Fecha', 'Suc. Origen', 'Prod.Origen', 'Secuencial', 'Cliente', 'Nombre', 'Clase', 'Valor Origen', 'Mon.Origen', 'Cambio', 'Valor Destino', 'Mon.Destino', 'Compromiso', 'Tipo', 'Usuario', 'xCaja', 'Autorización', 'Estado', 'Hora', 'Valor', 'CorteBCCR', 'TCxMonto', 'DiaSemana', 'verificaMonex', 'Monto1', 'Precio', 'Monto2', 'Precio2', 'Producto1', 'Monto3', 'Precio3', 'Producto2', 'PL compras', 'PL Ventas', 'New PL', 'Spread', 'Client Type', 'Client Type 2', 'SubSegmento', 'Line of Busines?', 'Board Rate', 'Price T0 - T1', 'SpreadClient', 'Spread_Lob', 'Brok_Lob', 'Brok_GM', 'Client Type 2_Rev', 'HH', 'DISTRIBUCION']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try:
				# Create output file
				path = os.path.abspath('../Informes/Informe_Brokerage_' + data_date + '.txt')

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
					


				# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
				# especiales que puedan afectar la converisión a csv

				# Changes for all columns

				#Remove carring return
				df = df.replace({r'\\r': ' '}, regex=True)
				#Remove line breaks
				df = df.replace(r'\s+|\\n', ' ', regex=True)
				#Remove pipelines, single quote, semicolon
				df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

				# Tratamientos especificos para campos puntuales del MIS

				i = 0
				for column in df:
					# Fecha as date_origin or date_disb
					# Formato MM/DD/AAAA
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)

						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas que no corresponden para el mes de ejecución")

					# Suc. Origen as cod_offi
					# Solo datos numéricos
					if i == 2:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# Secuencial as idf_cto
					# Solo datos numéricos
					if i == 4:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# cliente as idf_cli
					# Solo datos numéricos
					if i == 5:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# Valor origen
					if (i == 8 or i == 10 or i == 11 or i == 13 or i == 20 or i == 22 or (i > 24 and i < 37) or (i > 40 and i < 45) or i == 48 or i == 49):
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

					# CorteBCCR
					# Formato MM/DD/AAAA
					if i == 21:
						df[column] = df[column].astype(str)
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column]).dt.strftime('%m/%d/%Y'), pd.to_datetime(df[column], dayfirst=True).dt.strftime('%m/%d/%Y'))		
					
					# client type as cod_subproduct
					# Alfabético. Las opciones son "CMB", "PFS", "GBM".
					if i == 37:
						df[column] = df[column].astype(str)
						subproductos = ['CMB', 'PFS', 'GBM']
						if (~df[column].isin(subproductos).all()):
							f.write("\nHay subproductos que no corresponden en la columna client type")

					# brok_Lob as pl importe de ROF para tesoreria		
					# Solo datos numéricos, no debe incluír numeros negatiivos
					if i == 45:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

						if (df[column] <= 0).any():
							f.write("\nHay importes negativos en la columna Brok_Lob")

					# brok_GM as pl importe de ROF para tesoreria		
					# Solo datos numéricos, no debe incluír numeros negatiivos 
					if i == 46:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

						if (df[column] <= 0).any():
							f.write("\nHay importes negativos en la columna Brok_GM")


					i = i + 1

				df = df.fillna('')
				df = df.replace('nan', '', regex=False)
				df = df.replace(np.nan, '', regex=False)


				f.close()

				df.rename(columns={"Precio2": "Precio"}, inplace=True)
				df.rename(columns={"Precio3": "Precio"}, inplace=True)

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 

				file = os.path.abspath('../Fuentes_procesadas/Brokerage_' + data_date + '_' + unix_time + '.xls')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja de datos', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su fuente')
				print(e)
				
		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [Empresa, Fecha, Suc. Origen, Prod.Origen, Secuencial, Cliente, Nombre, Clase, Valor Origen, Mon.Origen, Cambio, Valor Destino, Mon.Destino, Compromiso, Tipo, Usuario, xCaja, Autorización, Estado, Hora, Valor, CorteBCCR, TCxMonto, DiaSemana, verificaMonex, Monto1, Precio, Monto2, Precio2, Producto1, Monto3, Precio3, Producto2, PL compras, PL Ventas, New PL, Spread, Client Type, Client Type 2, SubSegmento, Line of Busines?, Board Rate, Price T0 - T1, SpreadClient, Spread_Lob, Brok_Lob, Brok_GM, Client Type 2_Rev, HH, DISTRIBUCION], teniendo en cuenta el orden, las mayusculas y minusculas')
	
	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su fuente')
		print(e)
except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")
