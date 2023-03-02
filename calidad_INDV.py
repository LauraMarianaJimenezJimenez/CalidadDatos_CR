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
	#data_date = '20230131'

	print('Procesando...')

	# file = os.path.abspath("../Fuentes_iniciales/ManualTransactions_"+ data_date "_.xls")
	file = os.path.abspath("../Fuentes_iniciales/IDNV_" + data_date + ".xlsx")

	# Raed file
	df = pd.read_excel(file, 'Detalle', header=None)

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
			df = df[['ta_num_tarjeta_titular', 'cuentatc', 'ta_id_linea', 'ta_id_operacion', 'ta_tmoneda', 'ta_saldo_principal', 'ta_indica_intra_extra', 'ta_tasa_int_vig', 'ta_fecha_corte', 'ta_id_deudor', 'ta_tpersona_deudor', 'Rango Mora', 'Ajuste de intereses', 'Tipo Tarjeta', 'Descripción Mora', 'En_Subsegmento']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try:
				path = os.path.abspath('../Informes/Informe_INDV_' + data_date + '.txt')

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
					# ta_num_tarjeta_titular
					if i == 0:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

						for items in df['ta_num_tarjeta_titular'].iteritems():
							if(len(items[1]) > 16):
								f.write("\nhay números de tarjeta con longitud mayor de 16")

							elif (len(items[1]) < 16):
								f.write("\nhay números de tarjeta con longitud menor de 16")
								break

					# cuentatc
					if i == 1:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

						for items in df['cuentatc'].iteritems():
							if(len(items[1]) > 13):
								f.write("\nhay cuentas tc con longitud mayor de 13")

							elif (len(items[1]) < 13):
								f.write("\nhay cuentas tc con longitud menor de 13")
								break

					if i == 2:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9NE\\s]+', '', regex=True)

						for items in df['ta_id_linea'].iteritems():
							if(len(items[1]) > 14):
								f.write("\nhay cuentas id linea con longitud mayor de 14")

							elif (len(items[1]) < 14):
								f.write("\nhay cuentas id linea con longitud menor de 14")
								break		

					if i == 3:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

						for items in df['ta_id_operacion'].iteritems():
							if(len(items[1]) > 25):
								f.write("\nhay identificación de operaciones con longitud mayor de 25")

							elif (len(items[1]) < 25):
								f.write("\nhay identificación de operaciones con longitud menor de 25")
								break

					if i == 4:
						df[column] = df[column].astype(str)
						moneda = ['1', '2']
						if (~df[column].isin(moneda).all()):
							f.write("\nHay monedas que no corresponden a 1 o 2")

					# ta_saldo_principal
					if i == 5:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

					# ta_indica_intra_extra
					if i == 6:
						df[column] = df[column].astype(str)
						intra_extra = ['I', 'E', np.nan, '']
						if (~df[column].isin(intra_extra).all()):
							f.write("\nHay indicadores de tipo de producto que no corresponden a intra, extra o banda")

					# ta_tasa_int_vig
					if i == 7:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].str.replace(" ","", regex=False)
						df[column] = df[column].str.strip()
						df[column] = df[column].astype(float)


						if (df[column] >= 100).any():
							f.write("\nHay tasas con porcentaje mayor que 100%")
							

					# ta_fecha_corte
					if i == 8:
						df[column] = df[column].astype(str)
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)
						if (df[column].str.slice(3, 5) != data_date[4:6]).any():
							f.write("\nHay fechas de corte que no corresponden para el mes de ejecución")

					# ta_id_deudor
					if i == 9:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.strip()
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)
						for items in df['ta_id_deudor'].iteritems():
							if(len(items[1]) > 19):
								f.write("\nhay identificación de deudor con longitud mayor de 19")

							elif (len(items[1]) < 19):
								f.write("\nhay identificación de operaciones con longitud menor de 19")
								break

					# ta_tpersona_deudor
					if i == 10:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.strip()
						df[column] = df[column].astype(str)
						deudor = ['F', 'J']
						if (~df[column].isin(deudor).all()):
							f.write("\nHay tipos de deudor que no corresponden a F o J")

					# Rango Mora
					if i == 11:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.strip()
						df[column] = df[column].astype(str)
						mora = ['Al día', 'De 1 a 30', 'De 31 a 60', 'De 61 a 90']
						if (~df[column].isin(mora).all()):
							f.write("\nHay rangos de mora que no corresponden a 'Al día', 'De 1 a 30', 'De 31 a 60', 'De 61 a 90'")

					# Ajuste de intereses
					if i == 12:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

					# Tipo de tarjeta
					if i == 13:
						df[column] = df[column].astype(str)
						tarjeta = ['VISA', 'MASTER']
						if (~df[column].isin(tarjeta).all()):
							f.write("\nHay tipos de tarjeta que no corresponden a VISA o MASTER")

					# Descripción Mora
					if i == 14:
						df[column] = df[column].astype(str)
						estado_mora = ['VIGENTE', 'VENCIDO']
						if (~df[column].isin(estado_mora).all()):
							f.write("\nHay estados de mora que no corresponden a VIGENTE o VENCIDO")

					# En_Subsegmento
					if i == 15: 
						df[column] = df[column].astype(str)
						df[column] = df[column].str.strip()
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^a-zA-Z-\\s]+', '', regex=True)


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
				file = os.path.abspath('../Fuentes_procesadas/IDNV_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Detalle', index=False)
				writer.save()

				print("Fuentes procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su fuente')
				print(e)
	
		except:
			print(' Hay un error en los nombres de las columnas, valide que sean [ta_num_tarjeta_titular, cuentatc, ta_id_linea, ta_id_operacion, ta_tmoneda, ta_saldo_principal, ta_indica_intra_extra, ta_tasa_int_vig, ta_fecha_corte, ta_id_deudor, ta_tpersona_deudor, Rango Mora, Ajuste de intereses, Tipo Tarjeta, Descripción Mora, En_Subsegmento], teniendo en cuenta el orden, las mayusculas y minusculas')
	
	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su fuente')
		print(e)

except:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")

	