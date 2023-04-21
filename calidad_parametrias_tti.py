# Importe de librerias a utilizar
import pandas as pd
import numpy as np
from pandas import ExcelWriter
import datetime
import time
import warnings
import os

# Ignore warnings
warnings.filterwarnings("ignore")

def MIS_PAR_TTI_ENG():

	try:
		# Input
		print("Inserte la fecha de la parametria MIS_PAR_TTI_ENG que desea procesar (yyyymm)")
		#data_date = input()
		data_date = '202301'

		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_TTI_ENG_' + data_date + '.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando MIS_PAR_TTI_ENG...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['cod_blce_prod','cod_business_line','cod_segment','cod_amrt_met','cod_blce_status','cod_bca_int','cod_currency','cod_curve','method_tti','ind_spread','term','term_unit','avg_period','avg_period_unit','term_factor','ind_volatility','des_volatility','perc_volatility','cod_curve_liq','method_liq','term_factor_liq','cod_curve_pea','cod_curve_enc','cod_curve_por','ind_adm']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_TTI_ENG.txt')

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

				f.write("\nSe eliminaron " + str(x - y) + " contratos repetidos")

				f.close()

				# Replace nan to empty
				df = df.fillna('')
				df = df.replace('nan', '', regex=False)

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()       
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_TTI_ENG_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_TTI_ENG procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_TTI_ENG sean [cod_blce_prod, cod_business_line, cod_segment, cod_amrt_met, cod_blce_status, cod_bca_int, cod_currency, cod_curve, method_tti, ind_spread, term, term_unit, avg_period, avg_period_unit, term_factor, ind_volatility, des_volatility, perc_volatility, cod_curve_liq, method_liq, term_factor_liq, cod_curve_pea, cod_curve_enc, cod_curve_por, ind_adm]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)

def MIS_PAR_TTI_ENG():

	try:
		# Input
		print("Inserte la fecha de la parametria MIS_PAR_TTI_ENG que desea procesar (yyyymmdd)")
		#data_date = input()
		data_date = '202301'

		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_TTI_SPE_' + data_date + '.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando MIS_PAR_TTI_SPE...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['idf_cto','cod_value','rate_tti','rate_liq','rate_enc','rate_pea','rate_por']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_TTI_SPE.txt')

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

				f.write("\nSe eliminaron " + str(x - y) + " contratos repetidos")

				f.close()

				# Replace nan to empty
				df = df.fillna('')
				df = df.replace('nan', '', regex=False)

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()       
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_TTI_SPE_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_TTI_SPE procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_TTI_SPE sean [idf_cto, cod_value, rate_tti, rate_liq, rate_enc, rate_pea, rate_por]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)


MIS_PAR_TTI_ENG()
MIS_PAR_TTI_SPE()