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

def MIS_PAR_BALAN_SEG_ENG():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_BALAN_SEG_ENG.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando MIS_PAR_BALAN_SEG_ENG...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['cod_entity','cod_gl_group','cod_acco_cent','cod_currency','cod_driver']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_BALAN_SEG_ENG.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_BALAN_SEG_ENG_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_BALAN_SEG_ENG procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_BALAN_SEG_ENG sean [cod_entity, cod_gl_group, cod_acco_cent, cod_currency, cod_driver]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)



def MIS_PAR_BALAN_SEG_DRI():

	try:
		
		print('Procesando MIS_PAR_BALAN_SEG_DRI...')

		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_BALAN_SEG_DRI.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['cod_driver','cod_segment','allocation_perc']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_BALAN_SEG_DRI.txt')

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

				drivers = df['cod_driver']
				drivers = drivers.drop_duplicates()
				drivers = drivers.astype(str)
				df['cod_driver'] = df['cod_driver'].astype(str)
				drivers = drivers.replace('nan', 'NA', regex=False)
				df['cod_driver'] = df['cod_driver'].replace('nan', 'NA', regex=False)




				for indx in drivers.iteritems():
					x = 0

					for index, row in df.iterrows():
						if (indx[1] == row['cod_driver']):
							x = x + float(row['allocation_perc'])

					if(x > 1):
						f.write("\nEl driver " + str(indx[1]) + " tiene como resultado acumulado un valor mayor que 1: " + str(x))


					if(x < 1):
						f.write("\nEl driver " + str(indx[1]) + " tiene como resultado acumulado un valor menor que 1: " + str(x))

				f.close()

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_BALAN_SEG_DRI_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_BALAN_SEG_DRI procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_BALAN_SEG_DRI sean [cod_driver, cod_acco_cent, cod_expense, %]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)

MIS_PAR_BALAN_SEG_DRI()
MIS_PAR_BALAN_SEG_ENG()