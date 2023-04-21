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

def MIS_PAR_ALLOC_AC_ENG():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_ALLOC_AC_ENG_' + data_date + '.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando MIS_PAR_ALLOC_AC_ENG...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['ENTIDAD','AGRUPADOR CONTABLE','COD CENTRO COSTO','COD_GASTO','MONEDA','DRIVER']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_ALLOC_AC_ENG.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_ALLOC_AC_ENG_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_ALLOC_AC_ENG procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_ALLOC_AC_ENG sean [ENTIDAD, AGRUPADOR CONTABLE, COD CENTRO COSTO, COD_GASTO, MONEDA, DRIVER]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)

def MIS_PAR_ALLOC_SEG_ENG():

	try:
		
		print('Procesando MIS_PAR_ALLOC_SEG_ENG...')

		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_ALLOC_SEG_ENG_' + data_date + '.xlsx')
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
			df = df[['cod_entity','cod_gl_group','cod_acco_cent','cod_expense','cod_currency','cod_driver']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_ALLOC_SEG_ENG.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_ALLOC_SEG_ENG_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_ALLOC_AC_ENG procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_ALLOC_SEG_ENG sean [cod_entity, cod_gl_group, cod_acco_cent, cod_expense, cod_currency, cod_driver]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)


def MIS_PAR_ALLOC_AC_DRI():

	try:
		
		print('Procesando MIS_PAR_ALLOC_AC_DRI...')

		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_ALLOC_AC_DRI_' + data_date + '.xlsx')
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
			df = df[['cod_driver','cod_acco_cent','cod_expense','%']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_ALLOC_AC_DRI.txt')

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
							x = x + float(row['%'])

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_ALLOC_AC_DRI_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_ALLOC_AC_DRI procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_ALLOC_AC_DRI sean [cod_driver, cod_acco_cent, cod_expense, %]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)

def MIS_PAR_ALLOC_SEG_DRI():
	try:
		
		print('Procesando MIS_PAR_ALLOC_SEG_DRI...')

		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_ALLOC_SEG_DRI_' + data_date + '.xlsx')
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
			df = df[['cod_driver','cod_segment','cod_blce_prod','%']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_ALLOC_SEG_DRI.txt')

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
					#print(indx[1])
					#print(x)
					x = 0
					for index, row in df.iterrows():
						if (indx[1] == row['cod_driver']):
							x = x + float(row['%'])

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_ALLOC_SEG_DRI_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()
				
				print("Paramteria MIS_PAR_ALLOC_SEG_DRI procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_ALLOC_AC_DRI sean [cod_driver, cod_segment, cod_blce_prod, %]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)


# Input
print("Inserte la fecha de la fuente que desea procesar (yyyymm)")
#data_date = input()
data_date = '202301'

MIS_PAR_ALLOC_AC_ENG()
MIS_PAR_ALLOC_SEG_ENG()
MIS_PAR_ALLOC_AC_DRI()
MIS_PAR_ALLOC_SEG_DRI()