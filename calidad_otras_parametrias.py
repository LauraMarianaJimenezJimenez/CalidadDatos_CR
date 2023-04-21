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

def MIS_PAR_ENTITY():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_ENTITY.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando MIS_PAR_ENTITY...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['ord_entity','cod_entity','des_entity']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_ENTITY.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_ENTITY_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_ENTITY procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_ENTITY sean [ord_entity, cod_entity, des_entity]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)

def MIS_PAR_REL_EXP_TYP():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_EXP_TYP.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando MIS_PAR_REL_EXP_TYP...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['AGRUPADOR CONTABLE','COD CENTRO COSTO','COD GASTO','Tipo Gasto','Familia Gasto']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_EXP_TYP.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_REL_EXP_TYP_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_REL_EXP_TYP procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_EXP_TYP sean [AGRUPADOR CONTABLE, COD CENTRO COSTO, COD GASTO, Tipo Gasto, Familia Gasto]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)

def MIS_PAR_REL_PROG_CARD():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_PROG_CARD.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando MIS_PAR_REL_PROG_CARD...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['bin','cod_prog_card','des_prog_card']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_PROG_CARD.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_REL_PROG_CARD_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_REL_PROG_CARD procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_PROG_CARD sean [bin,cod_prog_card,des_prog_card]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)


def MIS_PAR_REL_PROG_CARD():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_PROG_CARD.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando MIS_PAR_REL_PROG_CARD...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['bin','cod_prog_card','des_prog_card']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_PROG_CARD.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_REL_PROG_CARD_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_REL_PROG_CARD procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_PROG_CARD sean [bin,cod_prog_card,des_prog_card]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)

def MIS_PAR_REL_REG_DIMENSIONS():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_REG_DIMENSIONS.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando MIS_PAR_REL_REG_DIMENSIONS...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['IDF_CLI', 'IDF_CTO', 'ID_ECO_GRO', 'COU_ECO_GRO', 'IND_MUL_LAT', 'COU_CAR_OFF', 'COD_CONV']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_REG_DIMENSIONS.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_REL_REG_DIMENSIONS_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Paramteria MIS_PAR_REL_REG_DIMENSIONS procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_REG_DIMENSIONS sean [IDF_CLI, IDF_CTO, ID_ECO_GRO, COU_ECO_GRO, IND_MUL_LAT, COU_CAR_OFF, COD_CONV]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)


MIS_PAR_ENTITY()
MIS_PAR_REL_EXP_TYP()
MIS_PAR_REL_PROG_CARD()
MIS_PAR_REL_REG_DIMENSIONS()