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

def MIS_HIERARCHY_BL():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_HIERARCHY_BL.xlsx')
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
			df = df[['cod_segment','cod_blce_prod','cod_business_line']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_HIERARCHY_BL.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_HIERARCHY_BL_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Jerarquia MIS_HIERARCHY_BL procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_HIERARCHY_BL sean [cod_segment, cod_blce_prod, cod_business_line]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)


def MIS_HIERARCHY_BLCE_PROD():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_HIERARCHY_BLCE_PROD.xlsx')
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
			df = df[['ord_level_01','cod_level_01','desc_level_01','ord_level_02','cod_level_02','desc_level_02','ord_level_03','cod_level_03','desc_level_03','ord_level_04','cod_level_04','desc_level_04','ord_level_05','cod_level_05','desc_level_05','ord_level_06','cod_level_06','desc_level_06','ord_level_07','cod_level_07','desc_level_07','ord_level_08','cod_level_08','desc_level_08']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_HIERARCHY_BLCE_PROD.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_HIERARCHY_BLCE_PROD_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Jerarquia MIS_HIERARCHY_BLCE_PROD procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_HIERARCHY_BLCE_PROD sean [ord_level_01, cod_level_01, desc_level_01, ord_level_02, cod_level_02, desc_level_02, ord_level_03, cod_level_03, desc_level_03, ord_level_04, cod_level_04, desc_level_04, ord_level_05, cod_level_05, desc_level_05, ord_level_06, cod_level_06, desc_level_06, ord_level_07, cod_level_07, desc_level_07, ord_level_08, cod_level_08, desc_level_08]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)



def MIS_HIERARCHY_PL_ACC():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_HIERARCHY_PL_ACC.xlsx')
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
			df = df[['ord_level_01','cod_level_01','desc_level_01','ord_level_02','cod_level_02','desc_level_02','ord_level_03','cod_level_03','desc_level_03','ord_level_04','cod_level_04','desc_level_04','ord_level_05','cod_level_05','desc_level_05','ord_level_06','cod_level_06','desc_level_06','ord_level_07','cod_level_07','desc_level_07','ord_level_08','cod_level_08','desc_level_08']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_HIERARCHY_PL_ACC.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_HIERARCHY_PL_ACC_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Jerarquia MIS_HIERARCHY_PL_ACC procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_HIERARCHY_PL_ACC sean [ord_level_01, cod_level_01, desc_level_01, ord_level_02, cod_level_02, desc_level_02, ord_level_03, cod_level_03, desc_level_03, ord_level_04, cod_level_04, desc_level_04, ord_level_05, cod_level_05, desc_level_05, ord_level_06, cod_level_06, desc_level_06, ord_level_07, cod_level_07, desc_level_07, ord_level_08, cod_level_08, desc_level_08]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)


def MIS_HIERARCHY_PROD_BL():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_HIERARCHY_PROD_BL.xlsx')
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
			df = df[['cod_level_01','desc_level_01','ord_level_02','cod_level_02','desc_level_02','ord_level_03','desc_level_03','ord_level_04','desc_level_04']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_HIERARCHY_PROD_BL.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_HIERARCHY_PROD_BL_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Jerarquia MIS_HIERARCHY_PROD_BL procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_HIERARCHY_PROD_BL sean [cod_level_01, desc_level_01, ord_level_02, cod_level_02, desc_level_02, ord_level_03, desc_level_03, ord_level_04, desc_level_04]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)



def MIS_HIERARCHY_UN():

	try:
		# Create path
		file = os.path.abspath('../Fuentes_iniciales/MIS_HIERARCHY_UN.xlsx')
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
			df = df[['cod_business_line','cod_business_unit']]
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)


			try:
				path = os.path.abspath('../Informes/Informe_MIS_HIERARCHY_UN.txt')

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
				file = os.path.abspath('../Fuentes_procesadas/MIS_HIERARCHY_UN_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Jerarquia MIS_HIERARCHY_UN procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria MIS_HIERARCHY_UN')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_HIERARCHY_UN sean [cod_business_line, cod_business_unit]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria MIS_HIERARCHY_UN')
		print(e)


MIS_HIERARCHY_BL()
MIS_HIERARCHY_BLCE_PROD()
MIS_HIERARCHY_PL_ACC()
MIS_HIERARCHY_PROD_BL()
MIS_HIERARCHY_UN()

