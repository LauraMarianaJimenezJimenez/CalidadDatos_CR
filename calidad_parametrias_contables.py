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

def MIS_PAR_REL_CAF_ACC():

	try:
		# Create path
		file_acc = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_CAF_ACC.xlsx')
		# Raed file
		df_acc = pd.read_excel(file_acc, header=None)

		# Take dataframe
		df_acc.columns = df_acc.iloc[0]
		df_acc = df_acc[1:]

		# Remove entirely empty row
		df_acc = df_acc.dropna(how='all')

		try:
			# Delete withespace in headers
			df_acc = df_acc.rename(columns=lambda x: x.strip())
			# Take specific columns
			df_acc = df_acc[['ENTIDAD','MONEDA','CUENTA','AGRUPADOR CONT']]
			# Remove duplicate records
			df_acc = df_acc.drop_duplicates()


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_CAF_ACC.txt')

				f = open(path,"w+")
				f.write(file_acc)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df_acc))
				f.write("\nCantidad de Columnas: %d" % len(df_acc.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				for column in df_acc:
					text = str(column)
					f.write("\n")
					f.write(text)
					f.write(": ")
					text = str(df_acc[column].isnull().sum())
					f.write(text)

				i = 0
				for column in df_acc:
					if i == 1:
						df_acc[column] = df_acc[column].astype(str)
						moneda = ['CRC']
						if (~df_acc[column].isin(moneda).all()):
							f.write("\nHay valores que no corresponden con tipo de moneda 'CRC'")
							

					if i == 0:
						df_acc[column] = df_acc[column].astype(str)
						entidad = ['11', '15', '21', '23', '4', '6', '9']
						if (~df_acc[column].isin(entidad).all()):
							f.write("\nHay valores que no corresponden con las entidades '11', '15', '21', '23', '4', '6', '9' ")
							
					i = i + 1

				print("Fuente MIS_PAR_REL_CAF_ACC procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_CAF_ACC sean [cod_entity, cod_product, cod_subproduct, cod_currency, cod_blce_prod, cod_value, cod_gl_group]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)

def MIS_PAR_REL_BP_ACC():

	try:
		# Create path
		file_bp_acc = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_BP_ACC.xlsx')
		# Raed file
		df_bp_acc = pd.read_excel(file_bp_acc, header=None)

		# Take dataframe
		df_bp_acc.columns = df_bp_acc.iloc[0]
		df_bp_acc = df_bp_acc[1:]

		# Remove entirely empty row
		df_bp_acc = df_bp_acc.dropna(how='all')

		try:
			# Delete withespace in headers
			df_bp_acc = df_bp_acc.rename(columns=lambda x: x.strip())
			# Take specific columns
			df_bp_acc = df_bp_acc[['cod_entity','cod_currency','cod_gl_group','cod_blce_prod']]
			# Remove duplicate records
			df_bp_acc = df_bp_acc.drop_duplicates()

			try:	

				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_BP_ACC.txt')

				f = open(path,"w+")
				f.write(file_bp_acc)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df_bp_acc))
				f.write("\nCantidad de Columnas: %d" % len(df_bp_acc.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				for column in df_bp_acc:
					text = str(column)
					f.write("\n")
					f.write(text)
					f.write(": ")
					text = str(df_bp_acc[column].isnull().sum())
					f.write(text)

				# Create path
				file_acc = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_CAF_ACC.xlsx')
				# Raed file
				df_acc = pd.read_excel(file_acc, header=None)

				# Take specific columns
				df_acc.columns = df_acc.iloc[0]
				df_acc.rename(columns = {'AGRUPADOR CONT':'cod_gl_group'}, inplace = True)
				df_acc = df_acc[1:]

				left_join = pd.merge(df_bp_acc, df_acc, on ='cod_gl_group', how ='left')


				#print(left_join[left_join['CUENTA'].isna()])

				results = left_join[left_join['CUENTA'].isna()]

				f.write("\n")
				results['cod_gl_group']= results['cod_gl_group'].astype(str)
				f.write("\nCuentas contables que estan en la parametria MIS_PAR_REL_BP_ACC pero no estan en la parametria contable MIS_PAR_REL_CAF_ACC")
				f.write("\n")
				f.write(" ")
				f.write(results['cod_gl_group'].str.cat(sep='\n'))


				i = 0
				for column in df_bp_acc:
					if i == 0:
						df_bp_acc[column] = df_bp_acc[column].astype(str)
						entidad = ['11', '15', '21', '23', '4', '6', '9']
						if (~df_bp_acc[column].isin(entidad).all()):
							f.write("\nHay valores que no corresponden con las entidades '11', '15', '21', '23', '4', '6', '9' ")

					if i == 1:
						df_bp_acc[column] = df_bp_acc[column].astype(str)
						moneda = ['CRC']
						if (~df_bp_acc[column].isin(moneda).all()):
							f.write("\nHay valores que no corresponden con la moneda 'CRC'")

					
					if i == 2:
						df_bp_acc[column] = df_bp_acc[column].astype(str)
						df_bp_acc['cod_gl_group'] = df_bp_acc['cod_gl_group'].astype(str)


						for index, row in df_bp_acc.iterrows():
							if (row['cod_gl_group'][0] == '1' or row['cod_gl_group'][0] == '2' or row['cod_gl_group'][0] == '3'):
								if (row['cod_blce_prod'] == 'NO_BALANCE'):
									f.write("\nLa cuenta " + row['cod_gl_group'] + " tiene producto balance:  " + row['cod_blce_prod'])
					

					if i == 3:
						df_bp_acc[column] = df_bp_acc[column].astype(str)
						df_bp_acc[column] = df_bp_acc[column].str.replace('[a-zA-Z-\\s]+', '', regex=True)

					i = i + 1

				print("Fuente MIS_PAR_REL_BP_ACC procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_BP_ACC')
				print(e)			

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_BP_ACC sean [cod_entity, cod_currency, cod_product, cod_subproduct, cod_gl_group, cod_act_type, cod_rate_type, cod_blce_prod]')
			print(e)			

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_BP_ACC')
		print(e)

def MIS_PAR_REL_PL_ACC():

	try:
		# Create path
		file_pl_acc = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_PL_ACC.xlsx')
		# Raed file
		df_pl_acc = pd.read_excel(file_pl_acc, header=None)

		# Take dataframe
		df_pl_acc.columns = df_pl_acc.iloc[0]
		df_pl_acc = df_pl_acc[1:]

		# Remove entirely empty row
		df_pl_acc = df_pl_acc.dropna(how='all')

		try:
			# Delete withespace in headers
			df_pl_acc = df_pl_acc.rename(columns=lambda x: x.strip())
			# Take specific columns
			df_pl_acc = df_pl_acc[['ENTIDAD','MONEDA','AGRUPADOR CONT','CUENTA P&G', 'IND DETALLE']]
			# Remove duplicate records
			df_pl_acc = df_pl_acc.drop_duplicates()

			try:	

				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_PL_ACC.txt')

				f = open(path,"w+")
				f.write(file_pl_acc)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df_pl_acc))
				f.write("\nCantidad de Columnas: %d" % len(df_pl_acc.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				for column in df_pl_acc:
					text = str(column)
					f.write("\n")
					f.write(text)
					f.write(": ")
					text = str(df_pl_acc[column].isnull().sum())
					f.write(text)

				# Create path
				file_acc = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_CAF_ACC.xlsx')
				# Raed file
				df_acc = pd.read_excel(file_acc, header=None)

				# Take specific columns
				df_acc.columns = df_acc.iloc[0]
				#df_acc.rename(columns = {'AGRUPADOR CONT':'cod_gl_group'}, inplace = True)
				df_acc = df_acc[1:]

				left_join = pd.merge(df_pl_acc, df_acc, on ='AGRUPADOR CONT', how ='left')


				#print(left_join[left_join['CUENTA'].isna()])

				results = left_join[left_join['CUENTA'].isna()]

				f.write("\n")
				results['AGRUPADOR CONT']= results['AGRUPADOR CONT'].astype(str)
				f.write("\nCuentas contables que estan en la parametria MIS_PAR_REL_PL_ACC pero no estan en la parametria contable MIS_PAR_REL_CAF_ACC")
				f.write("\n")
				f.write(" ")
				f.write(results['AGRUPADOR CONT'].str.cat(sep='\n'))


				i = 0
				for column in df_pl_acc:
					if i == 0:
						df_pl_acc[column] = df_pl_acc[column].astype(str)
						entidad = ['11', '15', '21', '23', '4', '6', '9']
						if (~df_pl_acc[column].isin(entidad).all()):
							f.write("\nHay valores que no corresponden con las entidades '11', '15', '21', '23', '4', '6', '9' ")

					if i == 1:
						df_pl_acc[column] = df_pl_acc[column].astype(str)
						moneda = ['CRC']
						if (~df_pl_acc[column].isin(moneda).all()):
							f.write("\nHay valores que no corresponden con la moneda 'CRC'")

					# preguntar a william que es lo correcto

					if i == 2:
						df_pl_acc[column] = df_pl_acc[column].astype(str)
						df_pl_acc['AGRUPADOR CONT'] = df_pl_acc['AGRUPADOR CONT'].astype(str)


						for index, row in df_pl_acc.iterrows():
							if (row['AGRUPADOR CONT'][0] == '4' or row['AGRUPADOR CONT'][0] == '5'):
								if (row['CUENTA P&G'] == 'NO_PYG'):
									f.write("\nLa cuenta " + row['AGRUPADOR CONT'] + " tiene codigo P&G igual a:  " + row['CUENTA P&G'])

					if i == 3:
						df_pl_acc[column] = df_pl_acc[column].astype(str)
						df_pl_acc[column] = df_pl_acc[column].str.replace('[a-zA-Z-\\s]+', '', regex=True)

					i = i + 1

				print("Fuente MIS_PAR_REL_PL_ACC procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_PL_ACC')
				print(e)			

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_PL_ACC sean [cod_entity, cod_currency, cod_product, cod_subproduct, cod_gl_group, cod_act_type, cod_rate_type, cod_blce_prod]')
			print(e)			

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_PL_ACC')
		print(e)

def MIS_PAR_REL_BL_ACC():

	try:
		# Create path
		file_bl_acc = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_BL_ACC.xlsx')
		# Raed file
		df_bl_acc = pd.read_excel(file_bl_acc, header=None)

		# Take dataframe
		df_bl_acc.columns = df_bl_acc.iloc[0]
		df_bl_acc = df_bl_acc[1:]

		# Remove entirely empty row
		df_bl_acc = df_bl_acc.dropna(how='all')

		try:
			# Delete withespace in headers
			df_bl_acc = df_bl_acc.rename(columns=lambda x: x.strip())
			# Take specific columns
			df_bl_acc = df_bl_acc[['ENTIDAD','AGRUPADOR CONT','LINEA DE NEGOCIO']]
			# Remove duplicate records
			df_bl_acc = df_bl_acc.drop_duplicates()

			try:	

				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_BL_ACC.txt')

				f = open(path,"w+")
				f.write(file_bl_acc)


				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df_bl_acc))
				f.write("\nCantidad de Columnas: %d" % len(df_bl_acc.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				for column in df_bl_acc:
					text = str(column)
					f.write("\n")
					f.write(text)
					f.write(": ")
					text = str(df_bl_acc[column].isnull().sum())
					f.write(text)

				#print(df_bl_acc[df_bl_acc['AGRUPADOR CONT'].isna()])

				i = 0
				for column in df_bl_acc:

					if i == 2:
						df_bl_acc[column] = df_bl_acc[column].astype(str)
						linea = ['EMPRESAS','PERSONAS','TARJETA_DE_CREDITO_EMPRESAS','TARJETA_DE_CREDITO_PERSONAS','TESORERIA','UNIDAD_FONDEO']
						if (~df_bl_acc[column].isin(linea).all()):
							f.write("\nHay valores que no corresponden con las lineas de negocio ['EMPRESAS','PERSONAS','TARJETA_DE_CREDITO_EMPRESAS','TARJETA_DE_CREDITO_PERSONAS','TESORERIAS','UNIDAD_FONDEO'")

					i = i + 1

				print("Fuente MIS_PAR_REL_BL_ACC procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_PL_ACC')
				print(e)			

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_PL_ACC sean [cod_entity, cod_currency, cod_product, cod_subproduct, cod_gl_group, cod_act_type, cod_rate_type, cod_blce_prod]')
			print(e)			

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_PL_ACC')
		print(e)


MIS_PAR_REL_CAF_ACC()
MIS_PAR_REL_BP_ACC()
MIS_PAR_REL_PL_ACC()
MIS_PAR_REL_BL_ACC()