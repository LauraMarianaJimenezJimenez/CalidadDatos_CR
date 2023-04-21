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

def MIS_PAR_REL_CAF_OPER():

	try:
		# Create path
		file_oper = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_CAF_OPER.xlsx')
		# Raed file
		df_oper = pd.read_excel(file_oper, header=None)

		# Take dataframe
		df_oper.columns = df_oper.iloc[0]
		df_oper = df_oper[1:]

		# Remove entirely empty row
		df_oper = df_oper.dropna(how='all')

		try:
			# Delete withespace in headers
			df_oper = df_oper.rename(columns=lambda x: x.strip())
			# Take specific columns
			df_oper = df_oper[['cod_entity', 'cod_product', 'cod_subproduct', 'cod_currency', 'cod_blce_prod', 'cod_value', 'cod_gl_group']]
			# Remove duplicate records
			df_oper = df_oper.drop_duplicates()


			try:
				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_CAF_OPER.txt')

				f = open(path,"w+")
				f.write(file_oper)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df_oper))
				f.write("\nCantidad de Columnas: %d" % len(df_oper.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				for column in df_oper:
					text = str(column)
					f.write("\n")
					f.write(text)
					f.write(": ")
					text = str(df_oper[column].isnull().sum())
					f.write(text)

				# Create path
				file_acc = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_CAF_ACC.xlsx')
				# Raed file
				df_acc = pd.read_excel(file_acc, header=None)

				# Take specific columns
				df_acc.columns = df_acc.iloc[0]
				df_acc.rename(columns = {'AGRUPADOR CONT':'cod_gl_group'}, inplace = True)
				df_acc = df_acc[1:]


				left_join = pd.merge(df_oper, df_acc, on ='cod_gl_group', how ='left')

				results = left_join[left_join['CUENTA'].isna()]

				f.write("\n")
				results['cod_gl_group']= results['cod_gl_group'].astype(str)
				f.write("\nCuentas contables que estan en la parametria operacional MIS_PAR_REL_CAF_OPER pero no estan en la parametria contable MIS_PAR_REL_CAF_ACC")
				f.write("\n")
				f.write(" ")
				f.write(results['cod_gl_group'].str.cat(sep='\n'))

				duplicated = df_oper[df_oper[['cod_entity', 'cod_product', 'cod_subproduct', 'cod_currency', 'cod_blce_prod', 'cod_value']].duplicated(keep = False) == True]

				for column in duplicated:
					duplicated[column] = duplicated[column].astype(str)

				if (len(duplicated) > 0):
					f.write("\n")
					f.write("\nAgrupadores contables diferentes con combinaciones iguales")
					f.write("\n")
					dfAsString = duplicated.to_string(header=False, index=False)
					f.write(dfAsString)
									
				else:
					f.write("\n")
					f.write("\nNo hay combinaciones iguales con agrupadores contables diferentes")

				for column in df_oper:
					df_oper[column] = df_oper[column].astype(str)

				f.write("\n")
				f.write("\nAgrupadores contables con inconsistencias en su sexto caracter o en sus dos caracteres finales")
				f.write("\n")

				for index, row in df_oper.iterrows():
					x = row["cod_gl_group"]

					if row ["cod_currency"] == 'USD':
						if (x[5] != '2'):
							if(x[-2:] != 'ME'):
								f.write("\n" + str(row ["cod_currency"]) + ' ' + x)

					if row ["cod_currency"] == 'CRC':
						if (x[5] != '1'):
							if(x[-2:] != 'ML'):
								f.write("\n" + str(row ["cod_currency"]) + ' ' + x)
						
				f.write("\n")

				f.close()

				# Replace nan to empty
				df_oper = df_oper.fillna('')
				df_oper = df_oper.replace('nan', '', regex=False)

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_REL_CAF_OPER_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df_oper.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Fuente MIS_PAR_REL_CAF_OPER procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_CAF_OPER sean [cod_entity, cod_product, cod_subproduct, cod_currency, cod_blce_prod, cod_value, cod_gl_group]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)

def MIS_PAR_REL_BP_OPER():

	try:
		# Create path
		file_bp_oper = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_BP_OPER.xlsx')
		# Raed file
		df_bp_oper = pd.read_excel(file_bp_oper, header=None)

		# Take dataframe
		df_bp_oper.columns = df_bp_oper.iloc[0]
		df_bp_oper = df_bp_oper[1:]

		# Remove entirely empty row
		df_bp_oper = df_bp_oper.dropna(how='all')

		try:
			# Delete withespace in headers
			df_bp_oper = df_bp_oper.rename(columns=lambda x: x.strip())
			# Take specific columns
			df_bp_oper = df_bp_oper[['cod_entity', 'cod_currency', 'cod_product', 'cod_subproduct', 'cod_gl_group', 'cod_act_type', 'cod_rate_type', 'cod_blce_prod']]
			# Remove duplicate records
			df_bp_oper = df_bp_oper.drop_duplicates()

			try:	

				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_BP_OPER.txt')

				f = open(path,"w+")
				f.write(file_bp_oper)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df_bp_oper))
				f.write("\nCantidad de Columnas: %d" % len(df_bp_oper.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				for column in df_bp_oper:
					text = str(column)
					f.write("\n")
					f.write(text)
					f.write(": ")
					text = str(df_bp_oper[column].isnull().sum())
					f.write(text)

				# Create path
				file_oper = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_CAF_OPER.xlsx')
				# Raed file
				df_oper = pd.read_excel(file_oper, header=None)

				# Take specific columns
				df_oper.columns = df_oper.iloc[0]
				df_oper = df_oper[1:]

				left_join = pd.merge(df_bp_oper, df_oper, on ='cod_gl_group', how ='left')

				results = left_join[left_join['cod_blce_prod_y'].isna()]

				f.write("\n")
				results['cod_gl_group']= results['cod_gl_group'].astype(str)
				f.write("\nCuentas contables que estan en la parametria MIS_PAR_REL_BP_OPER pero no estan en la parametria MIS_PAR_REL_CAF_OPER")
				f.write("\n")
				f.write(" ")
				f.write(results['cod_gl_group'].str.cat(sep='\n'))
				
				duplicated = df_bp_oper[df_bp_oper[['cod_entity', 'cod_currency', 'cod_product', 'cod_subproduct', 'cod_gl_group', 'cod_act_type', 'cod_rate_type']].duplicated(keep = False) == True]
				
				for column in duplicated:
					duplicated[column] = duplicated[column].astype(str)

				if (len(duplicated) > 0):
					f.write("\n")
					f.write("\nAgrupadores contables diferentes con combinaciones iguales")
					f.write("\n")
					dfAsString = duplicated.to_string(header=False, index=False)
					f.write(dfAsString)
										
				else:
					f.write("\n")
					f.write("\nNo hay combinaciones iguales con agrupadores contables diferentes")

				# Replace nan to empty
				df_bp_oper = df_bp_oper.fillna('')
				df_bp_oper = df_bp_oper.replace('nan', '', regex=False)

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_REL_BP_OPER_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df_bp_oper.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print("Fuente MIS_PAR_REL_BP_OPER procesada con exito")
		
			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_BP_OPER')
				print(e)			

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_BP_OPER sean [cod_entity, cod_currency, cod_product, cod_subproduct, cod_gl_group, cod_act_type, cod_rate_type, cod_blce_prod]')
			print(e)			

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_BP_OPER')
		print(e)

def MIS_PAR_REL_BL_OPER():

	try:
		# Create path
		file_bl_oper = os.path.abspath('../Fuentes_iniciales/MIS_PAR_REL_BL_OPER.xlsx')
		# Raed file
		df_bl_oper = pd.read_excel(file_bl_oper, header=None)

		# Take dataframe
		df_bl_oper.columns = df_bl_oper.iloc[0]
		df_bl_oper = df_bl_oper[1:]

		# Remove entirely empty row
		df_bl_oper = df_bl_oper.dropna(how='all')

		try:
			# Delete withespace in headers
			df_bl_oper = df_bl_oper.rename(columns=lambda x: x.strip())
			# Take specific columns
			df_bl_oper = df_bl_oper[['cod_blce_prod','cod_typ_clnt','cod_business_line']]
			# Remove duplicate records
			df_bl_oper = df_bl_oper.drop_duplicates()

			try:	

				path = os.path.abspath('../Informes/Informe_MIS_PAR_REL_BL_OPER.txt')

				f = open(path,"w+")
				f.write(file_bl_oper)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df_bl_oper))
				f.write("\nCantidad de Columnas: %d" % len(df_bl_oper.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				for column in df_bl_oper:
					text = str(column)
					f.write("\n")
					f.write(text)
					f.write(": ")
					text = str(df_bl_oper[column].isnull().sum())
					f.write(text)

				duplicated = df_bl_oper[df_bl_oper[['cod_blce_prod','cod_typ_clnt']].duplicated(keep = False) == True]

				for column in duplicated:
					duplicated[column] = duplicated[column].astype(str)

				if (len(duplicated) > 0):
					f.write("\n")
					f.write("\nAgrupadores contables diferentes con combinaciones iguales")
					f.write("\n")
					dfAsString = duplicated.to_string(header=False, index=False)
					f.write(dfAsString)

				else:
					f.write("\n")
					f.write("\nNo hay combinaciones iguales con lineas de negocio diferentes")


				i = 0
				for column in df_bl_oper:
					if i == 1:
						df_bl_oper[column] = df_bl_oper[column].astype(str)
						cliente = ['FISICO','JURIDICO']
						if (~df_bl_oper[column].isin(cliente).all()):
							f.write("\nHay valores que no corresponden con tipo de cliente 'FISICO' o 'JURIDICO'")

					if i == 2:
						df_bl_oper[column] = df_bl_oper[column].astype(str)
						linea = ['EMPRESAS','PERSONAS','TARJETA_DE_CREDITO_EMPRESAS','TARJETA_DE_CREDITO_PERSONAS','TESORERIA','UNIDAD_FONDEO']
						if (~df_bl_oper[column].isin(linea).all()):
							f.write("\nHay valores que no corresponden con las lineas de negocio ['EMPRESAS','PERSONAS','TARJETA_DE_CREDITO_EMPRESAS','TARJETA_DE_CREDITO_PERSONAS','TESORERIAS','UNIDAD_FONDEO'")


				# Replace nan to empty
				df_bl_oper = df_bl_oper.fillna('')
				df_bl_oper = df_bl_oper.replace('nan', '', regex=False)

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/MIS_PAR_REL_BL_OPER_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df_bl_oper.to_excel(writer, 'Hoja 1', index=False)
				writer.save()


				print("Fuente MIS_PAR_REL_BL_OPER procesada con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_BL_OPER')
				print(e)			

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_REL_BL_OPER sean cod_blce_prod,cod_typ_clnt,cod_business_line]')
			print(e)			

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria MIS_PAR_REL_BL_OPER')
		print(e)



MIS_PAR_REL_CAF_OPER()
MIS_PAR_REL_BP_OPER()
MIS_PAR_REL_BL_OPER()