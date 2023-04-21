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

def PRESUPUESTO():

	try:

		# Input
		print("Inserte la fecha de la fuente que desea procesar")
		#data_date = input()
		data_date = '202301'

		# Create path
		file = os.path.abspath('../Fuentes_iniciales/PRESUPUESTO_' + data_date + '.xlsx')
		# Raed file
		df = pd.read_excel(file, header=None)

		print('Procesando PRESUPUESTO...')

		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')

		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['data_date','cod_cont','cod_gl_group','cod_business_line','cod_pl_acc','cod_blce_prod','cod_currency','ind_mensualiza','cod_entity','cod_ segment','cod_ acco_cent','cod_expense','exp_fam','exp_typ','pl','eopbal_cap','avgbal_cap','num_ctos','ini_am','escenario']]
			'''
			x = len(df)
			# Remove duplicate records
			df = df.drop_duplicates()
			y = len(df)
			'''

			try:
				path = os.path.abspath('../Informes/Informe_PRESUPUESTO_' + data_date + '.txt')

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

				#f.write("\nSe eliminaron " + str(x - y) + " contratos repetidos")

				#print(df)

				df['eopbal_cap'] = df['eopbal_cap'].astype(str)
				df['eopbal_cap'] = df['eopbal_cap'].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
				df['eopbal_cap'] = df['eopbal_cap'].str.replace(',', '.', regex=False)
				df['eopbal_cap'] = df['eopbal_cap'].fillna('0')
				df['eopbal_cap'] = df['eopbal_cap'].replace('nan', '0', regex=False)
				df['eopbal_cap'] = df['eopbal_cap'].replace('', '0', regex=False)
				df['eopbal_cap'] = df['eopbal_cap'].astype(float)


				df['pl'] = df['pl'].astype(str)
				df['pl'] = df['pl'].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
				df['pl'] = df['pl'].str.replace(',', '.', regex=False)
				df['pl'] = df['pl'].fillna('0')
				df['pl'] = df['pl'].replace('nan', '0', regex=False)
				df['pl'] = df['pl'].replace('', '0', regex=False)
				df['pl'] = df['pl'].astype(float)


				i = 0
				for column in df:
					# no_operacion
					if i == 0:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

						if (df[column].str.slice(6, 8) != '01').any():
							f.write("\nHay fechas que no corresponden con el primer día del mes")

					if i == 1:
						df[column] = 'PPTO'


					if i == 2:
						df[column] = df[column].astype(str)
						f.write("\n Validación cuentas 1")
						for j, v in df[column].items():
							#if(v == '2370320002060000'):
   							#	print('index: ', j, 'value: ', v, 'Importe: ', df.iloc[j-1]['pl'], 'saldo: ', df.iloc[j-1]['eopbal_cap'])

							if(v[0] == '1'):

								if(df.iloc[j - 1]['eopbal_cap'] < 0):
 									#print('index: ', j, 'value: ', v, 'saldo: ', df.iloc[j-1]['eopbal_cap'])
 									text = 'cod_gl_group: ' + v + ' Saldo: ' + str(df.iloc[j-1]['eopbal_cap'])
 									f.write("\n")
 									f.write(text)
 									

								if(df.iloc[j - 1]['pl'] != 0):
   									#print('index: ', j, 'value: ', v, 'Importe: ', df.iloc[j-1]['pl'])
   									text = 'cod_gl_group: ' + v + ' Importe: ' + str(df.iloc[j-1]['pl'])
   									f.write("\n")
   									f.write(text)

						f.write("\n Validación cuentas 2 y 3")
						for j, v in df[column].items():
							if(v[0] == '2' or v[0] == '3'):

   								if(df.iloc[j - 1]['eopbal_cap'] > 0):
   									#print('index: ', j, 'value: ', v, 'saldo: ', df.iloc[j-1]['eopbal_cap'])
   									text = 'cod_gl_group: ' + v + ' Saldo: ' + str(df.iloc[j-1]['eopbal_cap'])
   									f.write("\n")
   									f.write(text)

   								if(df.iloc[j - 1]['pl'] != 0):
   									#print('index: ', j, 'value: ', v, 'Importe: ', df.iloc[j-1]['pl'])
   									text = 'cod_gl_group: ' + v + ' Importe: ' + str(df.iloc[j-1]['pl'])
   									f.write("\n")
   									f.write(text)


						f.write("\n Validación cuentas 4")
						for j, v in df[column].items():
							if(v[0] == '4'):

   								if(df.iloc[j - 1]['eopbal_cap'] != 0):
   									#print('index: ', j, 'value: ', v, 'saldo: ', df.iloc[j-1]['eopbal_cap'])
   									text = 'cod_gl_group: ' + v + ' Saldo: ' + str(df.iloc[j-1]['eopbal_cap'])
   									f.write("\n")
   									f.write(text)

   								if(df.iloc[j - 1]['pl'] < 0):
   									#print('index: ', j, 'value: ', v, 'Importe: ', df.iloc[j-1]['pl'])
   									text = 'cod_gl_group: ' + v + ' Importe: ' + str(df.iloc[j-1]['pl'])
   									f.write("\n")
   									f.write(text)   

						f.write("\n Validación cuentas 5")
						for j, v in df[column].items():
							if(v[0] == '5'):

   								if(df.iloc[j - 1]['eopbal_cap'] != 0):
   									#print('index: ', j, 'value: ', v, 'saldo: ', df.iloc[j-1]['eopbal_cap'])
   									text = 'cod_gl_group: ' + v + ' Saldo: ' + str(df.iloc[j-1]['eopbal_cap'])
   									f.write("\n")
   									f.write(text)

   								if(df.iloc[j - 1]['pl'] > 0):
   									#print('index: ', j, 'value: ', v, 'Importe: ', df.iloc[j-1]['pl'])
   									text = 'cod_gl_group: ' + v + ' Importe: ' + str(df.iloc[j-1]['pl'])
   									f.write("\n")
   									f.write(text) 									
					if i == 3:
						df[column] = df[column].astype(str)
						linea = ['EMPRESAS','PERSONAS','TARJETA_DE_CREDITO_EMPRESAS','TARJETA_DE_CREDITO_PERSONAS','TESORERIA','UNIDAD_FONDEO','OTROS']
						if (~df[column].isin(linea).all()):
							f.write("\nHay valores que no corresponden con las lineas de negocio ['EMPRESAS','PERSONAS','TARJETA_DE_CREDITO_EMPRESAS','TARJETA_DE_CREDITO_PERSONAS','TESORERIAS','UNIDAD_FONDEO, 'OTROS'")


					if i == 6:
						df[column] = df[column].astype(str)
						monedas = ['CRC']
						if (~df[column].isin(monedas).all()):
								f.write("\nHay monedas que no corresponden a CRC ")

					if i == 7:
						df[column] = 'Y'

					if i == 8:
						df[column] = df[column].astype(str)
						entidad = ['4']
						if (~df[column].isin(entidad).all()):
								f.write("\nHay entidades que no corresponden a banco:4")

					i = i + 1 

				

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
				file = os.path.abspath('../Fuentes_procesadas/PRESUPUESTO_' + data_date + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()
				

				print("PRESUPUESTO procesado con exito")

			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su parametria')
				print(e)			


		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique que los titulos de la parametria MIS_PAR_ENTITY sean [ord_entity, cod_entity, des_entity]')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su parametria')
		print(e)


PRESUPUESTO()