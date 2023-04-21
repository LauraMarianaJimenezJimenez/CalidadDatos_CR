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

def Estado_portafolio_87():

	try:
		print('Procesando Estado de Portafolio 87...')

		file = os.path.abspath('../Fuentes_iniciales/Estado_de_Portafolio_87_' + data_date + '.csv')
		# Create path
		df = pd.read_csv(file, header=None, encoding='latin-1')  

		try:

			# Define headers
			df.columns = df.iloc[0]
			df.columns = ['Cod.', 'Instrumento', 'Cod1' ,'Emisor', 'Inversion', 'Inst', 'Vencimiento', 'Compra', 'Emision', 'Facial', 'Costo', 'Per', 'Spread', 'Tasa', 'Fec.Pago', 'Int.Acumul', 'Dia Acumul', 'Int.Dia', 'Des/Pri', 'Acu/Des Pri', 'Saldo Libros', 'Transado Original', 'Saldo Facial', 'Saldo Transado', 'Ope', 'Tipo Fondeo', 'Fec.Prox.Pago', 'Moneda', 'Descripcion', 'Tipo Tasa', 'Tasa Referencia', 'Desc.Tasa', 'Amortiza', 'Fec.Prox.Amortiza', 'Mto.Amortiza', 'Comision a Diferir Original', 'Saldo Comision a Diferir', 'Banco Desembolsa', 'Tasa Bco.Desembolsa', 'Int.Bco Desembolsa', 'Tasa Bco.Emisor', 'Int.Bco Emisor', 'Dias Bco.Emisor', 'Forma pago', 'Metodologia', 'Dias Metodologia', 'Fec.Tasa Aplicada']

			# Take data frame
			df = df[1:]

			# Remove entirely empty row
			df = df.dropna(how='all')
			# Remove duplicate records
			df = df.drop_duplicates()

			# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

			# Create output file
			path = os.path.abspath('../Informes/Informe_Estado_de_Portafolio_87_' + data_date + '.txt')

			f = open(path,"w+")
			f.write(file)

			# Print columns and rows
			f.write("\nCantidad de filas: %d" % len(df))
			f.write("\nCantidad de Columnas: %d" % len(df.columns))

			f.write("\n")

			f.write("\nCantidad de datos vacios por cada columna del archivo")

			# Validate empty cells
			for column in df:
				text = str(column)
				f.write("\n")
				f.write(text)
				f.write(": ")
				text = str(df[column].isnull().sum())
				f.write(text)
				df[column] = df[column].astype(str)
				df[column] = df[column].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

			# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
			# especiales que puedan afectar la converisión a csv

			# Changes for all columns

			#Remove carring return
			df = df.replace({r'\\r': ' '}, regex=True)
			#Remove line breaks
			df = df.replace(r'\s+|\\n', ' ', regex=True)
			#Remove pipelines, single quote, semicolon
			df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

			# Tratamientos especificos para campos puntuales del MIS según reglas de negocio definidas.
			i = 0
			for column in df:
				# Cod1 as cod_product
				if i == 0:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Instrumento as cod_subproduct
				if i == 1:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)

				# Emisor as idf_cli
				if i == 3:		
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Inversion as idf_cto
				# 
				if i == 4:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

				# Vencimiento as exp_date
				if i == 6  or i == 7 or i == 14:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Facial as eopbal_cap Primas por Bonos y Papeles Colones/Dolares
				# Int.Acumul as eopbal_cap Intereses por Bonos y Papeles Colones 
				# Acu/Des Pri as eopbalcap  Primas y Descuentos por Bonos y Papeles Colones

				if i == 9 or i == 10 or i == 15 or i == 19:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

				# Tasa as Rate_int
				if i == 13:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

					if (df[column] > 1).any():
						text = "\nHay tasas con porcentaje mayor que 1"
						f.write(text)

					if (df[column] < 0).any():
						text = "\nHay tasas con porcentaje menores que 0"
						f.write(text)
						

				if i == 26 or i == 33:
					df[column] = df[column].astype(str)
					#df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Moneda as cod_currency
				if i == 27:
					df[column] = df[column].astype(str)
					subproductos = ['1', '2']
					if (~df[column].isin(subproductos).all()):
						f.write("\nHay monedas que no corresponden a los valores 1 y 2")
								
				if i == 44:
					df[column] = df[column].astype(str)
					df[column] = df[column].replace('nan', 'N/A', regex=False)
					df[column] = df[column].replace('', 'N/A', regex=False)

				i = i + 1
								
			f.close()

			df = df.fillna('')
			df = df.replace('nan', '', regex=False)
			df.rename(columns={"Cod1": "Cod."}, inplace=True)


			# Generación del flag de validación, marcación de tiempo unix
			date_time = datetime.datetime.now()      
			unix_time = time.mktime(date_time.timetuple())
			unix_time = str(unix_time)
			unix_time = unix_time.split('.')[0]

			# Se escribe un nuevo archivo con la fuente procesada 

			file = os.path.abspath('../Fuentes_procesadas/Estado_de_Portafolio_87_' + data_date + '_' + unix_time + '.csv')
			df.to_csv(file, index=False)

			print("Fuente procesada con exito")

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique su fuente')
			print(e)

	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo")
		print(e)


def Estado_portafolio_88():

	try:
		print('Procesando Estado de Portafolio 88...') 

		file = os.path.abspath('../Fuentes_iniciales/Estado_de_Portafolio_88_' + data_date + '.csv')
		# Create path
		df = pd.read_csv(file, header=None, encoding='latin-1')  

		try:
			# Define headers
			df.columns = df.iloc[0]
			df.columns = ['Cod.', 'Instrumento', 'Cod1' ,'Emisor', 'Inversion', 'Inst', 'Vencimiento', 'Compra', 'Emision', 'Facial', 'Costo', 'Per', 'Spread', 'Tasa', 'Fec.Pago', 'Int.Acumul', 'Dia Acumul', 'Int.Dia', 'Des/Pri', 'Acu/Des Pri', 'Saldo Libros', 'Transado Original', 'Saldo Facial', 'Saldo Transado', 'Ope', 'Tipo Fondeo', 'Fec.Prox.Pago', 'Moneda', 'Descripcion', 'Tipo Tasa', 'Tasa Referencia', 'Desc.Tasa', 'Amortiza', 'Fec.Prox.Amortiza', 'Mto.Amortiza', 'Comision a Diferir Original', 'Saldo Comision a Diferir', 'Banco Desembolsa', 'Tasa Bco.Desembolsa', 'Int.Bco Desembolsa', 'Tasa Bco.Emisor', 'Int.Bco Emisor', 'Dias Bco.Emisor', 'Forma pago', 'Metodologia', 'Dias Metodologia', 'Fec.Tasa Aplicada']

			# Take data frame
			df = df[1:]

			# Remove entirely empty row
			df = df.dropna(how='all')
			# Remove duplicate records
			df = df.drop_duplicates()

			# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

			# Create output file
			path = os.path.abspath('../Informes/Informe_Estado_de_Portafolio_88_' + data_date + '.txt')

			f = open(path,"w+")
			f.write(file)

			# Print columns and rows
			f.write("\nCantidad de filas: %d" % len(df))
			f.write("\nCantidad de Columnas: %d" % len(df.columns))

			f.write("\n")

			f.write("\nCantidad de datos vacios por cada columna del archivo")

			# Validate empty cells
			for column in df:
				text = str(column)
				f.write("\n")
				f.write(text)
				f.write(": ")
				text = str(df[column].isnull().sum())
				f.write(text)
				df[column] = df[column].astype(str)
				df[column] = df[column].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

			# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
			# especiales que puedan afectar la converisión a csv

			# Changes for all columns

			#Remove carring return
			df = df.replace({r'\\r': ' '}, regex=True)
			#Remove line breaks
			df = df.replace(r'\s+|\\n', ' ', regex=True)
			#Remove pipelines, single quote, semicolon
			df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

			# Tratamientos especificos para campos puntuales del MIS según reglas de negocio definidas.
			i = 0
			for column in df:
				# Cod1 as cod_product
				if i == 0:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Instrumento as cod_subproduct
				if i == 1:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)

				# Emisor as idf_cli
				if i == 3:		
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Inversion as idf_cto
				# 
				if i == 4:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

				# Vencimiento as exp_date
				if i == 6  or i == 7 or i == 14:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Facial as eopbal_cap Primas por Bonos y Papeles Colones/Dolares
				# Int.Acumul as eopbal_cap Intereses por Bonos y Papeles Colones 
				# Acu/Des Pri as eopbalcap  Primas y Descuentos por Bonos y Papeles Colones

				if i == 9 or i == 10 or i == 15 or i == 19:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

				# Tasa as Rate_int
				if i == 13:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

					if (df[column] > 1).any():
						text = "\nHay tasas con porcentaje mayor que 1"
						f.write(text)

					if (df[column] < 0).any():
						text = "\nHay tasas con porcentaje menores que 0"
						f.write(text)
						

				if i == 26 or i == 33:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Moneda as cod_currency
				if i == 27:
					df[column] = df[column].astype(str)
					subproductos = ['1', '2']
					if (~df[column].isin(subproductos).all()):
						f.write("\nHay monedas que no corresponden a los valores 1 y 2")
								
				if i == 44:
					df[column] = df[column].astype(str)
					df[column] = df[column].replace('nan', 'N/A', regex=False)
					df[column] = df[column].replace('', 'N/A', regex=False)

				i = i + 1
								
			f.close()

			df = df.fillna('')
			df = df.replace('nan', '', regex=False)
			df.rename(columns={"Cod1": "Cod."}, inplace=True)


			# Generación del flag de validación, marcación de tiempo unix
			date_time = datetime.datetime.now()      
			unix_time = time.mktime(date_time.timetuple())
			unix_time = str(unix_time)
			unix_time = unix_time.split('.')[0]

			# Se escribe un nuevo archivo con la fuente procesada 

			file = os.path.abspath('../Fuentes_procesadas/Estado_de_Portafolio_88_' + data_date + '_' + unix_time + '.csv')
			df.to_csv(file, index=False)

			print("Fuente procesada con exito")

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique su fuente')
			print(e)

	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo")
		print(e)



def Estado_portafolio_100():
	
	try:
		print('Procesando Estado de Portafolio 100...')

		file = os.path.abspath('../Fuentes_iniciales/Estado_de_Portafolio_100_' + data_date + '.csv')
		# Create path
		df = pd.read_csv(file, header=None, encoding='latin-1')  

		try:

			# Define headers
			df.columns = df.iloc[0]
			df.columns = ['Cod.', 'Instrumento', 'Cod1' ,'Emisor', 'Inversion', 'Inst', 'Vencimiento', 'Compra', 'Emision', 'Facial', 'Costo', 'Per', 'Spread', 'Tasa', 'Fec.Pago', 'Int.Acumul', 'Dia Acumul', 'Int.Dia', 'Des/Pri', 'Acu/Des Pri', 'Saldo Libros', 'Transado Original', 'Saldo Facial', 'Saldo Transado', 'Ope', 'Tipo Fondeo', 'Fec.Prox.Pago', 'Moneda', 'Descripcion', 'Tipo Tasa', 'Tasa Referencia', 'Desc.Tasa', 'Amortiza', 'Fec.Prox.Amortiza', 'Mto.Amortiza', 'Comision a Diferir Original', 'Saldo Comision a Diferir', 'Banco Desembolsa', 'Tasa Bco.Desembolsa', 'Int.Bco Desembolsa', 'Tasa Bco.Emisor', 'Int.Bco Emisor', 'Dias Bco.Emisor', 'Forma pago', 'Metodologia', 'Dias Metodologia', 'Fec.Tasa Aplicada']

			# Take data frame
			df = df[1:]

			# Remove entirely empty row
			df = df.dropna(how='all')
			# Remove duplicate records
			df = df.drop_duplicates()

			# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

			# Create output file
			path = os.path.abspath('../Informes/Informe_Estado_de_Portafolio_100_' + data_date + '.txt')

			f = open(path,"w+")
			f.write(file)

			# Print columns and rows
			f.write("\nCantidad de filas: %d" % len(df))
			f.write("\nCantidad de Columnas: %d" % len(df.columns))

			f.write("\n")

			f.write("\nCantidad de datos vacios por cada columna del archivo")

			# Validate empty cells
			for column in df:
				text = str(column)
				f.write("\n")
				f.write(text)
				f.write(": ")
				text = str(df[column].isnull().sum())
				f.write(text)
				df[column] = df[column].astype(str)
				df[column] = df[column].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

			# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
			# especiales que puedan afectar la converisión a csv

			# Changes for all columns

			#Remove carring return
			df = df.replace({r'\\r': ' '}, regex=True)
			#Remove line breaks
			df = df.replace(r'\s+|\\n', ' ', regex=True)
			#Remove pipelines, single quote, semicolon
			df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

			# Tratamientos especificos para campos puntuales del MIS según reglas de negocio definidas.
			i = 0
			for column in df:
				# Cod1 as cod_product
				if i == 0:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Instrumento as cod_subproduct
				if i == 1:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)

				# Emisor as idf_cli
				if i == 3:		
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Inversion as idf_cto
				# 
				if i == 4:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

				# Vencimiento as exp_date
				if i == 6  or i == 7 or i == 14:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Facial as eopbal_cap Primas por Bonos y Papeles Colones/Dolares
				# Int.Acumul as eopbal_cap Intereses por Bonos y Papeles Colones 
				# Costo as apoyo para calculo eopbal
				# Acu/Des Pri as eopbalcap  Primas y Descuentos por Bonos y Papeles Colones

				if i == 9 or i == 10 or i == 15 or i == 19:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

				# Tasa as Rate_int
				if i == 13:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

					if (df[column] > 1).any():
						text = "\nHay tasas con porcentaje mayor que 1"
						f.write(text)

					if (df[column] < 0).any():
						text = "\nHay tasas con porcentaje menores que 0"
						f.write(text)
						

				if i == 26 or i == 33:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Moneda as cod_currency
				if i == 27:
					df[column] = df[column].astype(str)
					subproductos = ['1', '2']
					if (~df[column].isin(subproductos).all()):
						f.write("\nHay monedas que no corresponden a los valores 1 y 2")
								
				if i == 44:
					df[column] = df[column].astype(str)
					df[column] = df[column].replace('nan', 'N/A', regex=False)
					df[column] = df[column].replace('', 'N/A', regex=False)

				i = i + 1
								
			f.close()

			df = df.fillna('')
			df = df.replace('nan', '', regex=False)
			df.rename(columns={"Cod1": "Cod."}, inplace=True)


			# Generación del flag de validación, marcación de tiempo unix
			date_time = datetime.datetime.now()      
			unix_time = time.mktime(date_time.timetuple())
			unix_time = str(unix_time)
			unix_time = unix_time.split('.')[0]

			# Se escribe un nuevo archivo con la fuente procesada 

			file = os.path.abspath('../Fuentes_procesadas/Estado_de_Portafolio_100_' + data_date + '_' + unix_time + '.csv')
			df.to_csv(file, index=False)

			print("Fuente procesada con exito")

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique su fuente')
			print(e)

	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo")
		print(e)



def Estado_portafolio_118():
	
	try:
		print('Procesando Estado de Portafolio 118...')

		file = os.path.abspath('../Fuentes_iniciales/Estado_de_Portafolio_118_' + data_date + '.csv')
		# Create path
		df = pd.read_csv(file, header=None, encoding='latin-1')  

		try:

			# Se define la fila 1 como el header y el se toma el data frame desde la fila 3

			# Define headers
			df.columns = df.iloc[0]
			df.columns = ['Cod.', 'Instrumento', 'Cod1' ,'Emisor', 'Inversion', 'Inst', 'Vencimiento', 'Compra', 'Emision', 'Facial', 'Costo', 'Per', 'Spread', 'Tasa', 'Fec.Pago', 'Int.Acumul', 'Dia Acumul', 'Int.Dia', 'Des/Pri', 'Acu/Des Pri', 'Saldo Libros', 'Transado Original', 'Saldo Facial', 'Saldo Transado', 'Ope', 'Tipo Fondeo', 'Fec.Prox.Pago', 'Moneda', 'Descripcion', 'Tipo Tasa', 'Tasa Referencia', 'Desc.Tasa', 'Amortiza', 'Fec.Prox.Amortiza', 'Mto.Amortiza', 'Comision a Diferir Original', 'Saldo Comision a Diferir', 'Banco Desembolsa', 'Tasa Bco.Desembolsa', 'Int.Bco Desembolsa', 'Tasa Bco.Emisor', 'Int.Bco Emisor', 'Dias Bco.Emisor', 'Forma pago', 'Metodologia', 'Dias Metodologia', 'Fec.Tasa Aplicada']

			# Take data frame
			df = df[1:]

			# Remove entirely empty row
			df = df.dropna(how='all')
			# Remove duplicate records
			df = df.drop_duplicates()

			# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

			# Create output file
			path = os.path.abspath('../Informes/Informe_Estado_de_Portafolio_118_' + data_date + '.txt')

			f = open(path,"w+")
			f.write(file)

			# Print columns and rows
			f.write("\nCantidad de filas: %d" % len(df))
			f.write("\nCantidad de Columnas: %d" % len(df.columns))

			f.write("\n")

			f.write("\nCantidad de datos vacios por cada columna del archivo")

			# Validate empty cells
			for column in df:
				text = str(column)
				f.write("\n")
				f.write(text)
				f.write(": ")
				text = str(df[column].isnull().sum())
				f.write(text)
				df[column] = df[column].astype(str)
				df[column] = df[column].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

			# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
			# especiales que puedan afectar la converisión a csv

			# Changes for all columns

			#Remove carring return
			df = df.replace({r'\\r': ' '}, regex=True)
			#Remove line breaks
			df = df.replace(r'\s+|\\n', ' ', regex=True)
			#Remove pipelines, single quote, semicolon
			df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

			# Tratamientos especificos para campos puntuales del MIS según reglas de negocio definidas.
			i = 0
			for column in df:
				# Cod1 as cod_product
				if i == 0:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Instrumento as cod_subproduct
				if i == 1:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)

				# Emisor as idf_cli
				if i == 3:		
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Inversion as idf_cto
				# 
				if i == 4:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

				# Vencimiento as exp_date
				if i == 6  or i == 7 or i == 14:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Facial as eopbal_cap Primas por Bonos y Papeles Colones/Dolares
				# Int.Acumul as eopbal_cap Intereses por Bonos y Papeles Colones 
				# Acu/Des Pri as eopbalcap  Primas y Descuentos por Bonos y Papeles Colones

				if i == 9 or i == 10 or i == 15 or i == 19:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

				# Tasa as Rate_int
				if i == 13:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

					if (df[column] > 1).any():
						text = "\nHay tasas con porcentaje mayor que 1"
						f.write(text)

					if (df[column] < 0).any():
						text = "\nHay tasas con porcentaje menores que 0"
						f.write(text)
						

				if i == 26 or i == 33:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Moneda as cod_currency
				if i == 27:
					df[column] = df[column].astype(str)
					subproductos = ['1', '2']
					if (~df[column].isin(subproductos).all()):
						f.write("\nHay monedas que no corresponden a los valores 1 y 2")
								
				if i == 44:
					df[column] = df[column].astype(str)
					df[column] = df[column].replace('nan', 'N/A', regex=False)
					df[column] = df[column].replace('', 'N/A', regex=False)

				i = i + 1
								
			f.close()

			df = df.fillna('')
			df = df.replace('nan', '', regex=False)
			df.rename(columns={"Cod1": "Cod."}, inplace=True)


			# Generación del flag de validación, marcación de tiempo unix
			date_time = datetime.datetime.now()      
			unix_time = time.mktime(date_time.timetuple())
			unix_time = str(unix_time)
			unix_time = unix_time.split('.')[0]

			# Se escribe un nuevo archivo con la fuente procesada 

			file = os.path.abspath('../Fuentes_procesadas/Estado_de_Portafolio_118_' + data_date + '_' + unix_time + '.csv')
			df.to_csv(file, index=False)

			print("Fuente procesada con exito")

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique su fuente')
			print(e)

	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo")



def Estado_portafolio_119():
	
	try:
		print('Procesando Estado de Portafolio 119...')

		file = os.path.abspath('../Fuentes_iniciales/Estado_de_Portafolio_119_' + data_date + '.csv')
		# Create path
		df = pd.read_csv(file, header=None, encoding='latin-1')  

		try:

			# Se define la fila 1 como el header y el se toma el data frame desde la fila 3

			# Define headers
			df.columns = df.iloc[0]
			df.columns = ['Cod.', 'Instrumento', 'Cod1' ,'Emisor', 'Inversion', 'Inst', 'Vencimiento', 'Compra', 'Emision', 'Facial', 'Costo', 'Per', 'Spread', 'Tasa', 'Fec.Pago', 'Int.Acumul', 'Dia Acumul', 'Int.Dia', 'Des/Pri', 'Acu/Des Pri', 'Saldo Libros', 'Transado Original', 'Saldo Facial', 'Saldo Transado', 'Ope', 'Tipo Fondeo', 'Fec.Prox.Pago', 'Moneda', 'Descripcion', 'Tipo Tasa', 'Tasa Referencia', 'Desc.Tasa', 'Amortiza', 'Fec.Prox.Amortiza', 'Mto.Amortiza', 'Comision a Diferir Original', 'Saldo Comision a Diferir', 'Banco Desembolsa', 'Tasa Bco.Desembolsa', 'Int.Bco Desembolsa', 'Tasa Bco.Emisor', 'Int.Bco Emisor', 'Dias Bco.Emisor', 'Forma pago', 'Metodologia', 'Dias Metodologia', 'Fec.Tasa Aplicada']

			# Take data frame
			df = df[1:]

			# Remove entirely empty row
			df = df.dropna(how='all')
			# Remove duplicate records
			df = df.drop_duplicates()

			# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

			# Create output file
			path = os.path.abspath('../Informes/Informe_Estado_de_Portafolio_119_' + data_date + '.txt')

			f = open(path,"w+")
			f.write(file)

			# Print columns and rows
			f.write("\nCantidad de filas: %d" % len(df))
			f.write("\nCantidad de Columnas: %d" % len(df.columns))

			f.write("\n")

			f.write("\nCantidad de datos vacios por cada columna del archivo")

			# Validate empty cells
			for column in df:
				text = str(column)
				f.write("\n")
				f.write(text)
				f.write(": ")
				text = str(df[column].isnull().sum())
				f.write(text)
				df[column] = df[column].astype(str)
				df[column] = df[column].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

			# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
			# especiales que puedan afectar la converisión a csv

			# Changes for all columns

			#Remove carring return
			df = df.replace({r'\\r': ' '}, regex=True)
			#Remove line breaks
			df = df.replace(r'\s+|\\n', ' ', regex=True)
			#Remove pipelines, single quote, semicolon
			df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

			# Tratamientos especificos para campos puntuales del MIS según reglas de negocio definidas.
			i = 0
			for column in df:
				# Cod1 as cod_product
				if i == 0:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Instrumento as cod_subproduct
				if i == 1:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)

				# Emisor as idf_cli
				if i == 3:		
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Inversion as idf_cto
				# 
				if i == 4:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

				# Vencimiento as exp_date
				if i == 6  or i == 7 or i == 14:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Facial as eopbal_cap Primas por Bonos y Papeles Colones/Dolares
				# Int.Acumul as eopbal_cap Intereses por Bonos y Papeles Colones 
				# Acu/Des Pri as eopbalcap  Primas y Descuentos por Bonos y Papeles Colones

				if i == 9 or i == 10 or i == 15 or i == 19:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

				# Tasa as Rate_int
				if i == 13:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

					if (df[column] > 1).any():
						text = "\nHay tasas con porcentaje mayor que 1"
						f.write(text)

					if (df[column] < 0).any():
						text = "\nHay tasas con porcentaje menores que 0"
						f.write(text)
						

				if i == 26 or i == 33:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Moneda as cod_currency
				if i == 27:
					df[column] = df[column].astype(str)
					subproductos = ['1', '2']
					if (~df[column].isin(subproductos).all()):
						f.write("\nHay monedas que no corresponden a los valores 1 y 2")
								
				if i == 44:
					df[column] = df[column].astype(str)
					df[column] = df[column].replace('nan', 'N/A', regex=False)
					df[column] = df[column].replace('', 'N/A', regex=False)

				i = i + 1
								
			f.close()

			df = df.fillna('')
			df = df.replace('nan', '', regex=False)
			df.rename(columns={"Cod1": "Cod."}, inplace=True)


			# Generación del flag de validación, marcación de tiempo unix
			date_time = datetime.datetime.now()      
			unix_time = time.mktime(date_time.timetuple())
			unix_time = str(unix_time)
			unix_time = unix_time.split('.')[0]

			# Se escribe un nuevo archivo con la fuente procesada 

			file = os.path.abspath('../Fuentes_procesadas/Estado_de_Portafolio_119_' + data_date + '_' + unix_time + '.csv')
			df.to_csv(file, index=False)

			print("Fuente procesada con exito")

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique su fuente')
			print(e)

	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo")


def Estado_portafolio_124():
	
	try:
		print('Procesando Estado de Portafolio 124..')

		file = os.path.abspath('../Fuentes_iniciales/Estado_de_Portafolio_124_' + data_date + '.csv')
		# Create path
		df = pd.read_csv(file, header=None, encoding='latin-1')  

		try:

			# Se define la fila 1 como el header y el se toma el data frame desde la fila 3

			# Define headers
			df.columns = df.iloc[0]
			df.columns = ['Cod.', 'Instrumento', 'Cod1' ,'Emisor', 'Inversion', 'Inst', 'Vencimiento', 'Compra', 'Emision', 'Facial', 'Costo', 'Per', 'Spread', 'Tasa', 'Fec.Pago', 'Int.Acumul', 'Dia Acumul', 'Int.Dia', 'Des/Pri', 'Acu/Des Pri', 'Saldo Libros', 'Transado Original', 'Saldo Facial', 'Saldo Transado', 'Ope', 'Tipo Fondeo', 'Fec.Prox.Pago', 'Moneda', 'Descripcion', 'Tipo Tasa', 'Tasa Referencia', 'Desc.Tasa', 'Amortiza', 'Fec.Prox.Amortiza', 'Mto.Amortiza', 'Comision a Diferir Original', 'Saldo Comision a Diferir', 'Banco Desembolsa', 'Tasa Bco.Desembolsa', 'Int.Bco Desembolsa', 'Tasa Bco.Emisor', 'Int.Bco Emisor', 'Dias Bco.Emisor', 'Forma pago', 'Metodologia', 'Dias Metodologia', 'Fec.Tasa Aplicada']

			# Take data frame
			df = df[1:]

			# Remove entirely empty row
			df = df.dropna(how='all')
			# Remove duplicate records
			df = df.drop_duplicates()

			# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

			# Create output file
			path = os.path.abspath('../Informes/Informe_Estado_de_Portafolio_124_' + data_date + '.txt')

			f = open(path,"w+")
			f.write(file)

			# Print columns and rows
			f.write("\nCantidad de filas: %d" % len(df))
			f.write("\nCantidad de Columnas: %d" % len(df.columns))

			f.write("\n")

			f.write("\nCantidad de datos vacios por cada columna del archivo")

			# Validate empty cells
			for column in df:
				text = str(column)
				f.write("\n")
				f.write(text)
				f.write(": ")
				text = str(df[column].isnull().sum())
				f.write(text)
				df[column] = df[column].astype(str)
				df[column] = df[column].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

			# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
			# especiales que puedan afectar la converisión a csv

			# Changes for all columns

			#Remove carring return
			df = df.replace({r'\\r': ' '}, regex=True)
			#Remove line breaks
			df = df.replace(r'\s+|\\n', ' ', regex=True)
			#Remove pipelines, single quote, semicolon
			df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

			# Tratamientos especificos para campos puntuales del MIS según reglas de negocio definidas.
			i = 0
			for column in df:
				# Cod1 as cod_product
				if i == 0:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Instrumento as cod_subproduct
				if i == 1:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)

				# Emisor as idf_cli
				if i == 3:		
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^A-Za-z\\s]+', '', regex=True)
				# Inversion as idf_cto
				# 
				if i == 4:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

				# Vencimiento as exp_date
				if i == 6  or i == 7 or i == 14:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Facial as eopbal_cap Primas por Bonos y Papeles Colones/Dolares
				# Int.Acumul as eopbal_cap Intereses por Bonos y Papeles Colones 
				# Acu/Des Pri as eopbalcap  Primas y Descuentos por Bonos y Papeles Colones

				if i == 9 or i == 10 or i == 15 or i == 19:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

				# Tasa as Rate_int
				if i == 13:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)

					if (df[column] > 1).any():
						text = "\nHay tasas con porcentaje mayor que 1"
						f.write(text)

					if (df[column] < 0).any():
						text = "\nHay tasas con porcentaje menores que 0"
						f.write(text)
						

				if i == 26 or i == 33:
					df[column] = df[column].astype(str)
					# df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].str.replace('[^/0-9\\s]+', '', regex=True)
					df[column] = df[column].astype(str)

				# Moneda as cod_currency
				if i == 27:
					df[column] = df[column].astype(str)
					subproductos = ['1', '2']
					if (~df[column].isin(subproductos).all()):
						f.write("\nHay monedas que no corresponden a los valores 1 y 2")
								
				if i == 44:
					df[column] = df[column].astype(str)
					df[column] = df[column].replace('nan', 'N/A', regex=False)
					df[column] = df[column].replace('', 'N/A', regex=False)

				i = i + 1
								
			f.close()

			df = df.fillna('')
			df = df.replace('nan', '', regex=False)
			df.rename(columns={"Cod1": "Cod."}, inplace=True)


			# Generación del flag de validación, marcación de tiempo unix
			date_time = datetime.datetime.now()      
			unix_time = time.mktime(date_time.timetuple())
			unix_time = str(unix_time)
			unix_time = unix_time.split('.')[0]

			# Se escribe un nuevo archivo con la fuente procesada 

			file = os.path.abspath('../Fuentes_procesadas/Estado_de_Portafolio_124_' + data_date + '_' + unix_time + '.csv')
			df.to_csv(file, index=False)

			print("Fuente procesada con exito")

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique su fuente')
			print(e)

	except:
		print(" Hay un error en la fecha ingresada o en el nombre del archivo")


# Input
print("Inserte la fecha que desea procesar")
#data_date = input()
data_date = '20230117'

print('\n')
Estado_portafolio_87()
print('\n')
Estado_portafolio_88()
print('\n')
Estado_portafolio_100()
print('\n')
Estado_portafolio_118()
print('\n')
Estado_portafolio_119()
print('\n')
Estado_portafolio_124()
print('\n')