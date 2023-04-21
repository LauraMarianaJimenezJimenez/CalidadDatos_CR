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


# Se solicita la fecha de ejecución y con la ayuda de un diccionario se crea el path que leera el archivo

# Input data 
months={'01':'Enero', '02':'Febrero', '03':'Marzo', '04':'Abril', '05':'Mayo', '06':'Junio', '07':'Julio', '08':'Agosto', '09':'Septiembre',
        '10':'Octubre', '11':'Nomviembre', '12':'Diciembre'}
try:        
	# Input
	print("Inserte la fecha de la fuente Base de Gastos que desea procesar (yyyymmdd)")
	data_date = input()
	#data_date ='20230131'
	month_date = months

	print('Procesando Base de Gastos...')

	# Create path
	file = os.path.abspath('../Fuentes_iniciales/Base_Gastos_' + data_date + '.xlsx')
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
		df = df[['Fuente','Fecha de Pago ADC','Pago ADC','Cia','Llave Conciliacion','CC','Cod','Descripcion CC','Direccion','Direccion 2','Tipo','Agrup EF','Detalle EF','PUC','Clave CS','Gtos Administrados','Conca','Cuenta','Nombre Cuenta','Detalle','Neto','Proveedor','Monto','TC','Observaciones']]
		# Remove duplicate records
		#df = df.drop_duplicates()

		try:

			# Se realiza un informe incial de calidad que indica la cantidad de filas, la cantidad de columnas y la cantidad de datos vacios por cada una de las columnas

			path = os.path.abspath('../Informes/Informe_Base_Gastos_' + data_date + '.txt')

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

			f.close()

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

				if i == 1:
					df[column] = df[column].astype(str)
					df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
					df[column] = df[column].astype(str)

				# Cia cod_entity
				if i == 3:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace(' ', '')
					df[column] = df[column].str.replace('[^A-Za-z0-9\\s]+', '', regex=True)
					#df[column] = df[column].str.replace('[^0-9\\s]+', '')

				#CC cod_acco_cent
				if i == 5:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace(' ', '')
					df[column] = df[column].str.replace('[^A-Za-z0-9\\s]+', '', regex=True)
					#df[column] = df[column].str.replace('[^0-9\\s]+', '')

				#Cod cod_expense cod_nar
				if i == 6:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace(' ', '')
					df[column] = df[column].str.replace('[^A-Za-z0-9\\s]+', '', regex=True)
					#df[column] = df[column].str.replace('[^0-9\\s]+', '')

				#Cuenta cod_gl
				if i==17:		
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace(' ', '')
					df[column] = df[column].str.replace('[^A-Za-z0-9\\s]+', '', regex=True)
					#df[column] = df[column].str.replace('[^0-9\\s]+', '')

				#Nombre cuenta des_gl
				if i==18:
					df[column] = df[column].astype(str)

				#Monto pl	
				if i == 20 or i == 22:
					df[column] = df[column].astype(str)
					df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
					df[column] = df[column].str.replace(',', '.', regex=False)
					df[column] = df[column].fillna('0')
					df[column] = df[column].replace('nan', '0', regex=False)
					df[column] = df[column].replace('', '0', regex=False)
					df[column] = df[column].astype(float)
				
				i=i+1

			# Generación del flag de validación, marcación de tiempo unix
			date_time = datetime.datetime.now()      
			unix_time = time.mktime(date_time.timetuple())
			unix_time = str(unix_time)
			unix_time = unix_time.split('.')[0]


			# Se escribe un nuevo archivo con la fuente procesada 

			file = os.path.abspath('../Fuentes_procesadas/Base_Gastos_' + data_date + '_' + unix_time + '.xlsx')
			writer = ExcelWriter(file)
			df.to_excel(writer, 'Hoja1', index=False)
			writer.save()

			print("Fuente procesada con exito")

		except Exception as e:
			print(' Ha ocurrido un error, por favor verifique su parametria')
			print(e)			


	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique que los titulos de la fuente Base Gastos sean [Fuente, Fecha de Pago ADC, Pago ADC, Cia, Llave Conciliacion, CC, Cod, Descripcion CC, Dirección, Direccion 2, Tipo, Agrup EF, Detalle EF, PUC, Clave CS, Gtos Administrados, Conca, Cuenta, Nombre Cuenta, Detalle, Neto, Proveedor, Monto, TC, Observaciones]')
		print(e)			


except Exception as e:
	print(' Ha ocurrido un error, por favor verifique su parametria')
	print(e)