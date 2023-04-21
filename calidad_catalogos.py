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
dic_fuentes = { "MIS_PAR_CAT_ACCO_CENT":'COD_ACCO_CENT',"MIS_PAR_CAT_BL":'COD_LIN_NEG',"MIS_PAR_CAT_BP":'PRODUCTO BALANCE',
				"MIS_PAR_CAT_CAF":'COD_GL_GROUP',"MIS_PAR_CAT_CONV":'COD_CONV',"MIS_PAR_CAT_ECO_GRO":'ID_ECO_GRO',
				"MIS_PAR_CAT_ECO_GRO_REG":'ID_ECO_GRO_REG',"MIS_PAR_CAT_EXPENSE":'COD_EXPENSE',"MIS_PAR_CAT_INST_CLI":'IDF_CLI',
				"MIS_PAR_CAT_MANAGER":'COD_MANAGER',"MIS_PAR_CAT_MUL_LAT":'CODIGO MULTILATINO',"MIS_PAR_CAT_OFFI":'COD_OFFI',
				"MIS_PAR_CAT_PL":'CUENTA P&G',"MIS_PAR_CAT_PRODUCT":'COD_PRODUCT',"MIS_PAR_CAT_SECTOR_ECO":'IDF_CLI',
				"MIS_PAR_CAT_SUBPRODUCT":'COD_SUBPRODUCT'}

for key in dic_fuentes:

	try:

		file = os.path.abspath("../Fuentes_iniciales/" + key + ".xlsx")

		print('Fuente: ' + key) # fuente
		print('Llave: ' + dic_fuentes[key]) # llave

		# Raed file
		df = pd.read_excel(file, header=None)
		df.columns = df.iloc[0]
		df = df[1:]
		try:
			x = len(df)
			df = df.drop_duplicates(subset=[dic_fuentes[key]], keep='last')
			y = len(df)

			try:
				print('Para el catalogo ' + key + ' se eliminaron ' + str(x - y) + ' registros por llaves duplicados')

				# Generación del flag de validación, marcación de tiempo unix
				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/' + key + '_' + unix_time + '.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja 1', index=False)
				writer.save()

				print('Se creo el archivo ' + file)
				print(' ')

			except Exception as e:
				print(' Ha ocurrido un error')
				print(e)
				break

		except Exception as e:
			print(' Ha ocurrido un error con la llave de la fuente ' + key +'. Por favor verifique que la columna se llame ' + dic_fuentes[key])
			print(e)
			break

	except Exception as e:
		print(' Ha ocurrido un error intentando procesar el catalogo '+ key +'. Por favor verifique que exista y que el nombre sea correcto')
		print(e)
		break

