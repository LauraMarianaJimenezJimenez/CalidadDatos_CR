# Importe de librerias a utilizar
import pandas as pd
import numpy as np
from pandas import ExcelWriter
import datetime
import time
from pathlib import Path
import warnings
import os

warnings.filterwarnings("ignore")

# Se solicita la fecha del archivo para la creación del path que leera el archivo
try:
	# Input
	#print("Inserte la fecha de la fuente que desea procesar")
	#data_date = input()

	print('Procesando...')

	file = os.path.abspath("../Fuentes_iniciales/Operacion.txt")

	# Raed file
	df = pd.read_csv(file, header=None, sep='|')

	try:
		# Take dataframe
		df.columns = df.iloc[0]
		df = df[1:]

		# Remove entirely empty row
		df = df.dropna(how='all')
		# Remove entirely empty column
		# df = df.dropna(how='all', axis=1)
		try:
			# Delete withespace in headers
			df = df.rename(columns=lambda x: x.strip())
			# Take specific columns
			df = df[['op_carga_clase_dato','op_archivo','op_entidad','op_registro','op_cliente','op_tpersona_deudor','op_id_deudor','op_id_operacion','op_id_linea','op_toperacion','op_tip_cat_sugef','op_pais_destino','op_prov_destino','op_cant_destino','op_prov_depen','op_cant_depen','op_tcartera','op_estado_op','op_tmoneda','op_monto','op_cta_ctble_cap','op_saldo_cap','op_cta_cble_cap_dp','op_saldo_cap_dp','op_cta_cble_int','op_saldo_int','op_monto_estimado','op_cta_desem_c','op_saldo_desem_c','op_cta_comision','op_saldo_comision','op_cta_cble_saldo_sc','op_saldo_p_sc','op_monto_desem','op_fecha_formaliza','op_fecha_vence','op_frec_pgo_cap','op_frec_pgo_int','op_f_ven_gra_cap','op_tasa_int_vig','op_tipo_tasa','op_factor_tiempo','op_forma_pgo_cap','op_forma_pgo_int','op_f_corte_op','op_f_prox_pag_cap','op_f_prox_pag_int','op_f_amor_hasta','op_f_int_hasta','op_f_pgo_pac_cap','op_f_pgo_pac_int','op_plazo_dias','op_tcuota_cap','op_cuota_cap_act','op_cuota_int_act','op_nueva','op_recupera_cap','op_otros_aum_cap','op_otras_dis_cap','op_back_x_back','op_sindicado','op_especial','op_porc_resp_linea','op_linea_cr_ca','op_producto','op_seguro','op_saldototal_op','op_fecha3_pago_cap','op_cuota3_pago_cap','op_prg_crediticio','op_mto_original_lc','op_clausula_limite','op_monto_mitigador','op_fcamb_tasa_v','op_plazo_rev_v','op_tipo_tasa_v','op_tasa_base_v','op_spread_v','op_tasa_minima_v','op_tasa_maxima_v','op_tipo_op_SFN','op_distrito_destino','op_monto_form_op','op_tipo_prog_aut_SBD','op_cred_grup_soli_SBD','op_t_sec_prio_deu_SBD','op_cedida_en_garantia','op_ponderador_SPD','op_ponderador_SPC','op_indicador_LTV','op_tper_deudor_estima','op_id_deudor_estima','op_cambio_climat','op_tasa_ley_7472']]
			# Remove duplicate records
			df = df.drop_duplicates()

			try:
				# Create output file
				path = os.path.abspath('../Informes/Informe_Operaciones.txt')

				f = open(path,"w+")
				f.write(file)

				# Print columns and rows
				f.write("\nCantidad de filas: %d" % len(df))
				f.write("\nCantidad de Columnas: %d" % len(df.columns))

				f.write("\nCantidad de datos vacios por cada columna del archivo")

				# Validate empty cells
				i = 1
				for column in df:
					#print(column)
					#print(df[column])
					text = str(column)
					f.write("\n")
					f.write(str(i) + ". ")
					f.write(text)
					f.write(": ")
					text = str(df[column].isnull().sum())
					f.write(text)
					i = i + 1

				# Se realizan las reglas de calidad generales en la estructura del archivo, esto incluye eliminar filas vacias, saltos de linea, carring return y caracteres 
				# especiales que puedan afectar la converisión a csv

				# Changes for all columns

				#Remove carring return
				df = df.replace({r'\\r': ' '}, regex=True)
				#Remove line breaks
				df = df.replace(r'\s+|\\n', ' ', regex=True)
				#Remove pipelines, single quote, semicolon
				df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

				#print(df)
				
				i = 0
				for column in df:
					# print(str(i) + ' entro')
					# op_cliente as idf_cli
					# 0-9a-zA-Z
					if i == 4:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True) 

					# OP_ID_OPERACION as idf_cto 
					# 0-9a-zA-Z (E y N ??)
					if i == 7:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)

					# OP_ID_LINEA
					# 0-9a-zA-Z (E y N ??)
					if i == 8:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)

					# op_estado_op
					# 0 - 9 ( 1, 2, 3, 4 y 5? )
					if i == 17: 
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# op_tmoneda as cod_currency
					# 0 - 9 
					if i == 18:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

						moneda = ['1', '2', '3']
						if (~df[column].isin(moneda).all()):
							f.write("\nHay monedas que no corresponden a 1, 2 o 3")

					#OP_CTA_CTBLE_CAP as cod_subproduct
					# 0-9a-zA-Z 
					if i == 20:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)


					# OP_SALDO_CAP as eopbal_cap
					# 0-9Ee
					if i == 21:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False) 
						df[column] = df[column].astype(float)

					# op_cta_desem_c as cod_subproduct
					# 0-9a-zA-Z
					if i == 27:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)

					# OP_SALDO_DESEM_C as eopbal_cap
					# 0 - 9 Ee
					if i == 28:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)


					# OP_CTA_CBLE_SALDO_SC as cod_subproduct
					# 0-9a-zA-Z
					if i == 31:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

					# OP_SALDO_P_SC as eopbal_cap					
					# 0 - 9 Ee
					if i == 32:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

					#OP_FECHA_FORMALIZA as tate_origin 
					#OP_FECHA_VENCE as exp_date
					# OP_FCAMB_TASA_V as DATE_PRX_REV
					fechas =[34, 35, 44, 45, 46, 47, 48, 49, 50, 73]
					if i in fechas:
						df[column] = df[column].astype(str)
						df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
						df[column] = df[column].astype(str)
					
					#OP_FREC_PGO_INT as	FREQ_INT_PAY
					# 0 - 9 
					if i == 37:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)

					# OP_TASA_INT_VIG as Rate_int
					#  0 - 9 
					if i == 39:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
						df[column] = df[column].str.replace(',', '.', regex=False)
						df[column] = df[column].fillna('0')
						df[column] = df[column].replace('nan', '0', regex=False)
						df[column] = df[column].replace('', '0', regex=False)
						df[column] = df[column].astype(float)

						if (df[column] < 0).any():
							f.write("\nHay tasas negativos en la columna OP_TASA_INT_VIG")

						if (df[column] > 100).any():
							f.write("\nHay tasas mayores que 100 en la columna OP_TASA_INT_VIG")


					# OP_TIPO_TASA as COD_RATE_TYPE
					# 0 - 9 a-z A-Z  (f, fv, y v??)

					if i == 40:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)

					# OP_PRODUCTO as COD_PRODUCT
					# 0 - 9 a-z A-Z  (Opciones??)

					if i == 64:
						df[column] = df[column].astype(str)
						df[column] = df[column].str.replace('[^0-9a-zA-Z\\s]+', '', regex=True)


					i = i + 1 


				df = df.fillna('')
				df = df.replace('nan', '', regex=False)

				date_time = datetime.datetime.now()      
				unix_time = time.mktime(date_time.timetuple())
				unix_time = str(unix_time)
				unix_time = unix_time.split('.')[0]

				
				# Se escribe un nuevo archivo con la fuente procesada 
				file = os.path.abspath('../Fuentes_procesadas/Operacion_' + unix_time + '.txt')
				df.to_csv(file, index=None, sep='|', mode='a')				
				'''

				file = os.path.abspath('../Fuentes_procesadas/Operacion_' + unix_time +'.xlsx')
				writer = ExcelWriter(file)
				df.to_excel(writer, 'Hoja de datos', index=False)
				writer.save()
				'''

				print("Fuentes procesada con exito")


			except Exception as e:
				print(' Ha ocurrido un error, por favor verifique su fuente')
				print(e)

		except Exception as e:
			print(' Hay un error en los nombres de las columnas, valide que sean [Trade Date, Time, CCY1, Notional1, Client Price, Close Price, Client Type, Reference Price, PL CM, PL GBM, Total PL, PL CM2, PL GBM2, Total PL2, PL COL CB, Blank, Client Type 2, Criterio, Subsegmento, Ente, CCY, Type], teniendo en cuenta el orden, las mayusculas y minusculas')
			print(e)

	except Exception as e:
		print(' Ha ocurrido un error, por favor verifique su fuente')
		print(e)

except Exception as e:
	print(" Hay un error en la fecha ingresada o en el nombre del archivo")
	print(e)
