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

# Se solicita la fecha del archivo para la creación del path que leera el archivo

# Input
print("Inserte la fecha de la fuente que desea procesar (yyyymmdd)")
data_date = input()
#data_date = '20230306'

print('Procesando...')

file = os.path.abspath("../Fuentes_iniciales/Auxiliar_Inversiones_" + data_date + ".xlsx")

# Raed file
df = pd.read_excel(file, 'bdINV', header=None)

# Rename column nan to N/A
df.iloc[0] = df.iloc[0].astype(str)
df.iloc[0] = df.iloc[0].str.replace('nan', 'N/A', regex=False)

# Take dataframe
df.columns = df.iloc[0]
df = df[1:]

# Remove entirely empty row
df = df.dropna(how='all')
# Remove entirely empty column
df = df.dropna(how='all', axis=1)
# Delete withespace in headers
df = df.rename(columns=lambda x: x.strip())
# Take specific columns
df = df[['Tipo', 'Operacion', 'Emisor', 'Instru.', 'Serie', 'Fecha Vencimiento', 'Días al Venc.', 'Plus', 'Cód. ISIN', 'Facial', 'T. Neta', 'T. Facial', 'Valor Mercado', 'Intereses', 'Primas/ Desc. Diario', 'Primas/ Acumuladas', 'Costo', 'Valor  Libros Efectivo', 'Fecha Ult.', 'Fecha Compra', 'Perio.', 'Dias acumulados', 'Precio Libros', 'Interes diario', 'Precio  mercado', 'Diferencia', 'AjusteTasa', 'CIA', 'MONEDA', 'ValorLibros$', 'GanPerdNoRealizDol', 'GanPerdNoRealizCol', 'GaN/Acia', 'Perdida', 'cuentaContablePrinc', 'CuentaContIntAc', 'VMCOL', 'VMDOL', 'N/A', 'IntCol', 'Cta 12', 'Garantía', 'detGarantia', 'montoGarantia', 'montoGarantiaRealDol.', 'MontoGarantiaDol', 'Grado de liquidez', 'MontoLibre', 'DATO CABEX', 'RESERVAS', 'ClasificaGlobalMarket', 'PASIVOS', 'Homologación', 'Tas.Impuesto', 'Interes Neto', 'Int.Neto Diario', 'Primas/Desc Diaria Neta', 'Primas/Acumuladas Netas', 'Garantía Total', 'Precio Ajustado', 'Clasificación_Contable']]

df = df.drop_duplicates()

path = os.path.abspath('../Informes/informe_Auxiliar_Inversiones_' + data_date + '.txt')

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
	df[column] = df[column].astype(str)

#Remove carring return
df = df.replace({r'\\r': ' '}, regex=True)
#Remove line breaks
df = df.replace(r'\s+|\\n', ' ', regex=True)
#Remove pipelines, single quote, semicolon
df = df.replace(r'\| +|\' +|; +|´ +|\|', '', regex=True)

i = 0
for column in df:

	#Operación
	if i == 1:
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace('[^0-9\\s]+', '', regex=True)


	# Fecha Vencimiento , Fecha Ult , fecha compra
	if i == 5 or i == 18 or i == 19 :
		df[column] = np.where(df[column].str.contains('/'), pd.to_datetime(df[column], errors='coerce').dt.strftime('%d/%m/%Y'), pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y'))
		df[column] = df[column].astype(str)
		if (df[column].str.slice(3, 5) != data_date[4:6]).any():
			f.write("\nHay fechas que no corresponden para el mes de ejecución")

	if i == 11:
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
		df[column] = df[column].str.replace(',', '.', regex=False)
		df[column] = df[column].fillna('0')
		df[column] = df[column].replace('nan', '0', regex=False)
		df[column] = df[column].replace('', '0', regex=False)
		df[column] = df[column].astype(float)

		if (df[column] >= 100).any():
			f.write("\nHay tasas de interes con valor mayor a 100%")

		if (df[column] <0).any():
			f.write("\nHay tasas de interes con valor menor a 0%")

	# Garantia , homologación , tipo , emisor, cia
	if i == 41 or i == 52 or i == 0 or i == 2 or i == 27:
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace('[^a-zA-Z-\\s]+', '', regex=True)

	#detgarantia ,  costo , valormercado , intereses , Primas / Acumuladas , Facial
	if i == 42 or i == 16 or i == 12 or i == 13 or i == 15 or i == 9:
		df[column] = df[column].astype(str)
		df[column] = df[column].str.replace('[^Ee0-9-,.\\s]+', '', regex=True)
		df[column] = df[column].str.replace(',', '.', regex=False)
		df[column] = df[column].fillna('0')
		df[column] = df[column].replace('nan', '0', regex=False)
		df[column] = df[column].replace('', '0', regex=False)
		df[column] = df[column].astype(float)


		

	i = i + 1

f.close()

df = df.fillna('')
df = df.replace('nan', '', regex=False)


# Generación del flag de validación, marcación de tiempo unix
date_time = datetime.datetime.now()      
unix_time = time.mktime(date_time.timetuple())
unix_time = str(unix_time)
unix_time = unix_time.split('.')[0]


# Se escribe un nuevo archivo con la fuente procesada 
file = os.path.abspath('../Fuentes_procesadas/Auxiliar_Inversiones_' + data_date + '_' + unix_time + '.xlsx')
writer = ExcelWriter(file)
df.to_excel(writer, 'bdINV', index=False)
writer.save()

print("Fuentes procesada con exito")


