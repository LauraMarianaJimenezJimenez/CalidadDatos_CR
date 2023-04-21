import pandas as pd 
import datetime
import time
import os

# Validation flag, unix time
date_time = datetime.datetime.now()      
unix_time = time.mktime(date_time.timetuple())
unix_time = str(unix_time)
print(unix_time)
unix_time = unix_time.split('.')[0]
print(unix_time)

'''
date_time = datetime.datetime.now()

# print regular python date&time
print("date_time =>",date_time)
 
# displaying unix timestamp after conversion
print("unix_timestamp => ",
      (time.mktime(date_time.timetuple())))

file = os.path.relpath('../../Fuentes_iniciales/Curva_Captacion_Col_20230117.xls', start=os.curdir)
print(file)
#file = '../Curva_Captacion_Col_20230117.xls'
abspath = os.path.abspath("../Fuentes_iniciales/prueba.xlsx")
print('funciono: ')
print(abspath)

df = pd.read_excel(abspath, header=None)
df.columns = df.iloc[0]


path = os.path.abspath('../Informes/Informe_brokerage_20230116.txt')

dirname = os.path.dirname("C:")
filename = os.path.join(dirname, "../Fuentes_iniciales\\Curva_Captacion_Col_20230117.xlxs")
print(filename)

# Raed filFuentes_iniciales
Nombre  Estado  Valor   Estado  Prod.Origen New PL
df = df.rename(columns={ df.columns[28]: "Precio1", df.columns[31]: "Precio2" })


path = os.path.abspath('../Informes/Prueba.txt')
f = open(path,"w+")
f.write(file)

print(df)
# Print columns and rows
f.write("\nCantidad de filas: %d" % len(df))
f.write("\nCantidad de Columnas: %d" % len(df.columns))

f.write("\nCantidad de datos vacios por cada columna del archivo")

# Validate empty cells
for column in df:
    df[column] = df[column].astype(str)
    text = column + ": " + str(df[column].isnull().sum())
    f.write("\n")
    f.write(text)

print(df)



f= open("guru99.txt","w+")     
path = "asfgg-.234"
print(path)
f.write("This is line %d\r\n")
f.write(path)
f.write("\nFilas: ")

#path.replace('^[A-Za-z0-9]*$','',regex=True, inplace = True)

print(path)

def clean_input(m):
    print(m.group(0))
    if m:
        val = m.group(1)
        if m.group(2):
            val = val + '.' +m.group(2)
    return val

def clean_input(m):
    if m:
        val = m.group(1)
        if m.group(2):
            val = val + '.' +m.group(2)
    return val

a = pd.DataFrame({'colA':
   ['4350110002',
    '4350110002tr',
    '435011|0002|',
    '43  50110002',
    '435$0110002',
    '3.8*',
    '140',
    '5.5.',
    '14.5 of HGB',
    '>14.5',
    '<14.5',
    '14,5',
   '14. 5']})

	a['colA'] = pd.to_numeric(df['colA'])
	print(a)


#a = a['colA'].str.replace('[^\\d]*(\\d+)[^\\d]*(?:\\.)?[^\\d]*(\\d)*[^\\d].[^\\d]*', clean_input)

#a['colA'] = a['colA'].str.replace('/[^a-zA-Z ]', '', regex=True)
a['colA'] = a['colA'].str.replace(' ', '')
a['colA'] = a['colA'].str.replace('[^A-Za-z0-9\\s]+', '')
a['colA'] = a['colA'].str.replace('[^0-9\\s]+', '')






#df.column_name = df.column_name.replace('/[^a-zA-Z ]', '')
''' 