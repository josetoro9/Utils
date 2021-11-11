
"""
#-------------------------------------------------------------------------
# Author: Nestor Rivera #CD: 22/02/2017 #LUD 09/01/2020
# Description: Repositorio de funciones para orquestador python.
#
# Run: python funcs.py
#
# v1.1
#
# Versionados:
#
# Author: Juan Pablo Gallego
# Aporte: Ingresar repeticiones de queries en caso de error.
# 
# Author: Jose A Toro O
# Aporte: Errores en tablas LZ y JSON, mensajes de error en correo personalizado.
#
#-------------------------------------------------------------------------
"""
import glob
import io
import json
import logging
import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta
from ntpath import basename
from collections import OrderedDict

import pyodbc

#%%
logger =  logging.getLogger(__name__)
# Funciones este proceso de querys funciona con regular ex por lo tanto
# Es importante no comentar querys en el archivo SQL.

"""
#-------------------------------------------------------------------------
#--- Function to start the logging depending on the nameFile and path ----
#---------------- @author: Nestor Rivera (NRIVERA) -----------------------
#
# inputs:
# - path: Ruta donde se encontrara el archivo
# - namelog: Nombre del archivo que tendra el log
# output:
# - namelog.log
#
# usage:
# print_log_messages(String)
#-------------------------------------------------------------------------
"""
def inic_logger(path, namelog):
    global logger
    logger = logging.getLogger(namelog)
    
    directory = '/'.join([path, 'logs/'])
    print(directory) 
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    
    hdlr = logging.FileHandler(directory + namelog + '.log')
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(filename)s] :%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)
# end def
"""
#-------------------------------------------------------------------------
#----- Functiones to print in the log depending on the type of message -----
#---------------- @author: Jose Toro  (JOTORO) -----------------------
#
# inputs:
# - mensaje: Mensaje a mostrar
# output:
# - 
#
# usage:
# print messages in front and log.
#-------------------------------------------------------------------------
"""
def print_i(mensaje):
    print(mensaje)
    logger.info(mensaje)
# end def
def print_e(mensaje):
    print(mensaje)
    logger.error(mensaje)
# end def
def test_log():
    global logger
    logger.error('ESTO ES UNA PRUEBA')
# end def
int_rx = re.compile( '^(?:0*$|(?!0)[-+]?[0-9]*$)' )
createt_rx = '(create)\s*(table)\s*((if)\s*(not)\s*(exists)\s*)?(\w)+\.(\w)+[^;]+(;)'
table_rx ='\s*?(\w)+\.(\w)+\s*' 
dropt_rx = '(drop)\s*(table)\s*((if)\s*(exists)\s*)?(\w)+\.(\w)+\s*(purge)\s*?(;)'
truncatet_rx='(truncate)\s*(\w)+\.(\w)+\s*[^;]+(;)'
insert_rx = '(insert)\s*(into)\s*((table)\s*)?(\w)+\.(\w)+\s*(values)?[^;]+(;)'
insert_over = '(insert)\s*(overwrite)\s*((table)\s*)?(\w)+\.(\w)+\s*(values)?[^;]+(;)'
sample_control_rx = '(\-)(\-)(sample_control)(\-)(\-)'
compute_stats_rx='(compute)\s*((incremental)\s*)?(stats)\s*(\w)+\.(\w)+\s*(values)?[^;]+(;)'
alter_table_rx='(alter)\s*(table)\s*(\w)+\.(\w)+\s*(add)\s*((if)\s*(not)\s*(exists)\s*)?(partition)+\s*(values)?[^;]+(;)'
alter_table_drx='(alter)\s*(table)\s*(\w)+\.(\w)+\s*(drop)\s*((if)\s*(exists)\s*)?(partition)+\s*(values)?[^;]+(;)'
refresh_rx='(refresh)\s*(\w)+\.(\w)+\s*[^;]+(;)'

t_TRASH = "create|table|=|;|\s+|,|\t|\r"
double_rx = re.compile( r'^[-+]?([0-9]*\.[0-9]*((e|E)[-+]?[0-9]+)?'+
        r'|NaN|inf|Infinity)$')
date_rx = re.compile( r'^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$' );
time_rx = re.compile( r'[0-9][0-9]:[0-9][0-9](:[0-9][0-9])?' );
datetime_rx = re.compile(r'^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9](T| ).+')
datetime_rx2 = re.compile(r'\w\w\w \w\w\w \d\d \d\d:\d\d:\d\d \w\w\w \d\d\d\d')

list_sp_char = ['~', ':', "'", '+', '[', '\\', '@', '^', '{', '%', '(', '-', '"', '*', '|', ',', '&', '<', '`', '}', '_', '=', ']', '!', '>', ';', '?', '#', '$', ')', '/']

#%%


"""
#-------------------------------------------------------------------------
#--- Function to start the connection tih impala depending on the OS  ----
#---------------- @author: Nestor Rivera (NRIVERA) -----------------------
#------------------ @aporte: Jose Toro (JOTORO) --------------------------
#
# inputs:
# - basepath: se usa para traer el DSN del .json
#
# Usage:
# impala_connect(basePath)
#-------------------------------------------------------------------------
"""

def impala_connect(basePath):
    with open(os.path.join(basePath, "config.json")) as file:
        tablas_externas = json.load(file)
        CONN_STR = tablas_externas['conn_str']
    atp = 0
    while atp < 5:
        try:
            print_i("INICIA CONEXION CON IMPALA")
            cn = pyodbc.connect( CONN_STR, autocommit = True )
            print_i("Conexion exitosa") 
            return cn
            break
        except:
            time.sleep(10)
            print_e('Falla conexion, repitiendo... (intentos: '+str(atp+1)+') \n')
            atp+=1
            continue
    if atp == 5:
        error_msg_lz='Imposible realizar la conexion con LZ, buscar razon del error.'
        print_e(error_msg_lz)
        sys.exit(1) 
# end def

#%%
"""
#-------------------------------------------------------------------------
#--------- Function to get the list of the queries to execute  -----------
#---------------- @author: Nestor Rivera (NRIVERA) -----------------------
#
# inputs:
# - path: Ruta de ubicacion.
# - file_name: nombre del archivo que contiene los queries, debe ser .sql
#
# Usage:
# Obtiene la lista de queries a ejecutar
#-------------------------------------------------------------------------
"""


def getQueriesList(path, file_name):
    f = io.open( path + file_name, "r", encoding='utf8')
    content = f.read()
    f.close()
    
    it = re.finditer('|'.join([insert_rx, insert_over, createt_rx, dropt_rx, compute_stats_rx, alter_table_rx,alter_table_drx, truncatet_rx, refresh_rx]),  content, re.IGNORECASE) 
    sizeit = 0
    queries = list()
    
    for item in it:
        sizeit += 1
        itg = item.group()
        tname = re.search(table_rx, itg).group()
        tsample = 1 if re.search(sample_control_rx, itg) else 0
        re.search(table_rx, itg).group()
        typeq = itg.partition(' ')[0]
        queries.append([itg, typeq.lower(), tname.strip(), tsample])

    return queries
# end def    

"""
#-------------------------------------------------------------------------
#------------ Function to execute the list of the queries  ---------------
#---------------- @author: Nestor Rivera (NRIVERA) -----------------------
#---------- @contribution: Juan Pablo Gallego (JGVALENC) -----------------
#------------ @contribution: Jose Toro Ospina (JOTORO) -------------------
#
# inputs:
# - path: Ruta de ubicacion.
# - file_name: Nombre del archivo que contiene los queries, debe ser .sql
# - cn: Parametro de la conexion.
#
# Usage:
# Ejecuta la lista de queries determinada, crea archivo de muestra 
# ocurrencia errores, intenta de nuevo ejecuciones no exitosas.
#-------------------------------------------------------------------------
"""

def runQueriesV2(path, file_name, cn, path_config):
     
    print_i("Inicia lectura de archivo: " + file_name) 
    print_i("Inicia lectura de JSON config: ") 
    params_fn = os.path.join(path_config, "config.json")
    params_io = io.open(params_fn)
    params_str = params_io.read()
    params_io.close()
    params = json.loads(params_str , object_pairs_hook=OrderedDict)
    print_i("Finalizacion lectura de JSON config cerrando...")  
    ql = getQueriesList(path, file_name)
    d = [{'query': x[0], 'typeq': x[1], 'tname': x[2], 'tsample': x[3]} for x in ql]
    totalqueries = len(d)
    cursor = cn.cursor()
    print_i("Se encontraron: " + repr(totalqueries) + " queries") 
    print_i("Para proceder se crea tabla de repositorio de errores.")  
    largo_file=len(file_name)-4
    file_error=file_name[1:largo_file]
    #codigo_query = 'create table if not exists proceso_clientes_personas_y_pymes.' + file_error +' (error int) stored as parquet'
    #codigo_error_truncate='truncate proceso_clientes_personas_y_pymes.' + file_error
    codigo_query = params['codigo_query']
    codigo_query = codigo_query.format(table_error=file_error)
    codigo_error_truncate = params['codigo_error_truncate']
    codigo_error_truncate = codigo_error_truncate.format(table_error=file_error)
    # Introduccion JSON para errores
    data = {}
    data['error'] = 0
    # .json error
    json_error = file_error + '.json'

    
    try:
        with open(os.path.join(path, json_error), 'w') as file:
                json.dump(data, file, indent=4)
    except:
        print_e('Imposible crear el json.')

    
    error_creacion=0

    while error_creacion<5:
        try:
            cursor.execute(codigo_query)
            cursor.execute(codigo_error_truncate)
            break
        except:
            time.sleep(10)
            print_e('Falla query, repitiendo... (intentos: '+str(error_creacion+1)+') \n')
            print_e('\n')
            error_creacion+=1
            continue
    if error_creacion == 5:
        error_msg_lz='Imposible ejecutar el query, es posible que la conexion sea inestable, se crea solamente JSON.'
        print_e(error_msg_lz)
        email(error_msg_lz)
        sys.exit(1) 


    print_i("INICIA LA EJECUCION DE LAS QUERIES") 


    i = 0
    exito = 0
    errores = ''
    currentTime2 = datetime.now()
    for di in d:    
        
        query = di['query']
        tname = di['tname']
        typeq = di['typeq']
        tsample = di['tsample']
        
        try:
            currentTime = datetime.now()
            ###########################
            atp = 0
            while atp < 5:
                try:
                    cursor.execute(query)
                    break
                except:
                    time.sleep(10)
                    print_e('Falla query, repitiendo... (intentos: '+str(atp+1)+') \n')
                    if typeq == 'create':
                        #q_del = 'drop table if exists ' + tname + ' purge'
                        q_del = params['q_del']
                        q_del = q_del.format(tname=tname)
                        cursor.execute(q_del)
                        time.sleep(10)
                        print('\n')
                    atp+=1
                    continue
            if atp == 5:
                error_msg_lz='Imposible ejecutar el query, buscar razon del error.'
                print_e(error_msg_lz)
                raise Exception('El proceso se interrumpe')

            ###########################
            print(datetime.now()-currentTime)
            logger.info(str(datetime.now()-currentTime))
            savelogV2(typeq,tname, i)
            i+=1
            
            if typeq in ('insert', 'create'):
                executeConteoV2(tname, cn, params)
                
                #para las tablas que tienen la bandera --sample_control-- en el sql
                if tsample != 0:
                   executeSampleV2(tname, cn, 20, params) 
            exito+=1
        except:
            errores = '\n'.join([errores, typeq + ' - ' + tname])

            traceback.print_exc()
            print_e("Se encontro un error en el query ")
            print_i("Continua la ejecucion ...  ")
            continue          
       
    if exito ==  totalqueries:
        print_i(str(datetime.now()-currentTime2))
        exito_msg = "Se ejecutaron todas las queries EXITOSAMENTE " + file_name
        print_i(exito_msg)
    else:
        print_e("Fallo la ejecucion de  " + repr(totalqueries - exito) + " queries, abajo el detalle:")
        print_e(errores)
        print_e('Error ejecutando: Se crea tabla de respuesta.')

        #codigo_query_errores = 'insert into proceso_clientes_personas_y_pymes.' + file_error +' values '+' (cast(1 as int));'
        #compute_errores= 'compute incremental stats proceso_clientes_personas_y_pymes.' + file_error +';'
        codigo_query_errores = params['codigo_query_errores']
        codigo_query_errores = codigo_query_errores.format(table_error=file_error)
        compute_errores = params['compute_errores']
        compute_errores = compute_errores.format(table_error=file_error)
        data['error'] = 1
        try:
            with open(os.path.join(path, json_error), 'w') as file:
                json.dump(data, file, indent=4)
        except:
            print_e('Imposible actualizar el json conteniendo el error.')
        errores=0

        while errores<2:
            try:
                cursor.execute(codigo_query_errores)
                cursor.execute(compute_errores)
                break
            except:
                time.sleep(10)
                print_e('Falla query, repitiendo... (intentos: '+str(errores+1)+') \n')
                print_e('\n')
                errores+=1
                continue
        if errores == 2:
            error_msg_lz='Falla la insercion de datos en tablas de error, rutina de errores ejecuta con JSON.'
            print_e(error_msg_lz)
            print_i(codigo_query_errores)
            print_i(compute_errores)
        sys.exit(1) 
        
# end def   

"""
#-------------------------------------------------------------------------
#--------- Function to save the logs of the runing process  --------------
#---------------- @author: Nestor Rivera (NRIVERA) -----------------------
#
# inputs:
# - typeq: Muestra el texto con que inicia el query.
# - tname: Nombre de la tabla mencionada en el query
# - i: El numero del query en la lista del archivo .sql
#
# Usage:
# Guarda log de los queries que va ejecutando
#-------------------------------------------------------------------------
"""

def savelogV2(typeq, tname, i):
    print_i("query *****  " + repr(i) + " ******  ejecutada")
    print_i(typeq + "\t" + tname)
# end def

"""
#-------------------------------------------------------------------------
#--------- Funcion para ver cuantos registros tiene cada tabla  ----------
#---------------- @author: Nestor Rivera (NRIVERA) -----------------------
#
# inputs:
# - tname: Nombre de la tabla mencionada en el query
# - cn: Parametro de la conexion.
#
#-------------------------------------------------------------------------
"""

def executeConteoV2(tname, cn, params):

    cursor = cn.cursor()
    
    ###SAMPLE
    #query = 'select count(*) from %s'
    query = params['query']  

    query = query % (tname)
    try:        
        cursor.execute( query)
        conteo_pre = cursor.fetchone()
        conteo = conteo_pre[0] if conteo_pre else 0
        print_i( repr(conteo) + " registros")

    except:
        print_e("Encontrar el error")
# end def

"""
#-------------------------------------------------------------------------
#------------- Funcion para ver muestra de registros   -------------------
#---------------- @author: Nestor Rivera (NRIVERA) -----------------------
#
# inputs:
# - tname: Nombre de la tabla mencionada en el query
# - cn: Parametro de la conexion.
# - nrows: Tamanio de la muestra.
#
#-------------------------------------------------------------------------
"""

def executeSampleV2(tname, cn, nrows, params):
    
    cursor = cn.cursor()
        
    ###CONTEO
    query = params['query_sample']
    
    query = query % (tname, nrows)
    try:        
        cursor.execute( query)
        dataall = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        
        print_i( "Muestra de " + repr(nrows) + " de la tabla " + tname)
        
        ncols = 0
        cols_ol = '\t'
        for cl in columns:
            cols_ol = '\t'.join([cols_ol, cl])
            ncols+=1
        
        
        for data in dataall:            
            
            rinfo = ''
            for i in range(0, ncols):
                rinfo = '\t'.join([rinfo, str(data[i]) ])
            cols_ol = '\n'.join([cols_ol, str(rinfo)])
        logger.info( cols_ol)              
        print(cols_ol)
        
    except:
        print_i("Encontrar el error")
# end def

"""
#-------------------------------------------------------------------------
#------------------- Funcion reemplaza variables   -----------------------
#-------------------- @author: Comunidad Python --------------------------
#
# inputs:
# - filename_in: Archivo de entrada.
# - filename_out: Archivo de salida.
# - variables
#
#-------------------------------------------------------------------------
"""

def replaceVarFile(filename_in, variables, filename_out):
    print_i('Leyendo archivo: ' + filename_in.split('\\')[-1])
    # Open and read the file as a single buffer
    try:    
        fd = io.open(filename_in, 'r', encoding="utf8")
    except Exception:
        print_e('No se puede abrir archivo.')
        return        
    sqlFile = fd.read()
    fd.close()
    # changing variables
    for var in variables:
        # Counting
        print_i('Se encontraron ' + str(sqlFile.count(var)) + ' ocurrencias de ' + var)
        sqlFile = sqlFile.replace(var, variables[var])
        print_i('Se cambia por ' + variables[var])
    # Saving new file
    archivo = basename(filename_out)
    print_i('Guardando archivo: ' + archivo)
    escritura_replace=0
    while escritura_replace<2:
        try:
            fdestino = io.open(filename_out, 'w', encoding="utf8")
            fdestino.write(sqlFile)
            fdestino.close() 
            break
        except:
            time.sleep(10)
            print_e('Falla escritura, repitiendo... (intentos: '+str(escritura_replace+1)+') \n')
            escritura_replace+=1
            continue
    if escritura_replace == 2:
        print_e('Imposible crear el archivo. puede usarse archivo anterior')
# end def

"""
#-------------------------------------------------------------------------
#------------------- Funcion enviar LOGS Solo Windows --------------------
#--------------- @author: Jose Toro Ospina (JOTORO ) ---------------------
#
# inputs:
# - mensaje: Texto en el mensaje
# - adjunto: Por defecto nulo, si se expecifica debe ser un archivo para adjuntar
#
# Usage:
# Mandar Email cuando este en windows notificando finalizacion del proceso
# Este solo funciona especificando el destinatario
# Esta diseniado para pruebas en el uso local
#-------------------------------------------------------------------------
"""

def email(mensaje, adjunto=None):
    try:
        import win32com.client as win32
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        #Email Personalizado, los detinatarios se deben separa por ;
        mail.To = 'jotoro@bancolombia.com.co;alecontr@bancolombia.com.co'
        mail.Subject = 'El proceso ha finalizado.'
        mail.Body = mensaje
        if adjunto is None:
            mail.Send()
        else:
            mail.Attachments.Add(Source=adjunto)
            mail.Send()
        # end if
        print_i('Email de finalizacion del proceso enviado')
    except:
        print_i('Ya que se ejecuta en OS diferente a Windows no se envia mensaje de finalizacion.')
# end def   

"""
#----------------------------------------------------------------------------------
#- funcion para validar la existencia de tablas de resultados insumos del proceso -
#------------------------- @author: Jose Toro (jotoro) ----------------------------
#
# inputs:
# 
# output:
# - existencia_tablas --> Usado para saber si los insumos existen
#
# usage:
# fn_validacion_tablas_externas(PATH,CONECTION)
#----------------------------------------------------------------------------------
"""
def fn_validacion_tablas_externas(basepath,cn):
    global existencia_tablas
    existencia_tablas=0
    with open(os.path.join(basepath, "config.json")) as file:
        tablas_externas = json.load(file)
        proceso = tablas_externas['tablas_externas']
    for i in proceso:
        nombre_tabla=i
        try:
            cn.execute("describe " + i).fetchall()
            print_i('La tabla Insumo Existe :' + nombre_tabla)
        except:
            existencia_tablas = existencia_tablas + 1
            print_e('La tabla Insumo NO Existe :' + nombre_tabla)
    return existencia_tablas
# end def

"""
#--------------------------------------------------------------------------------------
#- funcion para validar los errores en el proceso de cargue a la nube por productizar -
#--------------------------- @author: Jose Toro (jotoro) ------------------------------
#
# inputs:
# 
# output:
# - existencia_tablas --> Usado para saber si los insumos existen
#
# usage:
# fn_validacion_productizar(CONECTION)
#--------------------------------------------------------------------------------------
"""
def fn_validacion_productizar(cn,path_config):
    global resultado_estatus
    params_fn = os.path.join(path_config, "config.json")
    params_io = io.open(params_fn)
    params_str = params_io.read()
    params_io.close()
    params = json.loads(params_str , object_pairs_hook=OrderedDict)
    fecha=datetime.now() - timedelta(days=1)
    year = str(fecha.year)
    month = str(fecha.month)
    day = str(fecha.day)
    generales='select end_status from resultados.reporte_coordinador_productizar where process_name="stoc_personas_generales" and year=' + str(fecha.year) + ' and ingestion_month='+ str(fecha.month) +' and ingestion_day=' + str(fecha.day)
    #generales = params['query_produc_gen']
    #generales = generales.format(pyear=year)
    #generales = generales.format(pmonth=month)
    #generales = generales.format(pday=day)
    preaprobados='select end_status from resultados.reporte_coordinador_productizar where process_name="stoc_personas_preaprobados" and year=' + str(fecha.year) + ' and ingestion_month='+ str(fecha.month) +' and ingestion_day=' + str(fecha.day)
    #preaprobados = params['query_produc_preap']
    #preaprobados = preaprobados.format(pyear=year)
    #preaprobados = preaprobados.format(pmonth=month)
    #preaprobados = preaprobados.format(pday=day)
    try:
        estatus_generales=cn.execute(generales).fetchall()[0][0]
        estatus_preaprobados=cn.execute(preaprobados).fetchall()[0][0]
        print_i('El resultado de productizar es : '+ estatus_generales + '. Proveniente de la tabla : resultados.reporte_coordinador_productizar')
    except:
        print_e('La tabla Insumo NO Existe : resultados.reporte_coordinador_productizar')
        estatus_generales='FAIL'
        estatus_preaprobados='FAIL'
    if estatus_generales=='OK' and estatus_preaprobados=='OK':
        resultado_estatus = 0
    else:
        resultado_estatus = 1
    return resultado_estatus
# end def

"""
#--------------------------------------------------------------------------------------
#-------------------- funcion para reporte resumido de ejecusion ----------------------
#--------------------------- @author: Jose Toro (jotoro) ------------------------------
#
# inputs:
# 
# output:
# - mensajes para log y para print
#
# usage:
# fn_resumen_ejecucion()
#--------------------------------------------------------------------------------------
"""
def fn_resumen_ejecucion(basepath,cn,flag_windows,flag_existencia,flag_registros_nuevos,flag_errores,path_config):
    cursor = cn.cursor()
    params_fn = os.path.join(path_config, "config.json")
    params_io = io.open(params_fn)
    params_str = params_io.read()
    params_io.close()
    params = json.loads(params_str , object_pairs_hook=OrderedDict)
    # Preliminar mensajes
    msg_windows = 'Ejecucion realizada en '
    msg_1_existencia = ''
    msg_2_existencia = ''
    msg_1_registros_nuevos = ''
    msg_2_registros_nuevos = ''
    msg_errores = ''
    msg_final = 'El proceso finaliza.'

    if flag_windows == True:
        msg_windows = msg_windows + 'Windows Publicacion en Proceso:'
        print_i(msg_windows)
    else:
        msg_windows = msg_windows + 'Linux Publicacion en Resultados:'
        print_i(msg_windows)
    if flag_existencia == True:
        msg_1_existencia = 'Todas las tablas insumo Existen.'
        print_i(msg_1_existencia)
        if flag_registros_nuevos == True:
            msg_1_registros_nuevos = 'Se identifican nuevos Registros.'
            msg_2_registros_nuevos = 'No se recupera informacion ni se generan Deltas de actualizacion.'
            print_i(msg_1_registros_nuevos)
            print_i(msg_2_registros_nuevos)
        else:
            if flag_errores == True:
                msg_errores = 'Se idenfifican errores en la ejecucion anterior.'
                print_i(msg_errores)
            else:
                msg_errores = 'No se identifican errores y por lo tanto se hace mantenimiento a la informacion.'
                print_i(msg_errores)
    else:
        msg_1_existencia = 'No todas las tablas insumo Existen.'
        msg_2_existencia = 'El proceso finaliza recreando el dia anterior.'
        print_i(msg_1_existencia)
        print_i(msg_2_existencia)
    print_i(msg_final)
    msg_completo = " ".join([msg_windows,msg_1_existencia,msg_2_existencia,msg_1_registros_nuevos,msg_2_registros_nuevos,msg_errores,msg_final])
    #query_reporte_preliminar = ['create table if not exists proceso_clientes_personas_y_pymes.stoc_personas_reporte_resumido (mensaje  string) stored as parquet',
    #                'insert into proceso_clientes_personas_y_pymes.stoc_personas_reporte_resumido values (cast("'+ msg_completo +'" as string))',
    #                'compute stats proceso_clientes_personas_y_pymes.stoc_personas_reporte_resumido']
    query_reporte_preliminar = params['query_resumen']
    query_reporte_preliminar = query_reporte_preliminar.format(msg_completo=msg_completo)
    with open(os.path.join(basepath, "config.json")) as file:
        tablas_externas = json.load(file)
        query_reporte = tablas_externas['publicacion_resumen']

        for row in query_reporte_preliminar:
            query_reporte.append(row)
    impala_connect(basepath)
    print_i('Se carga el reporte en LZ.')
    for i in query_reporte:
        try:
            cursor.execute(i)
        except:
            print_i('No se pudo ejecutar cargue en LZ de reporte.')
# end def


"""
#-------------------------------------------------------------------------
#------------------- Funcion reemplaza variables   -----------------------
#-------------------- @author: Comunidad Python --------------------------
#
# inputs:
# - basePath_in: Ruta de entrada.
# - basePath_out: Ruta de salida.
# 
# usage:
# fn_copiar_archivo(basePath_in, basePath_out)
#-------------------------------------------------------------------------
"""

def fn_copiar_archivo(basePath_in, basePath_out):
    print_i('Leyendo Ruta: ' + basePath_in.split('\\')[-1])
    archivos_rutina = glob.glob(basePath_in + '/*')
    #archivos_rutina.remove(basePath_in + '\\stoc_personas2_inicio.sql')
    #archivos_rutina.remove(basePath_in + '\\stoc_personas2_final.sql')
    #archivos_rutina.remove(basePath_in + '\\stoc_personas2_reporte.sql')
    
    # Recorrer lista de archivos:
    for item in archivos_rutina:
        # Open and read the file as a single buffer
        try:    
            fd = io.open(item, 'r', encoding="utf8")
        except Exception:
            print_e('No se puede abrir archivo.')
            return        
        File = fd.read()
        fd.close()
        archivo=basename(item)
        filename_out=os.path.abspath(os.path.join(basePath_out, archivo))
        # Saving new file
        print_i('Replicando archivo: ' + archivo)
        escritura_replace=0
        while escritura_replace<2:
            try:
                fdestino = io.open(filename_out, 'w', encoding="utf8")
                fdestino.write(File)
                fdestino.close() 
                break
            except:
                time.sleep(10)
                print_e('Falla escritura, repitiendo... (intentos: '+str(escritura_replace+1)+') \n')
                escritura_replace+=1
                continue
        if escritura_replace == 2:
            print_e('Imposible crear el archivo. puede usarse archivo anterior')
# end def

"""
#-------------------------------------------------------------------------
#------------------- Funcion cerrado definitivo de log  ------------------
#-------------------- @author: jotoro --------------------------
#
# input:
# - attachment1: ubicacion + nombre del archivo log
# 
# usage:
# fn_copiar_archivo(attachment1)
#-------------------------------------------------------------------------
"""

def fn_cerrar_log(attachment1):
    print_i('Cerrando log:')
    try:
        logging.shutdown()
        flog = open(attachment1, 'a')
        flog.close()
        print_i('Log cerrado exitosamente.')
    except:
        print_e('Imposible cerrar log.')

"""
#-------------------------------------------------------------------------
#------------------- Funcion print STOC  ------------------
#-------------------- @author: jotoro --------------------------
#
#-------------------------------------------------------------------------
"""

def print_stoc():
    print_i("""      
          ___           ___           ___           ___     
         /\  \         /\  \         /\  \         /\  \     
        /::\  \        \:\  \       /::\  \       /::\  \    
       /:/\ \  \        \:\  \     /:/\:\  \     /:/\:\  \   
      _\:\~\ \  \       /::\  \   /:/  \:\  \   /:/  \:\  \  
     /\ \:\ \ \__\     /:/\:\__\ /:/__/ \:\__\ /:/__/ \:\__\ 
     \:\ \:\ \/__/    /:/  \/__/ \:\  \ /:/  / \:\  \  \/__/ 
      \:\ \:\__\     /:/  /       \:\  /:/  /   \:\  \       
       \:\/:/  /     \/__/         \:\/:/  /     \:\  \      
        \::/  /                     \::/  /       \:\__\     
         \/__/                       \/__/         \/__/    
         """)
#%%
        