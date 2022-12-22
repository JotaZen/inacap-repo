import pymysql, requests, json, time
from datetime import datetime 

API_SISMOS      = 'https://chilealerta.com/api/query/?user=demo&select=ultimos_sismos_chile'
FORMATO_FECHA   = '%Y/%m/%d %H:%M:%S'
SISMO_UMBRAL    = 3.4
DATA_BD         = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'BD_SISMOS'
}

def consultar_api(api) -> dict:
    response = requests.get(api)
    if response.status_code != 200: return {}
    data = json.loads(response.text.encode('UTF-8'))
    return data

class ConexionBD:
    def __init__(self,data):
        self.host        = data['host']
        self.user        = data['user']
        self.password    = data['password']
        self.database    = data['database']
        salida = self.conectar_bd('')
        if self.active:
            try: 
                query = f'CREATE DATABASE {self.database};'
                self.cursor().execute(query)
                self.connection.commit()    
                self.connection.close()
            except: pass 
            finally:
                self.conectar_bd(self.database)
        else:
            return salida

    def conectar_bd(self, database):
        try:
            self.connection = pymysql.connect(
                host=self.host, 
                user=self.user,
                password=self.password,
                database=database
            )
            self.active = True
            return f'Conectado en {self.host} ...'
        except pymysql.Error as e: 
            self.active = False
            return f'{e.args[0]}, {e.args[1]}'

    def cursor(self):
        return self.connection.cursor()

class Sismo:
    id_sismo:   int
    latitude:   str
    longitude:  str
    fecha:      datetime
    referencia: str
    magnitude:  float

    def __init__(self,latitude,longitude,fecha,referencia,magnitude):
        self.latitude   = latitude
        self.longitude  = longitude
        self.fecha      = fecha
        self.referencia = referencia
        self.magnitude  = magnitude

    def sincronizar(self) -> str:
        pass
    def listar(self) -> str:
        pass
    def buscar(self) -> str:
        pass

class MenuSismos(ConexionBD):
    menu_activo = True
    def __init__(self,data):
        super().__init__(data)

        if not self.active: 
            print('Error al conectar MySQL')
            return

        try:
            query = ('CREATE TABLE `TB_SISMO` ('
                '`IDSISMO` INT NOT NULL AUTO_INCREMENT ,'
                '`LATITUDE` VARCHAR(20) NOT NULL ,'
                '`LONGITUDE` VARCHAR(20) NOT NULL ,' 
                '`FECHA` DATETIME NOT NULL ,'
                '`REFERENCIA` VARCHAR(50) NOT NULL ,' 
                '`MAGNITUD` DOUBLE NOT NULL ,' 
                'PRIMARY KEY (`IDSISMO`)) ENGINE = INNODB;')
            self.cursor().execute(query)
        except:
            pass
        
        opciones = {
            '1': self.sincronizar_api,
            '2': self.listar_sismos,
            '3': self.buscar_sismos,
            '4': self.eliminar_todo,
            '5': self.salir
        }

        try:
            while self.menu_activo:
                self.menu_principal()    
                opcion = input('Ingrese una opción: ')
                while opcion not in opciones.keys():
                    opcion = input('Ingrese una opción: ')
                opciones[opcion]()
                
        except pymysql.Error as e:
            print('Ha ocurrido un error en MySQL, saliendo...')
            self.salir()
        except:
            print('Error, saliendo...')
            self.salir()

    def menu_principal(self):
        print(
            '\n'
            '**********************************\n'
            '*----* PolanKaZo Sismología *----*\n'
            '**********************************\n'
            '\n'
            ' 1.- Sincronizar valores desde API.\n'
            ' 2.- Listar sismos ingresados.\n'
            ' 3.- Buscar sismo por ID.\n'
            ' 4.- Limpiar tabla sismos.\n'
            ' 5.- Salir.\n'
            ) 

    def sincronizar_api(self):
        print(
            '\n***********************\n'
            '*** Sincronizar API ***\n'
            '***********************\n'
            )
        # EJEMPLO QUERY:
        # INSERT INTO `tb_sismo`(`LATITUDE`, `LONGITUDE`, `FECHA`, `REFERENCIA`, `MAGNITUD`) 
        # VALUES ('-29.452','-71.028','2021/03/15 16:42:59','18 km al E de La Higuera',3.7)
        try:
            ultimos_sismos = list(filter(lambda x: x['magnitude'] > SISMO_UMBRAL,consultar_api(API_SISMOS)['ultimos_sismos_chile']))
        except KeyError:
            print(' Intente más tarde ...')
            time.sleep(1.5)
            return
        except:
            print('Error fatal')
            return

        # COMPROBAR SI HAY REGISTROS IGUALES EN BASE DE DATOS
        # WHERE (`REFERENCIA`={X} AND `FECHA`={Y}) OR ...
        comprobar_repetido = ''
        for sismo in ultimos_sismos:
           comprobar_repetido += f"(`REFERENCIA`='{sismo['reference']}' AND `FECHA`='{sismo['local_time']}') OR "
        # QUITAR EL ÚLTIMO OR
        comprobar_repetido = comprobar_repetido[:-4] + ';'
    
        query_comprobar = (
            f'SELECT `FECHA`, `REFERENCIA` FROM `TB_SISMO`'
            f'WHERE {comprobar_repetido};'
            )
        with self.cursor() as cursor:
            cursor.execute(query_comprobar)
            repetidos = cursor.fetchall()

        # FILTRAR REGISTROS REPETIDOS SEGÚN FECHA Y REFERENCIA
        # datetime -> str:
        # datetime.strftime(FECHA, FORMATO_FECHA)
        def filtro_repetidos(sismo):
            for i in repetidos:
                if (sismo['reference'] == i[1] and
                    sismo['local_time'] == datetime.strftime(i[0], FORMATO_FECHA)):
                    return False
            return True
        ultimos_sismos = list(filter(filtro_repetidos, ultimos_sismos))

        # CREAR QUERY CON VALUES
        values = ''
        for sismo in ultimos_sismos:
            values += f"('{sismo['latitude']}','{sismo['longitude']}','{sismo['local_time']}','{sismo['reference']}',{sismo['magnitude']}),"
                   
        if values != '': 
            values = values[:-1] + ';'  
            query_insert = (
                f'INSERT INTO `TB_SISMO`(`LATITUDE`, `LONGITUDE`, `FECHA`, `REFERENCIA`, `MAGNITUD`)' 
                f'VALUES {values}'
                )
            self.cursor().execute(query_insert)
            self.connection.commit()
        
        print('*** La base de datos de sismos, ha sido actualizada. ***')
        time.sleep(1.5)

    def listar_sismos(self):
        print(
            '\n*********************\n'
            '*** Listar Sismos ***\n'
            '*********************\n'
            )
        query = (
            f'SELECT `IDSISMO`, `LATITUDE`, `LONGITUDE`, `FECHA`, `REFERENCIA`, `MAGNITUD` FROM `TB_SISMO`;'
            )
        with self.cursor() as cursor:
            cursor.execute(query)
            sismos = cursor.fetchall()
        for sismo in sismos:
            print(
                f'SISMO ID: {sismo[0]}\n'
                f'| Magnitud: {sismo[5]}\n'
                f'| Latitud: {sismo[1]}\n'
                f'| Longitud: {sismo[2]}\n'
                f'| Fecha: {datetime.strftime(sismo[3], "Día: %d/%m/%Y Hora: %H:%M:%S")}\n'
                f'| Referencia: {sismo[4]}\n'      
                )
        input(' Enter para volver...')

    def buscar_sismos(self):
        print(
            '\n'
            '*********************\n'
            '*** Buscar Sismos ***\n'
            '*********************\n'
            )
        id_buscar = input('- Ingrese la ID del sismo a buscar ')
        if not id_buscar.isnumeric(): 
            print(' Error, ID = NaN.')
            input(' Enter para volver...')
            return
        query = (
            f'SELECT * FROM `TB_SISMO` WHERE `IDSISMO` = {id_buscar};'
            )
        with self.cursor() as cursor:
            cursor.execute(query)
            sismo = cursor.fetchone()
        
        if sismo:
            print(
                F'\n'
                f'SISMO ID: {sismo[0]}\n'
                f'| Magnitud: {sismo[5]}\n'
                f'| Latitud: {sismo[1]}\n'
                f'| Longitud: {sismo[2]}\n'
                f'| Fecha: {datetime.strftime(sismo[3], "Día: %d/%m/%Y Hora: %H:%M:%S")}\n'
                f'| Referencia: {sismo[4]}\n'      
                )
        else:
            print(f' No se encontró ID = {id_buscar}.')

        input(' Enter para volver...')

    def eliminar_todo(self):
        if input(' ¿Está seguro de eliminar el registro de sismos? (si/no) ') != 'si': return
        query = 'TRUNCATE TABLE `TB_SISMO`;'
        self.cursor().execute(query)
        print('\n*** La base de datos de sismos, ha sido limpiada. ***')
        time.sleep(1.5)

    def salir(self):
        self.menu_activo = False
        print('Fin...')

Certamen4 = MenuSismos(DATA_BD)
