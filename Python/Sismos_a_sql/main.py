import requests, json
from classes import ConexionDB

API = 'https://chilealerta.com/api/query/?user=demo&select=ultimos_sismos&country=chile'

def actualizarDBSismos():
    response = requests.get(API)
    if response.status_code == 200:
        data = json.loads(response.text.encode('UTF-8'))
        db = ConexionDB(database='python')

        try:
            with db.cursor() as cursor:
                query = f'DROP TABLE SISMOS'
                cursor.execute(query)
                db.connection.commit()
        except: pass

        with db.cursor() as cursor:
            # Crear tabla con campos
            columns = '(PK INT AUTO_INCREMENT PRIMARY KEY'
            columns2 = ''
            for key in data['ultimos_sismos_chile'][0].keys():
                columns += f',`{key}` VARCHAR(100)'
                columns2 += f',`{key}`'
            columns += ')'   
            columns2 = '(' + columns2[1:] +')'   
            query = f'CREATE TABLE SISMOS {columns}'
            cursor.execute(query)
            values = ""
            for i in data['ultimos_sismos_chile']:
                # COMPROBAR SI EL REGISTRO EXISTE
                values_l = ''
                for key, value in i.items():
                    values_l += f',\'{value}\''
                values_l = '(' + values_l[1:] + ')'
                values += values_l + ","
            values = values[:-1]
            query = f'INSERT INTO SISMOS{columns2} VALUES {values}'
            cursor.execute(query)
            db.connection.commit()
            print('Se guardaron los últimos registros de sismos')


if __name__ == '__main__':
    actualizarDBSismos()
