import pymysql

class ConexionDB:
    host: str
    user: str
    password: str
    database = ''

    def __init__(self, host='localhost', user='root', password='', database=''):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        try:
            self.connectToDB()
            self.active = True
        except pymysql.Error as e: 
            print(f'{e.args[0]}, {e.args[1]}')
            self.active = False

    def connectToDB(self):
        self.connection = pymysql.connect(
            host=self.host, 
            user=self.user,
            password=self.password,
            database=self.database
        )

    def changeDatabase(self, database):
        prev_db = self.database
        try:
            self.database = database
            self.connectToDB()
            return 1
        except pymysql.DatabaseError as e:
            self.database = prev_db
            return e

    def cursor(self):
        return self.connection.cursor()

def test():
    db = ConexionDB(database='PYTHON')

    with db.cursor() as cursor:
        query = 'CREATE TABLE TEST (ID INT NOT NULL PRIMARY KEY, TEST_DATA VARCHAR(100))'
        try:
            cursor.execute(query)
            db.connection.commit()

        except: pass


if __name__ == '__main__':

    #test()

    pass
