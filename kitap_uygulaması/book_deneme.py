import psycopg2 as psy
from psycopg2 import sql,extensions
import sys

database_name='book1'
user_name='postgres'
password='mustafa'
host='localhost'
port='5432'
connection=psy.connect (database=database_name,user=user_name,password=password,host=host,port=port)
cursor=connection.cursor()
autocommit=extensions.ISOLATION_LEVEL_AUTOCOMMIT
connection.set_isolation_level(autocommit)
def createDB(dbname):
    try :
        connection = psy.connect(
            dbname='postgres',
            user=user_name,
            password=password,
            host=host,
            port=port
        )
        # Bağlantının autocommit modunda olması için izolasyon seviyesini ayarlama
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        # Veritabanı oluşturma sorgusu
        create_db = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
        cursor.execute(create_db)
        print(f"{dbname} veritabanı başarıyla oluşturuldu.")

        
        
        
    except(Exception,psy.Error) as error:
        print(f"Veritabanı oluşturulurken hata oluştu: {error}")
        
       
       
       
       
    finally   :
        if connection :
            cursor.close()
            connection.close()
            print('PostgreSQL veri tabanı kapatılmıştır.')  
def createTable():
    try:
        # Yeni oluşturulan book1 veritabanına bağlanma
        connection = psy.connect(
            dbname='book1',
            user=user_name,
            password=password,
            host=host,
            port=port
        )
        # Bağlantının autocommit modunda olması için izolasyon seviyesini ayarlama
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        # Tablo oluşturma sorgusu
        create_table = '''
        CREATE TABLE IF NOT EXISTS book(
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            author_name TEXT NOT NULL,
            author_surname TEXT NOT NULL,
            published_date DATE,
            type TEXT,
            page_number INTEGER
        )'''
        cursor.execute(create_table)
        print("Tablo başarıyla oluşturulmuştur.")

    except psy.Error as error:
        print(f"PostgreSQL veritabanına bağlanırken bir hata oluştu: {error}")

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veritabanı bağlantısı kapatılmıştır.')
def insertTable(id,name,author_name,author_surname,published_date,type,page_number):
    try:
        connection = psy.connect(
            dbname='book1',
            user=user_name,
            password=password,
            host=host,
            port=port
        )
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        insert_table='INSERT INTO book(id,name,author_name,author_surname,published_date,type,page_number) VALUES(%s,%s,%s,%s,%s,%s,%s)'
        inserted_values=(id,name,author_name,author_surname,published_date,type,page_number) 
        cursor.execute(insert_table,inserted_values)   
        count=cursor.rowcount
        print(count,'Kayıt tabloya başarıyla eklenmiştir')       
    except psy.Error as error:
        print(f"PostgreSQL veritabanına bağlanırken bir hata oluştu: {error}")

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veritabanı bağlantısı kapatılmıştır.') 
def selectTable():
    try :
        connection = psy.connect(
            dbname='book1',
            user=user_name,
            password=password,
            host=host,
            port=port
        )
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        print("Veritabanına bağlandı.")
        selectQuery = 'SELECT * FROM book'
        cursor.execute(selectQuery)
        print("Sorgu çalıştırıldı.")
        books = cursor.fetchall()
        for kitap in books:
            print(kitap)
        count = len(books)  
        print(f'tabloda toplam {count} kayıt bulunmaktadır')
    except psy.Error as error:
        print(f"PostgreSQL veritabanına bağlanırken bir hata oluştu: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veritabanı bağlantısı kapatılmıştır.')

def updateTable(id,name):
    try:
         connection = psy.connect(
            dbname='book1',
            user=user_name,
            password=password,
            host=host,
            port=port
         )
         connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
         cursor = connection.cursor()
         updateQuery='UPDATE book SET name=%s WHERE id=%s'
         cursor.execute(updateQuery,(name,id))
         count=cursor.rowcount
         print(f'tabloda toplam{count} veri güncellenmiştir')
        
    except psy.Error as error:
        print(f"PostgreSQL veritabanına bağlanırken bir hata oluştu: {error}")

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veritabanı bağlantısı kapatılmıştır.')
def deleteTable(bookid):
    try:
        connection = psy.connect(
            dbname='book1',
            user=user_name,
            password=password,
            host=host,
            port=port
        )
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        deleteQuery='DELETE FROM book WHERE id = %s'
        cursor.execute(deleteQuery,(bookid,))
        count=cursor.rowcount
        print(f'{count} kayıt veritabanından başarıyla silinmiştir.')
    except psy.Error as error:
        print(f"PostgreSQL veritabanına bağlanırken bir hata oluştu: {error}")

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veritabanı bağlantısı kapatılmıştır.')  
def menu():
    print('Hosgeldiniz seçiminizi yapınız.') 
    print('1-Yeni kayıt ekle') 
    print('2-Kayıt güncelleme') 
    print('3-Kayıt silme')                      
    print('4-Kayıtları listeleme') 
    print('5-çıkış') 
    print('') 
def insert_case():
    id = input('ID: ')
    name = input('NAME: ')
    author_name = input('AUTHOR_NAME: ')
    author_surname = input('AUTHOR_SURNAME: ')
    published_date= input('PUBLISHED_DATE: ')
    type= input('TYPE: ')
    page_number=input('PAGE_NUMBER :')
    insertTable(id, name, author_name,author_surname, published_date, type,page_number)

def update_case():
    id = input('ID: ')
    name = input('NAME: ')
    updateTable(id, name)

def delete_case():
    id = input('ID: ')
    deleteTable(id)

def list_case():
    selectTable()

def exit_case():
    sys.exit()

def handle_invalid_selection():
    print('Yanlış bir seçim yaptınız.')

def switch_case(case):
    switcher = {
        '1': insert_case,
        '2': update_case,
        '3': delete_case,
        '4': list_case,
        '5': exit_case,
    }
    return switcher.get(case, handle_invalid_selection)
def main():
    while True:
        menu()
        case = input('Lütfen seçiminizi yapınız: ')
        func = switch_case(case)
        func()        
    
                
if __name__=='__main__' :   
    createDB('book1') 
    createTable() 
    main()  
          