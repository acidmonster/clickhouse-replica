""" Clickhouse-replica

"""
import decimal
import requests
import sys
import argparse
import pymysql
import re
import pickle
import os.path
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

import colorama
from colorama import Fore, Back, Style
colorama.init()

# Инициализация переменных
version = '1.1'                  # версия программы
src_package_size = 1000          # размер пакета, сколько записей за раз будет извлекаться из MySQL в одном запросе SELECT при копировании данных
dst_package_size = 10000         # размер пакета, сколько записей за раз будет вставляться в Clickhouse за раз при копировании данных

def mysql_connection():
    # создаем подключение к MySQL
    mysql_con = pymysql.connect(
        host        = namespace.src_host,
        port        = namespace.src_port,
        user        = namespace.src_user,
        password    = namespace.src_password,
        charset     = "utf8mb4",
        cursorclass = pymysql.cursors.DictCursor
        )
    return mysql_con

# Парсер команд
def createParser ():
    parser = argparse.ArgumentParser()
    # MySQL
    parser.add_argument ('--src-host', default = 'localhost', help = Fore.LIGHTYELLOW_EX + 'Хост MySQL' + Style.RESET_ALL)
    parser.add_argument ('--src-port', default = 3306, type=int , help = Fore.LIGHTYELLOW_EX + 'Порт, на котором доступна MySQL' + Style.RESET_ALL)
    parser.add_argument ('--src-user', default = '', help = Fore.LIGHTYELLOW_EX + 'Имя пользователя MySQL.' + Style.RESET_ALL)
    parser.add_argument ('--src-password', default = '', help = Fore.LIGHTYELLOW_EX + 'Пароль пользователя MySQL' + Style.RESET_ALL)
    parser.add_argument ('--src-schema', default = 'default', help = Fore.LIGHTYELLOW_EX + 'Имя схемы MySQL (базы данных), из которой будут реплицироватсья данные' + Style.RESET_ALL)
    parser.add_argument ('--src-tables', default = '', help = Fore.LIGHTYELLOW_EX + 'Наименования реплицируемых таблиц. Перечислять через запятую (пример: table_1,table_2,table_3...)' + Style.RESET_ALL)

    # Clickhouse
    parser.add_argument ('--dst-host', default = 'localhost', help = Fore.LIGHTYELLOW_EX + 'Хост Clickhouse' + Style.RESET_ALL)
    parser.add_argument ('--dst-port', default = '8123', help = Fore.LIGHTYELLOW_EX + 'Порт, на котором доступна Clickhouse' + Style.RESET_ALL)
    parser.add_argument ('--dst-user', default = 'default', help = Fore.LIGHTYELLOW_EX + 'Имя пользователя Clickhouse.' + Style.RESET_ALL)
    parser.add_argument ('--dst-password', default = '', help = Fore.LIGHTYELLOW_EX + 'Пароль пользователя Clickhouse' + Style.RESET_ALL)
    parser.add_argument ('--dst-schema', default = 'default', help = Fore.LIGHTYELLOW_EX + 'Имя схемы MySQL (базы данных), из которой будут реплицироватсья данные' + Style.RESET_ALL)

    # Support
    parser.add_argument ('--create-sql-tables-template', default = False, action = 'store_const', const = True, help = Fore.LIGHTYELLOW_EX + 'Позволяет сгенерировать SQL-шаблон для создания таблиц в Clickhouse' + Style.RESET_ALL)
    parser.add_argument ('--copy-data', default = False, action = 'store_const', const = True, help = Fore.LIGHTYELLOW_EX + 'Запускает процесс копирования данных из таблиц MySQL в таблицы Clickhouse' + Style.RESET_ALL)
    parser.add_argument ('--truncate', default = False, action = 'store_const', const = True, help = Fore.LIGHTYELLOW_EX + 'Выполняет предварительную очистку таблиц при использовании ключа --copy-data' + Style.RESET_ALL)
    parser.add_argument ('--replicate', default = False, action = 'store_const', const = True, help = Fore.LIGHTYELLOW_EX + 'Запускает процесс репликации из MySQL в Clickhouse по таблицам, указанным в параметре --src-tables' + Style.RESET_ALL)
    parser.add_argument ('--use-https', default = False, action = 'store_const', const = True, help = Fore.LIGHTYELLOW_EX + 'Указывает на необходимость использования протокола HTTPS при выполнении запросов к Clickhouse' + Style.RESET_ALL)
    parser.add_argument ('--reset-log', default = False, action = 'store_const', const = True, help = Fore.LIGHTYELLOW_EX + 'Укажите, если требуется сбросить позицию и журнал Bin Log' + Style.RESET_ALL)
    parser.add_argument ('--version', default = False, action = 'store_const', const = True, help = Fore.LIGHTYELLOW_EX + 'Показывает версию ПО' + Style.RESET_ALL)

    return parser

def convert_type(f_type: str) -> str:
    """Функция преобразует идентификотры типов MySQL в формат Clickhouse 
    
    
    Параметры
    ---------
    f_type : str
        Конвертируемый индентификатор типа.
        Например, передано значение int(8). 
        После конвертации будет возвращено UInt32
    """

    # int
    if re.match('int\(\d{1,2}\)', f_type):
        f_type = 'UInt32'

    # datetime
    if re.match('datetime', f_type):
        f_type = 'DateTime'

    # char
    if re.match('char\(\d{1,2}\)', f_type):
        f_type = 'String'

    # text
    if re.match('text', f_type):
        f_type = 'String'

    # varchar
    if re.match('varchar\(\d{1,3}\)', f_type):
        f_type = 'String'

    # decimal
    if re.match('decimal', f_type):
        f_type = 'Float32'
    
    # double
    if re.match('double', f_type):
        f_type = 'Float32'

    # mediumtext
    if re.match('mediumtext', f_type):
        f_type = 'String'
    
    # date
    if re.match('date', f_type):
        f_type = 'DateTime'
    
    return f_type

def print_event_header(event_name: str, log_pos: int, log_file: str):
    """Функция печатает заголовок события
    
    Параметры
    ---------
    event_name : str
        Наименование события

    log_pos : int
        Позиция в журнале Bin Log

    log_file : str
        Текущий журнал Bin Log

    """
    print(Fore.LIGHTYELLOW_EX + '============================' + Style.RESET_ALL)
    print(Fore.LIGHTYELLOW_EX + 'Событие: ' + event_name + Style.RESET_ALL)
    print(Fore.LIGHTYELLOW_EX + 'Журнал: ' + log_file + Style.RESET_ALL)
    print(Fore.LIGHTYELLOW_EX + 'Позиция в журнале: ' + str(log_pos) + '\n' + Style.RESET_ALL)

def print_event_footer():
    """Функция печатает окончание события
    """
    print(Fore.LIGHTYELLOW_EX + '============================\n' + Style.RESET_ALL)

def clickhouse_query(query: str, silent: bool = False) -> int:
    """Функция выполняет SQL запрос в таблицу базы данных Clickhouse. 
    Запрос выполняется с помощью HTTP-запроса методом POST.
    По умолчанию используется протокол HTTP. 
    Для использования протокола HTTPS необходимо указать атрибут --use-https


    Возвращает код выполнения HTTP-запроса.

    Параметры
    ---------
    query : str 
        Текст запроса

    silent : bool
        Признак определяет выводить информацию в командную строку или нет. По умолчанию имеет значение False.
    

    """

    # По умолчанию выполняется HTTP-запрос
    protocol = 'http'

    # Если определен ключ --use-https, то используется HTTPS
    if namespace.use_https: 
        protocol = 'https'
        
    dst_url = '{dst_protocol}://{dst_host}:{dst_port}?user={dst_user}&password={dst_password}'.format(
        dst_protocol    = protocol,
        dst_host        = namespace.dst_host,
        dst_port        = namespace.dst_port,
        dst_user        = namespace.dst_user,
        dst_password    = namespace.dst_password
    )

    # POST-запрос на на сервер Clickhouse
    r = requests.post(url = dst_url, data = query.encode())
    
    if not silent:
        if r.status_code == 200:
            print(Fore.GREEN + '\nSTATUS 200 OK\n' + Style.RESET_ALL)
        else:
            print('\n\n' + query + '\n\n')
            print(Fore.RED + '\nSTATUS ' + str(r.status_code) + Style.RESET_ALL)
            print(Fore.RED + 'REASON:  ' + str(r.reason) + '\n\n' + Style.RESET_ALL)
    
    return r.status_code

def event_insert(event_name: str, log_pos: int, log_file: str, event_row: dict, dst_object: str):
    """Функция обрабатывает события INSERT и UPDATE из журнала MySQL Bin Log.

    Функция формирует и выполняет SQL-запрос в формате Clickhouse.

    Параметры
    ---------
    
    event_name : str 
        Наименование события. Печатается в заголовке события в командной строке.
    log_pos : int
        Позиция события в журнале. Печатается в заголовке события в командной строке.
    log_file : str
        Текущий журнал события. Печатается в заголовке события в командной строке.
    event_row : dict, 
        Словарь содержащий данные по обрабатываемому событию
    dst_object : str
        Объект Clickhouse, в который будут вставляться данные (в формате <schema>.<table>)

    """

    # Событие UPDATE. Заменяется выражением INSERT при использовании движка ReplacingMergeTree()
    print_event_header(event_name, log_pos, log_file)
    
    # Если вставка данных, то берем значения из "values",
    # если обновление, то из "after_values"
    if event_name == 'INSERT':
        record = dict(event_row["values"].items())
    else:
        record = dict(event_row["after_values"].items())

    # Формируем тело запроса на вставку данных
    q_values = []

    # Преобразование значений
    for key in record.values():                                                
        # Если значение число, то добавлять без кавычек
        if isinstance(key, (int, float, decimal.Decimal)):
            q_values.append(str(key))
        else:
            q_values.append("'" + str(key) + "'")
    
    # Итоговый запрос
    query = 'INSERT INTO ' + dst_object + ' (' + ', '.join(('`' + str(key) + '`') for key in record.keys()) + ') VALUES (' + ', '.join(q_values).replace("'None'", 'NULL') + ')'
    
    print(query)
    print_event_footer()

    # Попытка выполнения запроса
    try:
        # Выполняем запрос
        clickhouse_query(query)

    except Exception as e:
        print(Fore.RED + 'Exception occured:{}'.format(e) + Style.RESET_ALL)
        sys.exit(1)

def event_delete(log_pos: int, log_file: str, event_row: dict, src_object: str, dst_object: str):
    """Функция обрабатывает события DELETE из журнала MySQL Bin Log.

    Функция формирует и выполняет SQL-запрос в формате Clickhouse.

    Параметры
    ---------
    
    log_pos : int
        Позиция события в журнале. Печатается в заголовке события в командной строке.
    log_file : str
        Текущий журнал события. Печатается в заголовке события в командной строке.
    event_row : dict, 
        Словарь содержащий данные по обрабатываемому событию
    src_object : str
        Объект MySQL, из которого пришла информация об удалении. Из этой таблицы извлекается информация о PIMARY KEY.
    dst_object : str
        Объект Clickhouse, в который будут вставляться данные (в формате <schema>.<table>)

    """
    # Событие DELETE. Заменяется выражением ALTER TABLE при использовании движка ReplacingMergeTree()
    print_event_header('DELETE', log_pos, log_file)
    
    # Подключаемся к MySQL
    mysql_con = mysql_connection()

    # Получить структуру PRIMARY KEY
    cursorObject = mysql_con.cursor()                                  
    cursorObject.execute("SHOW INDEX FROM {src_table} WHERE Key_name = 'PRIMARY'".format(src_table = src_object))

    # Получаем метаданные
    i_data = cursorObject.fetchone()

    # Обрабатываем каждое поле
    primary_key = i_data['Column_name']

    # Получить значение первичного ключа
    record = dict(event_row["values"].items())
    primary_key_value = record[primary_key]

    # TODO надо будет доработать проверку типа данных первичного ключа.
    # Сейчас подразумевается, что ключ имеет тип Integer 
    query = "ALTER TABLE {src_table} DELETE WHERE {pri_key}={pri_key_value}".format(
        src_table = dst_object, 
        pri_key = primary_key, 
        pri_key_value = primary_key_value
    )

    print(query)        
    print_event_footer()

    # Попытка выполнения запроса
    try:
        # Выполняем запрос
        clickhouse_query(query)

    except Exception as e:
        print(Fore.RED + 'Exception occured:{}'.format(e) + Style.RESET_ALL)
        sys.exit(1)

def replicate(namespace):
    """Функция запускает процесс репликации.
    Программа способна запоминать позицию последнего обработанного события и наименование журнала Bin Log.
    При перезапуске программа начинает читать события с той позиции, с которой остановилась после прошлого запуска.

    Если требуется запустить репликацию с самого первого доступного события, необходимо дополнительно указать атрибут --reset-log

    """

    # Считываем журнал репликации
    bin_log_cfg_name = '/opt/clickhouse-replica/' + namespace.src_host + '_bin_log.cfg'
    bin_log_cfg = {}

    # Если файл существует и не требуется сбросить журнал,
    # то считать конфигурацию
    if os.path.exists(bin_log_cfg_name) and not namespace.reset_log:
        with open(bin_log_cfg_name, 'rb') as f:
            bin_log_cfg = pickle.load(f)

    if not bin_log_cfg:
        log_pos         = None
        log_file        = None
        resume_stream   = False
    else:
        log_pos         = bin_log_cfg["log_pos"]
        log_file        = bin_log_cfg["log_file"]
        resume_stream   = True

    print(Fore.LIGHTBLUE_EX + 'Clickhouse Replica. Репликация данных.' + Style.RESET_ALL)

    # Стрим для чтения данных из MySQL Binary Log
    stream = BinLogStreamReader(
        connection_settings = {
            'host': namespace.src_host,
            'port': namespace.src_port,
            'user': namespace.src_user,
            'passwd': namespace.src_password
            },
        server_id           = 1,
        log_file            = log_file,
        log_pos             = log_pos,

        blocking            = True,
        only_events         = [
            DeleteRowsEvent,
            WriteRowsEvent,
            UpdateRowsEvent
        ],
        resume_stream       = resume_stream
    )

    for binlogevent in stream:
        # Получим текущую позицию и журнал Bin log
        log_pos  = binlogevent.packet.log_pos
        log_file = stream.log_file

        for row in binlogevent.rows:
            # Выполним обработку событий втом случае,
            # если схема и таблица
            # соответсвует кортежу src_tables
            src_tables = namespace.src_tables.split(',')
            src_object = namespace.src_schema + '.' + binlogevent.table
            dst_object = namespace.dst_schema + '.' + binlogevent.table


            if(binlogevent.schema == namespace.src_schema and binlogevent.table in src_tables):
                if isinstance(binlogevent, DeleteRowsEvent):
                    event_delete(
                        log_pos     = log_pos,
                        log_file    = log_file,
                        event_row   = row,
                        src_object  = src_object,
                        dst_object  = dst_object
                        )
                elif isinstance(binlogevent, UpdateRowsEvent):
                    event_insert(
                        event_name  = 'UPDATE',
                        log_pos     = log_pos,
                        log_file    = log_file,
                        event_row   = row,
                        dst_object  = dst_object
                        )

                elif isinstance(binlogevent, WriteRowsEvent):
                    event_insert(
                        event_name  = 'INSERT',
                        log_pos     = log_pos,
                        log_file    = log_file,
                        event_row   = row,
                        dst_object  = dst_object
                        )
        # Записать позицию и журнал в файл
        bin_log_cfg['log_pos']  = log_pos
        bin_log_cfg['log_file'] = log_file

        with open(bin_log_cfg_name, 'wb') as f:
            pickle.dump(bin_log_cfg, f)

    stream.close()
            
 
def create_sql_template(namespace):
    """Функция создает SQL-шаблон в формате Clickhouse для создания таблиц, указанных в параметре --src-tables"""
    # создаем подключение к MySQL
    mysql_con = mysql_connection()
    
    try:
        # Добавить создание базы данных
        f_meta_data = 'CREATE DATABASE IF NOT EXISTS ' + namespace.dst_schema + ';\n\n'

        for src_table in namespace.src_tables.split(sep = ','):
                
            # Пытаемся получить курсор
            cursorObject = mysql_con.cursor()                                  

            # Выполняем инструкцию DESCRIBE
            cursorObject.execute("DESCRIBE {schema}.{table}".format(schema = namespace.src_schema, table = src_table))

            # Получаем метаданные
            indexList = cursorObject.fetchall()

            f_meta_data += 'CREATE TABLE IF NOT EXISTS `{schema}`.`{table}`  ('.format(schema = namespace.dst_schema, table = src_table)
            f_fields    = ''
            f_pri_key   = ''

            # Обрабатываем каждое поле
            for f_data in indexList:
                f_type  = f_data['Type']
                f_null  = f_data['Null']
                f_key   = f_data['Key']
                f_null  = f_data['Null']
                

                # Выполнить замену типов
                f_type = convert_type(f_type)

                if (f_null == 'YES' and f_key not in ('PRI', 'MUL')):
                    f_part = 'Nullable({f_type})'.format(f_type = f_type)
                else:
                    f_part = f_type

                if f_key == 'PRI':
                    f_pri_key = f_data['Field']

                if f_fields == '':
                    f_fields = '\n\t`' + f_data['Field'] + '` ' + f_part
                else:
                    f_fields += ',\n\t`' + f_data['Field'] + '` ' + f_part
                
            f_meta_data += f_fields
            f_meta_data += '\n)\nENGINE = ReplacingMergeTree\nORDER BY `' + f_pri_key + '`;\n\n'

        print(f_meta_data)

    except Exception as e:
        print("Exception occured:{}".format(e))

    finally:
        mysql_con.close()

def truncate(namespace):
    """Функция выполняет очистку таблиц, указанных в параметре --src-tables"""
    # Выполняем очистку таблиц
    for src_table in namespace.src_tables.split(','):

        print(Fore.LIGHTYELLOW_EX + 'Очистка таблицы: {schema}.{table}'.format(schema = namespace.dst_schema, table = src_table) + Style.RESET_ALL)
        clickhouse_query('TRUNCATE TABLE {schema}.{table}'.format(schema = namespace.dst_schema, table = src_table))


def copy_data(namespace):
    """Функция выполняет копирование данных из таблиц, указанных --src-tables.
    
    Если программа запущена с атрибутом --truncate, то таблицы в Clickhouse будут предварительно очищены"""

    # Запускаем процесс копирования данных из таблиц MySQL в таблицы Clickhouse
    print(Fore.LIGHTBLUE_EX + 'Clickhouse Replica. Копирование данных.' + Style.RESET_ALL)

    # Подключение к MySQL
    mysql_con = mysql_connection()

    # Обработать все таблицы
    for src_table in namespace.src_tables.split(','):
        print(Fore.LIGHTYELLOW_EX + 'Таблица: ' + src_table + Style.RESET_ALL)
        # Получить количество записей в таблице
        query = 'SELECT COUNT(1) AS rec_cnt FROM {schema}.{table}'.format(schema = namespace.src_schema, table = src_table)

        cursor = mysql_con.cursor()
        cursor.execute(query)
        data = cursor.fetchone()
        record_count = data['rec_cnt']
        
        print(Fore.LIGHTYELLOW_EX + 'Записей: ' + str(record_count) + Style.RESET_ALL)

        # Выбираем записи пакетно, до тех пор, пока не будут выбраны все записи
        offset          = 0
        current_record  = 0
        all_record      = 0
        batch_query     = ''

        package_cursor = mysql_con.cursor()

        exit_flag = False

        while not exit_flag:
            query = 'SELECT * FROM {schema}.{table} LIMIT {offset}, {rowcount}'.format(
                schema = namespace.src_schema, 
                table = src_table,
                offset = offset,
                rowcount = src_package_size
            )

            # Обработать записи            
            package_cursor.execute(query)
            records = package_cursor.fetchall()

            for record_data in records:
                current_record  += 1
                all_record      += 1
                # Формируем тело запроса на вставку данных
                q_values = []

                
                # Преобразование значений
                for key in record_data.values():                                                
                    # Если значение число, то добавлять без кавычек
                    if isinstance(key, (int, float, decimal.Decimal)):
                        q_values.append(str(key))
                    else:
                        key =  str(key).replace("\\", "\\\\").replace("'", "\\'")

                        q_values.append("'" + key + "'")

                if current_record == 1:
                    batch_query = 'INSERT INTO {schema}.{table} ('.format(schema = namespace.dst_schema, table = src_table) + ', '.join(('`' + str(key) + '`') for key in record_data.keys()) + ') VALUES (' + ', '.join(q_values).replace("'None'", 'NULL') + ')'
                else:
                    batch_query += ',\n(' + ', '.join(q_values).replace("'None'", 'NULL') + ')'

                if (current_record == dst_package_size) or ((offset + src_package_size) >= record_count):
                    # print(batch_query)
                    # Попытка выполнения запроса
                    try:
                        # Выполняем запрос
                        clickhouse_query(query = batch_query, silent = False)
                        batch_query = ''
                        current_record = 0

                    except Exception as e:
                        print(Fore.RED + 'Exception occured:{}'.format(e) + Style.RESET_ALL)
                        sys.exit(1)

            print('{table}: {current_record}/{record_count}'.format(
                table           = src_table,
                current_record  = all_record,
                record_count    = record_count
                ), end='\r')

            offset += src_package_size    

            if offset >= record_count:
                exit_flag = True

        package_cursor.close()
        cursor.close()

def make_pepe():
    """Функция рисует Пепе и версию программы"""
    print(Fore.GREEN + '                       ...                        ' + Style.RESET_ALL)
    print(Fore.GREEN + '               :7Y5555YY?!^  .^!?JJJ?7^           ' + Style.RESET_ALL)
    print(Fore.GREEN + '             ~5P5YYYY555555YP555YYYYY555:         ' + Style.RESET_ALL)
    print(Fore.GREEN + '           .YP5Y5555555555555G5555555555P.        ' + Style.RESET_ALL)
    print(Fore.GREEN + '          ^' + Fore.LIGHTBLACK_EX + 'PP' + Fore.GREEN + 'YYY55YYYYYYY555PGG55Y55555555?!:               ' + Fore.LIGHTYELLOW_EX + '=============================================' + Style.RESET_ALL)
    print(Fore.GREEN + '       :?5##' + Fore.LIGHTBLACK_EX + 'BP' + Fore.GREEN + 'YYYYYY' + Fore.WHITE + '5PPPPP5555' + Fore.GREEN + 'PPP5P5555' + Fore.WHITE + 'PPPPPGJ' + Fore.GREEN + '^             ' + Fore.LIGHTYELLOW_EX + '#         CLICKHOUSE REPLICA v' + version + '           #' + Style.RESET_ALL)
    print(Fore.GREEN + '      :#BBPPG' + Fore.LIGHTBLACK_EX + '##B' + Fore.WHITE + 'PPPGG5' + Fore.BLACK + '?55G' + Fore.WHITE + '#57JP' + Fore.GREEN + '##&###' + Fore.WHITE + 'GGB&' + Fore.BLACK + '#PG#' + Fore.WHITE + '&&&' + Fore.GREEN + 'B           ' + Fore.LIGHTYELLOW_EX + '#         by A.Kalashnikov                  #' + Style.RESET_ALL)
    print(Fore.GREEN + '     ~P55G5YYY5G' + Fore.LIGHTBLACK_EX + '#&&&&&&&&&@@@@@@@G.G&&&&&@@@@@@@#           ' + Fore.LIGHTYELLOW_EX + '=============================================' + Style.RESET_ALL)
    print(Fore.GREEN + '    !GP555YYYYYYY' + Fore.LIGHTBLACK_EX + 'P&@@@@@@@@@@@@@&J:!#&@@@@@@@@&5. ' + Style.RESET_ALL)
    print(Fore.GREEN + '   .G5555YYYYYYYYY' + Fore.LIGHTBLACK_EX + 'PB&&&&&&&&&&#' + Fore.GREEN + 'G555YY5G' + Fore.LIGHTBLACK_EX + 'BB###G~.   ' + Style.RESET_ALL)
    print(Fore.GREEN + '   7G5555YYYYYYYYYYYY555PPPPP5YYYY5PPP555P5:      ' + Style.RESET_ALL)
    print(Fore.GREEN + '   YP555YYYYYYYY555YYYYY55YYYYYYYYYYY5YYYYYY^     ' + Style.RESET_ALL)
    print(Fore.GREEN + '  :BG555YYYYYYY' + Fore.RED + 'PGGGGP' + Fore.GREEN + '55YYYYYYYYYYYYYYYYYYYYYP:    ' + Style.RESET_ALL)
    print(Fore.GREEN + '  P@&#G5YYYYYYY' + Fore.RED + 'GGGBGGGGPPP' + Fore.GREEN + '5555555YYYYYY5555' + Fore.RED + 'PGP.   ' + Style.RESET_ALL)
    print(Fore.GREEN + '  ' + Fore.LIGHTCYAN_EX + 'B&&&&&' + Fore.GREEN + 'BP5YYYYY' + Fore.RED + 'PGGGGGGGGGGGGGGGGGGGGGGGGGGGP7    ' + Style.RESET_ALL)
    print(Fore.GREEN + ' ' + Fore.LIGHTCYAN_EX + '^&&&&&&&&&' + Fore.GREEN + '#BGP5YY' + Fore.RED + '55PPPGGGGGGGGGGGGGGGGGGBBB^     ' + Style.RESET_ALL)
    print(Fore.GREEN + ' ' + Fore.LIGHTCYAN_EX + 'Y@&&&&&&&&&&&&&&' + Fore.GREEN + '#BBGP55555555' + Fore.RED + 'PPPPPPPPPGP!^.      ' + Style.RESET_ALL)
    print(Fore.GREEN + ' ' + Fore.LIGHTCYAN_EX + 'G&&&&&&&&&&&&&&&&&&&&&&&' + Fore.GREEN + '#BYYYYYYYY' + Fore.LIGHTCYAN_EX + '5PB#&&GY!:.    ' + Style.RESET_ALL)
    print(Fore.GREEN + ' ' + Fore.LIGHTCYAN_EX + 'Y@&&&&&&&&&&&&&&&&&&&&&&&' + Fore.GREEN + 'GPPPPPB#' + Fore.LIGHTCYAN_EX + '&&@@@&@@@@&G^   ' + Style.RESET_ALL)
    print(Fore.GREEN + '  ' + Fore.LIGHTCYAN_EX + 'P&@&&&&&&&&&&&&&&&&&&&&' + Fore.GREEN + 'BGGGGB#' + Fore.LIGHTCYAN_EX + '&&&&&&&&&@@5.     ' + Style.RESET_ALL)
    print(Fore.GREEN + '   ' + Fore.LIGHTCYAN_EX + ':?G&&@@&&&&&&&&&&&&' + Fore.LIGHTBLUE_EX + '&&&&&&&&&&' + Fore.LIGHTCYAN_EX + '&&&&&&@@@&!       ' + Style.RESET_ALL)
    print(Fore.GREEN + '      ' + Fore.LIGHTCYAN_EX + '.^?P#&&@@@&&&&' + Fore.LIGHTBLUE_EX + '&&&&&&&&&&&&&' + Fore.LIGHTCYAN_EX + '##GY!~!~.        ' + Style.RESET_ALL)
    print(Fore.GREEN + '           ' + Fore.LIGHTCYAN_EX + '.^7JPGB' + Fore.LIGHTBLUE_EX + '##&&G7777!!~^::.                ' + Style.RESET_ALL)
    print(Fore.GREEN + '                   ' + Fore.LIGHTBLUE_EX + '...                            ' + Style.RESET_ALL)

def print_version():
    """Функция печатает версию программы и СУБД"""

    # Версия Clickhouse Replica
    print(Fore.WHITE + 'Clickhouse Replica: ' + Fore.LIGHTYELLOW_EX + version + Style.RESET_ALL)

    m_con = mysql_connection()
    m_cursor = m_con.cursor()
    m_cursor.execute("SELECT VERSION() AS m_version")
    m_version = m_cursor.fetchone()["m_version"]
    
    print(Fore.WHITE + 'MySQL: ' + Fore.LIGHTYELLOW_EX + m_version + Style.RESET_ALL)

    # Получим версию Clickhouse
    protocol = 'http'

    # Если определен ключ --use-https, то используется HTTPS
    if namespace.use_https: 
        protocol = 'https'
        
    dst_url = '{dst_protocol}://{dst_host}:{dst_port}?user={dst_user}&password={dst_password}&query=SELECT VERSION()'.format(
        dst_protocol    = protocol,
        dst_host        = namespace.dst_host,
        dst_port        = namespace.dst_port,
        dst_user        = namespace.dst_user,
        dst_password    = namespace.dst_password
    )

    # GET-запрос на на сервер Clickhouse
    r = requests.get(url = dst_url)
    print(Fore.WHITE + 'Clickhouse: ' + Fore.LIGHTYELLOW_EX + r.text + Style.RESET_ALL)

    return r.status_code


if __name__ == "__main__":
    # Создать парсер аргументов командной строки
    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])
    
    if namespace.version:
        # Отобразить версию ПО
        print_version()

    if namespace.create_sql_tables_template:
        # Сгенерировать SQL-шаблон 
        create_sql_template(namespace)

    if namespace.copy_data:
        # Скопировать данные
        make_pepe()

        # Если указали флаг --truncate, то выполнить очистку таблиц
        if namespace.truncate:
            truncate(namespace)

        copy_data(namespace)

    if namespace.replicate:
        # Запуск репликации 
        replicate(namespace)
