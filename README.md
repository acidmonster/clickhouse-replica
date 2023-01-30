# clickhouse-replica
Программа позволяет настроить обмен данными между СУБД MySQL и Clickhouse.

# Возможности
 * Формирование SQL-шаблонов для создания таблиц в Clickhouse
 * Первоначальное копирование данных из MySQL в Clikhouse. Поддерживается предварительная очистка таблиц с помощью оператора TRUNCATE.
 * Репликация данных по журналу Bin Log MySQL. Поддерживаются события INSERT, UPDATE, DELETE. Программа может "запоминать" последнее обработанное событие и при следующем запуске продолжит со следущего после этого события.

# Ограничения
 * Протестировано на версиях MySQL 5.7.32-35 и Clickhouse 22.12.3.5

# Установка
* Скачайте последню версию по [ссылке](https://github.com/acidmonster/clickhouse-replica/releases). Распакуйте сценарий в удобное место.

# Использование
```
NAME: 
  clickhouse-replica

USAGE: 
  clickhouse-replica.py [-h] [--src-host SRC_HOST] [--src-port SRC_PORT] [--src-user SRC_USER] [--src-password SRC_PASSWORD] [--src-schema SRC_SCHEMA] [--src-tables SRC_TABLES] [--dst-host DST_HOST] [--dst-port DST_PORT] [--dst-user DST_USER] [--dst-password DST_PASSWORD] [--dst-schema DST_SCHEMA] [--create-sql-tables-template] [--copy-data] [--truncate] [--replicate] [--use-https] [--reset-log]

options:
  -h, --help            show this help message and exit
  --src-host SRC_HOST   Хост MySQL
  --src-port SRC_PORT   Порт, на котором доступна MySQL
  --src-user SRC_USER   Имя пользователя MySQL
  --src-password SRC_PASSWORD
                        Пароль пользователя MySQL
  --src-schema SRC_SCHEMA
                        Имя схемы MySQL (базы данных), из которой будут реплицироватсья данные
  --src-tables SRC_TABLES
                        Наименования реплицируемых таблиц. Перечислять через запятую (пример: table_1,table_2,table_3...)
  --dst-host DST_HOST   Хост Clickhouse
  --dst-port DST_PORT   Порт, на котором доступна Clickhouse
  --dst-user DST_USER   Имя пользователя Clickhouse
  --dst-password DST_PASSWORD
                        Пароль пользователя Clickhouse
  --dst-schema DST_SCHEMA
                        Имя схемы MySQL (базы данных), из которой будут реплицироватсья данные
  --create-sql-tables-template
                        Позволяет сгенерировать SQL-шаблон для создания таблиц в Clickhouse
  --copy-data           Запускает процесс копирования данных из таблиц MySQL в таблицы Clickhouse
  --truncate            Выполняет предварительную очистку таблиц при использовании ключа --copy-data
  --replicate           Запускает процесс репликации из MySQL в Clickhouse по таблицам, указанным в параметре --src-tables
  --use-https           Указывает на необходимость использования протокола HTTPS при выполнении запросов к Clickhouse
  --reset-log           Укажите, если требуется сбросить позицию и журнал Bin Log
```
# Примеры использования
## Создание таблицы
```
  python .\clickhouse-replica.py \
    --src-host=localhost \
    --src-port=3306 \
    --src-user=mysql_user \
    --src-password=pass \
    --src-schema=epica \
    --src-tables="leads" \
    --dst-host=localhost \
    --dst-schema=iconica
    --dst-user=default \
    --dst-password=pass2 \
    --create-sql-tables-template

Пример сформированного шаблона:

CREATE DATABASE IF NOT EXISTS iconica;

CREATE TABLE IF NOT EXISTS `iconica`.`leads`  (
        `ID` UInt32,
        `DATE_CREATE` DateTime,
        `DATE_MODIFY` DateTime,
        `CREATED_BY_ID` UInt32,
        `MODIFY_BY_ID` Nullable(UInt32),
        `ASSIGNED_BY_ID` UInt32,
        `OPENED` Nullable(String),
        `COMPANY_ID` UInt32,
        `CONTACT_ID` UInt32,
        `STATUS_ID` String,
        `STATUS_DESCRIPTION` Nullable(String),
        `STATUS_SEMANTIC_ID` String,
        `PRODUCT_ID` Nullable(String),
        `OPPORTUNITY` Nullable(Float32),
        `CURRENCY_ID` Nullable(String),
        `OPPORTUNITY_ACCOUNT` Nullable(Float32),
        `ACCOUNT_CURRENCY_ID` Nullable(String),
        `SOURCE_ID` Nullable(String),
        `SOURCE_DESCRIPTION` Nullable(String),
        `TITLE` Nullable(String),
        `FULL_NAME` String,
        `NAME` Nullable(String),
        `LAST_NAME` Nullable(String),
        `SECOND_NAME` Nullable(String),
        `COMPANY_TITLE` Nullable(String),
        `POST` Nullable(String),
        `ADDRESS` Nullable(String),
        `COMMENTS` Nullable(String),
        `EXCH_RATE` Nullable(Float32),
        `WEBFORM_ID` UInt32,
        `ORIGINATOR_ID` Nullable(String),
        `ORIGIN_ID` Nullable(String),
        `DATE_CLOSED` DateTime,
        `BIRTHDATE` DateTime,
        `BIRTHDAY_SORT` UInt32,
        `HONORIFIC` Nullable(String),
        `HAS_PHONE` String,
        `HAS_EMAIL` String,
        `FACE_ID` UInt32,
        `SEARCH_CONTENT` String,
        `IS_RETURN_CUSTOMER` String,
        `HAS_IMOL` String,
        `IS_MANUAL_OPPORTUNITY` Nullable(String),
        `MOVED_TIME` Nullable(DateTime),
        `MOVED_BY_ID` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY `ID`;
```
## Копирование данных
```
python .\clickhouse-replica.py \
    --src-host=localhost \
    --src-port=3306 \
    --src-user=mysql_user \
    --src-password=pass \
    --src-schema=epica \
    --src-tables="leads" \
    --dst-host=localhost \
    --dst-schema=iconica
    --dst-user=default \
    --dst-password=pass2 \ 
    --copy-data \
    --truncate
```

## Репликация
```
python .\clickhouse-replica.py \
    --src-host=localhost \
    --src-port=3306 \
    --src-user=mysql_user \
    --src-password=pass \
    --src-schema=epica \
    --src-tables="leads" \
    --dst-host=localhost \
    --dst-schema=iconica
    --dst-user=default \
    --dst-password=pass2 \ 
    --replicate
```

# Дополнительная информация
## Движок таблиц Clickhouse
* Программа работает с движком таблиц **ReplacingMergeTree**. Этот движок позволяет выполнять операцию UPDATE простой вставкой данных с помощью оператора INSERT. Дубликат записи, созданный ранее будет автоматически удален со временем фоновым процессом Clickhouse. Подробнее [здесь](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree).
* Удаление записей осуществляется посредством мутаций. Движок **ReplacingMergeTree** не позволяет напрямую удалять данные с помощью оператора DELETE. Поэтмоу используется оператор ALTER TABLE ... DELETE WHERE ... Подробнее [здесь](https://clickhouse.com/docs/en/sql-reference/statements/alter/delete)
* Полем сортировки при генерации шаблона таблицы выбирается поле, отмеченное как PRIMARY KEY.
