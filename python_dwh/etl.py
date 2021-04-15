import os
import gzip
import json
import re
import requests
import dateparser
from datetime import datetime, timedelta
from sqlalchemy import create_engine, insert
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, BigInteger, String, JSON, TIMESTAMP
from requests.exceptions import HTTPError
from json import JSONDecodeError

# произвольная папка для загрузки файлов
DOWNLOADS = 'C:\\Users\\user47.CORP\\Downloads'

# подключаемся к postgres (запущена на удаленном сервере в docker-контейнере)
USERNAME = 'postgres'
PASSWORD = '12345678'
DB_NAME = 'postgres'
DB_HOST = '10.0.0.71'
DB_PORT = '5532'
PG_ENGINE = create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# определяем метаданные таблиц для работы с конструктором запросов
metadata = MetaData()
report_input = Table('report_input', 
    metadata,
    Column('user_id', BigInteger),
    Column('ts', TIMESTAMP),
    Column('context', JSON),
    Column('ip', String)
)

data_error = Table('data_error', 
    metadata,
    Column('api_report', String),
    Column('api_date', TIMESTAMP),
    Column('row_text', String),
    Column('error_text', String),
    Column('ins_ts', TIMESTAMP)
)

# функция для загрузки файла с удаленного сервера
def download_file(url):
    filename = url.split('/')[-1]
    filename = os.path.join(DOWNLOADS, filename)
    # поскольку файл может быть большим по размеру, загружаем его частями
    try:
        with requests.get(url, stream=True) as r: 
            r.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):  
                    f.write(chunk)
        return filename
    except HTTPError as e:
        print(str(e))
        return ''

# функция для разархивации и чтения JSON
def read_json_gz(json_gz):
    try:
        with gzip.open(json_gz, 'r') as f:
            json_bytes = f.read()                      
        
        json_str = json_bytes.decode('utf-8')
        users_data = json.loads(json_str)
    except Exception as e:
        print(f'Could not read or decode file. Error: {str(e)}')
        return

    return users_data

# функция для валидации пользовательских данных
def validate_user_data(ud, rdate):
    res_ud = ud.copy()
    # проверяем user_id на int
    try:
        int(ud['user_id'])
    # ловим TypeError или ValueError
    except Exception:
        raise ValueError(f'user_id field value is invalid: {ud["user_id"]}')
    # проверяем значение ts на datetime или timestamp
    date = dateparser.parse(res_ud['ts'])
    # если дата не определена или не равна дате отчёта, выбрасываем исключение
    if date is None or date.date() != rdate:
        raise ValueError(f'ts field value is invalid: {ud["ts"]}')
    else:
        res_ud['ts'] = date
    # проверяем поле context на валидный JSON
    try:
        ctx = res_ud['context']
        # если context пуст, 
        # не является списком со словарями (вложенные словари для удобства тестирования могут быть пустыми) 
        # или не пустым словарем, то выбрасываем исключение
        # проверку можно проводить более детально, если знать больше про возможную структуру поля context
        if ctx:
            if isinstance(ctx, list):
                for obj in ctx:
                    if not isinstance(obj, dict):
                        raise ValueError(f'context field value is invalid: {res_ud["context"]}')
            elif not isinstance(ctx, dict):
                raise ValueError(f'context field value is invalid: {res_ud["context"]}')
        else:
             raise ValueError(f'context field value is invalid: {res_ud["context"]}')                                 
    except (JSONDecodeError, TypeError):
        raise ValueError(f'context field value is invalid: {res_ud["context"]}')
    # с помощью регулярного выражения проверяем значение поля ip на соответствие стандарту ipv4
    try:
        check_ip = re.match(r'^([0-9]{1,3}\.){3}[0-9]{1,3}$', res_ud['ip'])
        if not check_ip:
            raise ValueError(f'ip field value is invalid: {res_ud["ip"]}')
    except TypeError:
        raise ValueError(f'ip field value is invalid: {res_ud["ip"]}')
                        
    return res_ud

def load_to_db(users_data, report, date):
    # если чтение успешно, перебираем в цикле структуры пользовательских данных
    if users_data:
        # начинаем транзакцию: если данные не будут записаны в одну из таблиц, все изменения
        # связанные с отчётом, отменятся
        with PG_ENGINE.begin() as conn:
            for row_n, ud in enumerate(users_data):
                try:
                    # валидируем данные
                    valid_ud = validate_user_data(ud, rdate=date)
                    # формируем и выполняем запрос на вставку данных в таблицу report_input
                    stmt = (
                        insert(report_input).values(valid_ud)
                    )
                    conn.execute(stmt)
                except ValueError as e:
                    try:
                        # если данные не валидны, формируем и выполняем запрос на вставку данных в таблицу data_error
                        stmt = (
                            insert(data_error).values(
                                api_report=report, 
                                api_date=date,
                                row_text=row_n,
                                error_text=str(e),
                                ins_ts=datetime.now()
                            )
                        )
                        conn.execute(stmt)
                    except Exception as e:
                        print(f'Unable to complete the processing of the report due to an error: {str(e)}')
                        return
        print(f'{report}-{date} report uploaded successfully')
    else: 
        print(f'{report}-{date} report is not found, empty or incorrect')

if __name__ == "__main__":
    base_date = datetime.today().date()
    days_ago = 5
    date_list = [base_date - timedelta(days=x) for x in range(days_ago)]
    report = 'input'
    for date in sorted(date_list):
        # скачиваем файл со стороннего сервиса
        # localhost:5000 - для тестирования, вместо него можно указать snap.datastream.center
        json_gz = download_file(f'http://localhost:5000/techquest/{report}-{date}.json.gz')
        # читаем архив
        users_data = read_json_gz(json_gz)
        # валидируем и загружаем в базу данных
        load_to_db(users_data, report, date)