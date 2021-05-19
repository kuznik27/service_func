import psycopg2
import pandas as pd
from datetime import datetime


def duration_analys(defect, tstart_pnd2, tend_pnd2, host = 10.202.248.96):
    """
    Функция позволяет:
    — получать информацию об уровне дефекта, его длительности и скорости изменения мощности в это время за заданный промежуток времени,
    — получать статистику по продолжительности дефетов за заданный промежуток времени с разбивкой по уровням развития дефекта.
    """

    
    # получение данных о названиях дефектов
    conn_params = "dbname=block002catalog user=user password=qwerty host = "+str(host)
    sensor = "KKS_02SP_10_E901__XQ02"
    sql = "SELECT def_id, def_name FROM defect;"
    def_names = {}

    with psycopg2.connect(conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for i in cur.fetchall():
                def_names[i[0]] = i[1]
    
    # получение данных датчика мощности за период tstart_pnd2 - tend_pnd2
    conn_params = "dbname=asutp user=user password=qwerty host= "+str(host)
    sql = "SELECT kks_timestamp, kks_value FROM {} WHERE kks_timestamp >= {}-5000 AND kks_timestamp <= {}+5000 ORDER BY kks_timestamp ;"

    with psycopg2.connect(conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.format(sensor, tstart_pnd2, tend_pnd2))
            sql_resp = cur.fetchall()
            timestamps = [i[0] for i in sql_resp]
            values = [i[1] for i in sql_resp]

    # Получение данных о начале дефекта и его продолжительности
    sql =  'SELECT value, timestamp, difference AS Продолжительность_дефекта\
        from\
        (SELECT  value, timestamp, -1*(timestamp-lead(timestamp)\
        OVER (ORDER BY timestamp)) AS difference\
        FROM def_{0}\
        WHERE value!= -1  AND timestamp >= {1} AND timestamp <= {2}) AS tab;'

    conn_params = "dbname=state user=user password=qwerty host= "+str(host)
    with psycopg2.connect(conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.format(defect, tstart_pnd2, tend_pnd2))
            results = cur.fetchall()

    # Определение скорости изменения мощности на интервалах времени, когда менялись дефекты
    print(def_names[defect])
    print('Интервал:',datetime.fromtimestamp(tstart_pnd2/1000).strftime('%Y-%m-%d %H:%M:%S'),'—', datetime.fromtimestamp(tend_pnd2/1000).strftime('%Y-%m-%d %H:%M:%S'))
    for i in range(len(results)-1):
        sensor_start = next(x for x, timestamp in enumerate(timestamps) if timestamp > results[i][1])
        sensor_end = next(x for x, timestamp in enumerate(timestamps) if timestamp > results[i+1][1])
        VN = (values[sensor_end] - values[sensor_start-1]) / (timestamps[sensor_end] - timestamps[sensor_start-1])
        print('Дефект:', results[i][0],'Начало:', datetime.fromtimestamp(results[i][1]/1000).strftime('%Y-%m-%d %H:%M:%S'),\
        'Продолжительность:', results[i][2]/1000, ',сек', 'Скорость изменения мощности ', abs(VN * 1000 * 60))

    # статистика продолжительности дефектов за период tstart_pnd2 - tend_pnd2
    thres = [0, 60000, 180000, 600000, 1800000, 3600000, "FLOAT8 '+infinity'"]
    levels = ['0 OR value = 1 OR value = 2 OR value = 3 OR value =4',0, 1, 2, 3, 4]
    columns_names = ['<1_мин', '1-3_мин', '3-10_мин', '10-30_мин', '30-60_мин', '>60_мин']

    sql =  'SELECT COUNT(difference) AS number\
        from\
        (SELECT  value, timestamp, -1*(timestamp-lead(timestamp)\
        OVER (ORDER BY timestamp)) AS difference\
        FROM def_{1}\
        WHERE value!= -1  AND timestamp >= {4} AND timestamp <= {5}) AS tab\
        WHERE difference > {2} AND difference < {0} AND (value = {3});'

    conn_params = "dbname=state user=user password=qwerty host= "+str(host)
    with psycopg2.connect(conn_params) as conn:
        with conn.cursor() as cur:
            res = pd.DataFrame(columns = columns_names)
            df = pd.DataFrame(columns = columns_names )
            for level in levels:
                results = []
                for i in range(len(thres)-1):
                    cur.execute(sql.format(thres[i+1],defect,thres[i], level, tstart_pnd2, tend_pnd2))
                    results.append(cur.fetchone()[0])
                df.loc[len(df)] = results
            df.index = [def_names[defect] + ' ' + str(level) for level in levels]
            res = df if len(res)==0 else res.append(df)
    print(res,'\n')
