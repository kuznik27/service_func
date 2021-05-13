import psycopg2
from itertools import groupby

def get_all_def_stats(tstart = 1483056000000, tend = 1617370357000, host = 96):
    
    """
    Сбор статистики по количеству срабатываний по каждому дефекту
    """

    # получение названий дефектов из бд по def_id
    conn_params = "dbname = block002catalog user = vti password = vti host = 10.202.248." + str(host)
    all_def_names = {}
    counter_all = {}
    sql = "SELECT def_id, def_name FROM defect;"
    with psycopg2.connect(conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for i in cur.fetchall():
                all_def_names[i[0]] = i[1]
    
    conn_params = "dbname = state user = vti password = vti host = 10.202.248." + str(host)    
    with psycopg2.connect(conn_params) as conn:
        query = "select value from def_{2} where value!= -1 AND timestamp > {0} and timestamp  < {1} order by timestamp desc"
        for table_name in all_def_names.keys():
            with conn.cursor() as cur:
                try:
                    cur.execute(query.format(tstart, tend, table_name))
                    counter_all[table_name] = cur.fetchall()    
                except Exception as err:
                    conn.rollback()
    
    for table_name in counter_all.keys():
        counter_all[table_name] = [el[0] for el, _ in groupby(counter_all[table_name])]   
    
    # вывод статистики по количеству дефектов
    for key, value in counter_all.items():
        print(key,';',all_def_names[key],';',len(counter_all[key]),';',counter_all[key].count(0),';',counter_all[key].count(1),';',counter_all[key].count(2),';',counter_all[key].count(3),';',counter_all[key].count(4),';')
