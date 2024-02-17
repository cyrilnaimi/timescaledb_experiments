import psycopg2
import json
import os
import numpy as np
import time
from datetime import datetime, timedelta
import traceback
from itertools import cycle

from concurrent import futures

import matplotlib.pyplot as plt
from matplotlib.pyplot import cm

CONNECTION = "postgres://postgres:admin123@172.16.15.136:5432/timescaledb"

def create_sensors_table(conn):
    query_create_sensors_table = "CREATE TABLE sensors (id SERIAL PRIMARY KEY, type VARCHAR(50), location VARCHAR(50));"
    cursor = conn.cursor()
    # see definition in Step 1
    cursor.execute(query_create_sensors_table)
    conn.commit()
    cursor.close()

def create_hypertable(conn):
    # create sensor data hypertable
    query_create_sensordata_table = """CREATE TABLE sensor_data (
                                           time TIMESTAMPTZ NOT NULL,
                                           sensor_id INTEGER,
                                           temperature DOUBLE PRECISION,
                                           cpu DOUBLE PRECISION,
                                           FOREIGN KEY (sensor_id) REFERENCES sensors (id)
                                           );"""

    query_create_sensordata_hypertable = "SELECT create_hypertable('sensor_data', 'time');"
    cursor = conn.cursor()
    cursor.execute(query_create_sensordata_table)
    cursor.execute(query_create_sensordata_hypertable)
    # commit changes to the database to make changes persistent
    conn.commit()
    cursor.close()

pool_location = cycle(['floor', 'car', 'room', 'ceiling', 'garden' ])
def cycle_location():
    return next(pool_location)

def init_sensors2(conn):
    sensors = [(str(i), cycle_location()) for i in range(1,500) ]
    cursor = conn.cursor()
    for sensor in sensors:
      try:
        cursor.execute("INSERT INTO sensors (type, location) VALUES (%s, %s);",
                    (sensor[0], sensor[1]))
      except (Exception, psycopg2.Error) as error:
        print(error.pgerror)
    conn.commit()


def init_sensors(conn):
    sensors = [('a', 'floor'), ('a', 'ceiling'), ('b', 'floor'), ('b', 'ceiling')]
    cursor = conn.cursor()
    for sensor in sensors:
      try:
        cursor.execute("INSERT INTO sensors (type, location) VALUES (%s, %s);",
                    (sensor[0], sensor[1]))
      except (Exception, psycopg2.Error) as error:
        print(error.pgerror)
    conn.commit()

def gnr_rnd():
    rng = np.random.default_rng(seed=int(datetime.now().timestamp()))
    arr = rng.random(200000)
    return arr

def gnr_time_array():
    return [datetime.now()-timedelta(minutes=5*i) for i in range(200000,0,-1)]

def fast_insert(conn):
    cursor = conn.cursor()
    # for sensors with ids 1-4
    ts = gnr_time_array()
    count=0
    for id in range(1, 500, 1):
        print(f" --- sensor {id} ---- ")
        rng = gnr_rnd()
        sql = " INSERT INTO sensor_data (time, sensor_id, temperature, cpu) VALUES (%s, %s, %s, %s);"
        # create random data
        for t,r in zip(ts,rng):
            count=+count+1
            try:
                cursor.execute(sql,[t,id,r*100,r*2.5])
            except Exception as e:
                traceback.print_exc()
                raise
            if count % 5000 == 0:
                print(f"row inserted :{count}")
                conn.commit()
    # commit after all sensor data is inserted
    # could also commit after each sensor insert is done
    conn.commit()


def th_insert(connection_str, id, ts_array):
    try:
        conn = psycopg2.connect(connection_str)
        cursor = conn.cursor()
        # for sensors with ids 1-4
        count=0
        print(f" --- sensor {id} ---- ")
        rng = gnr_rnd()
        sql = " INSERT INTO sensor_data (time, sensor_id, temperature, cpu) VALUES (%s, %s, %s, %s);"
        # create random data
        for t,r in zip(ts_array,rng):
            count=+count+1
            try:
                cursor.execute(sql,[t,id,r*100,r*2.5])
            except Exception as e:
                traceback.print_exc()
                raise
            if count % 5000 == 0:
                print(f" sensor {id} row inserted :{count}")
                conn.commit()
        # commit after all sensor data is inserted
        # could also commit after each sensor insert is done
        conn.commit()
    except Exception as e:
        print(traceback.print_exc())
    return id

def multi_th():
    ts = gnr_time_array()
    with futures.ThreadPoolExecutor(10) as ex:
        futs = []
        for i in range(0,500,1):
            f = ex.submit(th_insert,
                          CONNECTION,
                          i,
                          ts)
            futs.append(f)
        for f in futures.as_completed(futs):
            print(f'thread finised for {f.result()}')


def grabdata(start_date, end_date, bucket, sensor_id, conn):
    tic = time.perf_counter()
    cursor = conn.cursor()
    sql = """
           SELECT time_bucket('%s minutes', time) AS timebucket, percent(temperature, time)
           FROM sensor_data
           WHERE sensor_id = '%s' AND time between%s and %s
           GROUP BY timebucket
           ORDER BY timebucket DESC;
          """
    cursor.execute(sql,[bucket, sensor_id, start_date, end_date])
    data =  cursor.fetchall()
    output_ts = []
    output_d = []
    for r in data:
        output_ts.append(r[0])
        output_d.append(r[1])
    toc = time.perf_counter()
    print(f"Grab data in {toc - tic:0.4f} seconds")
    return output_ts, output_d


def plot(sensor1_blob, sensor2_blob,  sensor3_blob):
    fig = plt.figure(figsize=(12, 13), facecolor='white')
    fig.suptitle('timescaledb Testing')
    ax = plt.subplot(3, 1, 1)
    colors = iter(cm.rainbow(np.linspace(0, 1, len(sensor1_blob)*3+1)))
    for b in sensor1_blob:
        next(colors)
        next(colors)
        plt.plot(b['ts'], b['d'], '-', color=next(colors), label=f"bucket {b['bucket']} minutes")
        plt.title('Sensor 1')
        plt.ylabel('°C')
        ax.legend(loc='upper left', frameon=False, ncol=1)
        ax.grid(True)

    ax = plt.subplot(3,1 , 2)
    colors = iter(cm.coolwarm(np.linspace(0, 1, len(sensor1_blob)*3+1)))
    for b in sensor2_blob:
        next(colors)
        next(colors)
        plt.plot(b['ts'], b['d'], '-', color=next(colors), label=f"bucket {b['bucket']} minutes")
        plt.title('Sensor 2')
        plt.ylabel('°C')
        ax.legend(loc='upper left', frameon=False, ncol=1)
        ax.grid(True)

    ax = plt.subplot(3, 1, 3)
    colors = iter(cm.viridis(np.linspace(0, 1, len(sensor1_blob)*3+1)))
    for b in sensor3_blob:
        next(colors)
        next(colors)
        plt.plot(b['ts'], b['d'], '-', color=next(colors), label=f"bucket {b['bucket']} minutes")
        plt.title('Sensor 3')
        plt.ylabel('°C')
        ax.legend(loc='upper left', frameon=False, ncol=1)
        ax.grid(True)
    plt.show()

def main():
    conn = psycopg2.connect(CONNECTION)
    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     60,
                     44,
                     conn)
    blob1 = [{'ts': ts1, 'd': d1, 'bucket' : 60}]

    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     24*60,
                     34,
                     conn)
    blob1.append({'ts': ts1, 'd': d1, 'bucket' : 24*60})
    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     7*24*60,
                     24,
                     conn)
    blob1.append({'ts': ts1, 'd': d1, 'bucket' : 7*24*60})

    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     6*60,
                     24,
                     conn)
    blob1.append({'ts': ts1, 'd': d1, 'bucket' : 6*60})


    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     2*24*60,
                     24,
                     conn)
    blob1.append({'ts': ts1, 'd': d1, 'bucket' : 2*24*60})


    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     60,
                     16,
                     conn)

    blob2 = [{'ts': ts1, 'd': d1, 'bucket' : 60}]
    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     24*60,
                     56,
                     conn)
    blob2.append({'ts': ts1, 'd': d1, 'bucket' : 24*60})
    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     7*24*60,
                     108,
                     conn)
    blob2.append({'ts': ts1, 'd': d1, 'bucket' : 7*24*60})

    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     60,
                     46,
                     conn)
    blob3 = [{'ts': ts1, 'd': d1, 'bucket' : 60}]
    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     24*60,
                     88,
                     conn)
    blob3.append({'ts': ts1, 'd': d1, 'bucket' : 24*60})
    ts1,d1 = grabdata(datetime.now() - timedelta(weeks=104),
                     datetime.now(),
                     7*24*60,
                     77,
                     conn)
    blob3.append({'ts': ts1, 'd': d1, 'bucket' : 7*24*60})

    plot(blob1,blob2,blob3)


def insert_data():
    conn = psycopg2.connect(CONNECTION)
    cursor = conn.cursor()
    # create_sensors_table(conn)
    # create_hypertable(conn)
    # init_sensors(conn)
    # init_sensors2(conn)
    # fast_insert(conn)
    multi_th()
    cursor.execute("SELECT 'hello world'")
    print(cursor.fetchone())

if __name__ == "__main__":
    main()
    # insert_data()
