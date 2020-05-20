from flask import Flask
import pandas as pd
import time
import progressbar
from time import sleep
from shapely.geometry import Point, Polygon
from datetime import date, datetime
import mysql.connector

app = Flask(__name__)
@app.route("/")
def hello():
    return "Hello, ini adalah base url flask"
    
def df_coordinates_to_tuple(single_poly_coordinates_str):
        # split coordinate data by ','
        coordinates = single_poly_coordinates_str.split(',')

        # convert list of coordinate into list of tuple (split by space)
        b = []
        for i in range(len(coordinates)):
            b.append(tuple(coordinates[i].split(' ')[:]))
        b_tup = []

        # convert from string into float
        for i in range(len(b)):
            try:
                if float(b[i][0]):
                    c = float(b[i][0])
            except IndexError:
                return 0
            try:
                if float(b[i][1]):
                    d = float(b[i][1])
            except IndexError:
                return 0

            c = float(b[i][0])
            d = float(b[i][1])
            b_tup.append((c,d))
        return b_tup

def check_locations_pip(poin_odp, poly_cluster):
    poly = Polygon(poly_cluster)
    poin = Point(poin_odp)
    return poin.within(poly)

def print_to_file(log):
    path = "../point_in_polygon_uploader/storage/tmp/uploads/"
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    f=open(path+"log.txt", "a+")
    f.write("["+dt_string+"]\t"+log+"\n")
    f.close

def insert_db(user_id,status,percentage=0):
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    sql = "INSERT INTO processes (user_id, status, percentage, created_at, updated_at) VALUES (%s, %s, %s)"
    val = (user_id, 'running', 0)
    mycursor.execute(sql, val)

@app.route('/execute_pip/<int:user_id>')
def process_pip(user_id):
    #insert_db(user_id,'running',0)
    log = "execution by user_id: "+str(user_id)
    print(log)
    print_to_file(log)
    log = "step 1: load files by user_id: "
    print(log)
    print_to_file(log)
    # convert data excel koordinat cluster untuk polygon
    path = "../point_in_polygon_uploader/storage/tmp/uploads/"
    cluster_df = pd.read_excel(path+"cluster.xlsx")
    odp_df = pd.read_csv(path+"odp.csv", delimiter=";")

    odp_df['cluster_id'] = ''

    # convert data titik odp ke list of tuple
    print("step 2: convert data odp ke format list of tuple")
    odp_tuples = []
    for i in range(len(odp_df)):
        lat = odp_df.LATITUDE[i]
        long = odp_df.LONGITUDE[i]
        odp_tuples.append((lat,long))

    log = "step 3: convert data koordinat cluster ke tuple"
    print(log)
    print_to_file(log)
    list_cluster_tuple = []
    start = 0
    end = len(cluster_df)
    error_clusters = []

    for i in range(len(cluster_df)):
        if i >= start and i < end and i != 1732:
            if df_coordinates_to_tuple(cluster_df.coordinate[i]) != 0:
                list_cluster_tuple.append(df_coordinates_to_tuple(cluster_df.coordinate[i]))
            else:
                error_clusters.append(cluster_df.cluster_id[i])

    # loop all
    start_time = time.time()
    bar = progressbar.ProgressBar(maxval=len(list_cluster_tuple), \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

    # cek semua poin dalam polygon
    log = "step 4: cek setiap poin odp dalam cluster polygon"
    print(log)
    print_to_file(log)
    log = str(len(list_cluster_tuple))+" clusters to check"
    print(log)
    print_to_file(log)
    log = str(len(odp_tuples))+" ODP(s) to check"
    print(log)
    print_to_file(log)
    print("progress: ")
    log = "progress: "
    print(log)
    print_to_file(log)

    baris_cluster = 0
    bar.start()
    for i in range(len(list_cluster_tuple)):
        bar.update(i+1)
        baris_odp = 0
        found = []
        for odp in odp_tuples:
            if check_locations_pip(poin_odp=odp, poly_cluster=list_cluster_tuple[i]) == True:
                odp_df.at[baris_odp, 'cluster_id'] = cluster_df.cluster_id[i]
                found.append(odp)
            baris_odp = baris_odp + 1
        baris_cluster = i
        log = "ditemukan "+str(len(found))+" ODP di cluster_id: "+str(cluster_df.cluster_id[baris_cluster])+", polygon ke: "+str(baris_cluster)
        print(log)
        print_to_file(log)
    bar.finish()
    print("--- selesai dalam %s seconds ---" % (time.time() - start_time))
    log = "--- selesai dalam %s seconds ---" % (time.time() - start_time)
    print_to_file(log)

    print("step 6: generate excel file")
    log = "step 6: generate excel file"
    print_to_file(log)
    path = "../point_in_polygon_uploader/storage/tmp/uploads/"
    output_file = str(date.today())+" Koordinat ODP dengan cluster_id.xlsx"
    odp_df.to_excel(path+output_file)
    print('done and dusted')
    print_to_file('done and dusted')
    return 'done'

if __name__ == "__main__":
    app.run()
