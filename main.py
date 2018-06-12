import csv
import pyodbc
import io
from flask import Flask, render_template, request
import sqlite3 as sql
app = Flask(__name__)

server = 'quiz1db.database.windows.net'
database = 'course'
username = 'rootkartik'
password = 'Kartik123'
driver = '{SQL Server}'
cnxn = pyodbc.connect(
      'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST', 'GET'])
def insert_table():

   cursor = cnxn.cursor()

   cursor.execute('DROP TABLE IF EXISTS equake;')
   cursor.commit()

   cursor.execute("CREATE TABLE equake(time varchar(30), latitude float(20), longitude float(20), depth float(8), mag float(5), magType varchar(10), nst int, gap int, dmin float(10), ms float(6), id varchar(20), place varchar(50), depthError float(8), magError float(8), magNst int, locationSource varchar(10))")
   cursor.commit()
   msg = "Record added bro IMO"

   print('Connected to the Azure database')

   if request.method == 'POST':
       f = request.files['data_file']
       if not f:
           return "No file"


       stream = io.StringIO(f.stream.read().decode("UTF8"), newline=None)
       csv_input = csv.reader(stream)
       next(csv_input)
       for row in csv_input:
           print(row)
           try:
               cursor.execute(
                   "INSERT INTO equake(time, latitude, longitude, depth, mag, magType, nst, gap, dmin, ms, id, place, depthError, magError, magNst, locationSource) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   row)
               cnxn.commit()

           except Exception as e:
               print(e)
               cnxn.rollback()

   return render_template('index.html')

@app.route('/Search1', methods=['POST', 'GET'])
def print_table():
    if request.method == 'POST':
        cursor = cnxn.cursor()

        result1 = cursor.execute('SELECT * from equake;')

    return render_template('index.html', result=result1)

@app.route('/rangeofgap', methods=['POST', 'GET'])
def searchgap_q7():
    if request.method == 'POST':
        inputgap1 = request.form['gap1']
        inputgap2 = request.form['gap2']
        print(inputgap1,inputgap2)
        cursor.execute("select * from equake where gap between ? and ?", (inputgap1,inputgap2,))
        rows=cursor.fetchall()
        cursor.execute("select * from earthquake where gap <?", (inputgap1,))
        rows1 =cursor.fetchall()
        cursor.execute("select * from earthquake where gap >?",(inputgap2,))
        rows2 =cursor.fetchall()
        insidegap = len(rows)
        belowgap=len(rows1)
        abovegap=len(rows2)
        print(insidegap,belowgap,abovegap)
        return render_template('index.html', msg="B Range : {0} Inside Range : {1}, Outside Range : {2}".format(belowgap,insidegap,abovegap))


@app.route('/Search2', methods=['POST', 'GET'])
def print_eqs():
    if request.method == 'POST':
        cursor = cnxn.cursor()

        cursor.execute("SELECT count(*) from equake where mag > 5;")
        count = cursor.fetchone()

    return render_template('index.html', count=count)

@app.route('/magrange', methods=['POST', 'GET'])
def search_magrange():
    if request.method == 'POST':
        mag1 = request.form['mag1']
        mag2 = request.form['mag2']
        date1 = request.form['date1']
        date2 = request.form['date2']
        print(mag1, mag2, date1, date2)
        cursor.execute("select * from equake where ((mag between ? and ?) and (time between ? and ?)) ", (mag1,
                    mag2, date1, date2,))
        rows = cursor.fetchall()
        print(rows)

    return render_template("index.html", result=rows)

@app.route('/magplace', methods=['POST', 'GET'])
def search_magplace():
    if request.method == 'POST':
        distance = request.form['distance']
        place = request.form['place']
        print(distance, place)
        replace = '{0}km%{1}'.format(distance, place)
        print(replace)

        cursor.execute("SELECT * FROM equake WHERE place like ?", (replace,))
        rows = cursor.fetchall()
        print(rows)

    return render_template("index.html", result=rows)


if __name__ == '__main__':
    app.run(debug = True)