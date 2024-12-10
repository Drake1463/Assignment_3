import json
import requests
import sqlite3


# separate function for working with database.
def fun_database(meteorite_list):
    try:

        db_connection = sqlite3.connect('meteorite_db.db')
        cursor = db_connection.cursor()

        # query for create table
        query_create_met_data = """CREATE TABLE IF NOT EXISTS meteorite_data(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclat TEXT,
                                reclong TEXT);"""

        # NEW create a query for all 7 tables
        # query for create Table 1: Africa_MiddleEast_Meteorites – 148 rows
        query_create_table1_met_data = """CREATE TABLE IF NOT EXISTS table1_meteorite_data(
                                        name TEXT,
                                        mass TEXT,
                                        reclat TEXT,
                                        reclong TEXT);"""

        # query for create Table 2: Europe_Meteorites – 283 rows
        query_create_table2_met_data = """CREATE TABLE IF NOT EXISTS table2_meteorite_data(
                                                name TEXT,
                                                mass TEXT,
                                                reclat TEXT,
                                                reclong TEXT);"""

        # query for create Table 3: Upper_Asia_Meteorites – 86 rows
        query_create_table3_met_data = """CREATE TABLE IF NOT EXISTS table3_meteorite_data(
                                                name TEXT,
                                                mass TEXT,
                                                reclat TEXT,
                                                reclong TEXT);"""

        # query for create Table 4: Lower_Asia_Meteorites – 238 rows
        query_create_table4_met_data = """CREATE TABLE IF NOT EXISTS table4_meteorite_data(
                                                name TEXT,
                                                mass TEXT,
                                                reclat TEXT,
                                                reclong TEXT);"""

        # query for Table 5: Australia_Meteorites – 13 rows
        query_create_table5_met_data = """CREATE TABLE IF NOT EXISTS table5_meteorite_data(
                                                name TEXT,
                                                mass TEXT,
                                                reclat TEXT,
                                                reclong TEXT);"""

        # query for Table 6: North_America_Meteorites – 166 rows
        query_create_table6_met_data = """CREATE TABLE IF NOT EXISTS table6_meteorite_data(
                                                name TEXT,
                                                mass TEXT,
                                                reclat TEXT,
                                                reclong TEXT);"""

        # query for Table 7: South_America_Meteorites – 47 rows
        query_create_table7_met_data = """CREATE TABLE IF NOT EXISTS table7_meteorite_data(
                                                name TEXT,
                                                mass TEXT,
                                                reclat TEXT,
                                                reclong TEXT);"""

        # query for execute for meteor table
        cursor.execute(query_create_met_data)

        # NEW query for execute for the 7 Tables of data
        cursor.execute(query_create_table1_met_data)
        cursor.execute(query_create_table2_met_data)
        cursor.execute(query_create_table3_met_data)
        cursor.execute(query_create_table4_met_data)
        cursor.execute(query_create_table5_met_data)
        cursor.execute(query_create_table6_met_data)
        cursor.execute(query_create_table7_met_data)

        # make a table meteorite_data empty
        query_delete = """DELETE FROM meteorite_data"""
        cursor.execute(query_delete)

        # NEW make tables for all the 7 tables of data to empty the tables
        # make a table table1_meteorite_data empty
        query_table1_delete = """DELETE FROM table1_meteorite_data"""
        cursor.execute(query_table1_delete)

        # make a table table2_meteorite_data empty
        query_table2_delete = """DELETE FROM table2_meteorite_data"""
        cursor.execute(query_table2_delete)

        # make a table table3_meteorite_data empty
        query_table3_delete = """DELETE FROM table3_meteorite_data"""
        cursor.execute(query_table3_delete)

        # make a table table4_meteorite_data empty
        query_table4_delete = """DELETE FROM table4_meteorite_data"""
        cursor.execute(query_table4_delete)

        # make a table table5_meteorite_data empty
        query_table5_delete = """DELETE FROM table5_meteorite_data"""
        cursor.execute(query_table5_delete)

        # make a table table6_meteorite_data empty
        query_table6_delete = """DELETE FROM table6_meteorite_data"""
        cursor.execute(query_table6_delete)

        # make a table table7_meteorite_data empty
        query_table7_delete = """DELETE FROM table7_meteorite_data"""
        cursor.execute(query_table7_delete)

        # prepare a query with parameters
        query_insert = ("""INSERT INTO meteorite_data (name, mass, reclat, reclong)
               VALUES (?,?,?,?)""")

        # NEW Create insert query for each table
        # prepare a query with parameters for table 1
        table1_query_insert = ("""INSERT INTO table1_meteorite_data (name, mass, reclat, reclong)
               VALUES (?,?,?,?)""")

        # prepare a query with parameters for table 2
        table2_query_insert = ("""INSERT INTO table2_meteorite_data (name, mass, reclat, reclong)
               VALUES (?,?,?,?)""")

        # prepare a query with parameters for table 3
        table3_query_insert = ("""INSERT INTO table3_meteorite_data (name, mass, reclat, reclong)
               VALUES (?,?,?,?)""")

        # prepare a query with parameters for table 4
        table4_query_insert = ("""INSERT INTO table4_meteorite_data (name, mass, reclat, reclong)
               VALUES (?,?,?,?)""")

        # prepare a query with parameters for table 5
        table5_query_insert = ("""INSERT INTO table5_meteorite_data (name, mass, reclat, reclong)
               VALUES (?,?,?,?)""")

        # prepare a query with parameters for table 6
        table6_query_insert = ("""INSERT INTO table6_meteorite_data (name, mass, reclat, reclong)
               VALUES (?,?,?,?)""")

        # prepare a query with parameters for table 7
        table7_query_insert = ("""INSERT INTO table7_meteorite_data (name, mass, reclat, reclong)
               VALUES (?,?,?,?)""")


        # getting data of  one row and inserting that
        val = (meteorite_list[2]['name'],meteorite_list[2]['mass'],meteorite_list[2]['reclat'],meteorite_list[2]['reclong'])
        cursor.execute(query_insert, val)

        # looping through the meteorite list and adding values as a tuples
        for row in meteorite_list:

            # make a bounding box for each table
            table1_bb = {
                "left": -17.8,
                "bottom": -35.2,
                "right": 62.2,
                "top": 37.6,
                "class": "Africa_MiddleEast_Meteorites"
            }

            # make a bounding box for each table
            table2_bb = {
                "left": -24.1,
                "bottom": 38.0,
                "right": 32.1,
                "top": 71.1,
                "class": "Europe_Meteorites"
            }

            table3_bb = {
                "left": 33.0,
                "bottom": 38.0,
                "right":  190.4,
                "top": 72.7,
                "class": 'Upper_Asia_Meteorites'

            }

            table4_bb = {
                "left": 63.0,
                "bottom": -9.9,
                "right": 154.0,
                "top":  37.6,
                "class": 'Lower_Asia_Meteorites'

            }

            table5_bb = {
                "left": 112.9,
                "bottom": -43.8,
                "right": 154.3,
                "top": -11.1,
                "class": 'Australia_Meteorites'

            }

            table6_bb = {
                "left": -168.2,
                "bottom": 12.8,
                "right":  -52.0,
                "top": 71.5,
                "class": 'North_America_Meteorites'

            }

            table7_bb = {
                "left": -81.2,
                "bottom": -55.8,
                "right": -34.4,
                "top": 12.5,
                "class": 'South_America_Meteorites'

            }

            # it is ok to execute insert query in the database each time
            # but it is bad when we have big list, it is better to make list of tuples and use executemany
            cursor.execute(query_insert, (row.get('name', None), row.get('mass', None), row.get('reclat', None),row.get('reclong', None)))

            # get the bouding box values of reclat and reclong to compare to determine what table the values belong in
            reclatVar = str(row.get('reclat'))
            reclongVar = str(row.get('reclong'))

            # if the values are not null or blank
            if reclatVar != 'None':
                if reclongVar != 'None':
                    # check if the values are within the table 1 bounding box

                    # This is the code for table 1
                    if (float(reclatVar) >= table1_bb["bottom"]) & (float(reclatVar) <= table1_bb["top"]) & (float(reclongVar) >= table1_bb["left"]) & (
                                float(reclongVar) <= table1_bb["right"]):
                        #  query insert of the data into table 1
                        cursor.execute(table1_query_insert, (
                        row.get('name', None), row.get('mass', None), row.get('reclat', None), row.get('reclong', None)))

                    #This is the code for table 2
                    if (float(reclatVar) >= table2_bb["bottom"]) & (float(reclatVar) <= table2_bb["top"]) & (
                            float(reclongVar) >= table2_bb["left"]) & (
                            float(reclongVar) <= table2_bb["right"]):
                        #  query insert of the data into table 2
                        cursor.execute(table2_query_insert, (
                                row.get('name', None), row.get('mass', None), row.get('reclat', None),
                                row.get('reclong', None)))

                    #This is the code for table 3
                    if (float(reclatVar) >= table3_bb["bottom"]) & (float(reclatVar) <= table3_bb["top"]) & (
                            float(reclongVar) >= table3_bb["left"]) & (
                            float(reclongVar) <= table3_bb["right"]):
                        #  query insert of the data into table 3
                        cursor.execute(table3_query_insert, (
                            row.get('name', None), row.get('mass', None), row.get('reclat', None),
                            row.get('reclong', None)))
                    # This is the code for table 4
                    if (float(reclatVar) >= table4_bb["bottom"]) & (float(reclatVar) <= table4_bb["top"]) & (
                            float(reclongVar) >= table4_bb["left"]) & (
                            float(reclongVar) <= table4_bb["right"]):
                        #  query insert of the data into table 4
                        cursor.execute(table4_query_insert, (
                            row.get('name', None), row.get('mass', None), row.get('reclat', None),
                            row.get('reclong', None)))
                    # This is the code for table 5
                    if (float(reclatVar) >= table5_bb["bottom"]) & (float(reclatVar) <= table5_bb["top"]) & (
                            float(reclongVar) >= table5_bb["left"]) & (
                            float(reclongVar) <= table5_bb["right"]):
                        #  query insert of the data into table 5
                        cursor.execute(table5_query_insert, (
                            row.get('name', None), row.get('mass', None), row.get('reclat', None),
                            row.get('reclong', None)))
                    #This is the code for table 6
                if (float(reclatVar) >= table6_bb["bottom"]) & (float(reclatVar) <= table6_bb["top"]) & (
                        float(reclongVar) >= table6_bb["left"]) & (
                        float(reclongVar) <= table6_bb["right"]):
                    #  query insert of the data into table 6
                    cursor.execute(table6_query_insert, (
                        row.get('name', None), row.get('mass', None), row.get('reclat', None),
                        row.get('reclong', None)))

                    # This is the code for table 7
                if (float(reclatVar) >= table7_bb["bottom"]) & (float(reclatVar) <= table7_bb["top"]) & (
                        float(reclongVar) >= table7_bb["left"]) & (
                        float(reclongVar) <= table7_bb["right"]):
                    #  query insert of the data into table 7
                    cursor.execute(table7_query_insert, (
                        row.get('name', None), row.get('mass', None), row.get('reclat', None),
                        row.get('reclong', None)))

        db_connection.commit()

    except sqlite3.Error as db_error:
        print(f'A database error: {db_error}')

    finally:
        if db_connection:
            db_connection.close()
            print('')
            print('Table creation completed')

# separate function for working with API and json.
def fun_API():
        url = "https://data.nasa.gov/resource/gh4g-9sfh.json"
        response = requests.get(url)

        if response.status_code == 200:
            # convert to json
            data = response.json()

            # print the whole list
            print("")
            print('')
            print(data)
            return data

if __name__ == '__main__':
    # Call api function and storing data inside meteorite_list variable
    meteorite_list = fun_API()

    # check if there is a list before doing anything
    if meteorite_list:
        # Sending the list to the function that works with database
        fun_database(meteorite_list)
    else:
        print('Error: No Data To Process')

