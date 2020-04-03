import pyodbc
import csv
import datetime
from helpers.log import log as log

class DataAccess(object):
    def __init__(self,server,database, is_unit_test=False):
        self.server = server
        self.database = database
        self.connectionString= 'DRIVER={SQL Server};SERVER=' + server+';DATABASE='+database+';Trusted_connection=yes'
        
        if(is_unit_test == False):
            self.cnxn = pyodbc.connect(self.connectionString, autocommit=True)
            self.cursor = self.cnxn.cursor()
   
    def load(self, table, **kwargs):
        sql = f"SELECT * FROM {table}"

        i = 0
        params = list()
        for key in kwargs.keys():
            if(i == 0):
                sql += f" WHERE "
            else:
                sql += " AND "
                
            
            if(key[:2] == ">="):
                sql += f"{key[2:]} >= ?"
            elif(key[:2] == "<="):
                sql += f"{key[2:]} <= ?"
            elif(key[:1] == ">"):
                sql += f"{key[1:]} > ?"
            elif(key[:1] == "<"):
                sql += f"{key[1:]} < ?"            
            else:
                sql += f"{key} = ?"
            params += {kwargs[key]}

            i+=1

        log(__name__, 'load', f"Executing {sql} with parameters: {params}")
        
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def exists(self, table, idName, ids, condition='' ):
        sql = 'SELECT 1 FROM {} WHERE {} in ({}) {}'.format(table, idName, ids, condition)
        self.cursor.execute(sql)

        if (self.cursor.fetchval() is not None):
            return True
        return False

    def deleteById(self, table, idName, ids):
        sql = 'DELETE FROM {} WHERE {} in ({})'.format(table, idName, ids)
        self.cursor.execute(sql)

    def delete(self, table, **kwargs):
        sql = f"DELETE FROM {table}"

        i = 0
        params = list()
        for key in kwargs.keys():
            if(i == 0):
                sql += f" WHERE "
            else:
                sql += " AND "

            j = 0
            for value in kwargs[key]:
                if(j == 0):
                    sql += ' ('

                if(key[:2] == ">="):
                    sql += f"{key[2:]} >= ?"
                elif(key[:2] == "<="):
                    sql += f"{key[2:]} <= ?"
                elif(key[:1] == ">"):
                    sql += f"{key[1:]} > ?"
                elif(key[:1] == "<"):
                    sql += f"{key[1:]} < ?"    
                else:
                    sql += f"{key} = ?"
                params += {value}

                if(j == len(kwargs[key]) - 1):
                    sql += ') '
                else:
                    sql += ' or '

                j+=1

            i+=1


        log(__name__, 'delete', f"Executing {sql} with parameters: {params}")
        
        self.cursor.execute(sql, params)

    def bulkInsert(self, table_name, file_path, delimiter='|', truncate=False, first_row=2):
        sql = ""

        params = list()
        if(truncate):
            sql += f"TRUNCATE TABLE {table_name.strip()};"
           
        
        sql +=  f"BULK INSERT {table_name.strip()} FROM '{file_path}' WITH (FIRSTROW = {first_row}, FIELDTERMINATOR='{delimiter}');"

        log(__name__, 'bulkInsert', f"Executing {sql}")
        
        self.cursor.execute(sql)

    def executeRawSQL(self, sql):
        affectedCount = self.cursor.execute(sql).rowcount
        return affectedCount   
		
    def executeStoredProcedure(self, table, params=None):
        """Assumes params is a tuple"""
        sql = """
            exec  [""" + self.database + """].[dbo].[""" + table + """]
        """

        if(params is not None):
            for p in params:
                sql += '?,'

        sql = sql[:-1]  
        if(params is not None):
            result = self.cursor.execute(sql,params).fetchval() 
        else:
            result = self.cursor.execute(sql).fetchval() 

        return result

    def loadToCSV(self, sql, file_name, file_path=""):
        rows = self.cursor.execute(sql)
        if(len(file_path) > 0):
            if(file_path[:-1] != "\\"):
                file_path += "\\"

        with open(f"{file_path}{file_name}.csv", 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([x[0] for x in self.cursor.description])

            writer.writerows(rows)

    
    # String String String dict -> datetime.datetime
    # Consumes table and column name and returns the max. 
    # ASSUME: column_name is of type Date or DateTime
    def get_max_database_date(self, table_name, date_column_name, schema_name='dbo', **kwargs):
        sql = f"SELECT Max({date_column_name}) FROM [{schema_name}].[{table_name}]"

        i = 0
        params = list()
        for key in kwargs.keys():
            if(i == 0):
                sql += f" WHERE "
            else:
                sql += " AND "

            j = 0
            for value in kwargs[key]:
                if(j == 0):
                    sql += ' ('

                sql += f"{key} = ?"
                params += {value}

                if(j == len(kwargs[key]) - 1):
                    sql += ') '
                else:
                    sql += ' or '

                j+=1

            i+=1

        log(__name__, 'load', f"Executing {sql} with parameters: {params}")
        
        max_date = self.cursor.execute(sql, params).fetchval()

        if(max_date is not None):
            max_date = datetime.datetime.strptime(max_date, '%Y-%m-%d')
        else:
            max_date = datetime.datetime.strptime('2018-01-01', '%Y-%m-%d')

        return max_date

    
        
        
