

import sys
sys.path.append("..")
import helpers.dataAccess as dtAccss
import helpers.csvHelper as csvHelper
from helpers.log import log
from multiprocessing import Process
import os

class CsvIntegrator(object):
    def __init__(self, name, server, database, 
                    file_path, output_file_path, clean=None, table_name=None, 
                        truncate=True, column_for_delete = None, clean_arg= None, delimiter=None):
        self.name = name
        self.server = server
        self.database = database
        self.file_path = file_path
        self.output_file_path = output_file_path
        self.table_name = table_name
        self.truncate = truncate
        self.clean = clean
        self.column_for_delete = column_for_delete
        self.clean_arg=clean_arg
        self.delimiter=delimiter
        
        log(__name__, '__init__', f"Initialising {self.name}")

    def run(self, modified_file, **kwargs):
        log(__name__, '__init__', f"Running {self.name}")
        
        process = Process(target=self.__integrate__, args=(modified_file,))

        process.start()

    def __integrate__(self, modified_file, **kwargs):
        try:
            modified_file_name = modified_file.split('\\')[-1]
            
            table_name=self.table_name
            
            try:
                file_names = self.__getImportFileNames__()  
                for import_file in file_names:
                    if(modified_file_name.lower() == import_file.FileName.lower()):
                        self.delimiter = import_file.FieldSeparator
                        table_name = import_file.TableName 
            except: # there is no config for this import, continue by using the supplied db table name
                if(table_name is None):
                    log(__name__, 'run', f"No config saved for {self.name}", level="Error", email=True, emailSubject=f"{self.name}")
                
            
            df = csvHelper.getDataframe(modified_file, self.delimiter) 
            
            if(self.table_name is None):
                table_columns = self.__getDatabaseColumnNames__(table_name) 
                df = self.__configureDataFrame__(df, table_columns)
                
            
            if(self.clean is not None):
                if(self.clean_arg is not None):
                    df = self.clean(df, self.clean_arg)  
                else:
                    df = self.clean(df)  

            self.__saveToDB__(df, modified_file_name, table_name, **kwargs)
            
            log(__name__, '__integrate__', f"{self.name} has completed and {modified_file_name} has been imported", level="Info", email=True, emailSubject=self.name)
        except Exception as e:
            log(__name__, '__integrate__', f"{self.name} has failed for {modified_file_name}: {str(e)}", level="Error", email=True, emailSubject=self.name)

    def __configureDataFrame__(self, df, columns, delimiter=None):
        """
            Dataframe List -> Dataframe
            Consume a dataframe and a list of columns, return the configured dataframe or None
        """

        if(len(df.columns) != len(columns)):
            log(
                __name__, 
                'run', 
                f"Columns for the table {self.table_name} do not match those from the file", 
                level="Error", 
                email=True, 
                emailSubject=self.name
            )
            return None
        else:
            dataframe_columns = list()
            for column in columns:
                dataframe_columns.append(column)

            df.columns = dataframe_columns
        
        return df
        

    def __saveToDB__(self, df, file_name, table_name, truncate=True, **kwargs):
        dataAccess = dtAccss.DataAccess(self.server, self.database)
        
        output_full_path = f"{self.output_file_path}\\{file_name}"
        df.to_csv(output_full_path, sep='|', index=False)
        
        if(self.column_for_delete is not None):
            ids = ','.join("'" + str(x) + "'" for x in df[self.column_for_delete].unique())
            dataAccess.deleteById(table_name, self.column_for_delete, ids)

        dataAccess.bulkInsert(table_name, output_full_path, truncate=self.truncate)
    

    def __getImportFileNames__(self):
        """
            Returns list of tuples
            Gets a list of file names to import from the ImportFile table
        """
        dataAccess = dtAccss.DataAccess(self.server, self.database)
        return dataAccess.load("ImportFile")

    def __getDatabaseColumnNames__(self, table_name):
        """
            String -> [()]
            Consumes a database table name and produces a list of columns
        """
        dataAccess = dtAccss.DataAccess(self.server, self.database)
        return dataAccess.load("view_TableColumns", TableName= table_name)
    
    def __loadCSV__(self, file_name, sep=None):        
        return csvHelper.getDataframe(f"{self.file_path}\\{file_name}",delimiter=sep)


    