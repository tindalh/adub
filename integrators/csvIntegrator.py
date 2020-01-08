

import sys
sys.path.append("..")
import helpers.dataAccess as dtAccss
import helpers.csvHelper as csvHelper
from helpers.log import log
import pandas as pd
from multiprocessing import Process
import os

class CsvIntegrator(object):
    def __init__(self, name, server, database, file_path, output_file_path, table_name, 
                    file_name=None, file_columns=None, table_columns=None, clean=None, 
                        truncate=True, column_for_delete = None, clean_arg= None, delimiter=None, post_op_procedure=None):
        self.name = name # Name for this integrator, to appear in logs and emails
        self.server = server # Server name
        self.database = database 
        self.table_name = table_name # Table where the data will be inserted
        self.file_path = file_path # Path where the input file is         
        self.output_file_path = output_file_path # Path for the output file 
        
        self.file_name = file_name # [OPTIONAL] name of file. If blank, any file names in the path will be processed        
        self.file_columns = file_columns # [OPTIONAL] column headers for the file
        self.table_columns = table_columns # [OPTIONAL] table columns
        self.truncate = truncate # [OPTIONAL] column headers for the file. Default True
        self.clean = clean # [OPTIONAL] Function for cleaning the data for this Integrator
        self.column_for_delete = column_for_delete # [OPTIONAL] Column used to identify unique rows in the data file
        self.post_op_procedure = post_op_procedure # [OPTIONAL] a stored procedure to execute
        self.clean_arg=clean_arg
        self.delimiter=delimiter
        
        log(__name__, '__init__', f"Initialising {self.name}")

    def run(self, modified_file, **kwargs):
        #log(__name__, '__init__', f"Running {self.name}")
        
        process = Process(target=self.__integrate__, args=(modified_file,))

        process.start()

    def __integrate__(self, modified_file, **kwargs):
        try:
            modified_file_name = modified_file.split('\\')[-1]

            if(self.file_name is not None): # a file name has been provided for this integrator, only process this file
                if(self.file_name.lower() != modified_file_name.lower()):
                    return
                
            df = csvHelper.getDataframe(modified_file, self.delimiter, names=self.file_columns) 
            
            if(self.clean is not None):
                if(self.clean_arg is not None):
                    df = self.clean(df, self.clean_arg)  
                else:
                    df = self.clean(df) 

            if(self.table_columns is not None and self.file_columns is not None): # if we are only adding columns, a list can be passed in
                df = self.__arrangeColumns__(df, self.table_columns)

            self.__saveToDB__(df, modified_file_name, self.table_name, **kwargs)

            if(self.post_op_procedure is not None):
                self.__post_op_procedure__(self.post_op_procedure)
            
            log(__name__, '__integrate__', f"{self.name} has completed and {modified_file_name} has been imported", level="Info", email=True, emailSubject=self.name)
        except Exception as e:
            log(__name__, '__integrate__', f"{self.name} has failed for {modified_file_name}: {str(e)}", level="Error", email=True, emailSubject=self.name)

    def __arrangeColumns__(self, df, table_columns):
        """
            List Dataframe -> Dataframe
            Consume a dataframe and a list of columns, return the configured dataframe or None
        """
        dfs = []

        for column in table_columns:

            for df_column in df.columns:
                if(df_column.lower() == column.lower()):
                    dfs.append(df[df_column].to_frame())
                    
        return pd.concat(dfs, axis=1)
        

    def __saveToDB__(self, df, file_name, table_name, truncate=True, **kwargs):
        dataAccess = dtAccss.DataAccess(self.server, self.database)
        
        output_full_path = f"{self.output_file_path}\\{file_name}"
        df.to_csv(output_full_path, sep='|', index=False)
        
        if(self.column_for_delete is not None):
            ids = ','.join("'" + str(x) + "'" for x in df[self.column_for_delete].unique())
            dataAccess.deleteById(table_name, self.column_for_delete, ids)

        dataAccess.bulkInsert(table_name, output_full_path, truncate=self.truncate)

    def __post_op_procedure__(self, proc_name):
        dataAccess = dtAccss.DataAccess(self.server, self.database)
        
        dataAccess.executeStoredProcedure(proc_name, [])

    