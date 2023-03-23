from pycelonis import pql
import logging
import sys
from bosch_logging import init_logging
import pandas as pd

from ems_connection import EMS_Connection

class TheList(object):  
    __dm = 'b22e188f-e935-4269-a261-dbf6e2745627'            

    def __init__(self, Connection: EMS_Connection, reload_datamodel = False):
        print("######## THE LIST ###########")
        init_logging()
        self.__logger = logging.getLogger("the_list")

        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)        
        self._dm = Connection.datamodels.find(self.__dm)                    
        self.__the_list_tables = [] 
        self.__the_list_columns = []
        self.__the_list_system_mappings = []
        self.__the_list_anon_tables = []
        self.__DPL_Object_Ids = []
        self.__DPL_UseCase_Ids = []
        if reload_datamodel:
            self.__logger.info('Reload Data Model: start')
            r = self._dm.reload()        
            self.__logger.info('Reload Data Model: end')

    ##########################
    ### THE-LIST Tables
    def getTables(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        return self.__get_the_list_tables()
    
    def __get_the_list_tables(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        if len(self.__the_list_tables) == 0:
            self.__the_list_tables = self.__getTables()
        
        return self.__the_list_tables
    
    def getTablesByUseCase(self, pUseCase: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()                
        
        query_result = df_tab[(df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_TABLE_REQUESTED_BY_PROJECT'] == pUseCase)][[
            '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
            '_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE',
            '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]

        query_result.sort_values(by=["_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"], inplace = True)
    
        return query_result
        
    def getTablesByUseCaseAndSystem(self, pUseCase, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()
        
        query_result = df_tab[(df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_TABLE_REQUESTED_BY_PROJECT'] == pUseCase)
                              & (df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem)][[
                '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                '_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE',
                '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]
                
        query_result = query_result.drop_duplicates() 
        query_result.sort_values(by=["_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"], inplace = True)

        return query_result

    def getTablesByUseCaseAndSystemAsList(self, pUseCase, psystem) -> list:
        aTablesPD = self.getTablesByUseCaseAndSystem(pUseCase, psystem)
        aTables = []
        for index, row in aTablesPD.iterrows():  
            aTables.append(row['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']) 
        
        return aTables
    
    def getTablesByUseCaseSystemAndTable(self, pUseCase: str, psystem: str, ptable: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()
        
        query_result = df_tab[(df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_TABLE_REQUESTED_BY_PROJECT'] == pUseCase)
                              & (df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem)
                              & (df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE'] == ptable)][[
                '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                '_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE',
                '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]
                
        query_result = query_result.drop_duplicates() 
        query_result.sort_values(by=["_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"], inplace = True)

        return query_result

    def getTablesBySystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()
        
        query_result = df_tab[(df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem)][[
                '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                '_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE',
                '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]
                
        query_result = query_result.drop_duplicates()
        query_result.sort_values(by=["_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"], inplace = True)
        
        return query_result 

        
    def getTableBySystemAndTable(self, psystem, ptable):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()
        
        query_result = df_tab[(df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem)
                                           & (df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'] == ptable)][[
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]
                    
        query_result = query_result.drop_duplicates()
        query_result.sort_values(by=["_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"], inplace = True)

        return query_result  

    def getTablesBySystemAndTables(self, psystem, ptables):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()
        
        query_result = df_tab[(df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem)][[
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE',
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]

        query_result = query_result.drop_duplicates()
        query_result.sort_values(by=["_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"], inplace = True)

        column_names = ["_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME", "_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"]
        tables_result = pd.DataFrame(columns = column_names)

        for table in ptables:
            tab = query_result[(query_result['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'] == table)]
            ###pandas.concat tables_result = tables_result.append(tab)
            tables_result = pd.concat([tables_result, tab])

        tables_result = tables_result.drop_duplicates()

        return tables_result  

    def getTablesByDeletionFiltersBySystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()

        
        query_result = df_tab[
                            (
                                df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem) & (
                                    df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_FILTER_1_OPTIONAL'].notna()
                                    | df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_JOIN_FILTER'].notna()
                                    | df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_TIME_FILTER'].notna()
                            )
            ][[
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]

        query_result = query_result.drop_duplicates()
        
        return query_result
    
    #List all tables where TIME_FILTER_1_OPTIONAL is not empty
    def getTables_Time_Filter_not_Empty_BySystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()

        
        query_result = df_tab[
                            (
                                df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem) & (
                                    df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_TIME_FILTER_1_OPTIONAL'].notna()
                            )
            ][[
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]

        query_result = query_result.drop_duplicates()
        
        return query_result

    #List all tables where JOIN_TABLE_1 is not empty
    def getTables_Join_Table_not_Empty_BySystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()

        
        query_result = df_tab[
                            (
                                df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem) & (
                                    df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_JOIN_TABLE_1'].notna()
                            )
            ][[
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]

        query_result = query_result.drop_duplicates()
        
        return query_result
    
    #List all tables where DELETE_FILTER_1_OPTIONAL is not empty
    def getTables_Delete_Filter_not_Empty_BySystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()

        
        query_result = df_tab[
                            (
                                df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem) & (
                                    df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_FILTER_1_OPTIONAL'].notna()
                            )
            ][[
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                    '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]

        query_result = query_result.drop_duplicates()

    def getSystemsByUseCase(self, pUseCase):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()
        # Systems 
        query_result = df_tab[(df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_TABLE_REQUESTED_BY_PROJECT'] == pUseCase)][[
            '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME']]
        
        query_result = query_result.drop_duplicates()
        
        return query_result
    
    def getSystemsByUseCaseAsList(self, pUseCase) -> list:
        aSystemsPD = self.getSystemsByUseCase(pUseCase)
        aSystems = []
        for index, row in aSystemsPD.iterrows():  
            aSystems.append(row['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME']) 
        
        return aSystems
        

    def getTableConfigurationBySystemAndTable(self, psystem, ptable):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_tables()
        
        query_result = df_tab[(df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'] == psystem)
                                           & (df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'] == ptable)][[
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME', 
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_SCHEMA_', 
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE', 
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME', 
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_SIZE_ALLOCATED_GB', 
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_NUMBER_OF_ROWS',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_TABLE_USED',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_TABLE_USED_COUNT',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_NUM_MONTHS',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_JOIN_CONDITION_1_OPTIONAL',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_JOIN_CONDITION_2_OPTIONAL',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_TIME_FILTER_1_OPTIONAL',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_TIME_FILTER_2_OPTIONAL',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_OTHER_FILTER_1_OPTIONAL',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_OTHER_FILTER_2_OPTIONAL',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_OTHER_FILTER_3_OPTIONAL',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_JOIN_TABLE_1',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_JOIN_FILTER_1',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_FILTER_1_OPTIONAL',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_JOIN_FILTER',
                                                '_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_TIME_FILTER']]
                    
        query_result = query_result.drop_duplicates()
        return query_result

    def get_Time_Filter_1_Optional_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_TIME_FILTER_1_OPTIONAL'].iloc[0]
    
    def get_Other_Filter_1_Optional_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_OTHER_FILTER_1_OPTIONAL'].iloc[0]    

    def get_Join_Table_1_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_JOIN_TABLE_1'].iloc[0]    

    def get_Join_Filter_1_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_JOIN_FILTER_1'].iloc[0]

    def get_Join_Condition_1_Optional_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_JOIN_CONDITION_1_OPTIONAL'].iloc[0]            
    
    def get_Requested_Table_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE'].iloc[0] 

    def get_Oracle_Schema_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_SCHEMA_'].iloc[0] 

    def get_Delete_Filter_1_Optional_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_FILTER_1_OPTIONAL'].iloc[0] 

    def get_Delete_Join_Filter_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_JOIN_FILTER'].iloc[0] 

    def get_Delete_Time_Filter_fromTable(self, psystem: str, ptable: str):
        df_tab = self.getTableConfigurationBySystemAndTable(psystem, ptable)
        
        return df_tab['_V_IBC_TABLEEXPLORER_4_CELONIS_DELETE_TIME_FILTER'].iloc[0] 

    def __getTables(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        tab_tables_cols = [
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."TABLE_REQUESTED_BY_PROJECT"', 
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."SOURCESYSTEM_NAME"', 
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."ORACLE_SCHEMA_"', 
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."REQUESTED_TABLE"', 
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."ORACLE_TABLE_NAME"', 
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."SIZE_ALLOCATED_GB"', 
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."NUMBER_OF_ROWS"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."TABLE_USED"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."TABLE_USED_COUNT"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."NUM_MONTHS"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."JOIN_CONDITION_1_OPTIONAL"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."JOIN_CONDITION_2_OPTIONAL"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."TIME_FILTER_1_OPTIONAL"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."TIME_FILTER_2_OPTIONAL"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."OTHER_FILTER_1_OPTIONAL"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."OTHER_FILTER_2_OPTIONAL"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."OTHER_FILTER_3_OPTIONAL"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."JOIN_TABLE_1"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."JOIN_FILTER_1"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."DELETE_FILTER_1_OPTIONAL"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."DELETE_JOIN_FILTER"',
            '"V_IBC_TABLEEXPLORER_4_CELONIS"."DELETE_TIME_FILTER"'
            ]

        ### building and executing the query ###
        q = pql.PQL()

        for col in tab_tables_cols:
            q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

        # pull data
        return self._dm._get_data_frame(q)

    ##########################
    ### THE-LIST Columns
    def __get_the_list_columns(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        if len(self.__the_list_columns) == 0:
            self.__the_list_columns = self.__getColumns()
        
        return self.__the_list_columns

    def getColumns(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        query_result = self.__get_the_list_columns()

        return query_result


    def get_ColumnsBySchemaAndTable(self, pschema: str, ptable: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_cols = self.__get_the_list_columns()

        self.__logger.debug('Schema: ' + pschema)
        self.__logger.debug('Table: ' + ptable)

        query_result = df_cols[(df_cols['_V_IBC_ALLTABCOL_4_CELONIS_SCHEMA'] == pschema)
                                           & (df_cols['_V_IBC_ALLTABCOL_4_CELONIS_ORIGINAL_TABLE_NAME'] == ptable)][[
                                                '_V_IBC_ALLTABCOL_4_CELONIS_SCHEMA', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_SOURCE_SYSTEM', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_ORIGINAL_TABLE_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_TARGET_TABLE_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_NOT_REQUIRED_COLUMN',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_PK_',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_FILTER_COL',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_ID']]
        
        query_result = query_result.drop_duplicates()
        return query_result

    def get_ColumnsBySystemAndTable(self, psystem: str, ptable: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_cols = self.__get_the_list_columns()

        self.__logger.debug('Schema: ' + psystem)
        self.__logger.debug('Table: ' + ptable)

        query_result = df_cols[
                                (df_cols['_V_IBC_ALLTABCOL_4_CELONIS_SOURCE_SYSTEM'] == psystem)
                                & (df_cols['_V_IBC_ALLTABCOL_4_CELONIS_ORIGINAL_TABLE_NAME'] == ptable)
                                & (df_cols['_V_IBC_ALLTABCOL_4_CELONIS_NOT_REQUIRED_COLUMN'] != 'X')
                              ][[
                                                '_V_IBC_ALLTABCOL_4_CELONIS_SCHEMA', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_SOURCE_SYSTEM', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_ORIGINAL_TABLE_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_TARGET_TABLE_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_NOT_REQUIRED_COLUMN',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_PK_',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_FILTER_COL',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_ID']]
        
        query_result = query_result.drop_duplicates()
        return query_result

    def get_ColumnsWithAnonymization(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_cols = self.__get_the_list_columns()

        query_result = df_cols[
                                 (df_cols['_V_IBC_ALLTABCOL_4_CELONIS_NOT_REQUIRED_COLUMN'] != 'X')
                              ][[
                                                '_V_IBC_ALLTABCOL_4_CELONIS_SCHEMA', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_SOURCE_SYSTEM', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_ORIGINAL_TABLE_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_TARGET_TABLE_NAME', 
                                                '_V_IBC_ALLTABCOL_4_CELONIS_NOT_REQUIRED_COLUMN',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_PK_',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_FILTER_COL',
                                                '_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_ID']]
        
        query_result['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'] = query_result['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'].fillna(0).astype(int)
        query_result = query_result.drop_duplicates()
        return query_result

    def get_table_key_columns(self, sSchema: str, sTable: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_cols = self.__get_the_list_columns()

        keys_ = df_cols[(df_cols['_V_IBC_ALLTABCOL_4_CELONIS_SCHEMA'] == sSchema) &
                (df_cols['_V_IBC_ALLTABCOL_4_CELONIS_ORIGINAL_TABLE_NAME'] == sTable) &
                (df_cols['_V_IBC_ALLTABCOL_4_CELONIS_PK_'] == 'X')]['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].tolist()
            
        return keys_

    def __getColumns(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        tab_all_tab_cols = [
            '"V_IBC_ALLTABCOL_4_CELONIS"."SCHEMA"', 
            '"V_IBC_ALLTABCOL_4_CELONIS"."SOURCE_SYSTEM"', 
            '"V_IBC_ALLTABCOL_4_CELONIS"."ORIGINAL_TABLE_NAME"', 
            '"V_IBC_ALLTABCOL_4_CELONIS"."TABLE_NAME"', 
            '"V_IBC_ALLTABCOL_4_CELONIS"."COLUMN_NAME"', 
            '"V_IBC_ALLTABCOL_4_CELONIS"."TARGET_TABLE_NAME"', 
            '"V_IBC_ALLTABCOL_4_CELONIS"."NOT_REQUIRED_COLUMN"',
            '"V_IBC_ALLTABCOL_4_CELONIS"."PSEUDONYMIZATION"',
            '"V_IBC_ALLTABCOL_4_CELONIS"."PK_"',
            '"V_IBC_ALLTABCOL_4_CELONIS"."FILTER_COL"',
            '"V_IBC_ALLTABCOL_4_CELONIS"."COLUMN_ID"'
            ]

        ### building and executing the query ###
        q = pql.PQL()

        for col in tab_all_tab_cols:
            q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

        # pull data
        return self._dm._get_data_frame(q)

    ##########################
    ### THE-LIST Object IDs
    def __getDPLObjectIds(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        tab_cols = [
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."SOURCESYSTEM"', 
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."SCHEMA"', 
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."DATA_POOL_ID"', 
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."PERFORMANCE_JOB_ID"', 
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."ANONYMIZATION_JOB_ID"', 
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."DELETION_JOB_ID"',             
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."FULL_DATA_JOB_ID"', 
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."FULL_EXTRACTION_ID"',
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."DELTA_DATA_JOB_ID"',
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."DELTA_EXTRACTION_ID"',
            '"IBC_CTRL_V_DPL_OBJECT_IDS"."TECH_TIMESTAMP_DATA_JOB_ID"'
            ]
        
        ### building and executing the query ###
        q = pql.PQL()

        for col in tab_cols:
            q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

        # pull data
        return self._dm._get_data_frame(q)
    
    def __get_DPL_Object_Ids(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        if len(self.__DPL_Object_Ids) == 0:
            self.__DPL_Object_Ids = self.__getDPLObjectIds()
        
        return self.__DPL_Object_Ids

    def getDPLSchemaBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_SCHEMA']]
                    
        return query_result.iat[0,0]
    
    def getDPLDataPoolIDBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_DATA_POOL_ID']]
                    
        return query_result.iat[0,0]   
    
    def getDPLPerformanceJobIDBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_PERFORMANCE_JOB_ID']]
                    
        return query_result.iat[0,0] 
    
    def getDPLAnonymizationJobIDBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_ANONYMIZATION_JOB_ID']]
                    
        return query_result.iat[0,0] 
    
    def getDPLFullJobIDBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_FULL_DATA_JOB_ID']]
                    
        return query_result.iat[0,0]   

    def getDPLDeltaJobIDBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_DELTA_DATA_JOB_ID']]
                    
        return query_result.iat[0,0] 

    def getDPLTechTimestampJobIDBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_TECH_TIMESTAMP_DATA_JOB_ID']]
                    
        return query_result.iat[0,0]  

    def getDPLDeletionJobIDBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_DELETION_JOB_ID']]
                    
        return query_result.iat[0,0]
    
    def getDPLFullExtractionIDBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_FULL_EXTRACTION_ID']]
                                
        return query_result.iat[0,0] 
    
    
    def getDPLDeltaExtractionIDBySourceSystem(self, psystem):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_Object_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_OBJECT_IDS_SOURCESYSTEM'] == psystem)][[
                    '_IBC_CTRL_V_DPL_OBJECT_IDS_DELTA_EXTRACTION_ID']]  
        
        return query_result.iat[0,0] 

    ##########################
    ### THE-LIST Use Case IDs
    def __getDPLUseCaseIds(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        tab_cols = [
            '"IBC_CTRL_V_DPL_USECASE_IDS"."USE_CASE"', 
            '"IBC_CTRL_V_DPL_USECASE_IDS"."DATA_CONNECTION"', 
            '"IBC_CTRL_V_DPL_USECASE_IDS"."DATA_POOL_ID"', 
            '"IBC_CTRL_V_DPL_USECASE_IDS"."USE_CASE_JOB_ID"'
            ]
        
        ### building and executing the query ###
        q = pql.PQL()

        for col in tab_cols:
            q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

        # pull data
        return self._dm._get_data_frame(q)

    def __get_DPL_UseCase_Ids(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        if len(self.__DPL_UseCase_Ids) == 0:
            self.__DPL_UseCase_Ids = self.__getDPLUseCaseIds()
        
        return self.__DPL_UseCase_Ids
        
    def getDPLUseCaseDataPoolID(self, pusecase: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_UseCase_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_USECASE_IDS_USE_CASE'] == pusecase)][[
                    '_IBC_CTRL_V_DPL_USECASE_IDS_DATA_POOL_ID']]
                    
        return query_result.iat[0,0]

    def getDPLUseCaseDataConnection(self, pusecase: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_UseCase_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_USECASE_IDS_USE_CASE'] == pusecase)][[
                    '_IBC_CTRL_V_DPL_USECASE_IDS_DATA_CONNECTION']]
                    
        return query_result.iat[0,0]

    def getDPLUseCaseUseCaseJobIDB(self, pusecase: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_DPL_UseCase_Ids()
        
        query_result = df_tab[(df_tab['_IBC_CTRL_V_DPL_USECASE_IDS_USE_CASE'] == pusecase)][[
                    '_IBC_CTRL_V_DPL_USECASE_IDS_USE_CASE_JOB_ID']]
                    
        return query_result.iat[0,0]

    ##########################
    ### THE-LIST Tables with anonimization
    def __getAnonTables(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        tab_tables_cols = [
            '"IBC_CTRL_V_TABLES_W_ANON"."OWNER"', 
            '"IBC_CTRL_V_TABLES_W_ANON"."SOURCESYSTEM"', 
            '"IBC_CTRL_V_TABLES_W_ANON"."ORACLE_TABLE_NAME"',
            '"IBC_CTRL_V_TABLES_W_ANON"."REQUESTED_TABLE"',
            '"IBC_CTRL_V_TABLES_W_ANON"."CLOUD_TABLE_NAME"'
            ]

        ### building and executing the query ###
        q = pql.PQL()

        for col in tab_tables_cols:
            q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

        # pull data
        return self._dm._get_data_frame(q)

    def getAnonTablesBySystem(self, sSystem: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__get_the_list_anon_tables()

        query_result = df_tab[(df_tab['_IBC_CTRL_V_TABLES_W_ANON_SOURCESYSTEM'] == sSystem)] 

        return query_result 

    def __get_the_list_anon_tables(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        if len(self.__the_list_anon_tables) == 0:
            self.__the_list_anon_tables = self.__getAnonTables()

        return self.__the_list_anon_tables

    ##########################
    ### THE-LIST System Mappings
    def __getSystemMappings(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        tab_tables_cols = [
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."OWNER"', 
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."SOURCESYSTEM"', 
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."TECH_TOKEN_SOURCESYSTEM"', 
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."SOURCESYSTEM_OLD"', 
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."DATABASE_SOURCE"', 
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."CURR_CONV"', 
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."ANONYMIZATION"',
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."DELTA_LOAD"',
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."TIMEZONE"',
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."SOURCE_"',
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."FILTER_CNT"',
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."DIVISION"',
            '"IBC_CTRL_V_SOURCESYSTEM_MAPPING"."HAS_FILTER"'            
            ]

        ### building and executing the query ###
        q = pql.PQL()

        for col in tab_tables_cols:
            q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

        # pull data
        return self._dm._get_data_frame(q)

    def getSystemMappings(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        return self.__get_the_list_system_mappings()

    def getSystemMappingsbySystem(self, sSystem: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        df_tab = self.getSystemMappings()
        
        # Systems 
        query_result = df_tab[(df_tab['_IBC_CTRL_V_SOURCESYSTEM_MAPPING_SOURCESYSTEM'] == sSystem)]    
        
        return query_result

    def SystemMappingsbySystemisZeusP(self, sSystem: str) -> bool:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        df_tab = self.getSystemMappings()
        
        # Systems 
        query_result = df_tab[(df_tab['_IBC_CTRL_V_SOURCESYSTEM_MAPPING_SOURCESYSTEM'] == sSystem)
                            & (df_tab['_IBC_CTRL_V_SOURCESYSTEM_MAPPING_SOURCE_'] == 'ZEUS Database')]    
        
        return not query_result.empty      

    def SystemMappingsbySystemisDelta(self, sSystem: str) -> bool:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        df_tab = self.getSystemMappings()
        
        # Systems 
        query_result = df_tab[(df_tab['_IBC_CTRL_V_SOURCESYSTEM_MAPPING_SOURCESYSTEM'] == sSystem)
                            & (df_tab['_IBC_CTRL_V_SOURCESYSTEM_MAPPING_DELTA_LOAD'] == 1)]    
        
        return not query_result.empty 

    def SystemMappingsbySystemisTimezone(self, sSystem: str) -> bool:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        df_tab = self.getSystemMappings()
        
        # Systems 
        query_result = df_tab[(df_tab['_IBC_CTRL_V_SOURCESYSTEM_MAPPING_SOURCESYSTEM'] == sSystem)
                            & (df_tab['_IBC_CTRL_V_SOURCESYSTEM_MAPPING_TIMEZONE'] == 1)]    
        
        return not query_result.empty  

    def SystemMappingsbySystemHasFilter(self, sSystem: str) -> bool:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        df_tab = self.getSystemMappings()
        
        # Systems 
        query_result = df_tab[(df_tab['_IBC_CTRL_V_SOURCESYSTEM_MAPPING_SOURCESYSTEM'] == sSystem)
                            & (df_tab['_IBC_CTRL_V_SOURCESYSTEM_MAPPING_HAS_FILTER'] == 1)]    
        
        return not query_result.empty 

    
    def __get_the_list_system_mappings(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        if len(self.__the_list_system_mappings) == 0:
            self.__the_list_system_mappings = self.__getSystemMappings()

        return self.__the_list_system_mappings

class TheList_EMS(object):
    __dm = 'b22e188f-e935-4269-a261-dbf6e2745627'
    
    def __init__(self, Connection: EMS_Connection, reload_datamodel = False):
        self.__logger = logging.getLogger("the_list")
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name) 
        self._dm = Connection.datamodels.find(self.__dm)
        self.__the_list_EMS_tables = []
        if reload_datamodel:
            self.__logger.info('Reload Data Model: start')
            r = self._dm.reload()
            self.__logger.info('Reload Data Model: end')
        
    def getTablesDeltaLoaded(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        columns = ['"TABLES_IN_POOL_DELTALOADED"."table_schema"', '"TABLES_IN_POOL_DELTALOADED"."table_name"']

        ### building and executing the query ###
        q = pql.PQL()

        # case key column names start with underscore
        for col in columns:
            q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

        # pull data
        return self._dm._get_data_frame(q)    

    def getTablesAllTables(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        return self.__get_the_list_EMS_tables()
    
    def __get_the_list_EMS_tables(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        if len(self.__the_list_EMS_tables) == 0:
            self.__the_list_EMS_tables = self.__getEMSTables()
        
        return self.__the_list_EMS_tables

    def getTableByTableName(self, sTable: str):
        df_tab = self.__get_the_list_EMS_tables()
        
        query_result = df_tab[(df_tab['_TABLES_IN_POOL_table_name'] == sTable)]
        
        query_result = query_result.drop_duplicates()
        
        return query_result
    
    def __getEMSTables(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        columns = ['"TABLES_IN_POOL"."table_schema"', '"TABLES_IN_POOL"."table_name"']

        ### building and executing the query ###
        q = pql.PQL()

        for col in columns:
            q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

        # pull data
        return self._dm._get_data_frame(q)

    def getUseCaseViews(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        columns = ['"IBC_CTRL_V_ADMIN_STAMP"."table_name"', '"IBC_CTRL_V_ADMIN_STAMP"."object_type"', '"IBC_CTRL_V_ADMIN_STAMP"."connection"']

        ### building and executing the query ###
        q = pql.PQL()

        for col in columns:
            q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

        # pull data
        return self._dm._get_data_frame(q)