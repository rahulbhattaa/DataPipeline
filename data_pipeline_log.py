import pandas as pd
from datetime import datetime
from pycelonis import pql
import time
import logging

from pyrsistent import pset
from bosch_logging import init_logging
import sys
from constants import Const

from ems_connection import EMS_Connection


class DataPipelineLog(object):    
    __pipeline_datapool_id = '9a9d72fe-8ee1-49b8-912f-3ac62f4d7a96'
    __pipeline_datamodel_id = 'b8c9716e-cc0e-41d6-aea1-0ce0610b5d34'

    __logging_steps_completion_cols = ['run_id', 'step_id', 'completed', 'system', 'end_time']    
    ###pandas.concat __logging_execution_config_cols = ['run_id', 'steps', 'use_case', 'systems', 'tables', 'start_time']
    __logging_execution_config_cols = [
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_RUN_ID', 
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_STEPS', 
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_USE_CASE', 
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_SYSTEMS', 
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_TABLES',
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_START_TIME'
            ]

    def __init__(self, Connection: EMS_Connection):
        print("######## DATA PIPELINE LOG ###########")
        init_logging()
        self.__logger = logging.getLogger("data_pipeline_log")        
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        
        self.__steps_completion_df = pd.DataFrame(columns = self.__logging_steps_completion_cols, dtype=object) 
        self.__execution_conf_df = pd.DataFrame(columns = self.__logging_execution_config_cols, dtype=object) 

        self.__EMS_Connection = Connection 
        self.__c = Const()

        self.__metadata_pool = self.__EMS_Connection.pools.find(self.__pipeline_datapool_id)            
        self._dm = Connection.datamodels.find(self.__pipeline_datamodel_id)        
        #if reload_datamodel:
        #    self.__logger.info('Reload Data Model: start')
        #    r = self._dm.reload()        
        #    self.__logger.info('Reload Data Model: end') 

        self.__ExecutionConfiguration = []
        #self.__load_steps_completion()
        self.__TEST_MODE = True

    def init_log(self, reload_datamodel: bool = True):
        if reload_datamodel:
            self.__logger.info('Reload Data Model: start')
            r = self._dm.reload()        
            self.__logger.info('Reload Data Model: end')
        self.__load_steps_completion()
        

    def get_steps_completion_df(self):
        return self.__steps_completion_df

    def set_steps_completion_df(self, value):
        self.__steps_completion_df = value

    def _set_TestMode(self, active: bool):        
        self.__TEST_MODE = active

    steps_completion_df = property( get_steps_completion_df, set_steps_completion_df )

    ### EXECUTIONS
    def add_execution(self, run_id: str, pSteps: list, pUseCase: str, pSystems: list, pTables: list):    
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        
        p_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        bAddUseCase = False
        if pUseCase != '':
            bAddUseCase = True

        if self.__TEST_MODE == False:
            p_log = 'DATA_PIPELINE_EXECUTION_CONFIGURATION'
            p_recreateTable = False

            if self.execution_exists(run_id):
                raise NameError("run_id already exists. It can not be handled as new execution")
            else:
                # Insert            
                aSteps = pSteps
                if not bAddUseCase and self.__c.CONST_STEP_ID_USECASE in pSteps:
                    aSteps.remove(self.__c.CONST_STEP_ID_USECASE)
                    if not aSteps: # if empty
                        aSteps.append(self.__c.CONST_STEP_NONE)

                s_Steps = list(map(str, aSteps))
                ###pandas.concat log = pd.Series([run_id, ','.join(s_Steps), pUseCase, ','.join(pSystems), ','.join(pTables), p_timestamp], index = self.__logging_execution_config_cols)
                d_data = [[run_id, ','.join(s_Steps), pUseCase, ','.join(pSystems), ','.join(pTables), p_timestamp]]
                log = pd.DataFrame(d_data, columns = self.__logging_execution_config_cols)

                ###pandas.concat self.__execution_conf_df = self.__execution_conf_df.append(log, ignore_index=True)
                self.__execution_conf_df = pd.concat([self.__execution_conf_df, log], ignore_index=True)

                ###pandas.concat self.__ExecutionConfiguration = self.__ExecutionConfiguration.append(log, ignore_index=True) 
                self.__ExecutionConfiguration = pd.concat([self.__ExecutionConfiguration, log], ignore_index=True)

                ###pandas.concat
                self.__execution_conf_df.rename(
                    columns={
                        '_DATA_PIPELINE_EXECUTION_CONFIGURATION_RUN_ID': 'run_id', 
                        '_DATA_PIPELINE_EXECUTION_CONFIGURATION_STEPS': 'steps', 
                        '_DATA_PIPELINE_EXECUTION_CONFIGURATION_USE_CASE': 'use_case',
                        '_DATA_PIPELINE_EXECUTION_CONFIGURATION_SYSTEMS': 'systems',
                        '_DATA_PIPELINE_EXECUTION_CONFIGURATION_TABLES': 'tables',
                        '_DATA_PIPELINE_EXECUTION_CONFIGURATION_START_TIME': 'start_time',
                    },
                    inplace=True
                )

                # Save                            
                if p_recreateTable: 
                    print('Re-Creating: ' + p_log)
                    
                    ttt_column_config = [
                        {"columnName":"run_id","fieldLength":80,"columnType":"STRING"},
                        {"columnName":"steps","fieldLength":4000,"columnType":"STRING"},
                        {"columnName":"use_case","fieldLength":80,"columnType":"STRING"},
                        {"columnName":"systems","fieldLength":4000,"columnType":"STRING"},
                        {"columnName":"tables","fieldLength":4000,"columnType":"STRING"},
                        {"columnName":"start_time","fieldLength":80,"columnType":"STRING"}
                    ]
                                
                    ttt = self.__metadata_pool.create_table(
                        table_name=p_log,
                        df_or_path=self.__execution_conf_df,
                        if_exists="drop",
                        column_config=ttt_column_config,
                        wait_for_finish = True
                    )

                    print('Table: ' + p_log + ' created')
                else:
                    self.__logger.info('Adding new execution into ' + p_log)
                    self.__metadata_pool.upsert_table(
                        table_name = p_log, 
                        df_or_path = self.__execution_conf_df, 
                        #primary_keys = ['run_id']
                        primary_keys = []
                    )

    def get_execution(self, run_id: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        df_tab = self.__getExecutionConfiguration()                
        
        query_result = df_tab[(df_tab['_DATA_PIPELINE_EXECUTION_CONFIGURATION_RUN_ID'] == run_id)][[
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_STEPS', 
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_USE_CASE',
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_SYSTEMS',
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_TABLES',
            '_DATA_PIPELINE_EXECUTION_CONFIGURATION_START_TIME'
            ]]

        return query_result

    def get_steps_from_execution(self, run_id: str) -> list:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
                
        query_result = self.get_execution(run_id)

        res = query_result['_DATA_PIPELINE_EXECUTION_CONFIGURATION_STEPS'].iloc[0]
        return res.split(',')

    def get_usecase_from_execution(self, run_id: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
                
        query_result = self.get_execution(run_id)

        return query_result['_DATA_PIPELINE_EXECUTION_CONFIGURATION_USE_CASE'].iloc[0]
    
    def get_systems_from_execution(self, run_id: str) -> list:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
                
        query_result = self.get_execution(run_id)

        res = query_result['_DATA_PIPELINE_EXECUTION_CONFIGURATION_SYSTEMS'].iloc[0]
        if res != '':
            return res.split(',')
        else:
            return []

    def get_tables_from_execution(self, run_id: str) -> list:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
                
        query_result = self.get_execution(run_id)

        res = query_result['_DATA_PIPELINE_EXECUTION_CONFIGURATION_TABLES'].iloc[0]
        if res != '':
            return res.split(',')
        else:
            return []


    def execution_exists(self, run_id: str):

        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
                
        query_result = self.get_execution(run_id)

        res = True
        if query_result.empty:
            res = False    
        return res

    def __getExecutionConfiguration(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        if len(self.__ExecutionConfiguration) == 0:        

            tab_tables_cols = [
            '"DATA_PIPELINE_EXECUTION_CONFIGURATION"."RUN_ID"', 
            '"DATA_PIPELINE_EXECUTION_CONFIGURATION"."STEPS"', 
            '"DATA_PIPELINE_EXECUTION_CONFIGURATION"."USE_CASE"', 
            '"DATA_PIPELINE_EXECUTION_CONFIGURATION"."SYSTEMS"', 
            '"DATA_PIPELINE_EXECUTION_CONFIGURATION"."TABLES"',
            '"DATA_PIPELINE_EXECUTION_CONFIGURATION"."START_TIME"'
            ]

            ### building and executing the query ###
            q = pql.PQL()

            for col in tab_tables_cols:
                q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

            # pull data
            self.__ExecutionConfiguration = self._dm._get_data_frame(q)
        
        return self.__ExecutionConfiguration

    ### STEPS COMPLETION
    def __load_steps_completion(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        if len(self.__steps_completion_df) == 0:

            tab_tables_cols = [
            '"DATA_PIPELINE_STEPS_COMPLETION"."RUN_ID"', 
            '"DATA_PIPELINE_STEPS_COMPLETION"."STEP_ID"', 
            '"DATA_PIPELINE_STEPS_COMPLETION"."COMPLETED"', 
            '"DATA_PIPELINE_STEPS_COMPLETION"."SYSTEM"',
            '"DATA_PIPELINE_STEPS_COMPLETION"."END_TIME"'
            ]

            ### building and executing the query ###
            q = pql.PQL()

            for col in tab_tables_cols:
                q += pql.PQLColumn(col,'_' + col.replace('"','').replace('.','_'))

            # pull data
            self.steps_completion_df = self._dm._get_data_frame(q)

            self.steps_completion_df.rename(
                columns={
                    '_DATA_PIPELINE_STEPS_COMPLETION_RUN_ID': 'run_id', 
                    '_DATA_PIPELINE_STEPS_COMPLETION_STEP_ID': 'step_id', 
                    '_DATA_PIPELINE_STEPS_COMPLETION_COMPLETED': 'completed',
                    '_DATA_PIPELINE_STEPS_COMPLETION_SYSTEM': 'system',
                    '_DATA_PIPELINE_STEPS_COMPLETION_END_TIME': 'end_time'
                },
                inplace=True
            )
        


    def add_step_completed(self, run_id: str, pStep: str, pSystem: str):    
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        
        p_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        if self.__TEST_MODE == False:
            self.__logger.debug('Set step completed <' + str(pStep) + '> for run_id <' + run_id + '>')
            p_completed = 'X'
            
            if pSystem != '':
                self.steps_completion_df.loc[                
                        (self.steps_completion_df['run_id'] == run_id) &  
                        (self.steps_completion_df['system'] == pSystem) &
                        (self.steps_completion_df['step_id'] == pStep)
                    , 'completed'
                ] = p_completed

                self.steps_completion_df.loc[                
                        (self.steps_completion_df['run_id'] == run_id) &  
                        (self.steps_completion_df['system'] == pSystem) &
                        (self.steps_completion_df['step_id'] == pStep)
                    , 'end_time'
                ] = p_timestamp

                self.save_steps_completion()    
            else: ## for data provisioning system is not knew
                if int(pStep) == self.__c.CONST_STEP_ID_USECASE:
                    self.steps_completion_df.loc[                
                            (self.steps_completion_df['run_id'] == run_id) &                          
                            (self.steps_completion_df['step_id'] == pStep)
                        , 'completed'
                    ] = p_completed

                    self.steps_completion_df.loc[                
                            (self.steps_completion_df['run_id'] == run_id) &                          
                            (self.steps_completion_df['step_id'] == pStep)
                        , 'end_time'
                    ] = p_timestamp

                    self.save_steps_completion()
                else:
                    raise NameError("Empty system not allowed for this step")    


  
    def add_all_steps(self, run_id: str, pSteps: list, pSystems: list, pUseCase: str):    
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        
        bAddUseCase = False        
        if pUseCase != '':
            bAddUseCase = True

        if self.__TEST_MODE == False:
            p_log = 'DATA_PIPELINE_STEPS_COMPLETION'
            p_completed = ''        
            p_timestamp = ''
            
            self.__logger.info('Adding all steps for execution ' + p_log)            

            ###pandas.concat if not self.execution_exists(run_id):
            if self.execution_exists(run_id):
                # Insert
                for s in pSteps:
                    str_Step = str(s)
                    if (s == self.__c.CONST_STEP_ID_USECASE and bAddUseCase) or (s != self.__c.CONST_STEP_ID_USECASE): 
                        for sy in pSystems:
                            s_system = sy                    
                            ###pandas.concat log = {'run_id': run_id, 'step_id': str_Step, 'completed': p_completed , 'system': s_system, 'end_time': p_timestamp}
                            ###pandas.concat  self.steps_completion_df = self.steps_completion_df.append(log, ignore_index=True)
                            
                            d_data = [[run_id, str_Step, p_completed, s_system, p_timestamp]]
                            log = pd.DataFrame(d_data, columns = self.__logging_steps_completion_cols)
                            self.steps_completion_df = pd.concat([self.steps_completion_df, log], ignore_index=True)



                self.save_steps_completion()

            else:
                ###pandas.concat raise NameError("run_id <" + str(run_id) + "> already exists.")    
                raise NameError("run_id <" + str(run_id) + "> does not exists.")

    def get_next_steps_completion(self, run_id):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)        

        df = self.steps_completion_df[
                    (self.steps_completion_df['run_id'] == run_id) &
                    (self.steps_completion_df['completed'] != 'X')
        ]

        if df.empty:
            min_val = -1
        else:
            min_val = df[df.step_id == df.step_id.min()]['step_id'].iloc[0]
        
        return min_val

    def get_not_completed_steps(self, run_id: str) -> list:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)        

        df = self.steps_completion_df[
                    (self.steps_completion_df['run_id'] == run_id) &
                    (self.steps_completion_df['completed'] != 'X')
        ]

        res = df['step_id'].tolist()  
        res = list(dict.fromkeys(res))
        res.sort()

        res_int = list(map(int, res))
        return res_int
    
    def get_not_completed_steps_by_system(self, run_id: str, pSystem: str) -> list:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)        

        df = self.steps_completion_df[
                    (self.steps_completion_df['run_id'] == run_id) &
                    (self.steps_completion_df['system'] == pSystem) &
                    (self.steps_completion_df['completed'] != 'X')
        ]

        res = df['step_id'].tolist()  
        res.sort()

        res_int = list(map(int, res))
        return res_int

    def is_step_completed(self, run_id: str, pSystem: str, pStep: int) -> bool:
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)        

        s_step = str(pStep)
        df = self.steps_completion_df[
                    (self.steps_completion_df['run_id'] == run_id) &
                    (self.steps_completion_df['system'] == pSystem) &
                    (self.steps_completion_df['step_id'] == s_step) &
                    (self.steps_completion_df['completed'] == 'X')
        ]

        res = True
        if df.empty:
            res = False
        return res

    def save_steps_completion(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)        

        if self.__TEST_MODE == False:
            p_log = 'DATA_PIPELINE_STEPS_COMPLETION'

            self.__logger.info('Updating steps completion ' + p_log)
            ttt = self.__metadata_pool.create_table(
                    table_name=p_log,
                    df_or_path=self.steps_completion_df,
                    if_exists="drop",
                    #column_config=ttt_column_config,
                    wait_for_finish = True
                )
