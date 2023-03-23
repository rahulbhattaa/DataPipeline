import uuid
import sys
import re

import bosch_utils
from constants import Const
from ems_connection import EMS_Connection
from the_list import TheList, TheList_EMS
import logging
from bosch_logging import init_logging
import pandas as pd
from step_evironment import system_environment, usecase_environment
from data_pipeline_log import DataPipelineLog
      

class DataPipeline(object):  

    # Global Parameters from Central Data Pool
    __p_startDate_13 = 'fc0711a8-ffd6-45c6-a646-0adcd348cd7b'
    __p_startDate_25 = 'f3f68465-cded-4aab-868a-597768f18dd0'
    __p_startDate_31 = '0cdc5b67-96a1-4d07-90fb-6b7b76aa1d57'

    __p_startYear_13 = 'ffc765db-a486-4213-b55f-ca3655b6b073'
    __p_startYear_25 = 'a42a40da-6368-48bc-af03-18a0e2246ceb'
    __p_startYear_31 = 'a42a40da-6368-48bc-af03-18a0e2246ceb'

    __p_year_13 = '4bd7dc27-7375-482b-a22b-98ff846d83fc'
    __p_year_25 = '5765eb92-7cf7-4854-9ae4-cdeb5cd7c3c7'
    __p_year_31 = '069832d1-7460-41ff-b7c4-89fb58a136ae'    
    
    def __init__(self, Connection: EMS_Connection, reload_datamodel = False):
        print("######## DATA PIPELINE ###########")
        init_logging()
        self.__logger = logging.getLogger("data_pipeline")
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        self.__c = Const()
        self.__EMS_Connection = Connection        
        self.__Tables = TheList(self.__EMS_Connection, reload_datamodel)
        self.__Tables_EMS = TheList_EMS(self.__EMS_Connection)
        self.__dp_log = DataPipelineLog(self.__EMS_Connection)
        self.__TEST_MODE = True

        self.__lSteps = []
        self.__sUseCase = ""
        self.__lSystems = []
        self.__lTables = []  
        self.__sJobID = ""
        self.__max_extracted_records = 0
        self.__full_name_suffix = False

        self.__EMS_full_extractions_mode = self.__c.CONST_MODE_ADD
        self.__EMS_delta_extractions_mode = self.__c.CONST_MODE_ADD
        self.__EMS_extention_transformations_mode = self.__c.CONST_MODE_ADD
        self.__EMS_anonymization_transformations_mode = self.__c.CONST_MODE_ADD
        self.__EMS_performance_transformations_mode = self.__c.CONST_MODE_ADD
        self.__EMS_data_provisioning_transformations_mode = self.__c.CONST_MODE_RECREATE

        self.__max_internal_execution_mode = self.__c.CONST_MODE_UPDATE

        self.__execute_EMS_jobs = True

        self.__Reload_LogsDM = False

        # To be set to True into the execution methods
        # Value to be Checked in "execute" method of DataPipeline object
        self.__execution_started = False

    ##### PROPERTIES
    @property
    def EMS_full_extractions_mode(self) -> int: 
        result = self.__c.CONST_MODE_ADD
        if self.__EMS_full_extractions_mode <= self.__max_internal_execution_mode:        
            result = self.__EMS_full_extractions_mode
        return result
    
    @property
    def EMS_delta_extractions_mode(self) -> int: 
        result = self.__c.CONST_MODE_ADD
        if self.__EMS_delta_extractions_mode <= self.__max_internal_execution_mode:        
            result = self.__EMS_delta_extractions_mode
        return result
    
    @property
    def EMS_extension_transformations_mode(self) -> int: 
        result = self.__c.CONST_MODE_ADD
        if self.__EMS_extention_transformations_mode <= self.__max_internal_execution_mode:        
            result = self.__EMS_extention_transformations_mode
        return result
    
    @property
    def EMS_anonymization_transformations_mode(self) -> int: 
        result = self.__c.CONST_MODE_ADD
        if self.__EMS_anonymization_transformations_mode <= self.__max_internal_execution_mode:        
            result = self.__EMS_anonymization_transformations_mode
        return result
    
    @property
    def EMS_performance_transformations_mode(self) -> int: 
        result = self.__c.CONST_MODE_ADD
        if self.__EMS_performance_transformations_mode <= self.__max_internal_execution_mode:        
            result = self.__EMS_performance_transformations_mode
        return result
    
    @property
    def EMS_data_provisioning_transformations_mode(self) -> int: 
        ### should not be possible at the moment. Only RECREATE MODE allowed.
        ### RECREATE mode is set automatically in constructor. No Set possible
        result = self.__c.CONST_MODE_RECREATE
        '''
        if self.__EMS_data_provisioning_transformations_mode <= self.__max_internal_execution_mode:        
            result = self.__EMS_data_provisioning_transformations_mode
        '''
        return result

    ##### CONFIGURATION
    def set_Steps(self, lSteps: list):
        """Sets the steps which should be run in the execution.
        If not step provided, all steps will be executed.

        Args:
            lSteps (list): steps to be executed.
        """
        self.__lSteps = lSteps
     
    def set_UseCase(self, sUseCase: str):
        """Sets the use case the data pipeline objects will be created for.

        Args:
            sUseCase (str): name of the use case
        """
        self.__sUseCase = sUseCase
        
    def set_Systems(self, lSystems: list):
        """Sets the systems the data pipeline objects will be created for.
        No systems provided means "all systems".

        Args:
            lSystems (list): list of systems
        """
        self.__lSystems = lSystems
        
    def set_Tables(self, lTables: list):
        """Sets the tables the data pipeline objects will be created for.
        No tables provided means "all tables".

        Args:
            lTables (list): _description_
        """
        self.__lTables = lTables
        
    def set_JobId(self, sJobID: str):
        """Set the jobID of a previous execution to start the execution from the next configured step.
        A data pipeline execution will always stop its execution after triggering an EMS execution step

        Args:
            sJobID (str): JobId of execution which should be executed from next step.
        """
        self.__sJobID = sJobID

    def set_TestMode(self, active: bool):
        """Enables or disables the execution in test mode.
        An execution in test mode will neither creates objects not executes EMS actions

        Args:
            active (bool): True: enables test mode / False: disables test mode
        """
        self.__TEST_MODE = active
        self.__dp_log._set_TestMode(active)

    def set_MaxExtractedRecords(self, num_recs: int):
        """Sets max number of records which should be extracted.
        If set to "0" no limits will be set (all available records will be extracted)

        Args:
            num_recs (int): max number of records
        """
        self.__max_extracted_records = num_recs

    def set_FullTableNameSuffix(self, active: bool):
        """Enables table suffix for table name in full extraction.
           Enables table renaming introduction in extensions.
        Default Value is false (no suffix)

        Args:
            active (bool): table name suffix enabled or not
        """
        self.__full_name_suffix = active

    def set_EMS_full_extractions_mode(self, iMode: int):
        """Set the execution mode for EMS full extraction objects

        Args:
            iMode (int): 
                CONST_MODE_RECREATE: deletes all existing objects and recreate them (only valid if only system given) 
                CONST_MODE_UPDATE: deletes all per parameter given objects. New objects will be created
                CONST_MODE_ADD (default): Only add new objects
        """

        self.__EMS_full_extractions_mode = iMode
        

    def set_EMS_delta_extractions_mode(self, iMode: int):
        """Set the execution mode for EMS delta extraction objects

        Args:
            iMode (int): 
                CONST_MODE_RECREATE: deletes all existing objects and recreate them (only valid if only system given) 
                CONST_MODE_UPDATE: deletes all per parameter given objects. New objects will be created
                CONST_MODE_ADD (default): Only add new objects
        """

        self.__EMS_delta_extractions_mode = iMode
        

    def set_EMS_extension_transformations_mode(self, iMode: int):
        """Set the execution mode for EMS extension transformation objects

        Args:
            iMode (int): 
                CONST_MODE_RECREATE: deletes all existing objects and recreate them (only valid if only system given) 
                CONST_MODE_UPDATE: deletes all per parameter given objects. New objects will be created
                CONST_MODE_ADD (default): Only add new objects
        """

        self.__EMS_extention_transformations_mode = iMode
        

    def set_EMS_anonymization_transformations_mode(self, iMode: int):
        """Set the execution mode for EMS anonymization transformation objects

        Args:
            iMode (int): 
                CONST_MODE_RECREATE: deletes all existing objects and recreate them (only valid if only system given) 
                CONST_MODE_UPDATE: deletes all per parameter given objects. New objects will be created
                CONST_MODE_ADD (default): Only add new objects
        """

        self.__EMS_anonymization_transformations_mode = iMode
        
    
    def set_EMS_performance_transformations_mode(self, iMode: int):
        """Set the execution mode for EMS performance transformation objects

        Args:
            iMode (int): 
                CONST_MODE_RECREATE: deletes all existing objects and recreate them (only valid if only system given) 
                CONST_MODE_UPDATE: deletes all per parameter given objects. New objects will be created
                CONST_MODE_ADD (default): Only add new objects
        """

        self.__EMS_performance_transformations_mode = iMode


    ### should not be possible at the moment. Only RECREATE MODE allowed.
    ### RECREATE mode is set automatically in constructor. No Set possible
    '''
    def set_EMS_data_provisioning_transformations_mode(self, iMode: int):
        """Set the execution mode for EMS data provisioning transformation objects

        Args:
            iMode (int): 
                CONST_MODE_RECREATE: deletes all existing objects and recreate them (only valid if only system given) 
                CONST_MODE_UPDATE: deletes all per parameter given objects. New objects will be created
                CONST_MODE_ADD (default): Only add new objects
        """

        self.__EMS_data_provisioning_transformations_mode = iMode  
    '''

    def set_execute_EMS_jobs(self, active: bool):
        """Tells the execution whether the created EMS objects should be triggered (executed) or not

        Args:
            active (bool): True: EMS objects will be executed / False: EMS objects will not be executed
        """
        self.__execute_EMS_jobs = active

    def set_Delete_String(self, delete_str: str):
        """sets the delete string uses in the creation of deletions

        Args:
            delete_str (str): delete string uses in the creation of deletion transformations
            Examples:
            "DELETE "
            or
            "SELECT COUNT(*) "
            or
            "INSERT INTO pme_counts SELECT '{}', COUNT(*)"
        """
        self.__Delete_String = delete_str

    def set_Reload_LogsDM(self, active: bool):
        """Tells the execution engine whether the logging data model should be reload after execution or not

        Args:
            active (bool): True: DM reload will be executed / False: DM reload will not be executed (default)
        """
        self.__Reload_LogsDM = active
    
    ##### EXECUTION  
    
    def execute(self) -> str:
        """
        Executes the Data Pipeline object base on the given configuration.        
        
        For more documentation regarding configuration, see methods "Set_...".

        Parameters:
            none

        Returns:
            none
        """
        
        bDeletionStepRemoved = False
        bTimezoneStepRemoved = False

        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)         

        self.__dp_log.init_log(not self.__TEST_MODE)

        bvalid = self.__check_input_params()
        if bvalid == True:
                  
            if self.__sJobID != "":
                if self.__dp_log.execution_exists(self.__sJobID):  
                    next_steps = self.__dp_log.get_not_completed_steps(self.__sJobID) 

                    if not next_steps:
                        raise NameError("All steps of run_id <" + str(self.__sJobID) + "> already completed.")

                    self.set_Steps(next_steps)    
                    self.set_UseCase(self.__dp_log.get_usecase_from_execution(self.__sJobID))
                    self.set_Systems(self.__dp_log.get_systems_from_execution(self.__sJobID))
                    self.set_Tables(self.__dp_log.get_tables_from_execution(self.__sJobID))                    
                else:
                    raise NameError("Provided run_id <" + str(self.__sJobID) + "> not exists.")
            else:
                self.__sJobID = str(uuid.uuid4())                                                                                

                if self.__lSteps:                    

                    if self.__sUseCase != '' and not self.__lSystems:
                        if self.__c.CONST_STEP_ID_DELETIONS in self.__lSteps:
                            self.__lSteps.remove(self.__c.CONST_STEP_ID_DELETIONS)
                            self.__logger.info('CONST_STEP_ID_DELETIONS removed because use case parameter set')
                            bDeletionStepRemoved = True

                        if self.__c.CONST_STEP_ID_TIMEZONE in self.__lSteps:
                            self.__lSteps.remove(self.__c.CONST_STEP_ID_TIMEZONE)
                            self.__logger.info('CONST_STEP_ID_TIMEZONE removed because use case parameter set')
                            bTimezoneStepRemoved = True

                        aSystems = self.__Tables.getSystemsByUseCaseAsList(self.__sUseCase)
                        aUseCase = self.__sUseCase
                    else:
                        aSystems = self.__lSystems
                        if self.__sUseCase != '':
                            if self.__c.CONST_STEP_ID_DELETIONS in self.__lSteps:
                                self.__lSteps.remove(self.__c.CONST_STEP_ID_DELETIONS)
                                self.__logger.info('CONST_STEP_ID_DELETIONS removed because use case parameter set')
                                bDeletionStepRemoved = True
                            
                            if self.__c.CONST_STEP_ID_TIMEZONE in self.__lSteps:
                                self.__lSteps.remove(self.__c.CONST_STEP_ID_TIMEZONE)
                                self.__logger.info('CONST_STEP_ID_TIMEZONE removed because use case parameter set')
                                bTimezoneStepRemoved = True

                            aUseCase = self.__sUseCase
                        else:
                            aUseCase = ''

                    if self.__lSteps:
                        self.__dp_log.add_execution(self.__sJobID, self.__lSteps, self.__sUseCase, self.__lSystems, self.__lTables)
                        self.__dp_log.add_all_steps(self.__sJobID, self.__lSteps, aSystems, aUseCase)
                else:
                    aSteps = [self.__c.CONST_STEP_ID_FULL, self.__c.CONST_STEP_ID_EXTENSION, self.__c.CONST_STEP_ID_DELTA, self.__c.CONST_STEP_ID_PERF_VIEW, self.__c.CONST_STEP_ID_ANON_VIEW, self.__c.CONST_STEP_ID_USECASE]                    

                    if self.__sUseCase != '' and not self.__lSystems:
                        aSystems = self.__Tables.getSystemsByUseCaseAsList(self.__sUseCase)                                                                        
                        aUseCase = self.__sUseCase
                    else:
                        aSystems = self.__lSystems
                        if self.__sUseCase != '':
                            aUseCase = self.__sUseCase
                        else:
                            aUseCase = ''

                    self.__dp_log.add_execution(self.__sJobID, aSteps, self.__sUseCase, self.__lSystems, self.__lTables)
                    self.__dp_log.add_all_steps(self.__sJobID, aSteps, aSystems, aUseCase)

            self.__set_max_internal_execution_mode()
            self.__show_config()

            # Only Deletion and/or Timezone step was given                             
            if (bDeletionStepRemoved == True or bTimezoneStepRemoved == True) and not self.__lSteps:
                self.__logger.info('Only Deletion/Timezone step(s) set. DPL Execution not necessary')
            else:
                #### --> FULL
                if not self.__lSteps or self.__c.CONST_STEP_ID_FULL in self.__lSteps:
                    self.__full_load(sUseCase = self.__sUseCase, lSystems = self.__lSystems, lTables = self.__lTables)

                if self.__execution_started == False:
                    #### --> EXTENSION                
                    if not self.__lSteps or self.__c.CONST_STEP_ID_EXTENSION in self.__lSteps:                
                        self.__table_extensions(sUseCase = self.__sUseCase, lSystems = self.__lSystems, lTables = self.__lTables)

                if self.__execution_started == False:
                    #### --> DELTA
                    if not self.__lSteps or self.__c.CONST_STEP_ID_DELTA in self.__lSteps:
                        self.__delta_load(sUseCase = self.__sUseCase, lSystems = self.__lSystems, lTables = self.__lTables)
                
                #### --> DELETIONS
                if self.__execution_started == False:
                    if self.__c.CONST_STEP_ID_DELETIONS in self.__lSteps:
                        self.__table_deletions(sUseCase = self.__sUseCase, lSystems = self.__lSystems, lTables = self.__lTables)

                #### --> TIMEZONE
                if self.__execution_started == False:
                    if self.__c.CONST_STEP_ID_TIMEZONE in self.__lSteps:
                        self.__timezone(sUseCase = self.__sUseCase, lSystems = self.__lSystems, lTables = self.__lTables)

                #### --> PERF. VIEWS
                if self.__execution_started == False:
                    if not self.__lSteps or self.__c.CONST_STEP_ID_PERF_VIEW in self.__lSteps:
                        self.__perf_view(sUseCase = self.__sUseCase, lSystems = self.__lSystems, lTables = self.__lTables)
                
                #### --> ANON. VIEWS
                if self.__execution_started == False:
                    if not self.__lSteps or self.__c.CONST_STEP_ID_ANON_VIEW in self.__lSteps:
                        self.__anon_view(sUseCase = self.__sUseCase, lSystems = self.__lSystems, lTables = self.__lTables)

                #### --> DATA PROVISIONING (USE CASE)
                if self.__execution_started == False:
                    if not self.__lSteps or self.__c.CONST_STEP_ID_USECASE in self.__lSteps:
                        self.__data_provisioning(sUseCase = self.__sUseCase, lSystems = self.__lSystems, lTables = self.__lTables)                   

            if self.__Reload_LogsDM == True:
                self.__dp_log.init_log(not self.__TEST_MODE)

        else:
            raise NameError("WRONG CONFIGURATION PROVIDED !!!")

        return self.__sJobID        
    
    def __show_config(self):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        print('#### CONFIG #####')
        
        print('#        - TEST MODE: ' + str(self.__TEST_MODE))
        print('')

        print('#        - max_internal_execution_mode: ' + bosch_utils.get_execution_mode(self.__max_internal_execution_mode))
        print('')
        print('#        - EMS_full_extractions_mode: ' + bosch_utils.get_execution_mode(self.__EMS_full_extractions_mode) + ' -> ' + bosch_utils.get_execution_mode(self.EMS_full_extractions_mode))
        print('#        - EMS_delta_extractions_mode: ' + bosch_utils.get_execution_mode(self.__EMS_delta_extractions_mode) + ' -> ' + bosch_utils.get_execution_mode(self.EMS_delta_extractions_mode))
        print('#        - EMS_extension_transformations_mode: ' + bosch_utils.get_execution_mode(self.__EMS_extention_transformations_mode) + ' -> ' + bosch_utils.get_execution_mode(self.EMS_extension_transformations_mode))
        print('#        - EMS_anonymization_transformations_mode: ' + bosch_utils.get_execution_mode(self.__EMS_anonymization_transformations_mode) + ' -> ' + bosch_utils.get_execution_mode(self.EMS_anonymization_transformations_mode))
        print('#        - EMS_performance_transformations_mode: ' + bosch_utils.get_execution_mode(self.__EMS_performance_transformations_mode) + ' -> ' + bosch_utils.get_execution_mode(self.EMS_performance_transformations_mode))
        print('#        - EMS_data_provisioning_transformations_mode: ' + bosch_utils.get_execution_mode(self.__EMS_data_provisioning_transformations_mode) + ' -> ' + bosch_utils.get_execution_mode(self.EMS_data_provisioning_transformations_mode))
        print('')

        print('#        - execute EMS jobs: ' + str(self.__execute_EMS_jobs))        
        print('')

        print('#        - JOBID: ' + self.__sJobID)
        print('#        - Use Case: ' + self.__sUseCase)
        print('#        - Systems: ' + str(self.__lSystems))
        print('#        - Tables: ' + str(self.__lTables))
        print('#        - Steps: ' + bosch_utils.get_steps(self.__lSteps))        
        print('#        - Max Extracted Records (0: no restriction): ' + str(self.__max_extracted_records))
        print('#        - Full Table Name Suffix (false: no suffix): ' + str(self.__full_name_suffix))
        
        print('')
    
    def __check_input_params(self) -> bool: 
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)       
        bvalid = True
        
        #if self.__sJobID and (self.__sUseCase or self.__lSystems or self.__lTables):   # jobId and some other parameter provided
        #    print("NOT VALID INPUT PARAMETERS (1)")
        #    bvalid = False
        
        if not self.__sJobID and self.__sUseCase and self.__lSystems and self.__lTables:               # use case + systems + tables            
            msg = "NOT VALID INPUT PARAMETERS: input parameters - use case + systems + tables - NOT ALLOWED"
            self.__logger.error(msg)
            bvalid = False
                    
        elif not self.__sJobID and self.__sUseCase and not self.__lSystems and self.__lTables:           # use case + tables
            msg = "NOT VALID INPUT PARAMETERS: input parameters - use case + tables - NOT ALLOWED"
            self.__logger.error(msg)
            bvalid = False
            
        elif not self.__sJobID and not self.__sUseCase and not self.__lSystems and self.__lTables:       # tables
            msg = "NOT VALID INPUT PARAMETERS: input parameters - tables - NOT ALLOWED"
            self.__logger.error(msg)
            bvalid = False
            
        elif not self.__sJobID and not self.__sUseCase and not self.__lSystems and not self.__lTables:   # empty
            msg = "NOT VALID INPUT PARAMETERS: empty input parameters - NOT ALLOWED"
            self.__logger.error(msg)
            bvalid = False
        
        else:            
            return bvalid 

    def __set_max_internal_execution_mode(self):        
        """Execution mode levels are:
              RECREATE (highest)
              UPDATE   (mid)
              ADD      (lowest)
        """
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        if not self.__sUseCase and self.__lSystems and not self.__lTables:
            self.__max_internal_execution_mode = self.__c.CONST_MODE_RECREATE
        else:
            self.__max_internal_execution_mode = self.__c.CONST_MODE_UPDATE

    
    ##### ACTIVITIES 
    ##### --> FULL
    def __full_configuration(self, sSystem: str, sTable: str, env: system_environment, payload):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_FULL_CONFIGURATION + " (" + sSystem + "." + sTable + ")")    
            
        #reset config to empty

        sSchema = env.schema
        if sTable == 'IBC_CTRL_V_BOT':
            sSchema = 'MARS_CELONIS_FW_CENTRAL_ADM'
        

        if self.__max_extracted_records != 0:
            l_connectorSpecificConfiguration = [{'key': 'MAX_EXTRACTED_RECORDS', 'value': self.__max_extracted_records}]
        else:
            l_connectorSpecificConfiguration = []


        reset_config = {'id': '',
            'taskId': str(env.full_extraction_taskID),
            'jobId': env.full_extraction.parent.id,
            'tableExecutionItemId': None,
            'tableName': sTable,
            'renameTargetTable': False,
            'targetTableName': None,
            'columns': [],
            'joins': [],
            'dependentTables': [],
            'useManualPKs': False,
            'filterDefinition': '',
            'deltaFilterDefinition': '',
            'schemaName': sSchema,
            'creationDateColumn': None,
            'creationDateValueStart': None,
            'creationDateValueEnd': None,
            'creationDateParameterStart': None,
            'creationDateParameterEnd': None,
            'creationDateValueToday': False,
            'changeDateColumn': None,
            'changeDateOffset': 0,
            'changeDateOffsetParameter': None,
            'tableExtractionType': 'PARENT_TABLE',
            'parentTable': None,
            'dependsOn': None,
            'columnValueTable': None,
            'columnValueColumn': None,
            'columnValueTargetColumn': None,
            'columnValuesAtATime': 10000,
            'joinType': 'NONE',
            'disabled': False,
            #'connectorSpecificConfiguration': [],
            'connectorSpecificConfiguration': l_connectorSpecificConfiguration,
            'calculatedColumns': [],
            'endDateDisabled': False,
            'disableChangeLog': False,
            'parent': True}
        
        
        ######Update relevant configuration parameters on table level
        tar_tablename = sTable.replace('V_CUSN_', '') + '_' + env.system        
        if sTable == 'IBC_CTRL_V_BOT':
            tar_tablename = 'BOTLIST' + '_' + env.system
        elif self.__full_name_suffix == True:
            tar_tablename = tar_tablename + '_RELOAD'

        self.__logger.debug("tar_tablename: " + tar_tablename)

        reset_config.update(targetTableName = tar_tablename)
        reset_config.update(renameTargetTable = True)
        
        # Set Filters
        s_time_filter_optional = self.__Tables.get_Time_Filter_1_Optional_fromTable(env.system, sTable)
        b_time_filter_optional = pd.isna(s_time_filter_optional)
        s_other_filter_1_optional = self.__Tables.get_Other_Filter_1_Optional_fromTable(env.system, sTable)
        b_other_filter_1_optional = pd.isna(s_other_filter_1_optional)

        if (b_time_filter_optional == False or b_other_filter_1_optional == False):
            if (b_time_filter_optional == False and b_other_filter_1_optional == False):
                s_filter = s_time_filter_optional + ' AND ' + s_other_filter_1_optional

            if (b_time_filter_optional == False and b_other_filter_1_optional == True):
                s_filter = s_time_filter_optional 

            if (b_time_filter_optional == True and b_other_filter_1_optional == False):
                s_filter = s_other_filter_1_optional

            reset_config.update(filterDefinition = s_filter)  
        
        s_join_table_1 = self.__Tables.get_Join_Table_1_fromTable(env.system, sTable)
        b_join_table_1 = pd.isna(s_join_table_1)

        if b_join_table_1 == False: 

            s_join_condition_1_optional = self.__Tables.get_Join_Condition_1_Optional_fromTable(env.system, sTable)
            s_join_filter_1 = self.__Tables.get_Join_Filter_1_fromTable(env.system, sTable)

            new_join_config = {
                    'parentSchema': env.schema, 
                    'parentTable': s_join_table_1,
                    'childTable': sTable,
                    'usePrimaryKeys': False,
                    'customJoinPath': s_join_condition_1_optional,
                    'joinFilter': s_join_filter_1, 
                    'order': 0
            }
            reset_config['joins'] = [new_join_config]
            reset_config.update(joinType = 'JOIN')        
        
        ########## Update relevant configuration parameters on column level            

        df_col_filtered = self.__Tables.get_ColumnsBySchemaAndTable(env.schema, sTable)
        
        if 'X' not in df_col_filtered['_V_IBC_ALLTABCOL_4_CELONIS_NOT_REQUIRED_COLUMN'].unique().tolist():       #if we can extract all columns from table just add a dict for the PK flag
            df_col_filtered_pk = df_col_filtered[(df_col_filtered['_V_IBC_ALLTABCOL_4_CELONIS_PK_'] == 'X')]
            new_column_config_list = []
            for j in range(len(df_col_filtered_pk)):                   
                
                new_column_config = {
                            'columnName': df_col_filtered_pk['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].iloc[j], 
                            'fromJoin': False,
                            'anonymized': False, #anonymized,
                            'primaryKey': True, #pk,
                }

                new_column_config_list.append(new_column_config)
                
        else:       #if we cannot extract all columns from a table, add a dict for each column to extract and then add a dict for each PK
            df_col_filtered_req = df_col_filtered[df_col_filtered['_V_IBC_ALLTABCOL_4_CELONIS_NOT_REQUIRED_COLUMN'] != 'X']  #only take columns which we should extract and append it as dic
            new_column_config_list = []
            for j in range(len(df_col_filtered_req)):
                
                #print("Column Name: " + df_col_filtered_req['COLUMN_NAME'].iloc[j])
                
                new_column_config = {
                            'columnName': df_col_filtered_req['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].iloc[j], 
                            'fromJoin': False,
                            'anonymized': False, 
                            'primaryKey': False,
                }

                new_column_config_list.append(new_column_config)
                
            df_col_filtered_pk = df_col_filtered[(df_col_filtered['_V_IBC_ALLTABCOL_4_CELONIS_PK_'] == 'X')]   #afterwards append the PK columns again
            for k in range(len(df_col_filtered_pk)):

                new_column_config = {
                            'columnName': df_col_filtered_pk['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].iloc[k], 
                            'fromJoin': False,
                            'anonymized': False,
                            'primaryKey': True, #pk,
                }

                new_column_config_list.append(new_column_config)
                       

        reset_config.update(columns = new_column_config_list)  #column information of reset_config
            
        payload.append(reset_config)                

        return True
        
    def __full_execution(self, sSystem, env: system_environment, ptables):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_FULL_EXECUTION + " (" + sSystem + ")") 

        # Configuration Extraction Execution                         
        params = {
            "poolId":env.full_data_job.parent.id,
            "jobId":env.full_data_job_id,
            "mode":"FULL",            
            "executeOnlySubsetOfExtractions":True,
            "executeOnlySubsetOfTransformations":True,
            "transformations":[],
            "extractions":[{
                "extractionId":env.full_extraction_id,
                #"tables":["IBC_CTRL_DIVISIONS","IBC_CTRL_V_BOT"],
                "tables":ptables,
                "loadOnlySubsetOfTables":True
            }],
            "loadOnlySubsetOfDataModels":False,
            "dataModels":[]}     

        # Execution
        url = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/execute'.format(self.__c.CONST_CELONIS_TEAM, env.datapool_id, env.full_data_job_id)          
        d = self.__EMS_Connection.api_request(url, json = params, method = 'POST')
        
        self.__execution_started = True
        #self.__dp_log.save_steps_completion()
        #sys.exit(0)

    ##### --> EXTENSION
    def __extension_creation(self, sSystem: str, sTable: str, env: system_environment):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_EXTENSION_CREATTION + " (" + sSystem + "." + sTable + ")")
        
        t_name = sTable
        ### TODO: take care on tables which are not comming from ZEUSP
        t_name = t_name.replace('V_CUSN_', '')
        t_name = t_name + '_' + sSystem
        
        

        if self.__full_name_suffix == False:
            sql_str = """ALTER TABLE {} ADD COLUMN IF NOT EXISTS TECH_DML_FLAG VARCHAR(3);\r\n\n""".format(t_name)
        else:
            sql_str = """ALTER TABLE {} RENAME TO {}_DPL_OLD;\r\n\n
                         ALTER TABLE {}_RELOAD RENAME TO {};\r\n\n

                         ALTER TABLE {} ADD COLUMN IF NOT EXISTS TECH_DML_FLAG VARCHAR(3);\r\n\n;""".format(t_name, t_name, t_name, t_name, t_name)

        ### THE REAL EXECUTION
        if self.__TEST_MODE == False:
            Trans = env.extensions_data_job.create_transformation(t_name, statement=sql_str) 

            #self.__dp_log.add_step_completed(self.__sJobID, str(self.__c.CONST_STEP_ID_EXTENSION), sSystem) 
            return Trans.id

    ##### --> PERF VIEW
    def __perf_view_creation(self, sSystem: str, sTable: str, env: system_environment):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_PERF_VIEW_CREATTION + " (" + sSystem + "." + sTable + ")")
        

        # Create transformation task (here: Performance view) for each data source (all tables)

        ##### Tables where delta load took place

                       
        #Connect to target data job of source system
        data_job_perf = env.perf_data_job            
            
        ### Check which Performance views have been already created and exclude them depending on source system
        df_col_perf = self.__Tables.get_ColumnsBySystemAndTable(sSystem, sTable)        

        # Sort column order by Columns order index
        df_col_perf = df_col_perf.sort_values(by=["_V_IBC_ALLTABCOL_4_CELONIS_SCHEMA", "_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME","_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_ID"])
            
        df_col_perf['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'] = df_col_perf['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'].fillna(0).astype(int)

        col = df_col_perf.loc[~df_col_perf['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'].isin([1,2,3,5,6])]['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME']
        col_pseud = df_col_perf.loc[df_col_perf['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'].isin([1,2,3,5,6])]['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME']

        sCloudTableName = sTable.replace('V_CUSN_', '')        

        str_start_perf = """    
            DROP VIEW IF EXISTS "{}_{}_PERF"; 
            CREATE VIEW "{}_{}_PERF" AS
            (SELECT 
            """.format(sCloudTableName, sSystem, sCloudTableName, sSystem)

        if self.__Tables.SystemMappingsbySystemisDelta(sSystem): # If Delta Extraction
        
            from_string_perf = """                 
                ,"{}"."_CELONIS_CHANGE_DATE" AS _CELONIS_CHANGE_DATE                
                ,"{}"."TECH_DML_FLAG" 
                FROM "{}_{}" AS "{}" )
                """.format(sCloudTableName, sCloudTableName, sCloudTableName, sSystem, sCloudTableName)

        elif self.__Tables.SystemMappingsbySystemisZeusP(sSystem) == False: # If Full Extraction and Data not comming from ZeusP
            from_string_perf = """            
            ,"{}"."_CELONIS_CHANGE_DATE" AS _CELONIS_CHANGE_DATE            

            FROM "{}_{}" AS "{}")
            """ .format(sCloudTableName, sCloudTableName, sSystem, sCloudTableName)

        else: # If Full Extraction and Data comming from ZeusP
            from_string_perf = """
                ,"{}"."_CELONIS_CHANGE_DATE" AS _CELONIS_CHANGE_DATE                

                FROM "{}_{}" AS "{}")
                """ .format(sCloudTableName, sCloudTableName, sSystem, sCloudTableName)


        str_perf = ""
        bFirst = True 
        for k in col:  
            if bFirst:          
            #if k == col[0]:
                str_perf = ' "{}"."{}"'.format(sCloudTableName, k)
            else: 
                str_perf = str_perf + '\r\n,"{}"."{}"'.format(sCloudTableName, k)
            
            bFirst = False
        
        str_perf_pseud = ""
        bFirst = True 
        if col_pseud.empty:
            str_perf_pseud = ''
        else:
            for l in col_pseud:
                if bFirst:
                #if l == col_pseud[0]:
                    str_perf_pseud = '\r\n,NULL AS "{}" -- replace with NULL'.format(l)
                else: 
                    str_perf_pseud = str_perf_pseud + '\r\n,NULL AS "{}" -- replace with NULL'.format(l)

                bFirst = False
                
        sql_perf = str_start_perf+str_perf+str_perf_pseud+from_string_perf
        
        ### THE REAL EXECUTION
        if self.__TEST_MODE == True:
            ###################
            trans_name = sCloudTableName
            try:    
                print("Transformation: <<" + trans_name + ">> already exists in Data Job: <<" +  data_job_perf.name + ">>")
            except:
                print("Test Mode Active --Following transformation would be created: <<" + trans_name + ">> in Data Job: <<" +  data_job_perf.name + ">>")
            #####################        
        else:            
            
            ###################
            trans_name = sCloudTableName
            try:    
                target_transformation = data_job_perf.transformations.find(trans_name)
                self.__logger.info("Transformation: <<" + trans_name + ">> already exists in Data Job: <<" +  data_job_perf.name + ">>")
            except:
                self.__logger.info("Creating transformation: <<" + trans_name + ">> in Data Job: <<" +  data_job_perf.name + ">>")
                t_perf = data_job_perf.create_transformation(trans_name, statement=sql_perf)                 
                #self.__dp_log.add_step_completed(self.__sJobID, str(self.__c.CONST_STEP_ID_PERF_VIEW), sSystem)       
                return t_perf.id
            #####################                                   
                
    def __perf_view_execution(self, sSystem, env: system_environment, ptransformations):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_PERF_VIEW_EXECUTION + " (" + sSystem + ")") 

        # Configuration Transformation Execution
        params = {
            "poolId":env.perf_data_job.parent.id,
            "jobId":env.perf_data_job_id,
            "mode":"FULL",            
            "executeOnlySubsetOfExtractions":True,
            "executeOnlySubsetOfTransformations":True,
            "transformations":ptransformations,
            "extractions":[],
            "loadOnlySubsetOfDataModels":False,
            "dataModels":[]}     

        # Execution
        url = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/execute'.format(self.__c.CONST_CELONIS_TEAM, env.datapool_id, env.perf_data_job_id)          
        d = self.__EMS_Connection.api_request(url, json = params, method = 'POST')

        self.__execution_started = True
        #self.__dp_log.save_steps_completion()
        #sys.exit(0)

    def __anon_view_execution(self, sSystem, env: system_environment, ptransformations):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_ANON_VIEW_EXECUTION + " (" + sSystem + ")") 

        # Configuration Transformation Execution
        params = {
            "poolId":env.anon_data_job.parent.id,
            "jobId":env.anon_data_job_id,
            "mode":"FULL",            
            "executeOnlySubsetOfExtractions":True,
            "executeOnlySubsetOfTransformations":True,
            "transformations":ptransformations,
            "extractions":[],
            "loadOnlySubsetOfDataModels":False,
            "dataModels":[]}     

        # Execution
        url = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/execute'.format(self.__c.CONST_CELONIS_TEAM, env.datapool_id, env.anon_data_job_id)          
        d = self.__EMS_Connection.api_request(url, json = params, method = 'POST')

        self.__execution_started = True
        #self.__dp_log.save_steps_completion()
        #sys.exit(0)           

           
    def __extension_execution(self, sSystem, env: system_environment, ptransformations):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_EXTENSION_EXECUTION + " (" + sSystem + ")") 

        if env.extensions_data_job_id != self.__c.CONST_DUMMY_OBJECT:
            # Configuration Transformation Execution
            params = {
                "poolId":env.extensions_data_job.parent.id,
                "jobId":env.extensions_data_job_id,
                "mode":"FULL",            
                "executeOnlySubsetOfExtractions":True,
                "executeOnlySubsetOfTransformations":True,
                "transformations":ptransformations,
                "extractions":[],
                "loadOnlySubsetOfDataModels":False,
                "dataModels":[]}     

            # Execution
            url = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/execute'.format(self.__c.CONST_CELONIS_TEAM, env.datapool_id, env.extensions_data_job_id)          
            d = self.__EMS_Connection.api_request(url, json = params, method = 'POST')
            
            self.__execution_started = True

    ##### --> DELTA       
    def __delta_configuration(self, sSystem: str, sTable: str, env: system_environment, payload):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_DELTA_CONFIGURATION + " (" + sSystem + "." + sTable + ")")

        tab_hist = sTable.replace('V_CUSN_', 'V_HIST_')
        tab_cusn = sTable

        sSchema = env.schema
        if sTable == 'IBC_CTRL_V_BOT':
            sSchema = 'MARS_CELONIS_FW_CENTRAL_ADM'
        

        if self.__max_extracted_records != 0:
            l_connectorSpecificConfiguration = [{'key': 'BATCH_SIZE', 'value': 2000},
                                            {'key': 'METADATA_SOURCE', 'value': 'DRIVER_METADATA'},
                                            {'key': 'MAX_EXTRACTED_RECORDS'},
                                            {'key': 'REMOVE_DUPLICATES_WITH_ORDER', 'value': 'TECH_TIMESTAMP'},
                                            {'key': 'CHANGELOG_EXTRACTION_STRATEGY_OPTIONS',
                                            'value': 'MultipleChangeLogTableStrategy'},
                                            {'key': 'CHANGELOG_TABLE_NAME'},
                                            {'key': 'CHANGELOG_TABLE_NAME_COLUMN'},
                                            {'key': 'CHANGELOG_ID_COLUMN'},
                                            {'key': 'SOURCE_SYSTEM_JOIN_COLUMN'},
                                            {'key': 'CHANGELOG_JOIN_COLUMN'},
                                            {'key': 'CHANGELOG_CHANGE_TYPE_COLUMN'},
                                            {'key': 'CHANGELOG_DELETE_CHANGE_TYPE_IDENTIFIER'},
                                            {'key': 'CHANGELOG_CLEANUP_METHOD', 'value': 'DeleteExtractedChangeLogRecords'},
                                            {'key': 'CHANGELOG_CLEANUP_STATUS_COLUMN'},
                                            {'key': 'CHANGELOG_CLEANUP_STATUS_VALUE'},
                                            {'key': 'MAX_EXTRACTED_RECORDS', 'value': self.__max_extracted_records}]
        else:
            l_connectorSpecificConfiguration = [{'key': 'BATCH_SIZE', 'value': 2000},
                                            {'key': 'METADATA_SOURCE', 'value': 'DRIVER_METADATA'},
                                            {'key': 'MAX_EXTRACTED_RECORDS'},
                                            {'key': 'REMOVE_DUPLICATES_WITH_ORDER', 'value': 'TECH_TIMESTAMP'},
                                            {'key': 'CHANGELOG_EXTRACTION_STRATEGY_OPTIONS',
                                            'value': 'MultipleChangeLogTableStrategy'},
                                            {'key': 'CHANGELOG_TABLE_NAME'},
                                            {'key': 'CHANGELOG_TABLE_NAME_COLUMN'},
                                            {'key': 'CHANGELOG_ID_COLUMN'},
                                            {'key': 'SOURCE_SYSTEM_JOIN_COLUMN'},
                                            {'key': 'CHANGELOG_JOIN_COLUMN'},
                                            {'key': 'CHANGELOG_CHANGE_TYPE_COLUMN'},
                                            {'key': 'CHANGELOG_DELETE_CHANGE_TYPE_IDENTIFIER'},
                                            {'key': 'CHANGELOG_CLEANUP_METHOD', 'value': 'DeleteExtractedChangeLogRecords'},
                                            {'key': 'CHANGELOG_CLEANUP_STATUS_COLUMN'},
                                            {'key': 'CHANGELOG_CLEANUP_STATUS_VALUE'}]    
                
        
        #reset config to empty
        reset_config = {'id': '',
            'taskId': str(env.delta_extraction_taskID),
            'jobId': env.delta_extraction.parent.id,
            'tableExecutionItemId': None,
            'tableName': tab_hist,
            'renameTargetTable': False,
            'targetTableName': None,
            'columns': [],
            'joins': [],
            'dependentTables': [],
            'useManualPKs': False,
            'filterDefinition': '',
            'deltaFilterDefinition': '',
            'schemaName': sSchema,
            'creationDateColumn': None,
            'creationDateValueStart': None,
            'creationDateValueEnd': None,
            'creationDateParameterStart': None,
            'creationDateParameterEnd': None,
            'creationDateValueToday': False,
            'changeDateColumn': None,
            'changeDateOffset': 0,
            'changeDateOffsetType': None,
            'tableExtractionType': 'PARENT_TABLE',
            'parentTable': None,
            'dependsOn': None,
            'columnValueTable': None,
            'columnValueColumn': None,
            'columnValueTargetColumn': None,
            'columnValuesAtATime': 10000,
            'joinType': 'NONE',
            'disabled': False,
            'connectorSpecificConfiguration': l_connectorSpecificConfiguration,
            'calculatedColumns': [],
            'endDateDisabled': False,
            'disableChangeLog': False,
            'parent': True}
        
        
        ######Update relevant configuration parameters on table level    
        tar_tablename = sTable.replace('V_CUSN_', '') + '_' + env.system
        reset_config.update(targetTableName = tar_tablename)
        reset_config.update(renameTargetTable = True)
        
        reset_config.update(changeDateColumn = 'TECH_TIMESTAMP')
        reset_config.update(changeDateOffset = 10)
        reset_config.update(changeDateOffsetType = 'MINUTES')                
            
        ########## Update relevant configuration parameters on column level

        df_col_filtered = self.__Tables.get_ColumnsBySchemaAndTable(env.schema, sTable)        
        
        if 'X' not in df_col_filtered['_V_IBC_ALLTABCOL_4_CELONIS_NOT_REQUIRED_COLUMN'].unique().tolist():       #if we can extract all columns from table just add a dict for the PK flag            

            df_col_filtered_pk = df_col_filtered[(df_col_filtered['_V_IBC_ALLTABCOL_4_CELONIS_PK_'] == 'X')]
            new_column_config_list = []
            for j in range(len(df_col_filtered_pk)):

                new_column_config = {
                            'columnName': df_col_filtered_pk['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].iloc[j], 
                            'fromJoin': False,
                            'anonymized': False, #anonymized,
                            'primaryKey': True, #pk,
                }

                new_column_config_list.append(new_column_config)
                
        else:       #if we cannot extract all columns from a table, add a dict for each column to extract and then add a dict for each PK

            df_col_filtered_req = df_col_filtered[df_col_filtered['_V_IBC_ALLTABCOL_4_CELONIS_NOT_REQUIRED_COLUMN'] != 'X']  #only take columns which we should extract and append it as dic
            new_column_config_list = []
            for j in range(len(df_col_filtered_req)):

                new_column_config = {
                            'columnName': df_col_filtered_req['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].iloc[j], 
                            'fromJoin': False,
                            'anonymized': False, 
                            'primaryKey': False,
                }

                new_column_config_list.append(new_column_config)
                
            df_col_filtered_pk = df_col_filtered[(df_col_filtered['_V_IBC_ALLTABCOL_4_CELONIS_PK_'] == 'X')]   #afterwards append the PK columns again
            for k in range(len(df_col_filtered_pk)):            

                new_column_config = {
                            'columnName': df_col_filtered_pk['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].iloc[k], 
                            'fromJoin': False,
                            'anonymized': False,
                            'primaryKey': True,
                }

                new_column_config_list.append(new_column_config)
                
            new_column_config_list.append({
                        'columnName': 'TECH_DML_FLAG', 
                        'fromJoin': False,
                        'anonymized': False,
                        'primaryKey': False,
                        })

        reset_config.update(columns = new_column_config_list)  #column information of reset_config
        payload.append(reset_config)

        #self.__dp_log.add_step_completed(self.__sJobID, str(self.__c.CONST_STEP_ID_DELTA), sSystem) 

        return True
        
    def __delta_execution(self, sSystem, env: system_environment, ptables):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_DELTA_EXECUTION + " (" + sSystem + ")") 

        if env.delta_data_job_id != self.__c.CONST_DUMMY_OBJECT:
            # Configuration Extraction Execution                     
            params = {
                "poolId":env.delta_data_job.parent.id,
                "jobId":env.delta_data_job_id,
                "mode":"DELTA",            
                "executeOnlySubsetOfExtractions":True,
                "executeOnlySubsetOfTransformations":True,
                "transformations":[],
                "extractions":[{
                    "extractionId":env.delta_extraction_id,
                    "tables":ptables,
                    "loadOnlySubsetOfTables":True
                }],
                "loadOnlySubsetOfDataModels":False,
                "dataModels":[]}     

            # Execution
            url = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/execute'.format(self.__c.CONST_CELONIS_TEAM, env.datapool_id, env.delta_data_job_id)          
            d = self.__EMS_Connection.api_request(url, json = params, method = 'POST')
            self.__execution_started = True
        

        
    ##### --> DELETIONS
    def __get_sql1(self, table, t_table_wo_alias, join_table, join_condition, delete_join_filter, sSystem):        
        # sql_1 only exists if join_table != null
        # --> condition joined_table_delete_filter has to be fullfilled (to be checked!!) 
        
        sql_1 = ""
        
        if join_table:     
            if not delete_join_filter:
                print('WRONG CONFIGURATION FOR TABLE ' + table + ' delete_join_filter MISSING')
            else:
                # Transform REDLake table names to cloud names
                aux_joined_table = join_table.replace('V_CUSN_', '')
                t_joined_table = aux_joined_table + '_' + sSystem + ' ' + aux_joined_table
                #t_joined_table_wo_alias = aux_joined_table + '_' + sSystem
                #t_joined_table_alias =  aux_joined_table
                
                # Removes V_CUSN_
                t_join_condition = join_condition.replace('V_CUSN_', '')
                # Replace table with table_sourcesystem
                t_join_condition = t_join_condition.replace(table, t_table_wo_alias)
                
                t_delete_join_filter = delete_join_filter.replace('V_CUSN_', '')             
                
                sql_1 = "--- sql_1 \n\r"
                sql_1 = sql_1 + self.__Delete_String + """ FROM """ + t_table_wo_alias + """
                                        WHERE 
                                            EXISTS (
                                                SELECT 
                                                NULL

                                            FROM                                             
                                            """ + t_joined_table

                sql_1 = sql_1 + """                                                  
                                                WHERE 
                                                    """ +  t_join_condition + """                                          
                                                    AND """ + t_delete_join_filter + """
                                                ); 
                                        """                
                
        return sql_1

    def __get_sql2(self, table, t_table_wo_alias, time_filter, delete_time_filter):        
        # sql_2 only exists if time_filter != null
    
        sql_2 = ""
        if time_filter:
            if not delete_time_filter:
                print('WRONG CONFIGURATION FOR TABLE ' + table + ' delete_time_filter MISSING')
            else:
                t_delete_time_filter = delete_time_filter.replace('V_CUSN_', '')
                # Replace table with table_sourcesystem
                t_delete_time_filter = t_delete_time_filter.replace(table, t_table_wo_alias)                                    
                
                sql_2 = "--- sql_2 \n\r"
                sql_2 = sql_2 + self.__Delete_String + """ FROM """ + t_table_wo_alias + """                                                    
                                                WHERE """ + t_delete_time_filter + """;
                                                """
        return sql_2

    def __get_sql3(self, table, t_table_wo_alias, other_filter, delete_filter):        
        # sql_3 only exists if other_filter != null

        sql_3 = ""
        if other_filter:                    
            if not delete_filter:
                print('WRONG CONFIGURATION FOR TABLE ' + table + ' delete_filter MISSING')
            else:   
                # Removes V_CUSN_
                t_delete_filter = delete_filter.replace('V_CUSN_', '')
                # Replace table with table_sourcesystem
                t_delete_filter = t_delete_filter.replace(table, t_table_wo_alias)

                sql_3 = "--- sql_3 \n\r"
                
                # checks delete_filter for multi-query
                if "_query_" in delete_filter:
                    queries = delete_filter.split('_query_')
                    i = 1
                    for q in queries:
                        qry = q.strip()
                        if qry:
                            qry = qry.replace('V_CUSN_', '')
                            qry = qry.replace(table, t_table_wo_alias)
                            sql_3 = sql_3 + "------ subquery(" + str(i) + ") \n\r"
                            sql_3 = sql_3 + qry + """; \n\r"""
                            i = i + 1
                else:              
                    sql_3 = "--- sql_3 \n\r"
                    sql_3 = sql_3 + self.__Delete_String + """ FROM """ + t_table_wo_alias + """ WHERE """ + t_delete_filter + """;""" 
                
        return sql_3

    def __create_dynamic_parameter(self, tran, param, dp_param):
        
        poolId = tran.data['poolId']
        var = {
                #"id":"28b22dff-d2f7-4fb9-88de-d9a8c0e65db7",
                "poolId":poolId,
                #"taskId":"d19da5b6-9d28-4b42-b46b-6339770e908e",
                "name":param,
                "placeholder":param,
                "description":param,
                "type":"PRIVATE_CONSTANT",
                "dynamicVariableOpType":"FIND_MAX",
                "dataType":"STRING",
                "dynamicTable":"",
                "dynamicColumn":"",
                #"dynamicDataSourceId":null,
                "parameterType":"CUSTOM",
                "defaultValues":[],
                "defaultSettings":"",
                "values":[
                    {
                        "value":"",
                        "taskInstanceId":""
                    }
                ],
                "settings":{"poolVariableId":dp_param}
            }        

        tran.create_transformation_parameter(var)   

    def __create_dynamic_parameter_list(self, tran, param, dp_param):
    
        poolId = tran.data['poolId']
        var = {
            #"id":null,
            "poolId":poolId,
            #"taskId":"e41e8d63-f9e1-49bb-b2a5-29c9550085da",
            "name":param,
            "placeholder":param,
            "description":param,
            "type":"PRIVATE_CONSTANT",
            "dynamicVariableOpType":"FIND_MAX",
            "dataType":"LIST_STRING",
            "dynamicTable":"",
            "dynamicColumn":"",
            "dynamicDataSourceId":"",
            "defaultValues":[],
            "defaultSettings":"",
            "values":[
                    {
                        "value":"",
                        "taskInstanceId":""
                    }
                ],
            "settings":{"poolVariableId":dp_param}
        }
        
        tran.create_transformation_parameter(var)

    def __deletions_creation(self, sSystem, env: system_environment, ptables):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_DELETIONS_CREATTION + " (" + sSystem + ")")        

        for index, row in ptables.iterrows():  
            l_system_name = row['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME']
            l_table_name = row['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']                                        
                            
            ###
            #table: contains name of table. Eg. BSEG
            #t_table: contains name of table with source system and alias. EG. BSEG_POE POE
            #t_table_wo_alias: contains name of table with source system (Celonis Table Name. EG. BSEG_POE)

            ora_table = l_table_name
            table = l_table_name.replace('V_CUSN_', '')
            
            ora_schema = env.schema
                
            ### FOR SQL 1        
            join_table = self.__Tables.get_Join_Table_1_fromTable(sSystem, l_table_name)
            
            delete_join_filter = self.__Tables.get_Delete_Join_Filter_fromTable(sSystem, l_table_name)
            
            join_condition = self.__Tables.get_Join_Condition_1_Optional_fromTable(sSystem, l_table_name)
        
            ### FOR SQL 2
            time_filter = self.__Tables.get_Time_Filter_1_Optional_fromTable(sSystem, l_table_name)
            
            delete_time_filter = self.__Tables.get_Delete_Time_Filter_fromTable(sSystem, l_table_name)
            
            ### FOR SQL 3
            other_filter = self.__Tables.get_Other_Filter_1_Optional_fromTable(sSystem, l_table_name)

            delete_filter = self.__Tables.get_Delete_Filter_1_Optional_fromTable(sSystem, l_table_name)
                    
            sql_1 = ""
            sql_2 = ""
            sql_3 = ""        

            t_table = table + '_' + sSystem + ' ' + table
            t_table_wo_alias = table + '_' + sSystem                                                    
            
            self.__Delete_String = self.__Delete_String.format(l_table_name)        
            
            sql_1 = self.__get_sql1(table, t_table_wo_alias, join_table, join_condition, delete_join_filter, sSystem)
            sql_2 = self.__get_sql2(table, t_table_wo_alias, time_filter, delete_time_filter)     
            sql_3 = self.__get_sql3(table, t_table_wo_alias, other_filter, delete_filter)        
            
            s_sql = sql_1 + '\r\n' + sql_2 + '\r\n' + sql_3 + '\r\n'
            s_sql = s_sql.strip()    

            if s_sql:            
                ### THE REAL EXECUTION - ONE DELETION TASK PER TABLE              
                if join_table:
                    aux_joined_table = join_table.replace('V_CUSN_', '')
                    tr_name = table + ' (joined table: ' + aux_joined_table + ')'
                else:
                    tr_name = table
                        
                if self.__TEST_MODE == True:
                    print("Test Mode Active -- NO EXECUTION - " + tr_name) 
                else:  

                    # Create empty dataframe
                    column_names = ["_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME", "_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"]
                    tables_result = pd.DataFrame(columns = column_names)

                    n_row = row
                    n_row['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'] = tr_name
                    ###pandas.concat tables_result = tables_result.append(n_row)
                    tables_result = pd.concat([tables_result, n_row])

                    print("EXECUTION MODE: CREATE TRANSFORMATION {} FOR TABLE {}".format(tr_name, l_table_name))                
                    t_del = env.deletion_data_job.create_transformation(tr_name, statement=s_sql)
                    
                    #startDate_XX
                    self.__create_dynamic_parameter(t_del, 'startDate_13', self.__p_startDate_13)
                    self.__create_dynamic_parameter(t_del, 'startDate_25', self.__p_startDate_25)
                    self.__create_dynamic_parameter(t_del, 'startDate_31', self.__p_startDate_31)    

                    #startYear_XX
                    self.__create_dynamic_parameter(t_del, 'startYear_13', self.__p_startYear_13)
                    self.__create_dynamic_parameter(t_del, 'startYear_25', self.__p_startYear_25)
                    self.__create_dynamic_parameter(t_del, 'startYear_31', self.__p_startYear_31)         
                    
                    #year_XX
                    self.__create_dynamic_parameter_list(t_del, 'year_13', self.__p_year_13)
                    self.__create_dynamic_parameter_list(t_del, 'year_25', self.__p_year_25)
                    self.__create_dynamic_parameter_list(t_del, 'year_31', self.__p_year_31)
            else:
                print("EMPTY CONFIGURATION FOR TABLE {}".format(l_table_name))        

    #### TIMEZONE

    def __timezone_creation(self, env: system_environment, sSystem: str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_TIMEZONE_CREATTION + " (" + sSystem + ")") 


        str_sql = """
        DROP VIEW IF EXISTS Z_TIMEZONES_{}; 

        CREATE VIEW Z_TIMEZONES_{} AS
        ( 

            SELECT DISTINCT
                U.TZONE AS TZONE,
                U.TECH_TOKEN_SOURCESYSTEM,
                U.MANDT,
                CAST(TTZR.UTCSIGN || (Substr(TTZR.UTCDIFF,1,2)+Substr(TTZR.UTCDIFF,3,2)/60+Substr(TTZR.UTCDIFF,5,2)/60/60) AS FLOAT)  AS UTCDIFF,
                TTZD.DSTRULE,
                CAST(Substr(TTZD.DSTDIFF,1,2)+Substr( TTZD.DSTDIFF,3,2)/60+Substr( TTZD.DSTDIFF,5,2)/60/60 AS FLOAT) AS DSTDIFF,
                TTZDV.DSTRULE   AS TTZD_DSTRULE,
                TTZDV.YEARFROM,
                TTZDV.MONTHFROM,
                CASE WHEN TTZDV.WEEKDFROM - 1 = 0 THEN '7' ELSE TTZDV.WEEKDFROM END AS WEEKDFROM,
                TTZDV.WEEKDCFROM,
                TTZDV.TIMEFROM,
                TTZDV.MONTHTO,
                CASE WHEN TTZDV.WEEKDTO - 1 = 0 THEN '7' ELSE TTZDV.WEEKDTO END AS WEEKDTO,
                TTZDV.WEEKDCTO,
                TTZDV.TIMETO,
                '999' AS USR_IND
                FROM 
                    USR02_{} U
                LEFT JOIN  TTZZ_{} TTZZ
                    ON TTZZ.TECH_TOKEN_SOURCESYSTEM = U.TECH_TOKEN_SOURCESYSTEM
                    AND TTZZ.CLIENT = U.MANDT 
                    AND TTZZ.TZONE = TRIM(U.TZONE)                      
                LEFT JOIN  TTZR_{} TTZR
                    ON TTZZ.TECH_TOKEN_SOURCESYSTEM = TTZR.TECH_TOKEN_SOURCESYSTEM
                    AND TTZZ.CLIENT = TTZR.CLIENT
                    AND TTZZ.ZONERULE = TTZR.ZONERULE 
                LEFT JOIN TTZD_{} TTZD
                    ON TTZZ.TECH_TOKEN_SOURCESYSTEM = TTZD.TECH_TOKEN_SOURCESYSTEM
                    AND TTZZ.CLIENT = TTZD.CLIENT
                    AND TTZZ.DSTRULE = TTZD.DSTRULE
                LEFT JOIN (
                    SELECT
                        TECH_TOKEN_SOURCESYSTEM,
                        CLIENT,
                        YEARFROM,
                        DSTRULE,
                        MONTHFROM,
                        WEEKDFROM,
                        WEEKDCFROM,
                        TIMEFROM,
                        MONTHTO,
                        WEEKDTO,
                        WEEKDCTO,
                        TIMETO,
                        ROW_NUMBER() OVER(PARTITION BY TECH_TOKEN_SOURCESYSTEM, CLIENT, DSTRULE ORDER BY YEARFROM desc) AS RN 
                    FROM 
                        TTZDV_{}
                )   TTZDV
                    ON TTZDV.TECH_TOKEN_SOURCESYSTEM = TTZD.TECH_TOKEN_SOURCESYSTEM
                    AND TTZDV.CLIENT = TTZD.CLIENT
                    AND TTZDV.DSTRULE = TTZD.DSTRULE
                    AND TTZDV.RN = 1
            WHERE TRIM(U.TZONE) IS NOT NULL 

            UNION ALL

            SELECT DISTINCT 
                TTZCU.TZONESYS, 
                TTZCU.TECH_TOKEN_SOURCESYSTEM,
                TTZCU.CLIENT,
                CAST(TTZR.UTCSIGN || (Substr(TTZR.UTCDIFF,1,2)+Substr(TTZR.UTCDIFF,3,2)/60+Substr(TTZR.UTCDIFF,5,2)/60/60) AS FLOAT)  AS UTCDIFF,
                TTZD.DSTRULE,
                CAST(Substr(TTZD.DSTDIFF,1,2)+Substr( TTZD.DSTDIFF,3,2)/60+Substr( TTZD.DSTDIFF,5,2)/60/60 AS FLOAT) AS DSTDIFF,
                TTZDV.DSTRULE  AS TTZD_DSTRULE,
                TTZDV.YEARFROM,
                TTZDV.MONTHFROM,
                CASE WHEN TTZDV.WEEKDFROM - 1 = 0 THEN '7' ELSE TTZDV.WEEKDFROM END AS WEEKDFROM,
                TTZDV.WEEKDCFROM,
                TTZDV.TIMEFROM,
                TTZDV.MONTHTO,
                CASE WHEN TTZDV.WEEKDTO - 1 = 0 THEN '7' ELSE TTZDV.WEEKDTO END AS WEEKDTO,
                TTZDV.WEEKDCTO,
                TTZDV.TIMETO,
                CAST(NULL AS VARCHAR(10)) AS USR_IND
                FROM 
                    TTZCU_{} TTZCU
                LEFT JOIN TTZZ_{} TTZZ
                    ON TTZZ.TECH_TOKEN_SOURCESYSTEM = TTZCU.TECH_TOKEN_SOURCESYSTEM
                    AND TTZZ.CLIENT = TTZCU.CLIENT 
                    AND TTZZ.TZONE = TRIM(TTZCU.TZONESYS)
                LEFT JOIN  TTZR_{} TTZR
                    ON TTZZ.TECH_TOKEN_SOURCESYSTEM = TTZR.TECH_TOKEN_SOURCESYSTEM
                    AND TTZZ.CLIENT = TTZR.CLIENT
                    AND TTZZ.ZONERULE = TTZR.ZONERULE 
                LEFT JOIN TTZD_{} TTZD
                    ON TTZZ.TECH_TOKEN_SOURCESYSTEM = TTZD.TECH_TOKEN_SOURCESYSTEM
                    AND TTZZ.CLIENT = TTZD.CLIENT
                    AND TTZZ.DSTRULE = TTZD.DSTRULE
                LEFT JOIN (
                    SELECT
                        TECH_TOKEN_SOURCESYSTEM,
                        CLIENT,
                        YEARFROM,
                        DSTRULE,
                        MONTHFROM,
                        WEEKDFROM,
                        WEEKDCFROM,
                        TIMEFROM,
                        MONTHTO,
                        WEEKDTO,
                        WEEKDCTO,
                        TIMETO,
                        ROW_NUMBER() OVER(PARTITION BY TECH_TOKEN_SOURCESYSTEM, CLIENT, DSTRULE ORDER BY YEARFROM desc) AS RN
                    FROM 
                        TTZDV_{} AS TTZDV
                ) TTZDV
                    ON TTZDV.TECH_TOKEN_SOURCESYSTEM = TTZD.TECH_TOKEN_SOURCESYSTEM
                    AND TTZDV.CLIENT = TTZD.CLIENT
                    AND TTZDV.DSTRULE = TTZD.DSTRULE
                    AND TTZDV.RN = 1
                WHERE TTZCU.TZONESYS IS NOT NULL
        );
        """.format(sSystem, sSystem, sSystem, sSystem, sSystem, sSystem, sSystem, sSystem, sSystem, sSystem, sSystem, sSystem)        

        #print(str_sql)

        sql_perf = str_sql
        trans_name = self.__c.CONST_STANDARD_TIMEZONE
        ### THE REAL EXECUTION
        if self.__TEST_MODE == True:
            ###################
            
            try:
                target_transformation = env.full_data_job.transformations.find(trans_name)
                print("Transformation: <<" + trans_name + ">> already exists in Data Job: <<" +  target_transformation.name + ">>")
            except:
                print("Test Mode Active --Following transformation would be created: <<" + trans_name + ">> in Data Job: <<" + env.full_data_job.name  + ">>")
            #####################        
        else:

            ###################
            try:    
                target_transformation = env.full_data_job.transformations.find(trans_name)
                print("Transformation: <<" + trans_name + ">> already exists in Data Job: <<" +  target_transformation.name + ">> . It will be deleted and recreated ")
                self.__EMS_delete_transformation_by_name(env.datapool_id,env.full_data_job,trans_name)
                t_perf = env.full_data_job.create_transformation(trans_name, statement=sql_perf)         
            except:
                print("Creating transformation: <<" + trans_name + ">> in Data Job: <<" +  env.full_data_job.name + ">>")
                t_perf = env.full_data_job.create_transformation(trans_name, statement=sql_perf)         
            #####################        

            #self.__dp_log.add_step_completed(self.__sJobID, str(self.__c.CONST_STEP_ID_TIMEZONE), sSystem) 
            return t_perf.id
            
    def __timezone_execution(self, sSystem, env: system_environment, ptransformations):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_TIMEZONE_EXECUTION + " (" + sSystem + ")") 

        # Configuration Transformation Execution
        params = {
            "poolId":env.full_data_job.parent.id,
            "jobId":env.full_data_job_id,
            "mode":"FULL",            
            "executeOnlySubsetOfExtractions":True,
            "executeOnlySubsetOfTransformations":True,
            "transformations":ptransformations,
            "extractions":[],
            "loadOnlySubsetOfDataModels":False,
            "dataModels":[]}     

        # Execution
        url = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/execute'.format(self.__c.CONST_CELONIS_TEAM, env.datapool_id, env.full_data_job_id)          
        d = self.__EMS_Connection.api_request(url, json = params, method = 'POST')

        self.__execution_started = True
        #self.__dp_log.save_steps_completion()
        #sys.exit(0)


    #### ANONYMIZATION 
    def __anon_view_creation(self, sSystem: str, sTable: str, env: system_environment):
        #here goes logic 
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_ANON_VIEW + " (" + sSystem + ")")
        
        exceptions = {'CDHDR':'MANDANT',
             'CDPOS':'MANDANT',
             'BUT000':'CLIENT',
             'CRMD_ORDERADM_H':'CLIENT',
             'CRMD_ORDERADM_I':'CLIENT',
             'CRMD_SRV_SUBJECT':'CLIENT',
             'CRMM_BUT_CLASS':'CLIENT',
             'SCAPPTSEG':'CLIENT',
             'BBP_PDISC':'CLIENT',
             'BBP_PDHSC':'CLIENT',
             'RBB3_ICR_RES':'CLIENT',
             'RBB3_ICR_RES_H':'CLIENT',
             'RBB3_ICR_AWARD':'CLIENT',
             'RBB3_ICR_AWAR_H':'CLIENT',
             'RBB3_ICR_BASIC':'CLIENT',
             'RBB3_ICR_BASI_H':'CLIENT',
             'RBB3_ICR_ITEM':'CLIENT',
             'RBB3_ICR_ITEM_H':'CLIENT',
             'RBB3_PO_OUTPUT':'CLIENT',
             'QALS':'MANDANT',
             'QAVE':'MANDANT',
             'ADRC':'CLIENT'}


        data_job_anon = env.anon_data_job
        df_col_anon2 = self.__Tables.get_ColumnsBySystemAndTable(sSystem, sTable)

        # Sort column order by Columns order index
        df_col_anon2 = df_col_anon2.sort_values(by=["_V_IBC_ALLTABCOL_4_CELONIS_SCHEMA", "_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME","_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_ID"])
        sCloudTableName = sTable.replace('V_CUSN_', '')
        
        df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'] = df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'].fillna(0).astype(int)


        col = df_col_anon2.loc[(df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME']==sCloudTableName) & (~df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'].isin([1,2,3,5,6]))]['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].unique().tolist() #columns that won't be anonmyized
        col_pseud_1 = df_col_anon2.loc[(df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME']==sCloudTableName) & (df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION']==1)]['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].unique().tolist() #columns that will be anonymized using indicator 1
        col_pseud_2 = df_col_anon2.loc[(df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME']==sCloudTableName) & (df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION']==2)]['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].unique().tolist() #columns that will be anonymized using indicator 2
        col_pseud_3 = df_col_anon2.loc[(df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME']==sCloudTableName) & (df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION']==3)]['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].unique().tolist() #columns that will be anonymized using indicator 3
        col_pseud_5 = df_col_anon2.loc[(df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME']==sCloudTableName) & (df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION']==5)]['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].unique().tolist() #columns that will be anonymized using indicator 5
        col_pseud_6 = df_col_anon2.loc[(df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_TABLE_NAME']==sCloudTableName) & (df_col_anon2['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION']==6)]['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].unique().tolist() #columns that will be anonymized using indicator 6
        
        str_start_anon = """DROP VIEW IF EXISTS "{}_{}_ANON";
                            CREATE VIEW "{}_{}_ANON" AS
                            (SELECT
                            """.format(sCloudTableName,sSystem,sCloudTableName,sSystem)
                                        
        for k in col:
            if k == col[0]:
                str_anon = """ "{}"."{}"
                """.format(sCloudTableName, k)
            else: str_anon = str_anon + ""","{}"."{}"
                """.format(sCloudTableName, k)

        if col_pseud_1==[] and col_pseud_2==[] and col_pseud_3==[] and col_pseud_5==[] and col_pseud_6==[]:
            str_anon_pseud = ''
            timezone_str = ''
            join_str = ''
                        
        else:
            str_anon_pseud = ''
            timezone_str = ''
            join_str = ''

            ### BEGIN 1 ###                
            for l in col_pseud_1:
                    str_anon_pseud = str_anon_pseud + """,NULL AS "{}" -- replace with NULL
            """.format(l)
            ### END 1 ###
                            
            ### BEGIN 2 ###                        
            for l in col_pseud_2:
                str_anon_pseud = str_anon_pseud + """
                    \r\n,CASE
                        WHEN BOTLIST_{}.BOT_USERID IS NOT NULL THEN BOTLIST_{}.BOT_USERID
                        WHEN "USR02_{}"."USTYP" = {} OR "USR02_{}"."USTYP" IS NULL OR USR02_{}.USTYP = ' ' THEN NULL
                        ELSE "{}"."{}"
                    END AS "{}" -- replace by NULL if {} has USTYP = {} or NULL else use {}""".format(l, l, l, "'A'", l, l, sCloudTableName, l, l, l , "'A'", l)
                                        
                timezone_str = timezone_str + """
                    \r\n,CASE
                        WHEN USR02_{}.TZONE IS NULL THEN TTZCU.TZONESYS
                        ELSE USR02_{}.TZONE
                    END AS "TIMEZONE_{}"
                    ,CASE
                        WHEN BOTLIST_{}.BOT_USERID IS NOT NULL THEN 'R'
                        ELSE "USR02_{}"."USTYP" 
                    END AS "USER_TYPE_{}" """.format(l, l, l, l, l,l)
                if sCloudTableName not in exceptions.keys():                            
                    join_str = join_str + """
                        LEFT JOIN "USR02_{}" AS "USR02_{}" ON 1=1
                            AND "USR02_{}"."MANDT" = "{}"."MANDT"
                            AND "USR02_{}"."BNAME" = "{}"."{}"
                        LEFT JOIN "BOTLIST_{}" AS BOTLIST_{} ON 1=1
                            AND BOTLIST_{}.BOT_USERID =  {}.{}""".format(sSystem, l, l, sCloudTableName, l, sCloudTableName, l,sSystem,l,l,sCloudTableName, l)
                else:
                    exceptions_item = exceptions[sCloudTableName]                        
                    join_str = join_str + """
                        LEFT JOIN "USR02_{}" AS "USR02_{}" ON 1=1
                            AND "USR02_{}"."MANDT" = "{}"."{}"
                            AND "USR02_{}"."BNAME" = "{}"."{}"
                        LEFT JOIN "BOTLIST_{}" AS BOTLIST_{} ON 1=1
                            AND BOTLIST_{}.BOT_USERID =  {}.{} """.format(sSystem, l, l,  sCloudTableName,exceptions_item, l, sCloudTableName, l,sSystem,l,l,sCloudTableName, l)
            ### END 2 ###
                                            
            ### BEGIN 3 ###                
            for l in col_pseud_3:
                str_anon_pseud = str_anon_pseud + """
                        \r\n,CASE
                        WHEN BOTLIST_{}.BOT_USERID IS NOT NULL THEN BOTLIST_{}.BOT_USERID 
                        WHEN USR02_{}.USTYP = {} OR USR02_{}.USTYP IS NULL OR USR02_{}.USTYP = ' ' THEN NULL  
                        ELSE {}.{}
                        END AS {} -- replace by NULL if {} has USTYP = {} or NULL else use {}""".format(l,l,l,  "'A'", l, l, sCloudTableName, l, l, l ,  "'A'", l)
                
                timezone_str = timezone_str + """
                    \r\n,CASE
                        WHEN USR02_{}.TZONE IS NULL THEN TTZCU.TZONESYS
                        ELSE USR02_{}.TZONE 
                    END AS TIMEZONE_{} 
                    ,CASE 
                        WHEN BOTLIST_{}.BOT_USERID IS NOT NULL THEN 'R'
                        ELSE "USR02_{}"."USTYP" 
                    END AS USER_TYPE_{}
                    ,USR21_{}.KOSTL AS KOSTL_{}
                    ,ADCP_{}.DEPARTMENT AS DEPARTMENT_{}""".format(l, l, l, l, l, l, l, l, l,l)
                if sCloudTableName not in exceptions.keys():
                    join_str = join_str + """
                        LEFT JOIN USR02_{} AS USR02_{} ON 1=1
                            AND USR02_{}.MANDT = {}.MANDT
                            AND USR02_{}.BNAME = {}.{}
                        LEFT JOIN USR21_{} AS USR21_{} ON 1=1
                            AND USR21_{}.MANDT = {}.MANDT
                            AND USR21_{}.BNAME = {}.{}
                        LEFT JOIN
                            (SELECT CLIENT, ADDRNUMBER, PERSNUMBER, DEPARTMENT
                            FROM ADCP_{} 
                            WHERE NATION = ' ') ADCP_{} ON
                            USR21_{}.MANDT= ADCP_{}.CLIENT AND
                            USR21_{}.ADDRNUMBER= ADCP_{}.ADDRNUMBER AND
                            USR21_{}.PERSNUMBER= ADCP_{}.PERSNUMBER 
                        LEFT JOIN "BOTLIST_{}" AS BOTLIST_{} ON 1=1 
                            AND BOTLIST_{}.BOT_USERID =  {}.{}""".format(sSystem, l, l, sCloudTableName, l, sCloudTableName, l, sSystem, l, l, sCloudTableName, l, sCloudTableName, l, sSystem, l, l, l, l, l, l, l, sSystem, l, l, sCloudTableName, l)
                else:
                    exceptions_item = exceptions[sCloudTableName]
                    join_str = join_str + """
                        LEFT JOIN USR02_{} AS USR02_{} ON 1=1
                            AND USR02_{}.MANDT = {}.{}
                            AND USR02_{}.BNAME = {}.{}
                        LEFT JOIN USR21_{} AS USR21_{} ON 1=1
                            AND USR21_{}.MANDT = {}.{}
                            AND USR21_{}.BNAME = {}.{}
                        LEFT JOIN
                            (SELECT CLIENT, ADDRNUMBER, PERSNUMBER, DEPARTMENT
                            FROM ADCP_{} 
                            WHERE NATION = ' ') ADCP_{} ON
                            USR21_{}.MANDT= ADCP_{}.CLIENT AND
                            USR21_{}.ADDRNUMBER= ADCP_{}.ADDRNUMBER AND
                            USR21_{}.PERSNUMBER= ADCP_{}.PERSNUMBER 
                        LEFT JOIN "BOTLIST_{}" AS BOTLIST_{} ON 1=1 
                            AND BOTLIST_{}.BOT_USERID =  {}.{} """.format(sSystem, l, l,  sCloudTableName,exceptions_item, l, sCloudTableName, l, sSystem, l, l, sCloudTableName, exceptions_item, l, sCloudTableName, l , sSystem, l, l, l, l, l, l, l,sSystem,l,l,sCloudTableName, l )
            ### END 3 ###

            ### BEGIN 5 ###
            for l in col_pseud_5:
                str_anon_pseud = str_anon_pseud + """
                    \r\n,CASE
                        WHEN BOTLIST_{}.BOT_USERID IS NOT NULL THEN BOTLIST_{}.BOT_USERID 
                        ELSE NULL
                    END AS {} """.format(l,l, l)
                
                timezone_str = timezone_str + """
                    \r\n,CASE 
                        WHEN BOTLIST_{}.BOT_USERID IS NOT NULL THEN 'R'
                        ELSE 'A' 
                    END AS USER_TYPE_{}""".format(l, l)
                                            
                join_str = join_str + """
                    LEFT JOIN "BOTLIST_{}" AS BOTLIST_{} ON 1=1 
                        AND BOTLIST_{}.BOT_USERID =  {}.{}""".format(sSystem, l, l, sCloudTableName, l)
                    
            ### END 5 ###

            ### BEGIN 6 ###
            for l in col_pseud_6:                       
                str_anon_pseud = str_anon_pseud + '\r\n,SHA1({}) AS "{}" -- replace with SHA1 '.format(l,l)
            
            ### END 6 ###      
        if self.__Tables.SystemMappingsbySystemisDelta(sSystem):                
            from_str_anon = """
                ,"{}"."_CELONIS_CHANGE_DATE" AS _CELONIS_CHANGE_DATE
                ,"{}"."TECH_DML_FLAG"
                FROM {}_{} AS {} """.format(sCloudTableName,sCloudTableName,sCloudTableName, sSystem, sCloudTableName)
        else:
            if self.__Tables.SystemMappingsbySystemisZeusP(sSystem):                
                from_str_anon = """
                    ,"{}"."_CELONIS_CHANGE_DATE" AS _CELONIS_CHANGE_DATE
                    """.format(sCloudTableName)
            
                from_str_anon = from_str_anon + '\r\nFROM "{}_{}" AS "{}" '.format(sCloudTableName, sSystem, sCloudTableName)
            else:
                from_str_anon = """
                    ,"{}"."_CELONIS_CHANGE_DATE" AS _CELONIS_CHANGE_DATE
                    """.format(sCloudTableName)
            
                from_str_anon = from_str_anon + '\r\nFROM "{}_{}" AS "{}" '.format(sCloudTableName, sSystem, sCloudTableName)

        if sCloudTableName not in exceptions.keys():
            if 'MANDT' in col and (col_pseud_2 != [] or col_pseud_3 != []):
                string_end = """
                    LEFT JOIN "TTZCU_{}" AS "TTZCU" ON 1=1
                        AND "{}"."MANDT" = "TTZCU"."CLIENT"
                    )""".format(sSystem, sCloudTableName)
                
            else:
                string_end = """
                    )"""
        else:
            if ('CLIENT' in col or 'MANDANT' in col) and (col_pseud_2 != [] or col_pseud_3 != []):
                string_end = """
                    LEFT JOIN "TTZCU_{}" AS "TTZCU" ON 1=1
                    AND "{}"."{}" = "TTZCU"."CLIENT"
                    ) """.format(sSystem, sCloudTableName, exceptions_item)
            else:
                string_end = """
                    )"""

        sql_anon = str_start_anon+str_anon+str_anon_pseud+timezone_str+from_str_anon+join_str+string_end
            
    
        #### this code will be executed for all tables
        #### independendly of table exception list 
        
        ### THE REAL EXECUTION
        if self.__TEST_MODE == True:                
            ###################
            trans_name = sCloudTableName
            try:    
                target_transformation = data_job_anon.transformations.find(trans_name)
                print("Transformation: <<" + trans_name + ">> already exists in Data Job: <<" +  data_job_anon.name + ">>")
            except:
                print("Test Mode Active --Following transformation would be created: <<" + trans_name + ">> in Data Job: <<" +  data_job_anon.name + ">>")
            ##################### 

        else:            

            ###################
            trans_name = sCloudTableName
            try:    
                target_transformation = data_job_anon.transformations.find(trans_name)
                print("Transformation: <<" + trans_name + ">> already exists in Data Job: <<" +  data_job_anon.name + ">>")
            except:
                print("Creating transformation: <<" + trans_name + ">> in Data Job: <<" +  data_job_anon.name + ">>")
                t_anon = data_job_anon.create_transformation(sCloudTableName, statement=sql_anon)
                #self.__dp_log.add_step_completed(self.__sJobID, str(self.__c.CONST_STEP_ID_ANON_VIEW), sSystem) 
                return t_anon.id
            #####################                                

    #### DATA PROVISIONING
    def __data_provisioning_creation(self, pSystems: pd.DataFrame, lTables: pd.DataFrame, env_usecase: usecase_environment):                
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_USECASE + " (" + env_usecase.usecase + ")")
        
        res = []
        use_case_data_source_name = env_usecase.data_connection
        data_job_use_case = env_usecase.usecase_data_job

        #get list of tables which have an anonymization view
        df_col = self.__Tables.get_ColumnsWithAnonymization()

        ##filter for PCD
        df_col = df_col[df_col['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].notnull()]
        df_col = df_col[~df_col['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].str.startswith('/')]
        df_col = df_col[~df_col['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME'].str.startswith('_')]

        #filter for TECH_TIMESTAMP
        df_col = df_col[df_col['_V_IBC_ALLTABCOL_4_CELONIS_COLUMN_NAME']!= 'TECH_TIMESTAMP']

        tables_to_anonymize_1 = df_col[(df_col['_V_IBC_ALLTABCOL_4_CELONIS_PSEUDONYMIZATION'].isin([1,2,3,5,6]))]['_V_IBC_ALLTABCOL_4_CELONIS_TARGET_TABLE_NAME'].unique().tolist()
        tables_to_anonymize = list(set(tables_to_anonymize_1))   

        source_systems = pSystems['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'].tolist()

        use_case_table_list = sorted(lTables[(lTables['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'].isin(source_systems))]['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'].unique().tolist())

        for i in use_case_table_list:
            sTable = i.replace('V_CUSN_', '')
            sql_use_case = ''
            perf_str = ''
            anon_str = ''

            bFirst = True            
            for index, row in pSystems.iterrows():
                j = row['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME']
                
                sTable_System = sTable + '_' + j
                print('--> ' + sTable_System)
                check = self.__Tables_EMS.getTableByTableName(sTable_System)

                check_ts = self.__Tables.getTablesByUseCaseSystemAndTable(env_usecase.usecase, j, sTable)

                if (not check.empty) and (not check_ts.empty):                
            
                    if bFirst:                        
                        perf_str = """-- Performance Views
                            DROP VIEW IF EXISTS <%=DATASOURCE:{}%>."{}_{}_PERF"; 
                            CREATE VIEW <%=DATASOURCE:{}%>."{}_{}_PERF" AS (SELECT * FROM <%=DATASOURCE:ZEUS_{}%>."{}_{}_PERF");
                            """.format(use_case_data_source_name, sTable, j,use_case_data_source_name, sTable, j, j, sTable, j)
                        
                    else:
                        perf_str = perf_str + """
                            DROP VIEW IF EXISTS <%=DATASOURCE:{}%>."{}_{}_PERF";
                            CREATE VIEW <%=DATASOURCE:{}%>."{}_{}_PERF" AS (SELECT * FROM <%=DATASOURCE:ZEUS_{}%>."{}_{}_PERF");
                            """.format(use_case_data_source_name, sTable, j,use_case_data_source_name, sTable, j, j, sTable, j)                    

                    #check if anonymization view should be created
                    if str(sTable_System) in tables_to_anonymize:

                        if bFirst:                            
                            anon_str = """-- Anonymization Views
                                DROP VIEW IF EXISTS <%=DATASOURCE:{}%>."{}_{}_ANON";
                                CREATE VIEW <%=DATASOURCE:{}%>."{}_{}_ANON" AS (SELECT * FROM <%=DATASOURCE:ZEUS_{}%>."{}_{}_ANON");
                                """.format(use_case_data_source_name, sTable, j,use_case_data_source_name, sTable, j, j, sTable, j)
                            
                        else:                            
                            anon_str = anon_str + """
                                DROP VIEW IF EXISTS <%=DATASOURCE:{}%>."{}_{}_ANON";
                                CREATE VIEW <%=DATASOURCE:{}%>."{}_{}_ANON" AS (SELECT * FROM <%=DATASOURCE:ZEUS_{}%>."{}_{}_ANON");
                                """.format(use_case_data_source_name, sTable, j,use_case_data_source_name, sTable, j, j, sTable, j)
                            
                else:
                    print(sTable_System + ' not added to transformation')
                    continue

                bFirst = False
            
            sql_use_case = perf_str + anon_str
                        
            ### THE REAL EXECUTION
            if self.__TEST_MODE == False:            
                t_use_case = data_job_use_case.create_transformation(sTable, statement=sql_use_case)                 
                res += [t_use_case.id]  


        ### Add timezone view
        timezone_str = ''
        for index, row in pSystems.iterrows():
            s = row['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME']
            if self.__Tables.SystemMappingsbySystemisTimezone(s) == True:
                timezone_str = timezone_str + """
                            DROP VIEW IF EXISTS <%=DATASOURCE:{}%>."Z_TIMEZONES_{}";
                            CREATE VIEW <%=DATASOURCE:{}%>."Z_TIMEZONES_{}" AS (SELECT * FROM <%=DATASOURCE:ZEUS_{}%>."Z_TIMEZONES_{}");
                            """.format(use_case_data_source_name, s, use_case_data_source_name, s, s, s)

        ### THE REAL EXECUTION
        if self.__TEST_MODE == True:
            print("Transformation << Timezone View >> would be added -- Test Mode Active -- NO EXECUTION")        
        else:
            t_use_case = data_job_use_case.create_transformation('Timezone View', statement=timezone_str)                 
            res += [t_use_case.id]

        #self.__dp_log.add_step_completed(self.__sJobID, str(self.__c.CONST_STEP_ID_USECASE), '') 
        return res
       

    def __data_provisioning_execution(self, env_usecase: usecase_environment, ptransformations):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__logger.info(self.__c.CONST_ACTIVITY_DESCRIPTION + self.__c.CONST_USECASE_EXECUTION + " (" + env_usecase.usecase + ")") 

        # Configuration Transformation Execution
        params = {
            "poolId":env_usecase.usecase_data_job.parent.id,
            "jobId":env_usecase.usecase_data_job_id,
            "mode":"FULL",            
            "executeOnlySubsetOfExtractions":True,
            "executeOnlySubsetOfTransformations":True,
            "transformations":ptransformations,
            "extractions":[],
            "loadOnlySubsetOfDataModels":False,
            "dataModels":[]}     

        # Execution
        url = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/execute'.format(self.__c.CONST_CELONIS_TEAM, env_usecase.datapool_id, env_usecase.usecase_data_job_id)          
        d = self.__EMS_Connection.api_request(url, json = params, method = 'POST')
        
        self.__execution_started = True

    ##### --> CASES    
    
    def __full_load(self, sUseCase: str=None, lSystems: list=None, lTables: list=None):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__execution_started = False
        self.__logger.info(self.__c.CONST_STEP_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_FULL)     
        self.__step_dispatcher(self.__c.CONST_STEP_ID_FULL, sUseCase, lSystems, lTables)                          
        
    def __table_extensions(self, sUseCase: str=None, lSystems: list=None, lTables: list=None):        
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__execution_started = False
        self.__logger.info(self.__c.CONST_STEP_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_EXTENSION)
        self.__step_dispatcher(self.__c.CONST_STEP_ID_EXTENSION, sUseCase, lSystems, lTables)                  
    
    def __delta_load(self, sUseCase: str=None, lSystems: list=None, lTables: list=None): 
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__execution_started = False
        self.__logger.info(self.__c.CONST_STEP_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_DELTA)
        self.__step_dispatcher(self.__c.CONST_STEP_ID_DELTA, sUseCase, lSystems, lTables)                  
        
    def __table_deletions(self, sUseCase: str=None, lSystems: list=None, lTables: list=None):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__execution_started = False
        self.__logger.info(self.__c.CONST_STEP_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_DELETIONS)
        self.__step_dispatcher(self.__c.CONST_STEP_ID_DELETIONS, sUseCase, lSystems, lTables)  

    def __timezone(self, sUseCase: str=None, lSystems: list=None, lTables: list=None):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__execution_started = False
        self.__logger.info(self.__c.CONST_STEP_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_TIMEZONE)
        self.__step_dispatcher(self.__c.CONST_STEP_ID_TIMEZONE, sUseCase, lSystems, lTables)                      
    
    def __perf_view(self, sUseCase: str=None, lSystems: list=None, lTables: list=None):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__execution_started = False
        self.__logger.info(self.__c.CONST_STEP_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_PERF_VIEW)
        self.__step_dispatcher(self.__c.CONST_STEP_ID_PERF_VIEW, sUseCase, lSystems, lTables)                      

    def __anon_view(self, sUseCase: str=None, lSystems: list=None, lTables: list=None):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__execution_started = False
        self.__logger.info(self.__c.CONST_STEP_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_ANON_VIEW)
        self.__step_dispatcher(self.__c.CONST_STEP_ID_ANON_VIEW, sUseCase, lSystems, lTables)  
    
    def __data_provisioning(self, sUseCase: str=None, lSystems: list=None, lTables: list=None):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        self.__execution_started = False
        self.__logger.info(self.__c.CONST_STEP_DESCRIPTION + self.__c.CONST_STEP_DESCRIPTION_USECASE)
        self.__step_dispatcher(self.__c.CONST_STEP_ID_USECASE, sUseCase, lSystems, lTables)  
    
        
    def __step_dispatcher(self,  p_step_id, sUseCase: str=None, lSystems: list=None, lTables: list=None):                          
        
        # 1. Loop over usecases
        # 2. Loop over systems
        # 3. Dispatch pro system 
        #      dispacher(step, job_id, system, lTables)

        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        iConf = self.__get_configuration(sUseCase, lSystems, lTables)           
        
        # wrong input
        if iConf == self.__c.CONST_CONFIG_WRONG:
            raise NameError("Wrong configuration provided.")

        # use case provided
        if iConf == self.__c.CONST_CONFIG_USECASE:
            # Systems 
            query_result = self.__Tables.getSystemsByUseCase(sUseCase)
            
            if p_step_id != self.__c.CONST_STEP_ID_USECASE:
                for index, row in query_result.iterrows(): 
                    system = row['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME']
                    
                    if self.__dp_log.is_step_completed(self.__sJobID, system, p_step_id) == False:                        
                        aTables = self.__Tables.getTablesByUseCaseAndSystemAsList(sUseCase, system)
                        env = system_environment(self.__EMS_Connection, self.__Tables, system)
                        env.show()                        
                        self.__step_dispatcher_single_object(p_step_id=p_step_id, ssystem=system, env=env, lTables=aTables)  

                        self.__dp_log.add_step_completed(self.__sJobID, str(p_step_id), system)                                
                    else:
                        self.__logger.info("Step already finished")         
            else:
                env_usecase = usecase_environment(self.__EMS_Connection, self.__Tables, sUseCase)
                env_usecase.show()
                self.__step_dispatcher_data_provisioning(query_result, env_usecase)
                self.__dp_log.add_step_completed(self.__sJobID, str(p_step_id), '')
        
        # use case + system(s) provided
        if iConf == self.__c.CONST_CONFIG_USECASE_SYSTEMS:
            query_result = self.__Tables.getSystemsByUseCase(sUseCase)
            
            sys_count = len(query_result.index)
            print('Number of Systems (all): ' + str(sys_count))
            new_query_result = query_result[query_result._V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME == 'DUMMY_SYSTEM'] # intitialize empty DF
            for system in lSystems:                                                               
                a = query_result[query_result._V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME == system]
                ###pandas.concat new_query_result = new_query_result.append(a)
                new_query_result = pd.concat([new_query_result, a])
                
            sys_count = len(new_query_result.index)
            print('Number of Systems (to be processed): ' + str(sys_count))
            
            if p_step_id != self.__c.CONST_STEP_ID_USECASE:
                for index, row in new_query_result.iterrows(): 
                    system = row['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME']
                    
                    if self.__dp_log.is_step_completed(self.__sJobID, system, p_step_id) == False:
                        aTables = self.__Tables.getTablesByUseCaseAndSystemAsList(sUseCase, system)
                        env = system_environment(self.__EMS_Connection, self.__Tables, system)                
                        env.show()
                        
                        self.__step_dispatcher_single_object(p_step_id=p_step_id, ssystem=system, env=env, lTables=aTables) 
                        self.__dp_log.add_step_completed(self.__sJobID, str(p_step_id), system)                                                       
                    else:
                        self.__logger.info("Step already finished")
            else:                
                env_usecase = usecase_environment(self.__EMS_Connection, self.__Tables, sUseCase)
                env_usecase.show()  
                self.__step_dispatcher_data_provisioning(new_query_result, env_usecase)  
                self.__dp_log.add_step_completed(self.__sJobID, str(p_step_id), '')                


        # system(s) +  table(s) provided
        if iConf == self.__c.CONST_CONFIG_SYSTEMS_TABLES:
            for system in lSystems:
                
                if self.__dp_log.is_step_completed(self.__sJobID, system, p_step_id) == False:
                    env = system_environment(self.__EMS_Connection, self.__Tables, system)                
                    env.show()
                    self.__step_dispatcher_single_object(p_step_id=p_step_id, ssystem=system, env=env, lTables=lTables)
                    self.__dp_log.add_step_completed(self.__sJobID, str(p_step_id), system)                                
                else:
                    self.__logger.info("Step already finished")

        # system(s) provided
        if iConf == self.__c.CONST_CONFIG_SYSTEMS:
            for system in lSystems:
                
                if self.__dp_log.is_step_completed(self.__sJobID, system, p_step_id) == False:
                    env = system_environment(self.__EMS_Connection, self.__Tables, system)                
                    env.show()
                    self.__step_dispatcher_single_object(p_step_id=p_step_id, ssystem=system, env=env, lTables=lTables)
                    self.__dp_log.add_step_completed(self.__sJobID, str(p_step_id), system)                                
                else:
                    self.__logger.info("Step already finished")
                
    def __get_configuration(self, sUseCase: str, lSystems: list, lTables: list):
        """
        Returns:
            0: wrong input
            1: use case provided
            2: use case + system(s) provided
            3: system(s) +  table(s) provided
            4: system(s) provided
        """

        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        if sUseCase and not lSystems and not lTables:
            return self.__c.CONST_CONFIG_USECASE
        elif sUseCase and lSystems and not lTables:
            return self.__c.CONST_CONFIG_USECASE_SYSTEMS
        elif not sUseCase and lSystems and lTables:
            return self.__c.CONST_CONFIG_SYSTEMS_TABLES
        elif not sUseCase and lSystems and not lTables:
            return self.__c.CONST_CONFIG_SYSTEMS
        else:
            return 0

    def __get_tables_for_deletion_creation(self, sSystem: str):    
        
        #List all tables where TIME_FILTER_1_OPTIONAL is not empty and that are already extracted
        tab_time_filter_list = self.__Tables.getTables_Time_Filter_not_Empty_BySystem(sSystem)
        
        #List all tables where JOIN_TABLE_1 is not empty and that are already extracted
        tab_join_filter_list = self.__Tables.getTables_Join_Table_not_Empty_BySystem(sSystem)
        
        #List all tables where DELETE_FILTER_1_OPTIONAL is not empty and that are already extracted
        tab_filter_list = self.__Tables.getTables_Delete_Filter_not_Empty_BySystem(sSystem)
        

        ### Concatenate all 3 list    
        #tab_result_list = []
        # Create empty dataframe
        column_names = ["_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME", "_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"]
        tab_result_list = pd.DataFrame(columns = column_names)

        if tab_join_filter_list is not None:            
            ###pandas.concat tab_result_list = tab_result_list.append(tab_join_filter_list, ignore_index=True)
            tab_result_list = pd.concat([tab_result_list, tab_join_filter_list])

        if tab_time_filter_list is not None:            
            ###pandas.concat tab_result_list = tab_result_list.append(tab_time_filter_list, ignore_index=True)
            tab_result_list = pd.concat([tab_result_list, tab_time_filter_list])
            
        if tab_filter_list is not None:            
            ###pandas.concat tab_result_list = tab_result_list.append(tab_filter_list, ignore_index=True)
            tab_result_list = pd.concat([tab_result_list, tab_filter_list])
        
        ### Due to several use cases -> drop duplicates     
        tab_result_list = tab_result_list.drop_duplicates()

        return tab_result_list

    def __step_dispatcher_single_object(self, p_step_id: int, ssystem: str, env: system_environment, lTables: list = None):
        
        # 1. Collects the necessary tables of the system
        # 2. Deletes the content of target object (if necessary)
        # 3. Creates the single step for the table (full creation, full configuration, extension creation, ....)
        # 4. Executes the data job

        msg = self.__class__.__name__ + '.' + sys._getframe().f_code.co_name
        self.__logger.debug('===>' + msg)
        self.__logger.debug(msg + '.' + bosch_utils.get_step_name(p_step_id) + '(' + ssystem + ')')

        only_system = True
        if lTables:
            only_system = False
        
        if p_step_id == self.__c.CONST_STEP_ID_DELETIONS: 
            # To be executed system wise and for all tables with filters

            query_result = self.__get_tables_for_deletion_creation(ssystem)

        else:
            if only_system == True:
                query_result = self.__Tables.getTablesBySystem(ssystem)
            else:
                query_result = self.__Tables.getTablesBySystemAndTables(ssystem, lTables)
        
        # Handling of FULL STEP
        if p_step_id == self.__c.CONST_STEP_ID_FULL:                

            tab_count = len(query_result.index)
            print('Number of tables (all): ' + str(tab_count))            

            if self.EMS_full_extractions_mode == self.__c.CONST_MODE_RECREATE:
                self.__EMS_delete_all_extractions(env.datapool_id, env.full_data_job, env.full_extraction)
            elif self.EMS_full_extractions_mode == self.__c.CONST_MODE_UPDATE:
                self.__EMS_delete_extractions(env.datapool_id, env.full_data_job, env.full_extraction, query_result)
            
            # Tables which have not been full extracted
            for i in env.full_extraction.tables:
                query_result = query_result[query_result._V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME != i.name]

            tab_count = len(query_result.index)
            print('Number of tables (to be processed): ' + str(tab_count))
            
        
        # Handling of EXTENSION STEP
        elif p_step_id == self.__c.CONST_STEP_ID_EXTENSION:
            
            tab_count = len(query_result.index)
            print('Number of tables (all): ' + str(tab_count))            

            query_result = self.__get_full_extracted_tables_by_tables(query_result)
        
            if self.__Tables.SystemMappingsbySystemisDelta(ssystem) == True:
                if self.EMS_extension_transformations_mode == self.__c.CONST_MODE_RECREATE:                
                    self.__EMS_delete_all_transformations(env.datapool_id, env.extensions_data_job)
                elif self.EMS_extension_transformations_mode == self.__c.CONST_MODE_UPDATE:
                    self.__EMS_delete_transformation(env.system, env.datapool_id, env.extensions_data_job, query_result)

                ### Tables which have not been extended (tramsformation missing) 
                for i in env.extensions_data_job.transformations.names:
                    sys_suffix = '_' + ssystem
                    eName = i.replace(sys_suffix, '')
                    isZEUSP = self.__Tables.SystemMappingsbySystemisZeusP(ssystem)
                    if isZEUSP == True:
                        eName = 'V_CUSN_' + eName

                    query_result = query_result[query_result._V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME != eName]

                tab_count = len(query_result.index)
                print('Number of tables (to be processed): ' + str(tab_count))
            
        # Handling of DELTA STEP
        elif p_step_id == self.__c.CONST_STEP_ID_DELTA:
            
            tab_count = len(query_result.index)
            print('Number of tables (all): ' + str(tab_count))

            query_result = self.__get_full_extracted_tables_by_tables(query_result)            

            if self.__Tables.SystemMappingsbySystemisDelta(ssystem) == True:
                if self.EMS_delta_extractions_mode == self.__c.CONST_MODE_RECREATE:                
                    self.__EMS_delete_all_extractions(env.datapool_id, env.delta_data_job, env.delta_extraction)
                elif self.EMS_delta_extractions_mode == self.__c.CONST_MODE_UPDATE:               
                    self.__EMS_delete_extractions(env.datapool_id, env.delta_data_job, env.delta_extraction, query_result)                                

                # Tables which have not been delta extracted
                for i in env.delta_extraction.tables:
                    eName = i.name.replace('V_HIST_', 'V_CUSN_')
                    query_result = query_result[query_result._V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME != eName]

                tab_count = len(query_result.index)
                print('Number of tables (to be processed): ' + str(tab_count))


        # Handling of DELETION STEP
        elif p_step_id == self.__c.CONST_STEP_ID_DELETIONS:            
            tab_count = len(query_result.index)
            print('Number of tables (all): ' + str(tab_count))                        

            # All existing transformations in data pool will be deleted 
            self.__EMS_delete_all_transformations(env.datapool_id, env.deletion_data_job)

            tab_count = len(query_result.index)

        # Handling of TIMEZONE
        elif p_step_id == self.__c.CONST_STEP_ID_TIMEZONE:            
            # For timezone there is no table wise execution needed
            # the deletion of timezone transformation will be made in in the creation itself.

            tab_count = 1

        # Handling of PERF VIEW
        elif p_step_id == self.__c.CONST_STEP_ID_PERF_VIEW:
            tab_count = len(query_result.index)
            print('Number of tables (all): ' + str(tab_count))     

            query_result = self.__get_tables_for_performance(query_result, ssystem, env, self.EMS_performance_transformations_mode)

            tab_count = len(query_result.index)
            print('Number of tables (to be processed): ' + str(tab_count))

            if self.EMS_performance_transformations_mode == self.__c.CONST_MODE_RECREATE:
                self.__EMS_delete_all_transformations(env.datapool_id, env.perf_data_job)
            elif self.EMS_performance_transformations_mode == self.__c.CONST_MODE_UPDATE:
                self.__EMS_delete_transformation(env.system, env.datapool_id, env.perf_data_job, query_result)

        # Handling of Anon View
        elif p_step_id == self.__c.CONST_STEP_ID_ANON_VIEW:
            tab_count = len(query_result.index)
            print('Number of tables (all): ' + str(tab_count))                           
            
            query_result = self.__get_tables_for_anonymization(query_result, ssystem, env, self.EMS_anonymization_transformations_mode)
            
            tab_count = len(query_result.index)
            print('Number of tables (to be processed): ' + str(tab_count))

            if self.EMS_anonymization_transformations_mode == self.__c.CONST_MODE_RECREATE:
                self.__EMS_delete_all_transformations(env.datapool_id, env.anon_data_job)
            elif self.EMS_anonymization_transformations_mode == self.__c.CONST_MODE_UPDATE:
                self.__EMS_delete_transformation(env.system, env.datapool_id, env.anon_data_job, query_result)

        # Creation and Configuration
        payload_configuration = []
        payload_tables = []
        table_appended_configuration = 0

        for index, row in query_result.iterrows():  
            l_system_name = row['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME']
            l_table_name = row['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']

            if p_step_id == self.__c.CONST_STEP_ID_FULL:                    

                if self.__full_configuration(l_system_name, l_table_name, env, payload_configuration) == True:                    
                    payload_tables += [l_table_name]
                    table_appended_configuration = table_appended_configuration + 1

            elif p_step_id == self.__c.CONST_STEP_ID_EXTENSION:
                
                if self.__Tables.SystemMappingsbySystemisDelta(ssystem) == True:
                    trans_id =  self.__extension_creation(l_system_name, l_table_name, env)
                    if trans_id != '':
                        payload_tables += [trans_id]
                else:
                    self.__logger.info('Extension Step not executed (not necessary). System <' + ssystem + '> has not delta.')

            elif p_step_id == self.__c.CONST_STEP_ID_DELTA:
                
                if self.__Tables.SystemMappingsbySystemisDelta(ssystem) == True:
                    if self.__delta_configuration(l_system_name, l_table_name, env, payload_configuration) == True:
                        delta_tab_name = l_table_name.replace('V_CUSN_', 'V_HIST_')
                        payload_tables += [delta_tab_name]
                        table_appended_configuration = table_appended_configuration + 1 
                else:
                    self.__logger.info('Delta Step not executed (not necessary). System <' + ssystem + '> has not delta.')
            
            elif p_step_id == self.__c.CONST_STEP_ID_PERF_VIEW:
                
                trans_id =  self.__perf_view_creation(l_system_name, l_table_name, env)
                if trans_id != '':
                    payload_tables += [trans_id]
            
            elif p_step_id == self.__c.CONST_STEP_ID_ANON_VIEW:
                
                trans_id =  self.__anon_view_creation(l_system_name, l_table_name, env)
                if trans_id != '':
                    payload_tables += [trans_id]
                                            
        
        #Push to Celonis
        if p_step_id == self.__c.CONST_STEP_ID_FULL:                                                                                    
            url = 'https://{}.celonis.cloud/integration//api/pools/{}//jobs/{}/extractions/{}/tables/'.format(self.__c.CONST_CELONIS_TEAM, env.datapool_id, env.full_data_job_id, env.full_extraction_id)                
            print(str(table_appended_configuration) + " Extractions will be created")
            if table_appended_configuration > 0:
                ### THE REAL EXECUTION 
                if self.__TEST_MODE == True:
                    print("Test Mode Active -- NO EXECUTION")
                else:                    
                    r = self.__EMS_Connection.api_request(url,json = payload_configuration, method = 'POST')

        elif p_step_id == self.__c.CONST_STEP_ID_DELTA:
            url = 'https://{}.celonis.cloud/integration//api/pools/{}//jobs/{}/extractions/{}/tables/'.format(self.__c.CONST_CELONIS_TEAM, env.datapool_id, env.delta_data_job_id, env.delta_extraction_id)                
            print(str(table_appended_configuration) + " Delta Extractions will be created")            
            if table_appended_configuration > 0:
                ### THE REAL EXECUTION 
                if self.__TEST_MODE == True:
                    print("Test Mode Active -- NO EXECUTION")
                else:
                    r = self.__EMS_Connection.api_request(url,json = payload_configuration, method = 'POST')

        # To be executed system wise and for all tables with filters
        elif p_step_id == self.__c.CONST_STEP_ID_DELETIONS:    
            if self.__Tables.SystemMappingsbySystemHasFilter(ssystem) == True:
                self.__deletions_creation(ssystem, env, query_result)   
            else:
                self.__logger.info('Deletion Step not executed (not necessary). System <' + ssystem + '> has not filter.')

        # To be executed system wise
        elif p_step_id == self.__c.CONST_STEP_ID_TIMEZONE:
            if self.__Tables.SystemMappingsbySystemisTimezone(ssystem) == True:
                trans_id = self.__timezone_creation(env, ssystem)
                if trans_id != '': 
                    payload_tables += [trans_id]           
            else:
                self.__logger.info('Timezone Step not executed (not necessary). System <' + ssystem + '> has not timezone.')

        # Data job execution
        if self.__TEST_MODE == False:
            if self.__execute_EMS_jobs == True:
                if tab_count > 0:        
                    if p_step_id == self.__c.CONST_STEP_ID_FULL:
                        
                        self.__logger.debug("Extractions to be executed" + str(payload_tables))
                        self.__full_execution(ssystem, env, payload_tables)
                    elif p_step_id == self.__c.CONST_STEP_ID_EXTENSION:
                        
                        self.__logger.debug("Transformations to be executed" + str(payload_tables))
                        self.__extension_execution(ssystem, env, payload_tables)               
                    elif p_step_id == self.__c.CONST_STEP_ID_DELTA:
                        
                        self.__logger.debug("Extractions to be executed" + str(payload_tables))
                        self.__delta_execution(ssystem, env, payload_tables)
                    elif p_step_id == self.__c.CONST_STEP_ID_TIMEZONE:
                        
                        self.__logger.debug("Transformations to be executed" + str(payload_tables))
                        self.__timezone_execution(ssystem, env, payload_tables)  
                    elif p_step_id == self.__c.CONST_STEP_ID_PERF_VIEW:
                        
                        self.__logger.debug("Transformations to be executed" + str(payload_tables))
                        self.__perf_view_execution(ssystem, env, payload_tables)
                    elif p_step_id == self.__c.CONST_STEP_ID_ANON_VIEW:
                        
                        self.__logger.debug("Transformations to be executed" + str(payload_tables))
                        self.__anon_view_execution(ssystem, env, payload_tables)

    def __get_tables_for_anonymization(self, ltables: pd.DataFrame, ssystem: str, env: system_environment, iMode: int) -> pd.DataFrame:        
        msg = self.__class__.__name__ + '.' + sys._getframe().f_code.co_name
        self.__logger.debug('===>' + msg)

        # Consider only tables that have been already extracted in delta mode
        df_deltaload = self.__Tables.getAnonTablesBySystem(ssystem)

        ### Check which Anonymization views have been already created and exclude them 
        if iMode == self.__c.CONST_MODE_ADD:
            data_job_anon = env.anon_data_job
            df_deltaload = df_deltaload[(~df_deltaload['_IBC_CTRL_V_TABLES_W_ANON_REQUESTED_TABLE'].isin(list(data_job_anon.transformations.names)))]

        # Removes anonymization tables
        df_deltaload = df_deltaload[(~df_deltaload['_IBC_CTRL_V_TABLES_W_ANON_REQUESTED_TABLE'].isin(['USR02', 'USR21', 'ADCP']))]

        available_tables_deltaload = df_deltaload['_IBC_CTRL_V_TABLES_W_ANON_ORACLE_TABLE_NAME'].tolist()    

        available_tables_deltaload = sorted(available_tables_deltaload)        

        res = ltables[(ltables['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'].isin(available_tables_deltaload))]

        return res

    def __get_tables_for_performance(self, ltables: pd.DataFrame, ssystem: str, env: system_environment, iMode: int) -> pd.DataFrame:        
        msg = self.__class__.__name__ + '.' + sys._getframe().f_code.co_name
        self.__logger.debug('===>' + msg)

        df_load = ltables

        ### Check which Performance views have been already created and exclude them 
        if iMode == self.__c.CONST_MODE_ADD:
            data_job_anon = env.perf_data_job
            df_load = df_load[(~df_load['_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE'].isin(list(data_job_anon.transformations.names)))]

        # Removes anonymization tables
        df_load = df_load[(~df_load['_V_IBC_TABLEEXPLORER_4_CELONIS_REQUESTED_TABLE'].isin(['USR02', 'USR21', 'ADCP']))]

        available_tables_deltaload = df_load['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'].tolist()    

        available_tables_deltaload = sorted(available_tables_deltaload)        

        res = ltables[(ltables['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'].isin(available_tables_deltaload))]

        return res
    
    def __step_dispatcher_data_provisioning(self, pSystems: pd.DataFrame, env_usecase: usecase_environment):
        
        msg = self.__class__.__name__ + '.' + sys._getframe().f_code.co_name
        self.__logger.debug('===>' + msg)
        self.__logger.debug(msg + '.' + bosch_utils.get_step_name(self.__c.CONST_STEP_ID_USECASE) + '(' + env_usecase.usecase + ')')
        
        query_result = self.__Tables.getTablesByUseCase(env_usecase.usecase)
        
        # Handling of data provisioning (use case)
        
        tab_count = len(query_result.index)
        print('Number of tables (all): ' + str(tab_count))       

        query_result.drop(query_result[query_result['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'] == 'V_CUSN_USR02'].index, inplace=True)
        query_result.drop(query_result[query_result['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'] == 'V_CUSN_USR21'].index, inplace=True)
        query_result.drop(query_result[query_result['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'] == 'V_CUSN_ADCP'].index, inplace=True)
        
        tab_count = len(query_result.index)
        print('Number of tables (to be processed): ' + str(tab_count))

        
        if self.EMS_data_provisioning_transformations_mode == self.__c.CONST_MODE_RECREATE:
            self.__EMS_delete_all_transformations(env_usecase.datapool_id, env_usecase.usecase_data_job)
        else:
            raise NameError("MODE not allowed")
            ### should not be possible at the moment. Only RECREATE MODE allowed.
            ### RECREATE mode is set automatically in constructor. No Set possible
        
            '''
            self.__EMS_delete_transformation_data_provisioning(env_usecase.datapool_id, env_usecase.usecase_data_job, query_result)
            self.__EMS_delete_transformation_by_name(env_usecase.datapool_id, env_usecase.usecase_data_job, '__delete_objects__')
            self.__EMS_delete_transformation_by_name(env_usecase.datapool_id, env_usecase.usecase_data_job, 'Timezone View')            
            '''
        
        # Creation and Configuration
        payload_tables = []

        ## Creation of transformation to delete all use case views (__delete_objects__)
        self.__create_use_case_views_deletion(env_usecase)

        ####
        payload_tables = self.__data_provisioning_creation(pSystems, query_result, env_usecase)                                       
                                             
        #Push to Celonis                
        # Data job execution
        if self.__TEST_MODE == False:
            if self.__execute_EMS_jobs == True:
                if tab_count > 0:        
                    self.__logger.debug("Transformations to be executed" + str(payload_tables))
                    self.__data_provisioning_execution(env_usecase, payload_tables)    

    def __create_use_case_views_deletion(self, env_usecase: usecase_environment):
        
        msg = self.__class__.__name__ + '.' + sys._getframe().f_code.co_name
        self.__logger.debug('===>' + msg)
        self.__logger.debug(msg + '.' + bosch_utils.get_step_name(self.__c.CONST_STEP_ID_USECASE) + '(' + env_usecase.usecase + ')')

        # it will be only created for all tables
        self.__logger.info("Creating << __delete_objects__ >> Transformation for Use Case: " + env_usecase.usecase_data_job_name)
        
        df_tab_del = self.__Tables_EMS.getUseCaseViews()  
        df_tab_del = df_tab_del[(df_tab_del["_IBC_CTRL_V_ADMIN_STAMP_object_type"] == 'VIEW') & (df_tab_del["_IBC_CTRL_V_ADMIN_STAMP_connection"] == env_usecase.usecase_data_job_name)]                        
        df_tab_del.sort_values(by=["_IBC_CTRL_V_ADMIN_STAMP_table_name"], inplace = True)
        
        sql_use_case = ''
        for i, row in df_tab_del.iterrows():
            sql_use_case = sql_use_case + 'DROP VIEW IF EXISTS <%=DATASOURCE:' + env_usecase.usecase_data_job_name + '%>."' + row["_IBC_CTRL_V_ADMIN_STAMP_table_name"] + '";\n'

        ### THE REAL EXECUTION
        if self.__TEST_MODE == True:
            print("Transformation << __delete_objects__ >> would be added -- Test Mode Active -- NO EXECUTION")        
        else:            
            t_use_case = env_usecase.usecase_data_job.create_transformation('__delete_objects__', statement=sql_use_case)    
                
    def __EMS_delete_extractions(self, data_pool_id, source_data_job, source_extraction, lTables):        

        msg = self.__class__.__name__ + '.' + sys._getframe().f_code.co_name
        self.__logger.debug('===>' + msg)
        
        tableId_delete = []  
        print('')          
        print("Extractions to be deleted in JOB <" + source_data_job.name + "> and in EXTRACTION <" + source_extraction.name + ">")

        for i in source_extraction.tables:
            id = re.search('id (.+?),', str(i)).group(1)            
            t_name = re.search('name (.+?)>', str(i)).group(1)
            t_name = t_name.replace('V_HIST_', 'V_CUSN_')
            if t_name in lTables['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'].unique() :
                tableId_delete.append(id)
                print("--> " + t_name)

        if self.__TEST_MODE == True:
            print("Test Mode Active -- NO EXECUTION")

        for i in tableId_delete:
            url_delete = 'https://{}.celonis.cloud/integration//api/pools/{}//jobs/{}/extractions/{}/tables/{}'.format(self.__c.CONST_CELONIS_TEAM, data_pool_id, source_data_job.id, source_extraction.id, i)
            ### THE REAL EXECUTION
            if self.__TEST_MODE == False:
                d = self.__EMS_Connection.api_request(url_delete, method = 'DELETE')   

    def __EMS_delete_all_transformations(self, data_pool_id, source_data_job):                
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        tableId_delete = []  
        print('')          
        print("Tranformations to be deleted in JOB <" + source_data_job.name + ">")
        

        for i in source_data_job.transformations:            
            t_name = i.name        

            tableId_delete.append(i.id)
            print("--> " + t_name)

        if self.__TEST_MODE == True:
                self.__logger.info("Test Mode Active -- NO EXECUTION")

        for i in tableId_delete:
            url_delete = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/tasks/{}'.format(self.__c.CONST_CELONIS_TEAM, data_pool_id, source_data_job.id, i)

            ### THE REAL EXECUTION
            if self.__TEST_MODE == False:
                d = self.__EMS_Connection.api_request(url_delete, method = 'DELETE')


    def __EMS_delete_transformation(self, system: str, data_pool_id, source_data_job, lTables):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        tableId_delete = []  
        print('')          
        print("Tranformations to be deleted in JOB <" + source_data_job.name + ">")

        isZEUSP = self.__Tables.SystemMappingsbySystemisZeusP(system)

        for i in source_data_job.transformations:            
            t_name = i.name        
            t_name = t_name.replace('_' + system, '')
            ### taking care on tables which are comming from ZEUSP            
            if isZEUSP == True:
                t_name = 'V_CUSN_' + t_name

            if t_name in lTables['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'].unique() :
                tableId_delete.append(i.id)
                print("--> " + t_name)

        if self.__TEST_MODE == True:
                self.__logger.info("Test Mode Active -- NO EXECUTION")

        for i in tableId_delete:
            url_delete = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/tasks/{}'.format(self.__c.CONST_CELONIS_TEAM, data_pool_id, source_data_job.id, i)

            ### THE REAL EXECUTION
            if self.__TEST_MODE == False:
                d = self.__EMS_Connection.api_request(url_delete, method = 'DELETE')

    def __EMS_delete_transformation_data_provisioning(self, data_pool_id, source_data_job, lTables):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        tableId_delete = []  
        print('')          
        print("Tranformations to be deleted in JOB <" + source_data_job.name + ">")

        system = ''

        for i in source_data_job.transformations:            
            t_name = i.name        
            t_name = t_name.replace('_' + system, '')            
            t_name = 'V_CUSN_' + t_name

            if t_name in lTables['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME'].unique() :
                tableId_delete.append(i.id)
                print("--> " + t_name.replace('V_CUSN_', ''))

        if self.__TEST_MODE == True:
                self.__logger.info("Test Mode Active -- NO EXECUTION")

        for i in tableId_delete:
            url_delete = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/tasks/{}'.format(self.__c.CONST_CELONIS_TEAM, data_pool_id, source_data_job.id, i)

            ### THE REAL EXECUTION
            if self.__TEST_MODE == False:
                d = self.__EMS_Connection.api_request(url_delete, method = 'DELETE')

    #Overloading the deletion with different parameters to delete transformation 
    def __EMS_delete_transformation_by_name(self, data_pool_id, source_data_job, transname:str):
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)
        
        tableId_delete = ''
        print(' ')          
        print("Tranformations to be deleted in JOB <" + source_data_job.name + ">")

        for i in source_data_job.transformations:            
            t_name = i.name
            if t_name == transname:
                tableId_delete = i.id
                print("--> " + t_name)

        if self.__TEST_MODE == True:
                print("Test Mode Active -- NO EXECUTION")

        if tableId_delete != '':
            url_delete = 'https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/tasks/{}'.format(self.__c.CONST_CELONIS_TEAM, data_pool_id, source_data_job.id, tableId_delete)

            ### THE REAL EXECUTION
            if self.__TEST_MODE == False:
                d = self.__EMS_Connection.api_request(url_delete, method = 'DELETE')


    def __get_full_extracted_tables_by_tables(self, query_result):
        # Tables which have already been full extracted
        # Create empty dataframe
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        column_names = ["_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME", "_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME"]
        tables_result = pd.DataFrame(columns = column_names)
        full = self.__Tables_EMS.getTablesAllTables()

        for index, row in query_result.iterrows():
            t_name = row['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']
            t_name = t_name.replace('V_CUSN_', '')
            t_system = row['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME']
            t_name = t_name + '_' + t_system    
            
            elem = full[(full['_TABLES_IN_POOL_table_name'] == t_name)]
            
            if not elem.empty:
                ###pandas.concat tables_result = tables_result.append(row)
                
                data = [[row['_V_IBC_TABLEEXPLORER_4_CELONIS_SOURCESYSTEM_NAME'], row['_V_IBC_TABLEEXPLORER_4_CELONIS_ORACLE_TABLE_NAME']]]
                df_new_row = pd.DataFrame(data, columns = tables_result.columns)
                tables_result = pd.concat([tables_result, df_new_row])
        
        return tables_result      

    def __EMS_delete_all_extractions(self, data_pool_id, source_data_job, source_extraction):        
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        tableId_delete = []  

        self.__logger.info("Extractions to be deleted in JOB <" + source_data_job.name + "> and in EXTRACTION <" + source_extraction.name + ">")

        for i in source_extraction.tables:
            id = re.search('id (.+?),', str(i)).group(1)  
            tableId_delete.append(id)
            t_name = re.search('name (.+?)>', str(i)).group(1)       
            self.__logger.info('--> ' + t_name)

        if self.__TEST_MODE == False:
            for i in tableId_delete:
                url_delete = 'https://{}.celonis.cloud/integration//api/pools/{}//jobs/{}/extractions/{}/tables/{}'.format(self.__c.CONST_CELONIS_TEAM, data_pool_id, source_data_job.id, source_extraction.id, i)                        
                d = self.__EMS_Connection.api_request(url_delete, method = 'DELETE')
        else:
            self.__logger.info("NO EXECUTION")



