from bosch_logging import init_logging
from ems_connection import EMS_Connection
import logging
import sys
from the_list import TheList
from constants import Const

class system_environment(object):
    #Object IDs
    __schema = ''
    __datapool_id = ''
    __full_data_job_id = ''
    __full_extraction_id = ''
    __delta_data_job_id = ''
    __delta_extraction_id = ''
    __deletion_data_job_id = ''
    __anon_data_job_id = ''
    __perf_data_job_id = ''
    __extensions_data_job_id = ''
    __full_extraction_taskID = ''
    __delta_extraction_taskID = ''

    __datapool = None
    __full_data_job = None
    __full_extraction = None
    __delta_data_job = None
    __delta_extraction = None
    __anon_data_job = None
    __perf_data_job = None
    __extensions_data_job = None
    __deletion_data_job = None
    
    def __init__(self, Connection: EMS_Connection, the_list: TheList, system: str):

        init_logging()        
        self.__logger = logging.getLogger("step_environment")
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        self.__EMS_Connection = Connection
        self.__Tables = the_list
        self.__c = Const()

        self.__system = system
        self.__schema = self.__Tables.getDPLSchemaBySourceSystem(system)
        self.__datapool_id = self.__Tables.getDPLDataPoolIDBySourceSystem(system)
        self.__full_data_job_id = self.__Tables.getDPLFullJobIDBySourceSystem(system)
        self.__full_extraction_id = self.__Tables.getDPLFullExtractionIDBySourceSystem(system)
        self.__delta_data_job_id = self.__Tables.getDPLDeltaJobIDBySourceSystem(system)
        self.__delta_extraction_id = self.__Tables.getDPLDeltaExtractionIDBySourceSystem(system)
        self.__anon_data_job_id = self.__Tables.getDPLAnonymizationJobIDBySourceSystem(system)
        self.__perf_data_job_id = self.__Tables.getDPLPerformanceJobIDBySourceSystem(system)
        self.__extensions_data_job_id = self.__Tables.getDPLTechTimestampJobIDBySourceSystem(system)
        self.__deletion_data_job_id = self.__Tables.getDPLDeletionJobIDBySourceSystem(system)

    @property
    def system(self): return self.__system

    @property
    def schema(self): return self.__schema

    @property
    def datapool_id(self): return self.__datapool_id

    @property
    def full_data_job_id(self): return self.__full_data_job_id

    @property
    def full_extraction_id(self): return self.__full_extraction_id

    @property
    def delta_data_job_id(self):         
        if self.__delta_data_job_id is None:
            res = self.__c.CONST_DUMMY_OBJECT
        else:
            res = self.__delta_data_job_id
        return res

    @property
    def delta_data_job(self):
        if self.delta_data_job_id != self.__c.CONST_DUMMY_OBJECT:
            self.__delta_data_job = self.datapool.data_jobs.find(self.delta_data_job_id)
        return self.__delta_data_job

    @property
    def delta_extraction_id(self): 
        if self.__delta_extraction_id is None:
            res = self.__c.CONST_DUMMY_OBJECT
        else:
            res = self.__delta_extraction_id
        return res
    
    @property
    def delta_extraction(self):
        if self.delta_extraction_id != self.__c.CONST_DUMMY_OBJECT:
            self.__delta_extraction = self.delta_data_job.extractions.find(self.delta_extraction_id)
        return self.__delta_extraction

    @property
    def anon_data_job_id(self): return self.__anon_data_job_id

    @property
    def deletion_data_job_id(self): 
        if self.__deletion_data_job_id is None:
            res = self.__c.CONST_DUMMY_OBJECT
        else:
            res = self.__deletion_data_job_id
        return res

    @property
    def deletion_data_job(self):
        if self.deletion_data_job_id != self.__c.CONST_DUMMY_OBJECT:
            self.__deletion_data_job = self.datapool.data_jobs.find(self.deletion_data_job_id)
        return self.__deletion_data_job

    @property
    def perf_data_job_id(self): return self.__perf_data_job_id

    @property
    def extensions_data_job_id(self): 
        if self.__extensions_data_job_id is None:
            res = self.__c.CONST_DUMMY_OBJECT
        else:
            res = self.__extensions_data_job_id
        return res

    @property
    def extensions_data_job(self):
        if self.extensions_data_job_id != self.__c.CONST_DUMMY_OBJECT:
            self.__extensions_data_job = self.datapool.data_jobs.find(self.extensions_data_job_id)
        return self.__extensions_data_job

    @property 
    def full_extraction_taskID(self):
        if not self.__full_extraction_taskID:
            taskId_r = self.__EMS_Connection.api_request('https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/extractions/{}'.format(self.__c.CONST_CELONIS_TEAM, self.datapool_id, self.full_data_job_id, self.full_extraction_id),method = 'GET')
            self.__full_extraction_taskID = taskId_r['taskId']
        
        return self.__full_extraction_taskID

    @property 
    def delta_extraction_taskID(self):
        if not self.__delta_extraction_taskID:
            taskId_r = self.__EMS_Connection.api_request('https://{}.celonis.cloud/integration//api/pools/{}/jobs/{}/extractions/{}'.format(self.__c.CONST_CELONIS_TEAM, self.datapool_id, self.delta_data_job_id, self.delta_extraction_id),method = 'GET')
            self.__delta_extraction_taskID = taskId_r['taskId']
        
        return self.__delta_extraction_taskID

    #--

    ####
    # DPL Object IDs
    @property
    def datapool(self):
        if self.__datapool_id != "":
            self.__datapool = self.__EMS_Connection.pools.find(self.datapool_id)
        return self.__datapool

    @property
    def datapool_name(self): 
        result = ""
        if self.datapool != None:
            result = self.datapool.name

        return result

    @property
    def full_data_job(self):
        if self.__full_data_job_id != "":
            self.__full_data_job = self.datapool.data_jobs.find(self.full_data_job_id)
        return self.__full_data_job

    @property
    def full_data_job_name(self): 
        result = ""
        if self.full_data_job != None:
            result = self.full_data_job.name

        return result

    @property
    def full_extraction(self):
        if self.__full_extraction_id != "":
            self.__full_extraction = self.full_data_job.extractions.find(self.full_extraction_id)
        return self.__full_extraction

    @property
    def full_extraction_name(self): 
        result = ""
        if self.full_extraction != None:
            result = self.full_extraction.name

        return result    

    @property
    def delta_data_job_name(self): 
        result = ""
        if self.delta_data_job != None:
            result = self.delta_data_job.name

        return result    

    @property
    def delta_extraction_name(self): 
        result = ""
        if self.delta_extraction != None:
            result = self.delta_extraction.name

        return result

    @property
    def anon_data_job(self):
        if self.__anon_data_job_id != "":
            self.__anon_data_job = self.datapool.data_jobs.find(self.anon_data_job_id)
        return self.__anon_data_job

    @property
    def anonymization_data_job_name(self): 
        result = ""
        if self.anon_data_job != None:
            result = self.anon_data_job.name

        return result
    
    @property
    def deletion_data_job_name(self): 
        result = ""
        if self.deletion_data_job != None:
            result = self.deletion_data_job.name

        return result

    @property
    def perf_data_job(self):
        if self.__perf_data_job_id != "":
            self.__perf_data_job = self.datapool.data_jobs.find(self.perf_data_job_id)
        return self.__perf_data_job

    @property
    def perf_data_job_name(self): 
        result = ""
        if self.perf_data_job != None:
            result = self.perf_data_job.name

        return result    

    @property
    def extentions_data_job_name(self): 
        result = ""
        if self.extensions_data_job != None:
            result = self.extensions_data_job.name

        return result

    def show(self):        
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        print('###########')
        print('# Environment (Objects)')
        print('#')
        print('#        - Schema: ' + self.schema)
        print('#        - Data Pool ID: ' + self.datapool_id)
        print('#        - Data Pool: ' + self.datapool_name)        
        print('#        - FULL Data Job ID: ' + self.full_data_job_id)
        print('#        - FULL Data Job: ' + self.full_data_job_name)
        print('#        - FULL Extraction ID: ' + self.full_extraction_id)
        print('#        - FULL Extraction: ' + self.full_extraction_name)        
        print('#        - DELTA Data Job ID: ' + self.delta_data_job_id)
        print('#        - DELTA Data Job: ' + self.delta_data_job_name)        
        print('#        - DELTA Extraction ID: ' + self.delta_extraction_id)
        print('#        - DELTA Extraction: ' + self.delta_extraction_name)                
        print('#        - ANON Data Job ID: ' + self.anon_data_job_id)
        print('#        - ANON Data Job: ' + self.anonymization_data_job_name)      
        print('#        - DELETION Data Job ID: ' + self.deletion_data_job_id)
        print('#        - DELETION Data Job: ' + self.deletion_data_job_name)      
        print('#        - PERF Data Job ID: ' + self.perf_data_job_id)
        print('#        - PERF Data Job: ' + self.perf_data_job_name)        
        print('#        - EXT Data Job ID: ' + self.extensions_data_job_id)
        print('#        - EXT Data Job: ' + self.extentions_data_job_name) 

class usecase_environment(object):
    __data_connection = ''
    __datapool_id = ''
    __usecase_data_job_id = ''

    __datapool = None
    __usecase_data_job = None
    
    def __init__(self, Connection: EMS_Connection, the_list: TheList, usecase: str):

        init_logging()        
        self.__logger = logging.getLogger("step_environment")
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        self.__EMS_Connection = Connection
        self.__Tables = the_list
        self.__c = Const()

        self.__usecase = usecase
        self.__data_connection = self.__Tables.getDPLUseCaseDataConnection(usecase)
        self.__datapool_id = self.__Tables.getDPLUseCaseDataPoolID(usecase)
        self.__usecase_data_job_id = self.__Tables.getDPLUseCaseUseCaseJobIDB(usecase)


    @property
    def usecase(self): return self.__usecase

    @property
    def data_connection(self): return self.__data_connection

    @property
    def datapool_id(self): return self.__datapool_id

    @property
    def usecase_data_job_id(self): return self.__usecase_data_job_id

    @property
    def datapool(self):
        if self.__datapool_id != "":
            self.__datapool = self.__EMS_Connection.pools.find(self.datapool_id)
        return self.__datapool

    @property
    def datapool_name(self): 
        result = ""
        if self.datapool != None:
            result = self.datapool.name

        return result

    @property
    def usecase_data_job(self):
        if self.__usecase_data_job_id != "":
            self.__usecase_data_job = self.datapool.data_jobs.find(self.usecase_data_job_id)
        return self.__usecase_data_job

    @property
    def usecase_data_job_name(self): 
        result = ""
        if self.usecase_data_job != None:
            result = self.usecase_data_job.name

        return result

    def show(self):        
        self.__logger.debug('===>' + self.__class__.__name__ + '.' + sys._getframe().f_code.co_name)

        print('###########')
        print('# Environment (Use Case)')
        print('#')
        print('#        - Data Connection: ' + self.data_connection)
        print('#        - Data Pool ID: ' + self.datapool_id)
        print('#        - Data Pool: ' + self.datapool_name)        
        print('#        - Use Case Data Job ID: ' + self.usecase_data_job_id)
        print('#        - Use Case Data Job: ' + self.usecase_data_job_name)
