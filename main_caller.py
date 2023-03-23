from data_pipeline import DataPipeline
from constants import Const
from ems_connection import EMS_Connection    
import logging

       
oConstants = Const()

##### PARAMETERS
#pUseCase = "CELONIS@DATA_PIPELINE"
pUseCase = ""
pSystems = ['P78'] #GP1, GP7
#pSystems = []
#pJobID_Man = "baf8bc3d-152b-4408-bc40-67e483f2d689" #can be auto-generated or user given!!!!
pJobID_Man = ""
#pTables = ['V_CUSN_BKPF', 'V_CUSN_CDPOS']
pTables = []

##### SETTINGS
pTEST_MODE = False
pexecute_EMS_jobs = True

pMaxExtractedRecords = 0
pFullTableNameSuffix = False

##### STEPS
#pSteps = []
#pSteps = [oConstants.CONST_STEP_ID_FULL, oConstants.CONST_STEP_ID_EXTENSION, oConstants.CONST_STEP_ID_DELTA]
#pSteps = [oConstants.CONST_STEP_ID_PERF_VIEW, oConstants.CONST_STEP_ID_ANON_VIEW]
#pSteps = [oConstants.CONST_STEP_ID_FULL, oConstants.CONST_STEP_ID_EXTENSION, oConstants.CONST_STEP_ID_DELTA, oConstants.CONST_STEP_ID_PERF_VIEW, oConstants.CONST_STEP_ID_ANON_VIEW]
#pSteps = [oConstants.CONST_STEP_ID_FULL, oConstants.CONST_STEP_ID_EXTENSION, oConstants.CONST_STEP_ID_DELTA]

#pSteps = [oConstants.CONST_STEP_NONE]
#pSteps = [oConstants.CONST_STEP_ID_FULL]
#pSteps = [oConstants.CONST_STEP_ID_EXTENSION]
#pSteps = [oConstants.CONST_STEP_ID_DELTA]
#pSteps = [oConstants.CONST_STEP_ID_PERF_VIEW]
#pSteps = [oConstants.CONST_STEP_ID_ANON_VIEW]
pSteps = [oConstants.CONST_STEP_ID_DELETIONS]
#pSteps = [oConstants.CONST_STEP_ID_TIMEZONE]
#pSteps = [oConstants.CONST_STEP_ID_USECASE]

##### MODES
pEMS_full_extractions_mode = oConstants.CONST_MODE_ADD
pEMS_delta_extractions_mode = oConstants.CONST_MODE_ADD
pEMS_extension_transformations_mode = oConstants.CONST_MODE_ADD
pEMS_anonymization_transformations_mode = oConstants.CONST_MODE_ADD
pEMS_performance_transformations_mode = oConstants.CONST_MODE_ADD

#-- DELETE Transformations
pDelete_String = "DELETE "

#-- Data Model re-load
pReload_DM = False              #METADATA
Reload_LogsDM = True           #Data Pipeline Logging (at the end of the execution)

#### LOGGERS
logging.getLogger("pycelonis").setLevel(logging.WARNING)
logging.getLogger("the_list").setLevel(logging.INFO)
logging.getLogger("step_environment").setLevel(logging.INFO)
logging.getLogger("data_pipeline").setLevel(logging.INFO)
logging.getLogger("ems_connection").setLevel(logging.INFO)

##### INITIALIZATIONS
EMS = EMS_Connection().c_source
oDataPipeLine = DataPipeline(EMS, pReload_DM)

#### SETTINGS
oDataPipeLine.set_Steps(pSteps)    
oDataPipeLine.set_UseCase(pUseCase)
oDataPipeLine.set_Systems(pSystems)
oDataPipeLine.set_Tables(pTables)
oDataPipeLine.set_JobId(pJobID_Man)
oDataPipeLine.set_MaxExtractedRecords(pMaxExtractedRecords)
oDataPipeLine.set_FullTableNameSuffix(pFullTableNameSuffix)

oDataPipeLine.set_Delete_String(pDelete_String)

oDataPipeLine.set_EMS_full_extractions_mode(pEMS_full_extractions_mode)
oDataPipeLine.set_EMS_delta_extractions_mode(pEMS_delta_extractions_mode)
oDataPipeLine.set_EMS_extension_transformations_mode(pEMS_extension_transformations_mode)
oDataPipeLine.set_EMS_anonymization_transformations_mode(pEMS_anonymization_transformations_mode)
oDataPipeLine.set_EMS_performance_transformations_mode(pEMS_performance_transformations_mode)


oDataPipeLine.set_execute_EMS_jobs(pexecute_EMS_jobs)

oDataPipeLine.set_Reload_LogsDM(Reload_LogsDM)

oDataPipeLine.set_TestMode(pTEST_MODE)

#### EXECUTION
sRun = oDataPipeLine.execute()
print("Run ID:" + sRun)