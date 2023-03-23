# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 12:05:34 2022

@author: MEP7FE
"""

from constants import Const
       
def get_steps(lSteps):
    result = ""
    if lSteps:
        for case in lSteps:
            result = result + get_step_name(case) + "|"
    else:
        result = result + "ALL"
    
    if result[-1] == '|':
        result = result[:-1]
    return result      


def get_step_name(step: int) -> str:
    result = ''
    if step == oConstants.CONST_STEP_NONE:
        result = oConstants.CONST_STEP_DESCRIPTION_NONE 
    elif step == oConstants.CONST_STEP_ID_FULL:
        result = oConstants.CONST_STEP_DESCRIPTION_FULL
    elif step == oConstants.CONST_STEP_ID_EXTENSION:
        result = oConstants.CONST_STEP_DESCRIPTION_EXTENSION
    elif step == oConstants.CONST_STEP_ID_DELTA:
        result = oConstants.CONST_STEP_DESCRIPTION_DELTA
    elif step == oConstants.CONST_STEP_ID_DELETIONS:
        result = oConstants.CONST_STEP_DESCRIPTION_DELETIONS
    elif step == oConstants.CONST_STEP_ID_TIMEZONE:
        result = oConstants.CONST_STEP_DESCRIPTION_TIMEZONE
    elif step == oConstants.CONST_STEP_ID_PERF_VIEW:
        result = oConstants.CONST_STEP_DESCRIPTION_PERF_VIEW
    elif step == oConstants.CONST_STEP_ID_ANON_VIEW:
        result = oConstants.CONST_STEP_DESCRIPTION_ANON_VIEW
    elif step == oConstants.CONST_STEP_ID_USECASE:
        result = oConstants.CONST_STEP_DESCRIPTION_USECASE
    else:
        result = result + str(step)

    return result

def get_execution_mode(iMode: int) -> str:
    result = ''
    if iMode == oConstants.CONST_MODE_ADD:
        result = oConstants.CONST_MODE_DESCRIPTION_ADD
    elif iMode == oConstants.CONST_MODE_UPDATE:
        result = oConstants.CONST_MODE_DESCRIPTION_UPDATE
    elif iMode == oConstants.CONST_MODE_RECREATE:
        result = oConstants.CONST_MODE_DESCRIPTION_RECREATE

    return result

oConstants = Const()


#pSteps = [oConstants.CONST_STEP_ID_FULL, oConstants.CONST_STEP_ID_EXTENSION]
