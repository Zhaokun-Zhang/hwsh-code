import json
from collections import defaultdict

def genPlanListAndAddSegInfo(plan, planList, curSegId):
    plan['Node Id'] = len(planList)
    plan['Segment Id'] = curSegId[0]

    if 'Stream' in plan['Node Type']:
        ndinfo = plan['Node Type'].replace('(', '').replace(')', '')
        if 'dop' in ndinfo:
            ndinfo, dopinfo = ndinfo.split('dop:')
            dop1str, dop2str = dopinfo.split('/')
            dopList = [int(dop1str), int(dop2str)]
        else:
            dopList = [1, 1]
        ndtype1, ndtype2 = map(lambda s:s.strip(), ndinfo.split('type:'))
        curSegId[0] += 1
        plan['Segment Id'] = curSegId[0]
        plan['Node Type'] = f'{ndtype1} {ndtype2}'
        plan['Dop'] = dopList
        planList[0][curSegId[0]] = plan['Node Id']

    # CTE Node and CTE Scan
    if 'CTE' in plan.get('Subplan Name', ''): # CTE Node
        plan['CTE Name'] = plan['Subplan Name'].replace('CTE','').strip()
        
    planList.append(plan)
    for subplan in plan.get('Plans', []):
        genPlanListAndAddSegInfo(subplan, planList, curSegId)

def addCTEInfo(planList):
    cteGroup = defaultdict(list) # {'CTE Name':[nodeId1, nodeId2, ..]}
    cteNodeList = []
    for plan in planList[1:]:
        if 'CTE' in plan.get('Subplan Name', ''):
            cteNodeList.append(plan['Node Id'])
        if 'CTE Name' in plan.keys():
            cteGroup[plan['CTE Name']].append(plan['Node Id'])
    for cteId, ndlist in enumerate(cteGroup.values()):
        for nid in ndlist:
            if nid in cteNodeList:
                planList[nid]['CTE Node Id List'] = ndlist
            planList[nid]['CTE Id'] = cteId

def planParser(planPath:str):
    with open(planPath) as f:
        planstr = f.read()
    textlist = planstr.split('QUERY PLAN')
    if len(textlist) == 1: return None

    
    planstr = '[' + textlist[1].partition('[')[2].replace('+\n', '').split('\n')[0]
    plan = json.loads(planstr)[0]['Plan']

    segId2nodeId = {0:1}
    planList = [segId2nodeId]

    genPlanListAndAddSegInfo(plan, planList, [0])

    addCTEInfo(planList)
    
    return planList
    

if __name__ == '__main__':
    planParser('/home/gpadmin/tpcBenchmark/ds/run/run.result')
