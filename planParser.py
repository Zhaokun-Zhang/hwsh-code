import re
from collections import defaultdict

def splitLines(path):
    def getLines(ptr, eptr):
        ptr = eptr + 1
        while '-' not in lines[ptr]: ptr += 1
        ptr += 1
        eptr = ptr
        while 'rows)' not in lines[eptr]: eptr += 1
        return ptr, eptr

    with open(path) as f:
        lines = f.readlines()
    linesDict = {}
    # Plan Table Information
    ptr = 0
    while 'id' not in lines[ptr]: ptr += 1
    eptr = ptr + 2
    while '|' in lines[eptr]: eptr += 1
    linesDict['table info'] = lines[ptr:eptr]

    try:
        # Predicate Information
        ptr, eptr = getLines(ptr, eptr)
        linesDict['predicate info'] = lines[ptr:eptr]

        # Memory Information
        ptr, eptr = getLines(ptr, eptr)
        linesDict['memory info'] = lines[ptr:eptr]

        # Targetlist Information
        ptr, eptr = getLines(ptr, eptr)
        linesDict['targetlist info'] = lines[ptr:eptr]

        # Datanode Information
        ptr, eptr = getLines(ptr, eptr)
        linesDict['datanode info'] = lines[ptr:eptr]

        # User Define Profiling
        ptr, eptr = getLines(ptr, eptr)
        linesDict['user define'] = lines[ptr:eptr]

        # ====== Query Summary ===== 
        ptr, eptr = getLines(ptr, eptr)
        linesDict['summary'] = lines[ptr:eptr]
    except:
        pass

    return linesDict

def tableParser(strList):
    childMap = defaultdict(list) # {parentNodeId : [childNodeId1, childNodeId2, ...]}
    titleList = list(map(lambda s:s.strip(), 
                         strList[0].split('|')))

    def addOperationInfo(plan, info):
        nonlocal childMap
        pos = info.index('-')
        plan['depth'] = pos // 3
        info = info[pos+2:].strip()
        curId = plan['Node Id']
        # TPC-DS has several types of node, we split them to 4 types
        # has()has[]
        #   Row Adatper [parentId, InitPlan initPlanId(returns $)]  # Row Adapter  [100, InitPlan 5 (returns $6)]
        #   Op [parentId, CTE alias(cteId)] # Vector Aggregate  [39, CTE avg_sales(1)]
        #   Join Op (childId1, childId2) [parentId, CTE alias(cteId)] # Vector Hash Aggregate  [10, CTE customer_total_return(0)]
        #   Subquery Scan on Alias [parentId, CTE alias(cteId)] # Vector Subquery Scan on "*SELECT* 0" [21, CTE *year_total* 1(0)]
        #   
        # has()no[]
        #   Scan on tbl(DRIVE) # CStore Scan on catalog_scales(DRIVE)
        #   CTE Scan on medColName(cteId) # 
        #   CTE Scan on medColName(cteId) alias # CTE Scan on all_sales(0) curr_yr
        #   Join Op (child1, child2)
        #   Append Op (child1, child2, ..)
        #   Stream Op (type: ..)
        #   Stream Op (type: .. dop: n/m)
        #
        # no() has[]
        #   Row Adapter [parentId, SubPlan subplanId]
        #
        # no() no[]
        #   Index Op using indexKey on colName
        #   Scan Op on colName
        #   Scan Op on colName alias
        #   Subquery Scan on medColName
        #   Op
        
        if '(' in info and '[' in info:
            #   Row Adatper [parentId, InitPlan initPlanId(returns $)]
            #   Op [parentId, CTE alias(cteId)]
            #   Join Op (childId1, childId2) [parentId, CTE alias(cteId)]
            #   Subquery Scan on Alias [parentId, CTE alias(cteId)]
            if 'InitPlan' in info:
                # info = 'Row Adapter  [4, InitPlan 13 (returns $12)]'
                reFind = re.findall(r'(\D+)\[(\d+), InitPlan (\d+) \(returns \$(\d+)\)\]', info)[0]
                ndtype, parentId, initPlanId, _ = reFind
                plan['Node Type'] = ndtype.strip()
                childMap[parentId].append(curId)



            elif 'CTE' in info:
                ndInfo, _,  cteinfo = info.partition('[')
                if '(' in ndInfo:
                    ndtype, _, chInfo = ndInfo.partition('(')
                    chId1, chId2 = map(int, re.findall(r'(\d+)', chInfo))
                    childMap[curId] += [chId1, chId2]

                elif 'on' in ndInfo:
                    ndtype = ndInfo.partition('on')

                else:
                    ndtype = ndInfo
                plan['Node Type'] = ndtype.strip()
                parentId, cteId = map(int, re.findall(r'(\d+)', cteinfo))
                childMap[parentId].append(curId)
                plan['CTE Id'] = cteId
                
            else:
                raise ValueError('Strange Node Type. # has()has[] type')
            
        elif '(' in info and '[' not in info:
            if 'CTE' in info:
            #   CTE Scan on medColName(cteId) # 
            #   CTE Scan on medColName(cteId) alias # CTE Scan on all_sales(0) curr_yr
                ndtype, _, cteinfo = info.partition('on')
                cteId = int(re.search(r'(\d+)', cteinfo).group(1))
                plan['Node Type'] = ndtype.strip()
                plan['CTE Id'] = cteId
            elif 'Stream' in info:
            #   Stream Op (type: ..)
            #   Stream Op (type: .. dop: n/m)
                ndinfo1, _, streaminfo = info.partition('(type:')
                if 'dop' in streaminfo:
                    ndinfo2, _, dopinfo = streaminfo.partition('dop:')

                    doplist = list(map(int, re.findall(r'(\d+)', dopinfo)))
                else:
                    ndinfo2 = streaminfo.partition(')')[0]
                    doplist = [1, 1]
                plan['Node Type'] = ndinfo1.strip() + ' ' + ndinfo2.strip()
                plan['Dop'] = doplist
            
            elif 'on' in info:
            #   Scan on tbl(DRIVE) # CStore Scan on catalog_scales(DRIVE)
                ndtype, _, tblinfo = info.partition('on')
                tbl = tblinfo.partition('(')[0]
                plan['Node Type'] = ndtype.strip()
                plan['Relation Name'] = tbl.strip()
            
            else:
            #   Join Op (child1, child2)
            #   Append Op (child1, child2, ..)
                ndtype, _, chinfo = info.partition('(')
                plan['Node Type'] = ndtype.strip()
                childMap[curId] = list(map(int, re.findall(r'(\d+)', chinfo)))

        elif '(' not in info and '[' in info:
        #   Row Adapter [parentId, SubPlan subplanId]
            ndtype, _, adpinfo = info.partition('[')
            parentId, subplanId = map(int, re.findall(r'(\d+)', adpinfo))
            plan['Node Type'] = ndtype.strip()
            plan['Subplan Id'] = subplanId
            childMap[parentId].append(curId)

        else:
            if 'using' in info:
            #   Index Op using indexKey on colName
                ndtype, _, elseinfo = info.partition('using')
                idxkey, _, tbl = elseinfo.partition('on')
                plan['Node Type'] = ndtype.strip()
                plan['Index Key'] = idxkey.strip()
                plan['Relation Name'] = tbl.strip()
            elif 'on' in info:
            #   Scan Op on colName
            #   Scan Op on colName alias
            #   Subquery Scan on medColName
                ndtype, _, tblinfo = info.partition('on')
                tbl = tblinfo.strip().partition(' ')[0]
                plan['Node Type'] = ndtype.strip()
                plan['Relation Name'] = tbl.strip()

            else:
            #   Op
                plan['Node Type'] = info.strip()

        

        

    # gen plan list
    planList = [None]
    for line in strList[2:]:
        plan = dict()
        infoList = line.split('|')
        for info, title in zip(infoList, titleList):
            if title == 'id':
                plan['Node Id'] = int(info)
            elif title == 'operation':
                addOperationInfo(plan, info)
            elif title == 'A-time':
                minTotTime, maxTotTime = eval(info)
                plan['Actual Min Total Time'] = minTotTime
                plan['Actual Max Total Time'] = maxTotTime
            elif title == 'A-rows':
                plan['Actual Total Rows'] = int(info)
            elif title == 'E-rows':
                plan['Plan Rows'] = 1
            elif title == 'Peak Memory':
                info = info.replace('B', '').replace('G', '*1000M')
                info = info.replace('M', '*1000K').replace('K', '*1000')
                minPkMem, maxPkMem = eval(info)
                plan['Min Peak Memory'] = minPkMem
                plan['Max Peak Memory'] = maxPkMem
            elif title == 'E-width':
                plan['Plan Width'] = int(info)
            elif title == 'E-costs':
                plan['Total Cost'] = float(info)
        planList.append(plan)
    assert len(planList) == planList[-1]['Node Id'] + 1

    # gen a plan Tree
    for plan in planList[1:-1]:
        curId = plan['Node Id']
        childIdList = childMap.get(curId, [])
        if len(childIdList):
            plan['Plans'] = [planList[chId] 
                             for chId in childIdList]

        nextId = curId + 1
        nextPlan = planList[nextId]
        curDepth = plan['depth']
        nextDepth = nextPlan['depth']
        if nextDepth > curDepth and nextId not in childIdList:
            if plan.get('Plans', False):
                plan['Plans'].append(nextPlan)
            else:
                plan['Plans'] = [nextPlan]

        del plan['depth']
    del planList[-1]['depth']

    return planList

def predicateParser(planList:list, strList:list):

    def hashCond(curId, title, info):
        # only supports "AND" logical
        if ' OR ' in info:
            raise ValueError("Hash Cond not supports OR.")
        info = info.replace('public.', '')
        andList = info.split(' AND ')
        andCondList = []
        for andCond in andList:
            andCond = andCond.replace('(', '').replace(')', '').strip()
            andCondList.append(andCond)
        if len(andCondList):
            planList[curId]['Hash Cond'] = andCondList

    def cleanAndCond(andCond):
        # NOT SUPPORT:
        # 1. string match        
        #      ((part.p_name)::text ~~ 'floral%'::text)
        # 2. aggragate function  
        #      (sum((sum(public.lineitem.l_quantity))) > 314::numeric)
        # 3. invalid value compare 
        #      (public.customer.c_acctbal > $0)

        # Strange Case:
        # 1. substring match
        #      ("substring"((public.customer.c_phone)::text, 1, 2) = ANY ('{20,25,17,21,32,15,12}'::text[])))
        # 2. compare between col. and col. (no value)
        #      l3.l_receiptdate > l3.l_commitdate  (exclude "lineitem.l_discount <= .05")
        #      Need to change its into Join Type
        if '~~' in andCond:
            # NOT SUPPORT 1
            return None
        elif 'sum(' in andCond or 'avg(' in andCond:
            # NOT SUPPORT 2
            return None
        elif '$' in andCond:
            # NOT SUPPORT 3
            return None
        elif 'substring' in andCond:
            # Strange Case 1
            andCond = andCond.replace('(','').replace(')','').replace('"substring"','')
            andCond = andCond.replace('[','').replace(']','').replace('::text','')
            # customer.c_phone, 1, 2 = ANY '{31,24,32,20,33,23,14}'
            al = andCond.split(' ')
            al[:2] = al[0][:-1], al[1][:-1]
            andCond = ' '.join(al[:1]+al[3:]+al[1:3])
            # customer.c_phone = ANY '{31,24,32,20,33,23,14}' 1 2
            return andCond

        # (l3.l_receiptdate > l3.l_commitdate) # Strange Case 2
        # (lineitem.l_discount <= .05)
        # ((orders.o_orderdate >= '1993-12-01 00:00:00'::timestamp(0) without time zone)
        # ((part.p_type)::text = 'SMALL POLISHED STEEL'::text)
        andCond = andCond.replace('(', '').replace(')', '')
        tblcol, _, elseinfo = andCond.partition(' ')
        op, _, rightval = elseinfo.partition(' ')
        tblcol = tblcol.partition('::')[0]
        rightval = rightval.partition('::')[0]
        andCond = f'{tblcol} {op} {rightval}'
        if '.' in rightval:
            try:
                float(rightval)
            except:
                if len(rightval.split('.')) == 2:
                    # Strange Case 2
                    planList[curId]['Join Cond'] = andCond
                    return None
        return andCond

    def filterCond(curId, title, info):
        # only supports "AND" logical
        if ' OR ' in info:
            raise ValueError("Filter not supports OR.")
        info = info.replace('public.', '')


        andList = info.split(' AND ')
        andCondList = []
        for andCond in andList:
            andCond = cleanAndCond(andCond)
            if andCond is not None:
                andCondList.append(andCond)
        if len(andCondList):
            planList[curId]['Filter'] = andCondList
            
    def joinFilter(curId, title, info):
        # not only 'AND', but also 'OR'
        info = info.replace('pubic.', '')
        orList = info.split(' OR ')
        orCondList = []
        for orCond in orList:
            andList = orCond.split(' AND ')
            andCondList = []
            for andCond in andList:
                andCond = cleanAndCond(andCond)
                if andCond is not None:
                    andCondList.append(andCond)
            if len(andCondList):
                orCondList.append(andCondList)
        
        if len(orCondList):
            planList[curId]['Join Filter'] = orCondList

    def indexCond(curId, title, info):
        # (lineitem.l_orderkey = $0)
        info = info.replace('public.', '').replace('(', '').split(' = ')[0].strip()
        planList[curId][title] = info

    ptr = 0
    while ptr < len(strList):
        curLine = strList[ptr]
        if '--' in curLine:
            curId = int(curLine.partition('--')[0])
        else:
            title, _, info = map(lambda s:s.strip(),
                              curLine.partition(':'))
            if title == 'Hash Cond':
                hashCond(curId, title, info)
            elif title == 'Join Filter':
                joinFilter(curId, title, info)
            elif title == 'Filter':
                filterCond(curId, title, info)
            elif title == 'Index Cond':
                indexCond(curId, title, info)
            elif title == 'Rows Removed by Filter':
                planList[curId][title] = int(info)
        ptr += 1

def summaryParser(planList:list, strList:list):
    summaryDict = dict()
    for info in strList:
        key, value = info.split(':')
        if 'ms' in value:
            value = float(value.split('ms')[0])
        summaryDict[key] = value
    planList[0] = summaryDict

def planParser(planPath:str, 
               doPredicateParse:bool=True)->list: 
    # first node is None.
         
    linesDict = splitLines(planPath)
    planList =  tableParser(linesDict['table info'])
    if doPredicateParse:
        predicateParser(planList, linesDict['predicate info'])
    return planList
