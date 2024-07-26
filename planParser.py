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
    childMap = dict()
    titleList = list(map(lambda s:s.strip(), 
                         strList[0].split('|')))

    def addOperationInfo(plan, info):
        nonlocal childMap
        pos = info.index('-')
        plan['depth'] = pos // 3
        info = info[pos+2:].strip()

        if 'Stream' in info:
            streamType, dopInfo = info.replace('(type:','').split('dop:')
            plan['Node Type'] = streamType.strip()
            plan['Dop'] = eval('('+dopInfo.replace('/',','))
            return
        
        elif 'Scan' in info:
            ndType, scanTbl = info.split(' on ')
            if 'Subquery' in ndType:
                plan['Node Type'] = ndType
                plan['Subquery Column'] = scanTbl
            elif 'Index' in ndType:
                ndType, usingIndex = ndType.split(' using ')
                plan['Node Type'] = ndType
                plan['Using Index'] = usingIndex
                plan['Relation Name'] = scanTbl.replace('public.', '')
            else:
                plan['Node Type'] = ndType
                plan['Relation Name'] = scanTbl.replace('public.', '')
            return
        
        elif ' [' in info: # InitPlan or CTEPlan
            if 'InitPlan' in info:
                ndType, info = info.split('[')
                plan['Node Type'] = ndType.strip()
                parentId, info = info.split(', ')
                curId = plan['Node Id']
                parentId = int(parentId)

                if childMap.get(parentId, False):
                    childMap[parentId].append(curId)
                else:
                    childMap[parentId] = [curId]
                return
            elif 'CTE' in info:
                raise('CTE not support')
            else:
                raise('Strange Type [...] info')
        
        elif ' (' in info:
            pos = info.index('(')
            plan['Node Type'] = info[:pos].strip()
            curId = plan['Node Id']
            for childId in eval(info[pos:]):
                if childMap.get(curId, False): 
                    childMap[curId].append(childId)
                else:
                    childMap[curId] = [childId]
            return
        
        else:
            plan['Node Type'] = info
            return 

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
