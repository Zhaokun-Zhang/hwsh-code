import os
import random
import pandas as pd
from query.planParser import planParser

dbName = "tpch_sf50"
skipQidList = [2, 9, 21]

mainDir = os.path.abspath('.')
resultsDir = f"{mainDir}/results"
queryDir = f'{mainDir}/query'
runSqlDir = f'{mainDir}/query/runSql'
# sql generate
def sqlTempRegen():
    os.system(f"bash {queryDir}/tools/genqueries.sh")
    print('sql regen.')

def genOneSql(qid, header, expFmt='explain performance') -> str: # return the path
    opath = f"{queryDir}/origSQL/{qid}.sql"
    with open(opath) as f:
        osql = f.read()
    # process Q15
    if qid == 15:
        viewSql, _, selSql = osql.partition(';')
        sql = viewSql + _ + header + expFmt + selSql
    else:
        sql = header + expFmt + osql
    spath = f"{queryDir}/sql/{qid}.sql"
    with open(spath, 'w') as f:
        f.write(sql)
    return spath

def genSqls(header, expFmt, isTempRegen=False) -> list: # return a list of the path
    if isTempRegen: sqlTempRegen()
    sqls = []
    for qid in range(1, 23):
        sqls.append(genOneSql(qid, header, expFmt))
    return sqls

# sql run
def runOneSql(db, sqlPath, savePath, isPrt=False):
    cmd = f'gsql -d {db} -U gpadmin -f {sqlPath} -p $GSPORT > {savePath}'
    if isPrt: print(cmd)
    os.system(cmd)

def queryDopTraver(qid, dopRange):
    os.makedirs(f"{resultsDir}/qDopTraver", exist_ok=True)
    planStructDict = dict()
    for qDop in list(dopRange)[::-1]:
        # if qid in [11, 16] and qDop > 15: continue # sf 1
        # if qid in [5, 8] and qDop > 12: continue  # sf 0.1

        header = f"set explain_perf_mode to summary;\nset query_dop={qDop};\n"
        expFmt = "explain\n"
        sql = genOneSql(qid, header, expFmt)
        resultp = f"{resultsDir}/qDopTraver/{qDop}"

        runOneSql(dbName, sql, resultp)

        planList = planParser(resultp)
        hashStr = ""
        for plan in planList[1:]:
            hashStr += plan['Node Type']
        
        planStructDict[hashStr] = qDop
    return planStructDict

        
def randomDopSet(planList, dopRange):
    sid = 0
    debug_segment_dop = ""
    for plan in planList[1:]:
        if 'Stream' in plan['Node Type']:
            sid += 1
            if plan['Dop'][1] != 1:
                dop = random.randint(min(dopRange),
                                     max(dopRange))
                debug_segment_dop += f"{sid},{dop};"
    return debug_segment_dop



def traindataEpoch(epoch, randomTimes=1, regenSql=False, dopRange=None):
    saveDir = f"{resultsDir}/DB-{dbName}/{epoch}"
    os.makedirs(saveDir, exist_ok=True)
    if regenSql: sqlTempRegen()
    if dopRange is None:  dopRange = range(2, 41)

    os.system(f"rm {runSqlDir}/*")
    runsqlList = []
    for qid in range(1, 23):
        if qid in skipQidList: continue
        print(f'query {qid} explain')
        print(f'-- query traverse')
        planStructDict = queryDopTraver(qid, dopRange)
        for qDop in planStructDict.values():
            print(f'---- set query dop:{qDop} t:{randomTimes}')
            planPath = f"{resultsDir}/qDopTraver/{qDop}"
            planList = planParser(planPath)

            for ri in range(randomTimes):
                segDopInfo = randomDopSet(planList, dopRange)
                header = f'''
                set explain_perf_mode to summary;
                set query_dop={qDop};
                set debug_segment_dop="{segDopInfo}";
                set enable_segment_dop=on;
                '''
                expInfo = 'explain performance'
                sql = genOneSql(qid, header, expInfo)
                suffix = f"Q{qid}-D{qDop}-T{ri}-{segDopInfo.replace(';','_')}"
                os.system(f"cp {sql} {runSqlDir}/{suffix}")
                runsqlList.append(suffix)
    print('-'*50)
    print(f'run sql from {saveDir}/*, total {len(runsqlList)} queries.')
    random.shuffle(runsqlList)
    for runsql in runsqlList:
        print(f'- - - - run {runsql}')
        sqlPath = f"{runSqlDir}/{runsql}"
        savePath = f"{saveDir}/{runsql}"
        runOneSql(dbName, sqlPath, savePath)


def main_trainDataset(totNum, regenSql, dopRange:range=None):
    for sid in range(totNum):
        print('-'*100)
        print(f'----  epoch: {sid}/{totNum} start  ----')
        print('-'*100)
        traindataEpoch(sid, randomTimes=5, regenSql=regenSql, dopRange=dopRange)

def queryDop(q):
    sdir = '/home/gpadmin/tpcBenchmark/tpch/run/results/queryDop6'
    os.makedirs(sdir)
    for dop in range(2, 21):
        for qid in range(1, 23):
            if qid in skipQidList: continue
            print(f'DOP{dop}|Q{qid}')
            header = f'''
            set explain_perf_mode to summary;
            set query_dop={dop};
            '''
            expInfo = 'explain performance'
            sqlPath = genOneSql(qid, header, expInfo)
            savePath = f'{sdir}/Q{qid}Dop{dop}'
            runOneSql(dbName, sqlPath, savePath)

def randomQueryDop(dopRange:range):
    sdir = '/data/tpcBenchmark/tpch/run/results/queryDopRun'
    os.makedirs(sdir)
    dopList = list(dopRange)
    print('queryDop traverse exe.')
    print('dop range:', dopList)
    qidDopList = []
    for qid in range(1,23):
        if qid in skipQidList: continue
        qidDopList += [(qid, qDop) for qDop in dopList]
    random.shuffle(qidDopList)
    for i, (qid, qDop) in enumerate(qidDopList):
        print(f'{i+1:>3}/{len(qidDopList):>3}: Q{qid}D{qDop}')
        header = f'''
        set explain_perf_mode to summary;
        set query_dop={qDop};
        '''
        expInfo = 'explain performance'
        sql = genOneSql(qid, header, expInfo)
        spath = f'{sdir}/Q{qid}-D{qDop}-T'
        runOneSql(dbName, sql, spath)


def loadCsvRun(segDopPath, saveDir):
    sdf = pd.read_csv(segDopPath)
    n = sdf.shape[0]
    ndir = saveDir
    os.makedirs(ndir)
    print(f'total run query num:{n}')
    for i in range(n):
        qid = sdf.loc[i, 'qid']
        qdop = sdf.loc[i, 'queryDop']
        header = sdf.loc[i, 'sqlHeader']
        print(f'{i+1} / {n} | Q{qid} D{qdop}')
        sql = genOneSql(qid, header, 'explain performance')
        
        p = f'{ndir}/Q{qid}Dop{qdop}'
        runOneSql(dbName, sql, p)

def bestQueryDopRun():
    bestQDopDf = pd.read_csv('/home/gpadmin/tpcBenchmark/tpch/run/python_script/querydop_stats.csv', index_col='qid')
    sdir = '/home/gpadmin/tpcBenchmark/tpch/run/results/queryDopCmp2'
    os.makedirs(sdir)
    for qid in range(1,23):
        if qid in skipQidList: continue
        qDop = int(bestQDopDf.loc[qid].dop )
        print(f'Q{qid} D{qDop}')
        header = f'''
        set explain_perf_mode to summary;
        set query_dop={qDop};
        '''
        cmd = 'explain performance'
        sql = genOneSql(qid, header, cmd)
        spath = f'{sdir}/Q{qid}Dop{qDop}'
        runOneSql(dbName, sql, spath)

if __name__ == '__main__':
    main_trainDataset(totNum=15, regenSql=True, dopRange=range(2, 21))
    # queryDop(6)
    # randomQueryDop(dopRange=range(2, 41))
    # loadCsvRun(segDopPath='./dopConfig-ml_search-1.csv', saveDir='./results/ml_search-n1')
    # loadCsvRun(segDopPath='./dopConfig-ml_search-2.csv', saveDir='./results/ml_search-n22')
    # loadCsvRun(segDopPath='./dopConfig-mp_ml-1.csv', saveDir='./results/mp_ml-n1')
    # loadCsvRun(segDopPath='./dopConfig-mp_ml-2.csv', saveDir='./results/mp_ml-n2')
    # bestQueryDopRun()
    # randomQueryDop(dopRange=range(2, 41))
    pass




