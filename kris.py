'''
DataWranglers Kris Vaultspeed FMC Conversion to Dagster Asset tool
Written by Kevin Lambeth
© TheDataWranglers LLC. 2026

Converts VaultSpeed FMC files, deployed to GitHub, to Dagster Asset.py files in the asset folders along with defining the files in the projects __init__.py folder
'''

import os
import json
import pytz
import ast
import argparse

prjnm = os.environ.get('PROJECT_NAME', 'DataWranglers') # Project Name
strtlddt = os.environ.get('STLD', 'False') # Use Mapping Info Start Date as Load Date
fmcnmo = os.environ.get('FMCN', 'False') # Allowing customized naming of FMC
tmzn = os.environ.get('TZN', 'UTC') # Use Mapping Info Start Date as Load Date
bvignr = os.environ.get('BVEX', '[]') # BV Jobs to Ignore for sensor triggering

def normalize_env_list(raw):
    if not raw:
        return []
    raw = raw.strip()
    if raw.startswith("["):
        try:
            v = ast.literal_eval(raw)
            if isinstance(v, list):
                return [str(x).strip() for x in v]
        except Exception:
            pass
    return [s.strip() for s in raw.split(",") if s.strip()]

class defAsset:
    def __init__(self, nm, grp, prc, ldt, dep=None, da=None, scs=None, initn=None, stldt=None, ft=None):
        if da:
            self.query = f'''f"CALL {prc}.{nm}('{da}', '{{newID[0][0]}}','{stldt}');"'''
            self.dec = f'@asset(group_name="{grp}", compute_kind="Execute_Procedure",)'
            if fmcnmo != 'False':
                self.fmcnm = fmcnmo
            else:
                self.fmcnm = prc.replace('PROC','FMC')
            self.asst = f"""{self.dec}
def {nm}(context, snowflake: SnowflakeResource,):
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DATAWRANGLERS.FMC_LOAD_ID.NEXTVAL;")
        newID = cursor.fetchall()
        context.log.info("Executing query: " + {self.query})
        cursor.execute({self.query})
        status = cursor.fetchone()
        context.log.info(f"Status: {{status[0] if status else 'No status returned'}}")
        context.log.info(f"Query ID: {{cursor.sfqid}}")
    context.log.info("Stored procedure {nm} executed successfully.")
    context.add_output_metadata({{"latest": newID[0][0]}})
    return newID[0][0]\n"""
        elif scs:
            datestr = "{datetime.now(pytz.timezone('" + tmzn + "')).strftime('%Y-%m-%d %H:%M:%S')}"
            if scs == '1':
                self.query = f'''f"CALL {prc}.{nm}('{{{initn}}}', '{scs}');"'''
                self.dec = f'@asset(group_name="{grp}", compute_kind="Execute_Procedure",ins={{"{initn}": AssetIn(key=AssetKey(["{initn}"]))}}, deps={dep})'
                nm = nm + '_' + ldt + '_SCSS'
                self.asst = f"""{self.dec}
def {nm}(context, snowflake: SnowflakeResource, {initn},) -> None:
    context.log.info("Executing query: " + {self.query})
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute({self.query})
        status = cursor.fetchone()
        context.log.info(f"Status: {{status[0] if status else 'No status returned'}}")
        context.log.info(f"Query ID: {{cursor.sfqid}}")
    context.log.info("Stored procedure {nm} executed successfully.")\n"""
            else:
                self.query = f'''f"CALL {prc}.{nm}('{{cycle_id}}', '{scs}');"'''
                self.dec = f'@asset(group_name="{grp}", compute_kind="Execute_Procedure",config_schema={{"last": int}})'
                nm = nm + '_' + ldt + '_FLL'
                self.asst = f"""{self.dec}
def {nm}(context, snowflake: SnowflakeResource, {initn},) -> None:
    cycle_id = context.op_config["last"]
    context.log.info("Executing query: " + {self.query})
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute({self.query})
        status = cursor.fetchone()
        context.log.info(f"Status: {{status[0] if status else 'No status returned'}}")
        context.log.info(f"Query ID: {{cursor.sfqid}}")
    context.log.info("Stored procedure {nm} executed successfully.")\n"""
        else:
            self.query = f'''"CALL {prc}.{nm}();"'''
            self.dec = f'@asset(group_name="{grp}", compute_kind="Execute_Procedure", deps={dep})'
            self.asst = f"""{self.dec}
def {nm}(context, snowflake: SnowflakeResource,) -> None:
    context.log.info("Executing query: " + {self.query})
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute({self.query})
        status = cursor.fetchone()
        context.log.info(f"Status: {{status[0] if status else 'No status returned'}}")
        context.log.info(f"Query ID: {{cursor.sfqid}}")
    context.log.info("Stored procedure {nm} executed successfully.")\n"""
        self.name = nm

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert VaultSpeed FMC files to Dagster Asset files')
    parser.add_argument('fmc_path', nargs='?', default='./dagster/vaultspeed/FMC', help='Path to FMC files directory (default: ./dagster/vaultspeed/FMC)')
    parser.add_argument('--output-path', default='./dagster', help='Base output path for Dagster files (default: ./dagster)')
    args = parser.parse_args()
    fmc_path = args.fmc_path
    output_path = args.output_path

    asstfls = {'INIT':{'FL':[],'BV':[]},'INCR':{'FL':[],'BV':[]}}
    # Create list of info files to process
    fmcFiles = [f for f in os.listdir(fmc_path) if 'FMC_info' in f]
    schs = []
    
    # Iterate through info files
    for fmcFile in fmcFiles:
        # Load Info File
        with open(f'{fmc_path}/{fmcFile}', 'r') as infoFile:
            fmcInfo = json.load(infoFile)
        print(f"Processing {fmcInfo['load_type']} FMC from {fmcFile}")
        # To ensure completely unique group
        #grpnm = fmcInfo['flow_type'].lower() + '_' + fmcInfo['load_type'].lower() + '_' + fmcInfo['src_name'].lower()
        # To match VaultSpeed paradigm
        grpnm = fmcInfo['dag_name'].lower()
        if fmcInfo['schedule_interval']:
            schs.append({'group':grpnm,'sch':fmcInfo['schedule_interval']})

        # Load Mapping File
        try:
            with open(f"{fmc_path}/{fmcInfo['map_mtd_file_name']}", 'r') as mapFile:
                fmcMap = json.load(mapFile)
        except:
            if fmcInfo['flow_type'] == "FL":
                prcn = "mappings_"
            elif fmcInfo["flow_type"] == "BV":
                prcn = "BV_mappings_"
            with open(f"{fmc_path}/{prcn}{fmcInfo['dag_name']}.json", 'r') as mapFile:
                fmcMap = json.load(mapFile)
        print(f"Opened Mapping File")

        # Iterate through mappings to generate Asset objects for building the file
        assts = []
        mpsch = ''
        initn = ''
        for asst in fmcMap:
            mpsch = fmcMap[asst]['map_schema'].replace('"', '').replace("'", '')
            if 'SET_FMC_MTD' in asst:
                #print(f"{grpnm}: {strtlddt}") #Debug start date as load date
                if strtlddt.lower() == 'true' and fmcInfo["load_type"] == 'INIT':
                    #print(fmcInfo['start_date'].replace('T',' ')) #Debug start date as load date
                    newasst = defAsset(asst,grpnm,mpsch,fmcInfo["load_type"],da=fmcInfo['dag_name'],stldt=fmcInfo['start_date'].replace('T',' '))
                else:
                    newasst = defAsset(asst,grpnm,mpsch,fmcInfo["load_type"],da=fmcInfo['dag_name'],stldt="{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                initn = asst
            elif 'FMC_UPD_RUN_STATUS' in asst:
                newasst = defAsset(asst,grpnm,mpsch,fmcInfo["load_type"],dep=fmcMap[asst]['dependencies'],scs='1',initn=initn,ft=fmcInfo["flow_type"])
                assts.append(newasst)
                newasst = defAsset(asst,grpnm,mpsch,fmcInfo["load_type"],dep=fmcMap[asst]['dependencies'],scs='0',initn=initn,ft=fmcInfo["flow_type"])
            else:
                newasst = defAsset(asst,grpnm,mpsch,fmcInfo["load_type"],dep=fmcMap[asst]['dependencies'])
            assts.append(newasst)

        # Write out python file
        toRecpt = '''{"message": {"subject": sbj,"body": {"contentType": "Text", "content": bdy},"toRecipients": [{"emailAddress": {"address": email}} for email in rcvr ],}}'''
        pyfl = f"""from datetime import datetime
from dagster_snowflake import SnowflakeResource
from dagster import asset, AssetIn, AssetKey
import requests
import json
import pytz
import os\n
csval = os.environ.get('CSVAL', 'False')
\n\n"""
        for asst in assts:
            pyfl += f"{asst.asst}\n"

        flnm = f'{output_path}/{prjnm}/assets/{grpnm}.py'
        #flnm = f'{grpnm}.py'
        with open(flnm, "w") as pyFile:
            pyFile.write(pyfl)
        print(f"Asset {grpnm}.py created")
        asstfls[fmcInfo["load_type"]][fmcInfo["flow_type"]].append({'group':grpnm,'sch':fmcInfo['schedule_interval'],'assets':assts,'initn':initn})

    # Create jobs
    print(asstfls.keys())
    jbs = []
    jbsi =[]
    for ldtyp in asstfls:
        print(ldtyp)
        for fltyp in asstfls[ldtyp]:
            for asstd in asstfls[ldtyp][fltyp]:
                asstf = asstd['group']
                fast = asstd['assets'][len(asstd['assets'])-1]
                fnlast = fast.name
                with open(f'{output_path}/{prjnm}/jobs/__init__.py', 'r') as initf:
                    rws = initf.readlines()
                
                # Write out updated init file
                with open(f'{output_path}/{prjnm}/jobs/__init__.py', 'w') as initf:
                    jbdfs = 0
                    jbdfsf = 0
                    for line in range(len(rws)):
                        if asstf + '_grp =' in rws[line]:
                            jbdfs = 1
                            initf.write(rws[line])
                        elif line+1 < len(rws) and 'define_asset_job' in rws[line+1] and jbdfs == 0:
                            initf.write(f'{asstf}_grp = AssetSelection.groups("{asstf}") - AssetSelection.assets(["{fnlast}"])\n\n')
                            jbdfs = 1
                        elif asstf + '_job' in rws[line]:
                            jbdfs = 2
                            initf.write(rws[line])
                            jbs.append(f'{asstf}_job')
                            if ldtyp == 'INCR':
                                jbsi.append(f'{asstf}_job')
                            else:
                                print(f'ldtyp: {ldtyp}: INCR')
                            jbs.append(f'{asstf}_failure')
                        elif asstf + '_failure =' in rws[line]:
                            jbdfsf = 1
                            initf.write(rws[line])
                        else:
                            initf.write(rws[line])
                    if jbdfs == 0:
                        initf.write(f'{asstf}_grp = AssetSelection.groups("{asstf}") - AssetSelection.assets(["{fnlast}"])\n')
                        initf.write(f'\n{asstf}_job = define_asset_job(name="{asstf}_load",selection={asstf}_grp,)\n')
                        jbs.append(f'{asstf}_job')
                        if ldtyp == 'INCR':
                            jbsi.append(f'{asstf}_job')
                        else:
                            print(f'ldtyp: {ldtyp}: INCR')
                        initf.write(f'{asstf}_failure = define_asset_job(name="{asstf}_load_failure",selection=AssetSelection.assets(["{fnlast}"]),)\n')
                        jbs.append(f'{asstf}_failure')
                    elif jbdfs == 1:
                        initf.write(f'\n{asstf}_job = define_asset_job(name="{asstf}_load",selection={asstf}_grp,)\n')
                        jbs.append(f'{asstf}_job')
                        if ldtyp == 'INCR':
                            jbsi.append(f'{asstf}_job')
                        else:
                            print(f'ldtyp: {ldtyp}: INCR')
                        initf.write(f'{asstf}_failure = define_asset_job(name="{asstf}_load_failure",selection=AssetSelection.assets(["{fnlast}"]),)\n')
                        jbs.append(f'{asstf}_failure')
                    if jbdfs == 2 and jbdfsf == 0: #Catch a successful find of a job and define job but failure was never found
                        initf.write(f'{asstf}_failure = define_asset_job(name="{asstf}_load_failure",selection=AssetSelection.assets(["{fnlast}"]),)\n')
                        
    jbsd = ''
    jbsdi = ''
    for jb in jbs:
        jbsd += jb+','
    jbsd = jbsd[:-1]
    print(jbsd)
    for jb in jbsi:
        jbsdi += jb+','
    jbsdi = jbsdi[:-1]
    print(f'Incremental Only: {jbsdi}')

    # Create Sensors
    snsrl = "incr_bv_generator,"
    for ldtyp in asstfls:
        for bvo in asstfls[ldtyp]['BV']:
            bvjn = bvo['group'] + '_job'
            bvfj = bvo['group'] + '_failure'
            skp = normalize_env_list(bvignr)
            skp.append(bvjn)
        for asstd in asstfls[ldtyp]['FL']:
            asstf = asstd['group']
            initn = asstd['initn']
            njb = f'{asstf}_job'
            fjb = f'{asstf}_failure'
            with open(f'{output_path}/{prjnm}/sensors/__init__.py', 'r') as initf:
                rws = initf.readlines()

            # Write out updated init file
            with open(f'{output_path}/{prjnm}/sensors/__init__.py', 'w') as initf:
                jbvar = 0
                jbff = 0
                jbbf = 0
                for line in range(len(rws)):
                    if 'from ..jobs import' in rws[line] and jbsd not in rws[line]: # Ensure all jobs are defined
                        initf.write(f'from ..jobs import {jbsd}\n')
                        print('Updating jobs import in sensors file')
                    elif f'@sensor(jobs=' in rws[line] and jbsdi not in rws[line]:   # make sure incremental bv sensor populated with jobs
                        initf.write(f'@sensor(jobs=[{jbsdi}])\n')
                        print('Updating jobs for incremental bv generator in sensors decorator')
                    elif 'jobs_to_monitor = [' in rws[line] and jbsdi not in rws[line]: # Make sure incremental jobs populated in job list
                        initf.write(f'    jobs_to_monitor = [{jbsdi}]\n')
                        print('Updating jobs for incremental bv generator in sensors job list')
                    elif 'bvjb = ' in rws[line] and bvjn not in rws[line]:          # Make sure BV Generator job is defined correctly
                        initf.write(f'    bvjb = {bvjn}\n')
                        print('Updating BV Job name for sensor ignore')
                    elif 'exclst = ' in rws[line] and not all(s in rws[line] for s in skp):          # Make sure BV Generator job is defined with skipped jobs
                        initf.write(f'    exclst = [{', '.join(skp)}]\n')
                        print('Updating Job ignore list name for sensor')
                    elif 'run_status=DagsterRunStatus.FAILURE' in rws[line] and njb in rws[line]: # Check for failure job sensor
                        jbff = 1
                        snsrl += f'update_{asstf}_status_failure,'
                        initf.write(rws[line])
                    elif 'run_status=DagsterRunStatus.FAILURE' in rws[line] and bvjn in rws[line]: # Check for bv failure job sensor
                        jbbf = 1
                        initf.write(rws[line])
                        if snsrl.find(f"update_{bvo['group']}_status_failure,") < 0:
                            snsrl += f"update_{bvo['group']}_status_failure,"
                        print(f"Line: {line} - {snsrl}")
                    else:
                        initf.write(rws[line])
                if jbff == 0: # Write Current Source Load failure if it doesn't exist
                    initf.write(f'\n@run_status_sensor(run_status=DagsterRunStatus.FAILURE,monitored_jobs=[{njb}],request_job={fjb},)\n')
                    initf.write(f'def update_{asstf}_status_failure(context: RunStatusSensorContext):\n')
                    initf.write(f'\tevent = context.instance.get_latest_materialization_event(asset_key=AssetKey(["{initn}"]))\n')
                    initf.write(f'\tif event is None:\n\t\tcontext.log.warning("No materialization found for {initn}; skipping failure handler.")\n\t\treturn None\n')
                    initf.write('\tlatest_meta = event.asset_materialization.metadata["latest"]\n')
                    initf.write('\tlatest = latest_meta.value\n')
                    initf.write('\treturn RunRequest(run_key=f"fail-load-{latest}",run_config={"ops": {"testFailProc": {"config": {"last": latest}}}})\n')
                    snsrl += f'update_{asstf}_status_failure,'
                if jbbf == 0: # Write BV Load failure if it doesn't exist
                    initf.write(f'\n@run_status_sensor(run_status=DagsterRunStatus.FAILURE,monitored_jobs=[{bvjn}],request_job={bvfj},)\n')
                    initf.write(f"def update_{bvo['group']}_status_failure(context: RunStatusSensorContext):\n")
                    initf.write(f'\tevent = context.instance.get_latest_materialization_event(asset_key=AssetKey(["{initn}"]))\n')
                    initf.write(f'\tif event is None:\n\t\tcontext.log.warning("No materialization found for {initn}; skipping failure handler.")\n\t\treturn None\n')
                    initf.write('\tlatest_meta = event.asset_materialization.metadata["latest"]\n')
                    initf.write('\tlatest = latest_meta.value\n')
                    initf.write('\treturn RunRequest(run_key=f"fail-load-{latest}",run_config={"ops": {"testFailProc": {"config": {"last": latest}}}})\n')
                    if snsrl.find(f"update_{bvo['group']}_status_failure,") < 0:
                        snsrl += f"update_{bvo['group']}_status_failure,"
    snsrl = snsrl[:-1]
    print(snsrl)

    # Define Schedules if they exist
    schl = ''
    for sch in schs:
        schl+=f"{sch['group']}_schedule,"
        with open(f'{output_path}/{prjnm}/schedules/__init__.py', 'r') as initf:
            rws = initf.readlines()

        # Write out updated init file
        fndsch = 0
        imptf = 0
        with open(f'{output_path}/{prjnm}/schedules/__init__.py', 'w') as initf:
            for line in range(len(rws)):
                if '..jobs import' in rws[line]:
                    if rws[line].find(f"{sch['group']}_job") == -1: #look for current job
                        jblstt = 19
                        impstr = f"{rws[line][:jblstt]}{sch['group']}_job,{rws[line][jblstt:]}"
                        if impstr[-1] == ',':
                            impstr = impstr[:-1]
                        initf.write(f"{impstr}\n")
                    else:
                        initf.write(rws[line])
                    imptf = 1
                elif f"{sch['group']}_job" in rws[line]:
                    fndsch = 1
                    initf.write(rws[line])
                else:
                    initf.write(rws[line])
            if imptf == 0:
                initf.write(f"from ..jobs import {sch['group']}_job\n")
            if fndsch == 0:
                if tmzn != 'UTC':
                    initf.write(f'''{sch['group']}_schedule = ScheduleDefinition(job={sch['group']}_job,cron_schedule="{sch['sch']}", execution_timezone="{tmzn}",)\n\n''')
                else:
                    initf.write(f'''{sch['group']}_schedule = ScheduleDefinition(job={sch['group']}_job,cron_schedule="{sch['sch']}",)\n\n''')
    schl = schl[:-1]

    # Define project level resources
    for ldtyp in asstfls:
        for fltyp in asstfls[ldtyp]:
            for asstd in asstfls[ldtyp][fltyp]:
                asstf = asstd['group']
                #Read project init file to ensure asset is defined in
                with open(f'{output_path}/{prjnm}/__init__.py', 'r') as initf:
                    rws = initf.readlines()

                # Write out updated init file
                with open(f'{output_path}/{prjnm}/__init__.py', 'w') as initf:
                    astvar = False
                    for line in range(len(rws)):
                        if 'from .assets import ' in rws[line] and asstf not in rws[line]: # Ensure asset is pulled in as object
                            print(len(rws[line]))
                            if len(rws[line]) < 22:
                                initf.write(rws[line][:20] + f'{asstf}' + rws[line][20:])
                            else:
                                initf.write(rws[line][:20] + f'{asstf}, ' + rws[line][20:])
                            print(f'Adding asset import {asstf}')
                        elif 'from .jobs import ' in rws[line] and jbsd not in rws[line]: # Ensure all jobs are defined
                            initf.write(f'from .jobs import {jbsd}\n')
                            print('Updating jobs import')
                        elif 'all_jobs=' in rws[line] and jbsd not in rws[line]: # Ensure all jobs are part of all_jobs variable
                            initf.write(f'all_jobs=[{jbsd}]\n')
                            print('Updating jobs variable')
                        elif 'from .schedules import ' in rws[line] and schl not in rws[line]: # Ensure all schedules are defined
                            initf.write(f'from .schedules import {schl}\n')
                            print('Updating schedules import')
                        elif 'all_schedules=' in rws[line] and schl not in rws[line]: # Ensure all schedules are part of all_schedules variable
                            initf.write(f'all_schedules=[{schl}]\n')
                            print('Updating Schedules variable')
                        elif 'from .sensors import ' in rws[line] and snsrl not in rws[line]: # Ensure all sensors are defined
                            initf.write(f'from .sensors import {snsrl}\n')
                            print('Updating sensors import')
                        elif 'all_sensors=' in rws[line] and snsrl not in rws[line]: # Ensure all sensors are part of all_sensors variable
                            initf.write(f'all_sensors=[{snsrl}]\n')
                            print('Updating Sensors variable')
                        elif f'{asstf}_assets'in rws[line]: # Asset object assigned to a variable already
                            astvar = True
                            initf.write(rws[line])
                        elif line+1 < len(rws) and 'defs = Definitions(' in rws[line+1] and not astvar: # Assign asset object and assign to a variable if definition lines are left without variable being assigned
                            initf.write(f"{asstf}_assets = load_assets_from_modules([{asstf}])\n\n")
                        elif 'assets=' in rws[line] and f"{asstf}_assets" not in rws[line]: # Add asset variable to assets definition if it doesn't already exist
                            brkloc = rws[line].find('[')
                            endl = -8+rws[line][-8:].find(',')
                            if brkloc > 0: # If multiple asset definition already exists, add the asset to the start of the set
                                initf.write(rws[line][:brkloc+1] + f'*{asstf}_assets, ' + rws[line][brkloc+1:])
                            else: # Convert to a multi asset definition and add asset
                                initf.write(rws[line][:rws[line].find('=')+1] + f'[*{asstf}_assets, *' + rws[line][rws[line].find('=')+1:endl] + '],' + rws[line][endl+1:])
                        else: # Write out file like normal
                            initf.write(rws[line])
            
