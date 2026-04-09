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
import re
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

def parse_items_from_line(line):
    """Extract comma-separated identifiers from import/variable/decorator lines.

    Handles patterns like:
        'from ..jobs import a,b,c'
        'all_jobs=[a,b,c]'
        '@sensor(jobs=[a,b,c])'
        '    jobs_to_monitor = [a,b,c]'
        '    exclst = [a, b, c]'
    """
    # Try bracket-delimited list first: [...]
    bracket_match = re.search(r'\[([^\]]*)\]', line)
    if bracket_match:
        items_str = bracket_match.group(1)
        return [item.strip() for item in items_str.split(',') if item.strip()]
    # Try import statement: from X import a, b, c
    if 'import' in line:
        parts = line.split('import', 1)
        if len(parts) == 2:
            items_str = parts[1].strip().rstrip('\n')
            return [item.strip() for item in items_str.split(',') if item.strip()]
    return []

def merge_items(existing, new):
    """Return ordered, deduplicated union of two lists. Existing items keep their position."""
    seen = set()
    merged = []
    for item in existing:
        if item not in seen:
            seen.add(item)
            merged.append(item)
    for item in new:
        if item not in seen:
            seen.add(item)
            merged.append(item)
    return merged

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
    context.log.info("Stored procedure {nm} executed successfully.")\n"""
        self.name = nm

# Parse CLI arguments
parser = argparse.ArgumentParser(description='VaultSpeed FMC to Dagster converter')
parser.add_argument('fmc_path', nargs='?', default='./dagster/vaultspeed/FMC', help='Path to FMC files directory (default: ./dagster/vaultspeed/FMC)')
parser.add_argument('--load-type', choices=['INIT', 'INCR', 'ALL'], default='ALL',
                    help='Filter by load type: INIT, INCR, or ALL (default)')
args = parser.parse_args()
fmc_path = args.fmc_path
selected_load_type = args.load_type

if __name__ == '__main__':
    print(f"Running with --load-type {selected_load_type}")
    asstfls = {'INIT':{'FL':[],'BV':[]},'INCR':{'FL':[],'BV':[]}}
    # Create list of info files to process
    fmcFiles = [f for f in os.listdir(fmc_path) if 'FMC_info' in f]
    schs = []

    # Iterate through info files
    for fmcFile in fmcFiles:
        # Load Info File
        with open(f'{fmc_path}/{fmcFile}', 'r') as infoFile:
            fmcInfo = json.load(infoFile)

        # Filter by load type if not ALL
        if selected_load_type != 'ALL' and fmcInfo['load_type'] != selected_load_type:
            print(f"Skipping {fmcInfo['load_type']} FMC from {fmcFile} (filtered by --load-type {selected_load_type})")
            continue

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

        flnm = f'./dagster/{prjnm}/assets/{grpnm}.py'
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
                with open(f'./dagster/{prjnm}/jobs/__init__.py', 'r') as initf:
                    rws = initf.readlines()

                # Write out updated init file
                with open(f'./dagster/{prjnm}/jobs/__init__.py', 'w') as initf:
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

    # Merge current run's job lists with any existing jobs from prior runs
    # This prevents overwriting definitions from the other load type
    with open(f'./dagster/{prjnm}/sensors/__init__.py', 'r') as f:
        sensor_content = f.read()
    sensor_lines = sensor_content.splitlines()

    # Parse existing items from sensors file for merging
    existing_sensor_jobs_import = []
    existing_sensor_decorator_jobs = []
    existing_monitor_jobs = []
    existing_exclst = []
    for sl in sensor_lines:
        if 'from ..jobs import' in sl:
            existing_sensor_jobs_import = parse_items_from_line(sl)
        elif '@sensor(jobs=' in sl:
            existing_sensor_decorator_jobs = parse_items_from_line(sl)
        elif 'jobs_to_monitor = [' in sl:
            existing_monitor_jobs = parse_items_from_line(sl)
        elif 'exclst = ' in sl:
            existing_exclst = parse_items_from_line(sl)

    # Build merged lists
    new_jbs = [j.strip() for j in jbsd.split(',') if j.strip()]
    new_jbsi = [j.strip() for j in jbsdi.split(',') if j.strip()]
    merged_sensor_jobs_import = merge_items(existing_sensor_jobs_import, new_jbs)
    merged_sensor_decorator_jobs = merge_items(existing_sensor_decorator_jobs, new_jbsi)
    merged_monitor_jobs = merge_items(existing_monitor_jobs, new_jbsi)

    # Preserve existing sensor function names across runs
    existing_sensor_names = re.findall(r'^def (\w+)\(', sensor_content, re.MULTILINE)
    snsrl = ','.join(existing_sensor_names) + ',' if existing_sensor_names else 'incr_bv_generator,'

    # Create Sensors
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
            with open(f'./dagster/{prjnm}/sensors/__init__.py', 'r') as initf:
                rws = initf.readlines()

            # Merge exclst with existing
            new_skp = skp[:]
            merged_exclst = merge_items(existing_exclst, new_skp)

            # Write out updated init file
            with open(f'./dagster/{prjnm}/sensors/__init__.py', 'w') as initf:
                jbvar = 0
                jbff = 0
                jbbf = 0
                for line in range(len(rws)):
                    if 'from ..jobs import' in rws[line]: # Ensure all jobs are defined (merge-aware)
                        initf.write(f'from ..jobs import {",".join(merged_sensor_jobs_import)}\n')
                        print('Updating jobs import in sensors file')
                    elif f'@sensor(jobs=' in rws[line]:   # make sure incremental bv sensor populated with jobs (merge-aware)
                        initf.write(f'@sensor(jobs=[{",".join(merged_sensor_decorator_jobs)}])\n')
                        print('Updating jobs for incremental bv generator in sensors decorator')
                    elif 'jobs_to_monitor = [' in rws[line]: # Make sure incremental jobs populated in job list (merge-aware)
                        initf.write(f'    jobs_to_monitor = [{",".join(merged_monitor_jobs)}]\n')
                        print('Updating jobs for incremental bv generator in sensors job list')
                    elif 'bvjb = ' in rws[line] and bvjn not in rws[line]:          # Make sure BV Generator job is defined correctly
                        initf.write(f'    bvjb = {bvjn}\n')
                        print('Updating BV Job name for sensor ignore')
                    elif 'exclst = ' in rws[line]:          # Make sure BV Generator job is defined with skipped jobs (merge-aware)
                        initf.write(f'    exclst = [{", ".join(merged_exclst)}]\n')
                        print('Updating Job ignore list name for sensor')
                    elif 'run_status=DagsterRunStatus.FAILURE' in rws[line] and njb in rws[line]: # Check for failure job sensor
                        jbff = 1
                        if snsrl.find(f'update_{asstf}_status_failure,') < 0:
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
                    if snsrl.find(f'update_{asstf}_status_failure,') < 0:
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
        with open(f'./dagster/{prjnm}/schedules/__init__.py', 'r') as initf:
            rws = initf.readlines()

        # Write out updated init file
        fndsch = 0
        imptf = 0
        with open(f'./dagster/{prjnm}/schedules/__init__.py', 'w') as initf:
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

    # Parse existing items from main __init__.py for merge-aware updates
    with open(f'./dagster/{prjnm}/__init__.py', 'r') as f:
        main_init_lines = f.readlines()

    existing_main_jobs_import = []
    existing_main_all_jobs = []
    existing_main_schedules_import = []
    existing_main_all_schedules = []
    existing_main_sensors_import = []
    existing_main_all_sensors = []
    for ml in main_init_lines:
        if 'from .jobs import ' in ml:
            existing_main_jobs_import = parse_items_from_line(ml)
        elif 'all_jobs=' in ml:
            existing_main_all_jobs = parse_items_from_line(ml)
        elif 'from .schedules import ' in ml:
            existing_main_schedules_import = parse_items_from_line(ml)
        elif 'all_schedules=' in ml:
            existing_main_all_schedules = parse_items_from_line(ml)
        elif 'from .sensors import ' in ml:
            existing_main_sensors_import = parse_items_from_line(ml)
        elif 'all_sensors=' in ml:
            existing_main_all_sensors = parse_items_from_line(ml)

    # Build merged lists for main __init__.py
    merged_main_jobs = merge_items(existing_main_jobs_import, new_jbs)
    merged_main_all_jobs = merge_items(existing_main_all_jobs, new_jbs)
    new_schl = [s.strip() for s in schl.split(',') if s.strip()]
    merged_main_schedules = merge_items(existing_main_schedules_import, new_schl)
    merged_main_all_schedules = merge_items(existing_main_all_schedules, new_schl)
    new_snsrl = [s.strip() for s in snsrl.split(',') if s.strip()]
    merged_main_sensors = merge_items(existing_main_sensors_import, new_snsrl)
    merged_main_all_sensors = merge_items(existing_main_all_sensors, new_snsrl)

    merged_main_jobs_str = ','.join(merged_main_jobs)
    merged_main_all_jobs_str = ','.join(merged_main_all_jobs)
    merged_main_schedules_str = ','.join(merged_main_schedules)
    merged_main_all_schedules_str = ','.join(merged_main_all_schedules)
    merged_main_sensors_str = ','.join(merged_main_sensors)
    merged_main_all_sensors_str = ','.join(merged_main_all_sensors)

    # Define project level resources
    for ldtyp in asstfls:
        for fltyp in asstfls[ldtyp]:
            for asstd in asstfls[ldtyp][fltyp]:
                asstf = asstd['group']
                #Read project init file to ensure asset is defined in
                with open(f'./dagster/{prjnm}/__init__.py', 'r') as initf:
                    rws = initf.readlines()

                # Write out updated init file
                with open(f'./dagster/{prjnm}/__init__.py', 'w') as initf:
                    astvar = False
                    for line in range(len(rws)):
                        if 'from .assets import ' in rws[line] and asstf not in rws[line]: # Ensure asset is pulled in as object
                            print(len(rws[line]))
                            if len(rws[line]) < 22:
                                initf.write(rws[line][:20] + f'{asstf}' + rws[line][20:])
                            else:
                                initf.write(rws[line][:20] + f'{asstf}, ' + rws[line][20:])
                            print(f'Adding asset import {asstf}')
                        elif 'from .jobs import ' in rws[line]: # Ensure all jobs are defined (merge-aware)
                            initf.write(f'from .jobs import {merged_main_jobs_str}\n')
                            print('Updating jobs import')
                        elif 'all_jobs=' in rws[line]: # Ensure all jobs are part of all_jobs variable (merge-aware)
                            initf.write(f'all_jobs=[{merged_main_all_jobs_str}]\n')
                            print('Updating jobs variable')
                        elif 'from .schedules import ' in rws[line]: # Ensure all schedules are defined (merge-aware)
                            initf.write(f'from .schedules import {merged_main_schedules_str}\n')
                            print('Updating schedules import')
                        elif 'all_schedules=' in rws[line]: # Ensure all schedules are part of all_schedules variable (merge-aware)
                            initf.write(f'all_schedules=[{merged_main_all_schedules_str}]\n')
                            print('Updating Schedules variable')
                        elif 'from .sensors import ' in rws[line]: # Ensure all sensors are defined (merge-aware)
                            initf.write(f'from .sensors import {merged_main_sensors_str}\n')
                            print('Updating sensors import')
                        elif 'all_sensors=' in rws[line]: # Ensure all sensors are part of all_sensors variable (merge-aware)
                            initf.write(f'all_sensors=[{merged_main_all_sensors_str}]\n')
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

