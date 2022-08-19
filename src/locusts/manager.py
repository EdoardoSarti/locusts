from locusts.support import *
from locusts.environment import *

def create_exec_file(id_list, command_template, indir, outdir, output_filename_templates,
        exec_filename, shared_inputs=[], inputs_for_clean_environment=[]):
    with open(exec_filename, "w") as exec_file:
        for ip, idx in enumerate(id_list):
            exec_file.write(('c{0}:\t{1}\n'.format(str(ip).zfill(6), command_template.replace("<id>", idx)))) 
            if inputs_for_clean_environment:
                inls = ' '.join([indir + x.replace("<id>", idx) for x in inputs_for_clean_environment])
                exec_file.write('i{0}:\t{1}\n'.format(str(ip).zfill(6), inls))
            if shared_inputs:
                shls = ' '.join([x.replace("<id>", idx).replace(":", ":"+indir) for x in shared_inputs])
                exec_file.write('s{0}:\t{1}\n'.format(str(ip).zfill(6), shls))
            ols = ' '.join([x.replace("<id>", idx) for x in output_filename_templates])
            exec_file.write('o{0}:\t{1}\n'.format(str(ip).zfill(6), ols))


def check_remote_path(remote_machine, root_path):
    """Checks existence of parent folder of remote root path
    and non-existence of remote root path itself"""

    print("ssh", remote_machine, "ls", root_path)
    lsdir = subprocess.Popen(["ssh", remote_machine, "ls", root_path], stdout=open('/dev/null', 'w'), stderr=subprocess.PIPE).stderr.read().decode('ascii').strip()
    lsdirup = subprocess.Popen(["ssh", remote_machine, "ls", os.path.dirname(root_path[:-1])], stdout=open('/dev/null', 'w'), stderr=subprocess.PIPE).stderr.read().decode('ascii').strip()
    # If parent directory of root path is not there, error
    if "ls: cannot access" in lsdirup: # If ls on parent directory gives error
        print('Failed to create {0}\nPath {1} not present' \
            .format(root_path, os.path.dirname(root_path[:-1])))
        print(lsdirup)
        exit(1)
    # If root path already there, error: you must delete it yourself
    elif "ls: cannot access" not in lsdir: # If ls on the exec directory does not give error
        print(('Exec path {0} is already present in remote location {1}\n'
            'No permission to overwrite it, please delete it manually.') \
            .format(root_path, remote_machine))
        print('ssh {1} rm -rf {0}'.format(root_path, remote_machine))
        exit(1)

def generate_exec_filesystem(protocol_triad, cache_dir, job_data, runtime_root_path, batch_job_code, 
        data_transfer_protocol, batch_size=10000, env_instr=None, build_envroot=None, only_gather=False):
    
    devnull = open('/dev/null', 'w')

    # Make sure root_path is in the format "/abspath/to/exec/" or "relpath/to/exec/" (no extra "/" or missing trailing "/")
    # NOTICE: root_path denotes where the locusts filesystem root directory will be at execution time
    #  if protocol == "remote", it is a path on the remote machine!
    runtime_root_path = reduceslash(runtime_root_path + "/")

    # Define localbuild_root_path
    # NOTICE: localbuild_root_path contains the filesystem root directory *during creation*
    #  It is thus always a local path.
    #  It can be an absolute path or a relative path starting from the current working dir (from where locusts was called)
    protocol, remote_machine, local_shared_folder = protocol_triad
    if protocol == "local":
        # localbuild_root_path <- real filesystem root
        if not os.path.exists(runtime_root_path):
             os.mkdir(runtime_root_path)
        localbuild_root_path = runtime_root_path = os.path.abspath(runtime_root_path) + '/'
    elif protocol == "remote":
        # localbuild_root_path <- temporary local path
        # WARNING: PAY ATTENTION TO THE FORMAT OF cache_dir!!!
        localbuild_root_path = reduceslash(cache_dir + "/" + batch_job_code + "_tmp_root/")
        if not os.path.exists(localbuild_root_path):
            os.mkdir(localbuild_root_path)
        localbuild_root_path = os.path.abspath(localbuild_root_path) + '/'
    elif protocol == "remote-sharedfs":
        # localbuild_root_path <- local shared folder (real filesystem root)
        localbuild_root_path = local_shared_folder
        if not os.path.exists(localbuild_root_path):
            os.mkdir(localbuild_root_path)
        localbuild_root_path = os.path.abspath(localbuild_root_path) + '/'

    # If the user provides the instruction file but not the local adress of the environment,
    #  he does not want to replicate the env (she thinks it is already in place)
    #  NOTICE: the snapshot will be taken anyway
    env_and_do_not_replicate = True if (env_instr and not build_envroot) else False

    # Filesystem locations dictionary contains the main locations of the environment
    fs_locations = {
        "build_root" : localbuild_root_path, 
        "runtime_root" : runtime_root_path, 
        "build_shared" : localbuild_root_path + "shared_contents/", 
        "runtime_shared" : runtime_root_path + "shared_contents/",
        "build_exec" : localbuild_root_path + "exec_dir/",
        "runtime_exec" : runtime_root_path+ "exec_dir/",
        "build_work" : localbuild_root_path + batch_job_code + "/",
        "runtime_work" : runtime_root_path + batch_job_code + "/"
    }

    # Step 1: create local versions of Shared, Exec and Work sub-environments ------------------------
    # Create main locations locally
    build_shared_path = fs_locations["build_shared"]
    if not os.path.exists(build_shared_path):
        os.mkdir(build_shared_path)
    if not os.path.exists(fs_locations["build_exec"]):
        os.mkdir(fs_locations["build_exec"])
    if not os.path.exists(fs_locations["build_work"]):
        os.mkdir(fs_locations["build_work"])

    # Must contain: 'batch_paths' -> list ; 'shared_path' 
    task_folders = {}  # For each task contains remote work path and shared files paths
    shared = {}
    # Step 2: create Shared sub-filesystem -------------------
    for jdi, jd in enumerate(job_data):
        batchno = jdi // batch_size
        # QUI QUI QUI
        #print(jdi)
        #print(jdi, jd)
        #print(task_folders)
        #print()
        replacements = {}

        for skey in jd['shared_inps']:
            if skey not in shared:
                # Create local batch dir, and copy file there
                batchisp = (len(shared) + 1)//batch_size
                build_shbatch_folder = build_shared_path + 'batch_' + str(batchisp) + '/'  # NOTICE: build_shared_path is local
                if not os.path.exists(build_shbatch_folder):
                    os.mkdir(build_shbatch_folder)
                build_shdest_path = build_shbatch_folder + os.path.basename(jd['shared_inps'][skey])
                shutil.copyfile(jd['shared_inps'][skey], build_shdest_path)

                # Runtime shared destination dir name
                runtime_shdest_path = (fs_locations["runtime_shared"]
                        + 'batch_' + str(batchisp) + '/' + os.path.basename(jd['shared_inps'][skey]))

                shared[skey] = batchisp  # shared folder path at execution time
            else:
                runtime_shdest_path = (fs_locations["runtime_shared"]
                        + 'batch_' + str(shared[skey]) + '/' + os.path.basename(jd['shared_inps'][skey]))
            replacements["<shared>"+skey] = runtime_shdest_path

    if protocol == 'remote' and (not env_and_do_not_replicate) and (not only_gather):
        check_remote_path(remote_machine, fs_locations["runtime_root"])

    # Step 3: create Work sub-filesystem: "custom" or "locusts" mode -------------------
    if env_instr:  # "custom" mode: environment is created following instruction file
        # Replace tags in instruction file lines
        #  Choose replacement whether it is in remote or in local
        if protocol == "remote":
            mkdircmd, copycmd = "mkdir", "cp"
            runtime_envroot_cp = fs_locations["build_work"]
            runtime_envroot_mkdir = fs_locations["build_work"]
            workpath = fs_locations["runtime_work"]
        elif protocol == 'remote-sharedfs':
            mkdircmd, copycmd = "mkdir", "cp"
            runtime_envroot_cp = fs_locations["runtime_work"]
            runtime_envroot_mkdir = fs_locations["runtime_work"]
            workpath = fs_locations["runtime_work"]
        elif protocol == "local":
            workpath = build_envroot

        #  Read the instruction file, replace tags and execute
        workdir, instructions = parse_fs_tree(env_instr, build_envroot)
        if protocol != 'local' and not env_and_do_not_replicate:
            compiled_instructions = []
            for instr in instructions:
                cmd = instr.replace("<build_envroot>", build_envroot) \
                    .replace("<mkdir>", mkdircmd) \
                    .replace("<runtime_envroot_mkdir>", runtime_envroot_mkdir) \
                    .replace("<copy>", copycmd) \
                    .replace("<runtime_envroot_cp>", runtime_envroot_cp)
                compiled_instructions.append(cmd)
            instruction_file = fs_locations["build_exec"] + '/cptree_instructions.sh'
            with open(instruction_file, 'w') as istf:
                for ci in compiled_instructions:
                    istf.write(ci+"\n")

            cmdlist = ["bash", instruction_file]
            if DEBUG:
                print("COMMAND", " ".join(cmdlist))
                p = subprocess.Popen(cmdlist)
            else:
                p = subprocess.Popen(cmdlist, stdout=devnull, stderr=devnull)
            p.wait()

        if not workdir:
            print(("ERROR (generate_filesystem): Please specify the directory "
                "from where to launch the specified commands\nOn top of the "
                "filesystem specifications file, add #WORKDIR <path> where "
                "<path> is a relative path from the environment root dir"))
            exit(1)
        else:
            workdir = workpath + '/' + workdir

        # Task files are in the Work sub-filesystem but outside the imported environment
        #  A cache in the main Work folder is created for containing them
        build_cache = fs_locations["build_work"] + '.cache/'
        runtime_cache = fs_locations["runtime_work"] + '.cache/'
        if os.path.exists(build_cache):
            shutil.rmtree(build_cache)
        os.mkdir(build_cache)
        build_task_dir = build_cache + 'tasks/'
        runtime_task_dir = runtime_cache + 'tasks/'
        os.mkdir(build_task_dir)
        for jdi, jd in enumerate(job_data):
            batchno = jdi // batch_size

            # Create local task folders and copy clean env files
            build_batch_folder = build_task_dir + 'batch_' + str(batchno) + '/'
            if not os.path.exists(build_batch_folder):
                os.mkdir(build_batch_folder)
            runtime_batch_folder = runtime_task_dir + 'batch_' + str(batchno) + '/'
            build_task_path = build_batch_folder + 'task_' + jd['unique_code'] + '.sh'
            runtime_task_path = runtime_batch_folder + 'task_' + jd['unique_code'] + '.sh'
            task_folders[jd['unique_code']] = (runtime_batch_folder, os.path.basename(runtime_task_path))

            # There, create individual task files
            #  Correctly indent the command, writes it in task.sh and gives it exe privileges
            new_command = beautify_bash_oneliner(
                "cd {0}; ".format(workdir) + jd['command'], 
                replacements=replacements)
#            print(new_command)
            with open(build_task_path, "w") as tf:
                tf.write(new_command)
            subprocess.call(["chmod", "777", build_task_path])


    else:  # "locusts" mode: optimized environment for parallel and safe execution
    
        # Batch folders for every 10000 tasks
        for i in range(len(job_data)//batch_size + 1):
            batch_folder = fs_locations["build_work"] + 'batch_' + str(i) + '/'
            if not os.path.exists(batch_folder):
                os.mkdir(batch_folder)
    
        # Job folders for individual tasks
        for jdi, jd in enumerate(job_data):
            batchno = jdi // batch_size
    
            # Work sub-environment --------------------------------------------
            # Create local task folders and copy clean env files
            job_folder = fs_locations["build_work"] + 'batch_' + str(batchno) + '/' + 'task_' + jd['unique_code'] + '/'
            if not os.path.exists(job_folder):
                os.mkdir(job_folder)
            for fpath in jd['clean_env_inps']:
                fdest_path = job_folder + os.path.basename(fpath)
                shutil.copyfile(fpath, fdest_path)

            #  There, create task.sh file
            # Correctly indent the command, writes it in task.sh and gives it exe privileges
            new_command = beautify_bash_oneliner(
                jd['command'], 
                replacements=replacements)
            task_filename = job_folder + "task.sh"
            with open(task_filename, "w") as tf:
                tf.write(new_command)
            subprocess.call(["chmod", "777", task_filename])

            # Runtime paths for task folders
            rem_jf = (fs_locations["runtime_work"] + 'batch_'
                    + str(batchno) + '/' + 'task_' + jd['unique_code'] + '/')
            task_folders[jd['unique_code']] = (rem_jf, "task.sh")
            if DEBUG:
                print("SIZE", jdi, get_obj_size(task_folders))

    return task_folders, fs_locations


def create_manager_scripts(protocol_triad, cache_dir, task_folders, partition,
        cpus_per_node, requested_nodes, batch_job_code, fs_locations, 
        data_transfer_protocol, singinfo=(None, None, None), task_cd=None,
        email_address="", email_type="ALL", tasks_per_core=1, min_stack_per_core=10,
        constraint='', nodescratch_folder="", nodescratch_mem="", walltime="24:00:00",
        outer_statements="", exclusive=False, mem='', mempercpu='', only_gather=False):

    protocol, remote_machine, hpc_shared_folder = protocol_triad
    devnull = open('/dev/null', 'w')

    # Check consistency of parameters
    turnon_mailtype, turnon_email_address = "#", "#"
    if email_address:
        if "@" not in email_address:
            print("ERROR (manager): The email address you provided is not valid")
            print("       "+email_address)
            exit(1)
        else: 
            turnon_mailtype, turnon_email_address = "", ""

    if nodescratch_folder and nodescratch_mem:
        turnon_nodescratch = ""
    elif (not nodescratch_folder) and (not nodescratch_mem):
        turnon_nodescratch = "#"
    else:
        print(("ERROR (manager): -nodescratch_folder and -nodescratch_mem options"
             "must be either both present or absent"))
        exit(1)

    turnon_exclusiveness = "#"
    if exclusive:
        turnon_exclusiveness = ""

    turnon_constraint = "#"
    if constraint:
        turnon_constraint = ""

    if len(walltime) < 7 or walltime[-3] != ":" or walltime[-6] != ":":
        print("ERROR (manager): walltime is not well formatted: XX:XX:XX")
        print("                 "+walltime)
        exit(1)

    # The location of the template file for the 1-node-manager script
    outer_template_filename = os.path.dirname(os.path.realpath(__file__)) + '/outer_template_manager.txt'
    inner_template_filename = os.path.dirname(os.path.realpath(__file__)) + '/inner_template_manager.txt'

    # Is there singularity? Does it use a module?
    if singinfo[0]:
        singularitypath, singularitycont, singmodload = singinfo
        singularitycmd = "{0} exec {1} ".format(singularitypath, singularitycont)
    else:
        singularitypath, singularitycont, singmodload = "", "", ""
        singularitycmd = ""

    # List of task ids
    taskid_list = sorted([k for k in task_folders])

    # General method for dividing len(taskid_list) elements in requested_nodes
    # lists. Ex: 6 jobs in 4 processors = [2,2,1,1]
    tasks_per_node_list = distribute_items_in_fixed_length_list(
        requested_nodes, 
        len(taskid_list),
        min_in_list = min_stack_per_core*cpus_per_node
    )
    requested_nodes = len(tasks_per_node_list)  # Modifies input!
    
    if mem:
        turnon_mem = ""
    else:
        turnon_mem = "#"

    if mempercpu:
        turnon_mempercpu = ""
    else:
        turnon_mempercpu = "#"

    print("Protocol used:", protocol)
    print("Total number of tasks:", len(taskid_list))
    print("Actual number of nodes used:", requested_nodes)
    print("Each with {0} cpus".format(cpus_per_node))
    print("tasks_per_node_list", tasks_per_node_list)

    # Each manager script handles one node
    #  The manager reads a file of adresses where it will find the clean
    #  environments to manage, knowing that in each of those folders it will
    #  find a task.sh executable to be run
    tasks_per_job = []
    for jobid in range(requested_nodes):
        # Define script variables
        tasks_per_node = tasks_per_node_list[jobid]
        ik = sum(tasks_per_node_list[:jobid])
        tfname = 'taskfile_{0}{1}'.format(batch_job_code, str(jobid).zfill(3))
        task_filename = fs_locations["runtime_exec"] + '{0}.txt'.format(tfname)
        outpath = fs_locations["runtime_exec"] + '{0}.out.txt'.format(tfname)
        errpath = fs_locations["runtime_exec"] + '{0}.err.txt'.format(tfname)

        # Compiles outer manager (with the SLURM keywords and possibly singularity) and inner manager (the core manager itself)
        for prefmng, template_filename in [('outer_', outer_template_filename), ('inner_', inner_template_filename)]:
            # Compile using the template 
            with open(template_filename) as tempf:
                text = tempf.read()

            text = text.replace('<jobd>', batch_job_code) \
                .replace('<jobid>', str(jobid).zfill(3)) \
                .replace('<time>', str(walltime)) \
                .replace('<cpuspertask>', str(cpus_per_node)) \
                .replace('<taskspercore>', str(tasks_per_core)) \
                .replace('<turnonnodescratch>', turnon_nodescratch) \
                .replace('<nodescratchfolder>', nodescratch_folder) \
                .replace('<nodescratchmem>', nodescratch_mem) \
                .replace('<turnonmailtype>', turnon_mailtype) \
                .replace('<mailtype>', email_type) \
                .replace('<turnonemailaddress>', turnon_email_address) \
                .replace('<turnonconstraint>', turnon_constraint) \
                .replace('<turnonexclusiveness>', turnon_exclusiveness) \
                .replace('<emailaddress>', email_address) \
                .replace('<constraint>', constraint) \
                .replace('<outpath>', outpath) \
                .replace('<errpath>', errpath) \
                .replace('<taskfile>', task_filename) \
                .replace('<exedir>', fs_locations["runtime_exec"]) \
                .replace('<partition>', partition) \
                .replace('<main_path>', fs_locations["runtime_root"]) \
                .replace('<extra_outer_statements>', outer_statements) \
                .replace('<singularity_module_load>', singmodload) \
                .replace('<singularity_command>', singularitycmd) \
                .replace('<inner_manager>', "inner_manager_{0}{1}.slurm".format(batch_job_code, str(jobid).zfill(3))) \
                .replace('<turnonmem>', turnon_mem) \
                .replace('<mem>', mem) \
                .replace('<turnonmempercpu>', turnon_mempercpu) \
                .replace('<mempercpu>', mempercpu)

            # Write manager file and give it exe privilege
            manager_filename = (
                fs_locations["build_exec"] + prefmng
                + 'manager_{0}{1}.slurm'.format(batch_job_code, str(jobid).zfill(3))
            )
            with open(manager_filename, 'w') as mf:
                mf.write(text)
            subprocess.call(["chmod", "777", manager_filename])

        # Write task file: the file containing the adresses of the clean env folders
        #  the manager has to deal with
        task_filename = (fs_locations["build_exec"] \
            + 'taskfile_{0}{1}.txt').format(batch_job_code, str(jobid).zfill(3))
        
        with open(task_filename, 'w') as tf:
            tf.write('\n'.join(
                [(x + '\t' + task_folders[x][0] + '\t' + task_folders[x][1]) 
                for x in taskid_list[ik:ik+tasks_per_node]]
            ) + '\n')
     
        # Job manager identifier and associated tasks
        tasks_per_job.append((jobid, taskid_list[ik:ik+tasks_per_node]))

        if DEBUG:
            print("SIZE", jobid, get_obj_size(tasks_per_job))

    if only_gather:
        return tasks_per_job, ""

    manager_cmd, macname = [], {}
    manager_cmd.append("(")
    manager_cmd.append("echo MANAGERS")
    for job_id, task_list in tasks_per_job:
        macname[job_id] = "{0}/.manager_activity_check_{1}{2}".format(
            fs_locations["runtime_exec"],
            batch_job_code,
            str(job_id).zfill(3)
        )
        manager_cmd.append(" ".join(["date", "-r", macname[job_id], "+%H:%M", "2>periodic_check.err.txt"]))
    manager_cmd.append("echo MACNAMES")
    for job_id, task_list in tasks_per_job:
        manager_cmd.append(" ".join(["cat", macname[job_id], "2>>periodic_check.err.txt"]))
    manager_cmd.append("echo STATUS")
    manager_cmd.append('squeue --format="%.18i %.9P %.100j %.50u %.2t %.10M %.6D %R"')
    manager_cmd.append("echo TIME")
    manager_cmd.append("echo $(date +%H:%M)")
    manager_cmd.append(") > {0}".format(fs_locations["runtime_exec"] + 'periodic_check.log.txt'))
    with open(fs_locations["build_exec"] + 'periodic_check.sh', 'w') as cmf:
        cmf.write("\n".join(manager_cmd))
    subprocess.call(["chmod", "777", fs_locations["build_exec"] + 'periodic_check.sh'])
    with open(fs_locations["build_exec"] + 'submit.sh', 'w') as sf:
        for job_id, task_list in tasks_per_job:
            sf.write("sbatch {0}/outer_manager_{1}{2}.slurm\n".format(fs_locations["runtime_exec"],
                batch_job_code, str(job_id).zfill(3)))
    subprocess.call(["chmod", "777", fs_locations["build_exec"] + 'submit.sh'])

    if protocol == 'remote':
        cpall_cmd = ["bash", data_transfer_protocol, fs_locations["build_root"], "{0}:{1}".format(remote_machine, fs_locations["runtime_root"])]
        if DEBUG:
            print("COMMAND", " ".join(cpall_cmd))
            p = subprocess.Popen(cpall_cmd)
        else:
            p = subprocess.Popen(cpall_cmd,
                stderr=devnull, stdout=devnull)
        out, err = p.communicate()

    log_txt = "Actual requested nodes :\t{0}\n".format(requested_nodes)

    # List of 2-tuples (job ID, [tasks IDs])
    return tasks_per_job, log_txt


def remote_job_control(protocol_triad, batch_job_code, fs_locations, tasks_per_job,
        waiting_time, data_transfer_protocol, log_folder, only_gather=False):
    # There must be a passwordless connection between the two machines: to
    # achieve it, ssh-agent and then ssh-add.
    # If the two machines share a folder - and thus there is no need to ssh to
    # the remote machine just for checking - it must be stated here

    protocol, remote_machine, hpc_shared_folder = protocol_triad
    devnull = open('/dev/null', 'w')

    if not only_gather:
        if protocol != 'local':
            chk_cmd = [
                "ssh",
                remote_machine,
                "sleep 60;",
                "bash",
                fs_locations["runtime_exec"] + 'submit.sh'
            ]
            if DEBUG:
                print("COMMAND", " ".join(chk_cmd))
                p = subprocess.Popen(chk_cmd)
            else:
                p = subprocess.Popen(chk_cmd,
                    stderr=devnull,
                    stdout=devnull
                )
        else:
            # If no hpc, there is only one node, i.e. one manager
            mname = 'outer_manager_{0}{1}.slurm'.format(batch_job_code, str(0).zfill(3))
            nhp_cmd = ["nohup", fs_locations["runtime_exec"] + mname]
            if DEBUG:
                print("COMMAND", nhp_cmd)
                p = subprocess.Popen(nhp_cmd)
            else:
                p = subprocess.Popen(nhp_cmd,
                    stderr=devnull, stdout=devnull
                )

    waitcount = {}
    for job_id, task_list in tasks_per_job:
        waitcount[job_id] = 0 

    is_over = False
    chrono_on = {job_id : False for job_id, task_list in tasks_per_job}
    time_start = {job_id : 0 for job_id, task_list in tasks_per_job}
    time_end = {job_id : 0 for job_id, task_list in tasks_per_job}
    machine_names = {job_id : 'NoHostname' for job_id, task_list in tasks_per_job}
    job_times = {job_id : 0 for job_id, task_list in tasks_per_job}
    hasrun = {job_id : False for job_id, task_list in tasks_per_job}
    sc, wc = 0, 0
    first_time = True
    while not is_over:
        status = {job_id : '' for job_id, task_list in tasks_per_job}
        if not (only_gather and first_time):
            time.sleep(waiting_time)
        else:
            first_time = False
        is_over = True

        # Start/stop chronometer
        for job_id, task_list in tasks_per_job:
            if chrono_on[job_id] and not time_start[job_id]:
                time_start[job_id] = time.time()
            if not chrono_on[job_id] and time_start[job_id]:
                time_end[job_id] = time.time()

        if protocol != 'local':
            manager_cmd = ["ssh", remote_machine, "bash", fs_locations["runtime_exec"] + 'periodic_check.sh']
            smallscp_cmd = ["bash", data_transfer_protocol, "{0}:{1}".format(remote_machine, fs_locations["runtime_exec"] + 'periodic_check.log.txt'), log_folder]
        else:
            manager_cmd = ["bash", fs_locations["build_exec"] + 'periodic_check.sh']
            smallscp_cmd = ["cp", "{0}".format(fs_locations["runtime_exec"] + 'periodic_check.log.txt'), log_folder]

        if DEBUG:
            print("COMMAND", manager_cmd)
            p = subprocess.Popen(
                manager_cmd,
                stdout=subprocess.PIPE
            )
        else:
            p = subprocess.Popen(
                manager_cmd, 
                stderr=devnull, 
                stdout=subprocess.PIPE
           )
        p.wait()
        if DEBUG:
            print("COMMAND", smallscp_cmd)
            p = subprocess.Popen(
                smallscp_cmd,
                stdout=subprocess.PIPE
            )
        else:
            p = subprocess.Popen(
                smallscp_cmd, 
                stderr=devnull, 
                stdout=subprocess.PIPE
           )
        p.wait()

        mng_lines, mac_lines, status_lines, time_line = False, False, False, False
        mng_times, mac_names = [], []
        with open(log_folder + '/periodic_check.log.txt') as logf:
            print("OPENING", log_folder + '/periodic_check.log.txt')
            for line in logf:
                if not line.strip():
                    continue
                if line == "MANAGERS":
                    mng_lines = True
                    continue
                elif line == "MACNAMES":
                    mng_lines = False
                    mac_lines = True
                    continue
                elif line == "STATUS":
                    mac_lines = False
                    status_lines = True
                    continue
                elif line == "TIME":
                    status_lines = False
                    time_line = True
                    continue
                if mng_lines:
                    mng_times.append(line.strip())
                elif mac_lines:
                    mac_names.append(line.strip()) 
                elif status_lines:
#                    print(line)
                    if line.split()[2] in [batch_job_code + str(x[0]).zfill(3) for x in tasks_per_job]:
#                        print("YE", line)
                        status[int(line.split()[2][len(batch_job_code):])] = line.split()[4]
                        hasrun[int(line.split()[2][len(batch_job_code):])] = True
                elif time_line:
                    real_time = line.strip()
                    real_time = int(real_time.split(":")[0])*60 + int(real_time.split(":")[1])
            if not (mng_times and mac_names):
                print("STILL NO CACHE FILES...")
                continue


        for it, t in enumerate(tasks_per_job):
            job_id, task_list = t

            if len(mng_times[it].split()) == 1:
                job_times[job_id] = int(mng_times[it].split(":")[0])*60 + int(mng_times[it].split(":")[1])
#            else:
#                print("WARNING: missing manager", it, job_id, mng_times[it])

            if len(mac_names[it].split()) == 1:
                machine_names[job_id] = mac_names[it]
#            else:
#                print("WARNING: missing machine name", it, job_id, mac_names[it])

            if status[job_id]:
                print(job_id, "Status:", status[job_id])
                is_over = False
            elif not hasrun[job_id]:
                print(job_id, "Waiting to be queued ({0}/3)".format(wc+1))
                wc += 1
                if wc > 3:
                    print(job_id, "Abort")
                else:
                    is_over = False
            else:
                print(job_id, "End")
                time_end[job_id] = job_times[job_id]

    log_txt = "\n\n#LOCUSTS JOBS LOG\n"
    time_now = time.time()
    tot_runtime, totsq = 0, 0
    for job_id, task_list in tasks_per_job:
        if not time_end[job_id] - time_start[job_id] == 0:
            runtime = waiting_time/2
        else: 
            runtime = time_end[job_id] - time_start[job_id]
        tot_runtime += runtime
        totsq += runtime**2
        log_txt += "LocustsJobID {0} :\tHostname {1}\tRuntime {2}\tRuntime (seconds) {3}\tTasks {4}\n".format(job_id, machine_names[job_id], datetime.timedelta(seconds = int(runtime)), int(runtime), len(task_list))

    av_runtime = tot_runtime/len(tasks_per_job)
    stdev_runtime = (totsq/len(tasks_per_job) - av_runtime**2)**0.5
    log_txt += "\n\nTotal computing time :\t{0}\tSeconds {1}\n".format(datetime.timedelta(seconds = int(tot_runtime)), int(tot_runtime))
    log_txt += "Average computing time per node:\t{0}\tSeconds {1}\n".format(datetime.timedelta(seconds = int(av_runtime)), int(av_runtime))
    log_txt += "Time imbalance (std dev):\t{0}\tSeconds {1}\n".format(datetime.timedelta(seconds = int(stdev_runtime)), int(stdev_runtime))

    print("Jobs are over")
    print(log_txt)

    return log_txt


def gather_results(protocol_triad, cache_dir, job_data, batch_job_code, 
        task_folders, fs_locations, tasks_per_job, log_dir,
        data_transfer_protocol, analysis_func=None, build_envroot=None, 
        snapshot=None, noenvrm=False):

    protocol, remote_machine, hpc_shared_folder = protocol_triad
    devnull = open('/dev/null', 'w')

    # The database is now connected with local machine
    if protocol == 'remote':
        shutil.rmtree(fs_locations['build_root'])
        scp_cmd = ["bash", data_transfer_protocol, "{0}:{1}".format(remote_machine, fs_locations["runtime_root"]), fs_locations['build_root']]
        if DEBUG:
            print("COMMAND", scp_cmd)
            p = subprocess.Popen(scp_cmd)
        else:
            p = subprocess.Popen(scp_cmd, stderr=devnull, stdout=devnull)
        p.wait()

    # Check if some job did not even start and adds it to the reschedule set 
    reschedule = set()
    for job_id, task_list in tasks_per_job:
        # Move the mail exec task files to logs/
        tfname = "taskfile_{0}{1}.*".format(batch_job_code, str(job_id).zfill(3))
        task_filename = fs_locations["build_exec"] + tfname
        mv_cmd = "mv {0} {1}".format(task_filename, log_dir)
        #mv_cmd = ["mv", task_filename, log_dir]
        if DEBUG:
            print("COMMAND", mv_cmd)
            p = subprocess.Popen(mv_cmd, shell=True)
        else:
            p = subprocess.Popen(mv_cmd,
                shell=True,
                stderr=devnull, 
                stdout=devnull
            )
        p.wait()

        # Check whether any of the task results still pending (not executed)
        #  or running (might have been interrupted). If so, adds to the reschedule set
        sname = "status_{0}{1}.txt".format(batch_job_code, str(job_id).zfill(3))
        status_filename = fs_locations["runtime_exec"] + sname
        grep_pending_cmd = ["grep", "'pending'", status_filename]
        if remote_machine:
            grep_pending_cmd = ["ssh", remote_machine] + grep_pending_cmd
        if DEBUG:
            print("COMMAND", grep_pending_cmd)
            txtlines = subprocess.Popen(
                grep_pending_cmd,
                stdout=subprocess.PIPE
            ).stdout.readlines()
        else:
            txtlines = subprocess.Popen(
                grep_pending_cmd, 
                stderr=devnull, 
                stdout=subprocess.PIPE
            ).stdout.readlines()

        for line in txtlines:
            (
                internal_id, 
                task_id, 
                status, 
                task_dirpath 
            ) = line.decode('ascii').split()
            if status in ['running', 'pending']:
                reschedule.add(task_id)

    # For the jobs that have completed, checks the expected outputs
    if not build_envroot:
        completed_with_error = set()
        output_paths = {}
        for jd in job_data:
            if jd['unique_code'] in reschedule:
                continue
            for output in jd['outputs']:
                # Get the Work, batch and task output addresses
                tfuc = task_folders[jd['unique_code']][0]
                slashlist = re.sub("/(/+)", "/", tfuc).split("/")
                (
                    relative_env_folder, 
                    relative_batch_folder, 
                    relative_task_folder
                ) = [x+"/" for x in slashlist][-4:-1]
    
                output_batch_dir = jd['output_dir'] + relative_batch_folder
                output_task_dir = (jd['output_dir'] + relative_batch_folder 
                    + relative_task_folder)
    
                # Create batch and task directories in the output folder
                if not os.path.exists(output_batch_dir):
                    os.mkdir(output_batch_dir)
                if not os.path.exists(output_task_dir):
                    os.mkdir(output_task_dir)
    
                # Move output from the parsed address to the output folder
                #  and checks if output is there
                output_path = (fs_locations["build_work"]
                    + relative_batch_folder + relative_task_folder + output)
                if os.path.exists(output_path) and ((analysis_func == None) or
                    (analysis_func(output_path))): 
                    mv_cmd = ["mv", output_path, output_task_dir]
                    if DEBUG:
                        print("COMMAND", mv_cmd)
                        p = subprocess.Popen(mv_cmd)
                    else:
                        p = subprocess.Popen(mv_cmd,
                            stderr=devnull, 
                            stdout=devnull
                        )
                    p.wait()
                    if jd['unique_code'] not in output_paths:
                        output_paths[jd['unique_code']] = {}
                    output_paths[jd['unique_code']][output] = output_task_dir + output
                else:
                    completed_with_error.add(jd['unique_code'])
    
        # Compile the main output file
        output_logfilename = jd['output_dir'] + "output.log"
        with open(output_logfilename, "w") as of:
            for jid, jd in sorted([(k['unique_code'], k) for k in job_data], key=lambda x: x[0]):
                if jid in output_paths:
                    for output in jd['outputs']:
                        if output in output_paths[jid]:
                            status = "present"
                            path = output_paths[jid][output]
                        elif jid in completed_with_error:
                            status = "error"
                            path = "-"
                        else:
                            status = "missing"
                            path = "-"
                        of.write("{0}\t{1}\t{2}\n".format(output, status, path))
    
        # This will be the new job_data, containing all jobs that
        #  have to be rescheduled
        output_d = { k['unique_code'] : k 
            for k in job_data if k['unique_code'] in reschedule }
    
    elif snapshot:
#        print("TAKE SNAPSHOT", protocol_triad, fs_locations['build_work'])
        new_snapshot = take_snapshot(protocol_triad, fs_locations['build_work'])
#        print(new_snapshot)
#        print("NEWLY ADDED")
        newly_added = compare_snapshots(snapshot, new_snapshot)
#        print(newly_added)
        snap_log = log_dir + 'modified.log'
        with open(snap_log, 'w') as snapf:
            # Creates all new folders
            for d in sorted(newly_added):
                if protocol != 'local' and not os.path.exists(build_envroot+d):
                    snapf.write("Directory created: {0}\n".format(build_envroot+d))
                    os.mkdir(build_envroot+d)

            # Populates new and old folders
            for d in sorted(newly_added):
                for f in sorted(newly_added[d]):
                    snapf.write("File created/modified: {0}\n".format(build_envroot+d+'/'+f))
                    if protocol != 'local':
                        fname = fs_locations["build_work"] + d+'/'+f  # Local copies have been restored at the beginning of this function
                        cpcmd = ["cp", fname, build_envroot+d+'/'+f]
#                        print("COMMAND", cpcmd)
                        if DEBUG:
#                            print("COMMAND", cpcmd)
                            p = subprocess.Popen(cpcmd)
                        else:
                            p = subprocess.Popen(cpcmd, stdout=devnull, stderr=devnull)
                        p.wait()

        output_d, output_paths, completed_with_error = {}, None, None

    # Remove all repositories
    if not DEBUG:
        if protocol != 'local':
            sshrm_cmd = ["ssh", remote_machine, "rm", "-rf", fs_locations['runtime_root']]
            if DEBUG:
                print("COMMAND", sshrm_cmd)
                p = subprocess.Popen(sshrm_cmd)
            else:
                p = subprocess.Popen(sshrm_cmd, stderr=devnull, stdout=devnull)
            p.wait()
        rm_cmd = ["rm", "-rf", fs_locations['build_root']]
        if DEBUG:
            print("COMMAND", rm_cmd)
            p = subprocess.Popen(rm_cmd)
        else:
            p = subprocess.Popen(rm_cmd, stderr=devnull, stdout=devnull)
        p.wait()

    return output_d, output_paths, completed_with_error

def take_snapshot(protocol_triad, root_dir):
    protocol, remote_machine, hpc_shared_folder = protocol_triad
    devnull = open('/dev/null', 'w')

    lsrcmd = ["ls", "-ltrhR", root_dir]

    if DEBUG:
        print("COMMAND", lsrcmd)
        textlines = subprocess.Popen(lsrcmd,
            stdout=subprocess.PIPE).stdout.readlines()
    else:
        textlines = subprocess.Popen(lsrcmd, 
            stdout=subprocess.PIPE, stderr=devnull).stdout.readlines()

    snapshot = {"::NEGLECT::" : []}
    for l in textlines:
        line = l.decode('ascii')
        if not line.strip() or line.startswith('total'):
            continue
        if len(line.split()) == 1:
            dirname = line.strip().replace(root_dir,"")[:-1]  # line terminates with ":"
            snapshot[dirname] = {}
        elif len(line.split()) == 9:
            priv, _, usr1, usr2, s, d1, d2, d3, filename = line.split()
            filename = filename.replace(root_dir,"")
            if priv.startswith('-'):  # If item is not a folder
                snapshot[dirname][filename] = (priv, usr1, usr2, s, d1, d2, d3)
        elif len(line.split()) == 11 and line[9].split() == "->":
            priv, _, usr1, usr2, s, d1, d2, d3, filename, _, slink = line.split()
            filename = filename.replace(root_dir,"")
            snapshot["::NEGLECT::"].append(filename)
        else:
            print(("WARNING (take_snapshot): output of ls -ltrhR is not"
                "in the expected format"))
            print(line)

    return snapshot


def compare_snapshots(snap1, snap2):
    newtosnap2 = {}
    for d in snap2:
        if d not in snap1 and d not in snap1["::NEGLECT::"]:
            newtosnap2[d] = snap2[d]
            continue
        for f in snap2[d]:
#            print("DECISION", f, f not in snap1[d], f in snap1[d] and snap1[d][f] != snap2[d][f], f not in snap1["::NEGLECT::"])
            if (f not in snap1[d] or snap1[d][f] != snap2[d][f]) and f not in snap1["::NEGLECT::"]:
                if d not in newtosnap2:
                    newtosnap2[d] = []
                newtosnap2[d].append(f)
    return newtosnap2


def highly_parallel_job_manager(options, exec_filename,
        batch_job_code, locout_dir, env_root_dir=None,
        env_instr=None, noenvcp=None, noenvrm=None):
    this_name = highly_parallel_job_manager.__name__

    # Creates cache dir
    #  The locout dir is a local directory where outputs will be collected
    #  in the end, but also where caches and local builds of remote filesystems
    #  will be stored
    cache_dir = locout_dir + ".cache/" 
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
    os.mkdir(cache_dir)
    cache_dir = os.path.realpath(cache_dir) + '/'

    # Creates log dir
    log_dir = locout_dir + "logs/"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.mkdir(log_dir)

    # Is there singularity?
    singinfo = (options['singularity'], options['singularity_container'], options['singularity_modload'])

    # Read exec file and compile job_data
    job_data = []
    jd = {}
    with open(exec_filename) as exec_file:
        for line in exec_file:
            if line.startswith("c"):
                if jd:
                    if os.path.exists(jd['log_filename']):
                        if options['force_redo']:
                            os.remove(jd['log_filename'])
                            job_data.append(jd)
                    else:
                        job_data.append(jd)
                jd = {
                    'command' : '',
                    'outputs' : [],
                    'output_dir' : '',
                    'success' : None,
                    'issues' : [],
                    'unique_code' : '',
                    'log_filename' : '',
                    'clean_env_inps' : [],
                    'shared_inps' : []
                }
                jd['command'] = line[8:].strip()
                fields = line.split()
                jd['output_dir'] = locout_dir
                jd['unique_code'] = batch_job_code + fields[0][1:-1]
                jd['log_filename'] = (log_dir + batch_job_code +
                    fields[0][1:-1] + '_log.txt')
            elif line.startswith("i"):
                fields = line.split()
                jd['clean_env_inps'] = fields[1:]
            elif line.startswith("s"):  # shared inputs must be declared in "s" line as file.txt:/path/of/file.txt, where file.txt is a filename appearing in the "c" line
                fields = line.split()
                jd['shared_inps'] = {k : p 
                    for (k,p) in [x.split(":") for x in fields[1:]]}
            elif line.startswith("o"):
                fields = line.split()
                jd['outputs'] = fields[1:]
        if jd:
            if os.path.exists(jd['log_filename']):
                if options['force_redo']:
                    os.remove(jd['log_filename'])
                    job_data.append(jd)
            else:
                job_data.append(jd)

    if options['run_on_hpc']:
        # Copy files in separate location (for hpc the option 'exec location' must be set to '/data/biowulf/sartie/'
        remote_machine = options['host_name']
        requested_nodes = options['requested_nodes']
        cpus_per_node = options['cpus_per_node']
        if options['local_shared_dir']:
            # The path specified in hpc_exec_dir must point to the same
            #  folder of local_shared_dir, only from the point of view of
            #  the remote machine
            protocol = 'remote-sharedfs'
            runtime_root_path = options['hpc_exec_dir'] 
            local_shared_folder = options['local_shared_dir']
        else:
            protocol = 'remote'
            runtime_root_path = options['hpc_exec_dir']
            local_shared_folder = None
    else:
        protocol = 'local'
        remote_exec_path = False
        remote_machine = None
        local_shared_folder = None
        requested_nodes = 1
        cpus_per_node = options['number_of_processors']
        runtime_root_path = cache_dir + batch_job_code + "_tmp_root/"
        if not os.path.exists(runtime_root_path):
            os.mkdir(runtime_root_path)
    protocol_triad = (protocol, remote_machine, local_shared_folder)

    completed_with_error = set()
    output_paths = {}
    if env_root_dir:
        env_root_dir = os.path.abspath(env_root_dir) + '/'
        gen_env_root_dir = None if noenvcp else env_root_dir
    else:
        env_root_dir, gen_env_root_dir = None, None

    data_transfer_protocol = options['data_transfer_protocol']
    email = options['email_address']
    nsf = options['nodewise_scratch_folder']
    nsm = options['nodewise_scratch_memory']
    wt = options['walltime']
    out_st = options['extra_outer_statements']
    partition = options['partition']
    exclusive = options['exclusive']
    task_stack = options['min_stack_per_core']
    constraint = options['constraint']
    waiting_time = options['waiting_time']
    memory = options['memory']
    memory_per_cpu = options['memory_per_cpu']
    only_gather = options['only_gather']

    log_txt = ("#RUN LOG\n\n"
        "Run type (local / shared / remote) :\t{0}\n").format(protocol)
    if protocol != 'local':
        log_txt += ("Remote machine :\t{0}\n"
            "Partition (queue) :\t{1}\n"
            "Constraint (node type) :\t{2}\n"
            "Number of reserved nodes :\t{3}\n"
            "(Minimum) number of cores per node :\t{4}\n"
            "Exclusive use of nodes :\t{5}\n"
            "Tasks :\t{6}\n"
            "Minimum number of tasks assigned per core :\t{7}\n"
            "Wall time :\t{8}\n"
            "Nodewise scratch folder :\t{9}\n"
            "Nodewise scratch memory :\t{10}\n"
        ).format(remote_machine, partition, constraint, requested_nodes, cpus_per_node, exclusive, len(job_data), task_stack, wt, nsf, nsm)   
    else:
        log_txt += "Number of local processors :\t{0}\n".format(cpus_per_node)
    print(log_txt)

    while job_data:
        # Create the hosting file system
        task_folders, fs_locations = generate_exec_filesystem(
            protocol_triad,
            cache_dir, 
            job_data, 
            runtime_root_path,
            batch_job_code,
            data_transfer_protocol,
            env_instr=env_instr,
            build_envroot=gen_env_root_dir,
            only_gather=only_gather
        )
        gc.collect()

        # Create local manager script that does the mpiq job, launchable on each node. It checks the situation regularly each 10 secs.
        tasks_per_job, tmp_log_txt = create_manager_scripts(
            protocol_triad,
            cache_dir, 
            task_folders,
            partition, 
            cpus_per_node, 
            requested_nodes, 
            batch_job_code,
            fs_locations,
            data_transfer_protocol,
            singinfo=singinfo,
            email_address=email,
            min_stack_per_core=task_stack,
            constraint=constraint,
            nodescratch_folder=nsf,
            nodescratch_mem=nsm,
            walltime=wt,
            outer_statements=out_st,
            exclusive=exclusive,
            mem=memory,
            mempercpu=memory_per_cpu,
            only_gather=only_gather
        )
        log_txt += tmp_log_txt
        gc.collect()

        if env_instr:
            snapshot = take_snapshot(
                (protocol, "", local_shared_folder), 
                env_root_dir
            )

#            print("FIRST SNAPSHOT", (protocol, "", local_shared_folder), env_root_dir)
#            print(snapshot)
            if not snapshot:
                print("ERROR: empty snapshot of {0} was taken".format(env_root_dir))
                print("    Locusts needs to take snapshot to compare filesystems")
                exit(1)
        else:
            snapshot = {}


        # Create extrenal master script that checks out from time to time (each 5 mins or so)
        # This step is over only when no process is active anymore
        lenlist = [len(x[1]) for x in tasks_per_job]
        if waiting_time == -1:
            waiting_time = min(600, 10*(1+len(job_data)//min(lenlist)))
        if not only_gather:
            time.sleep(max(60,waiting_time))
        log_txt += remote_job_control(
            protocol_triad,
            batch_job_code, 
            fs_locations, 
            tasks_per_job, 
            waiting_time,
            data_transfer_protocol,
            locout_dir,
            only_gather=only_gather
        )
        gc.collect()


        # Collect results, update job_data (only processes that remained pending on running are written in job_data again
        job_data, outp, witherr = gather_results(
            protocol_triad,
            cache_dir, 
            job_data, 
            batch_job_code, 
            task_folders, 
            fs_locations, 
            tasks_per_job, 
            log_dir,
            data_transfer_protocol,
            build_envroot=env_root_dir,
            snapshot=snapshot,
            noenvrm = noenvrm
        )
        if witherr:
            completed_with_error |= witherr
        if outp:
            for x in outp:
                output_paths[x] = outp[x]
        gc.collect()

    run_log_fn = cache_dir + 'run.log'
    with open(run_log_fn, 'w') as rf:
        rf.write(log_txt)
    print('Run log file available at {0}'.format(run_log_fn))
