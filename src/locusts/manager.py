from locusts.support import *

def create_exec_file(id_list, command_template, indir, outdir, output_filename_templates,
        exec_filename, shared_inputs=[], inputs_for_clean_environment=[]):
    with open(exec_filename, "w") as exec_file:
        for ip, idx in enumerate(id_list):
            print("create_exec_file", ip, idx)
            print(command_template)
            print(command_template.replace("<id>", idx))
            print(("ciao"
                "bao"))
            exec_file.write(('c{0}:\t{1}\n'.format(str(ip).zfill(6), command_template.replace("<id>", idx)))) 
            if inputs_for_clean_environment:
                inls = ' '.join([indir + x.replace("<id>", idx) for x in inputs_for_clean_environment])
                exec_file.write('i{0}:\t{1}\n'.format(str(ip).zfill(6), inls))
            if shared_inputs:
                shls = ' '.join([x.replace("<id>", idx).replace(":", ":"+indir) for x in shared_inputs])
                exec_file.write('s{0}:\t{1}\n'.format(str(ip).zfill(6), shls))
            ols = ' '.join([x.replace("<id>", idx) for x in output_filename_templates])
            exec_file.write('o{0}:\t{1}\n'.format(str(ip).zfill(6), ols))


def generate_exec_filesystem(cache_dir, job_data, exec_path, batch_job_code, 
        hpc_shared_folder=None, remote_machine=None):
    # If remote_exec_path == None, exe_dir must be a shared folder between the hpc and this machine
    # Otherwise, the folder is prepared here, then moved in the correct remote location, then deleted from here

    # Assure exec_path is in the format "/abspath/to/exec/" or "relpath/to/exec/" (no extra "/" or missing trailing "/")
    ft = "/" if exec_path[0] == "/" else ""
    exec_path = ft + "/".join([x for x in exec_path.split("/") if x]) + "/"

    if remote_machine and not hpc_shared_folder:
        # A temporary local path is created
        main_exec_path = cache_dir + "/" + batch_job_code + "_tmp_exec_path/"
        print("mkdir", main_exec_path)
        if not os.path.exists(main_exec_path):
            os.mkdir(main_exec_path)
    else:
        # The real location is used
        main_exec_path = exec_path

    exec_locations = {
        "main_local" : main_exec_path, 
        "main_remote" : "", 
        "shared" : "shared/", 
        "exe_dir" : "exec_folder/"}			
    shared_path = main_exec_path + exec_locations["shared"]
    exec_folder_path = main_exec_path + exec_locations["exe_dir"]
    if not os.path.exists(main_exec_path):
        os.mkdir(main_exec_path)
        os.mkdir(shared_path)
        os.mkdir(exec_folder_path)
    if not os.path.exists(shared_path):
        os.mkdir(shared_path)
    if not os.path.exists(exec_folder_path):
        os.mkdir(exec_folder_path)
    shared_path = os.path.abspath(shared_path) + "/"

    main_clean_folder = main_exec_path + batch_job_code + '/'
    if not os.path.exists(main_clean_folder):
        os.mkdir(main_clean_folder)

    # Batch folders for 10000 tasks
    for i in range(len(job_data)//10000 + 1):
        batch_folder = main_clean_folder + 'batch_' + str(i) + '/'
        if not os.path.exists(batch_folder):
            os.mkdir(batch_folder)

    # Job folders for individual tasks
    exec_filesystem = {}
    shared = {}
    for jdi, jd in enumerate(job_data):
        batchno = jdi // 10000
        batch_folder = main_clean_folder + 'batch_' + str(batchno) + '/'
        job_folder = batch_folder + 'task_' + jd['unique_code'] + '/'
        if remote_machine and hpc_shared_folder:
            rem_jf = (hpc_shared_folder + batch_job_code + '/' + 'batch_'
                + str(batchno) + '/' + 'task_' + jd['unique_code'] + '/')
        elif remote_machine and not hpc_shared_folder:
            rem_jf = (exec_path + batch_job_code + '/' + 'batch_'
                + str(batchno) + '/' + 'task_' + jd['unique_code'] + '/')
        else:
            rem_jf = job_folder
        exec_filesystem[jd['unique_code']] = [rem_jf, {}]
        if not os.path.exists(job_folder):
            os.mkdir(job_folder)
        for fpath in jd['clean_env_inps']:
            destpath = job_folder + os.path.basename(fpath)
            print("copy", fpath, destpath)
            shutil.copyfile(fpath, destpath)
        for skey in jd['shared_inps']:
            print("shared", skey)
            if skey not in shared:
                batchisp = (len(shared) + 1)//10000
                batch_folder = shared_path + 'batch_' + str(batchisp) + '/'
                if not os.path.exists(batch_folder):
                    os.mkdir(batch_folder)
                destpath = batch_folder + os.path.basename(jd['shared_inps'][skey])
                shutil.copyfile(jd['shared_inps'][skey], destpath) # Fill shdir
                if remote_machine and hpc_shared_folder:
                    destpath = (hpc_shared_folder + exec_locations["shared"]
                        + 'batch_' + str(batchisp) + '/' + os.path.basename(jd['shared_inps'][skey]))
                elif remote_machine and not hpc_shared_folder:
                    destpath = (exec_path + exec_locations["shared"]
                        + 'batch_' + str(batchisp) + '/' + os.path.basename(jd['shared_inps'][skey]))
                shared[skey] = destpath
            else:
                destpath = shared[skey]
            exec_filesystem[jd['unique_code']][1]["<shared>"+skey] = destpath

        new_command = beautify_bash_oneliner(
            jd['command'], 
            replacements=exec_filesystem[jd['unique_code']][1])
        task_filename = job_folder + "task.sh"
        with open(task_filename, "w") as tf:
            tf.write(new_command)
        subprocess.call(["chmod", "777", task_filename])

    if remote_machine and not hpc_shared_folder:
        exec_locations["main_remote"] = exec_path
        exec_locations["main_local"] = ''
        lsdir = subprocess.Popen(["ssh", remote_machine, "ls", exec_path], stdout=open('/dev/null', 'w'), stderr=subprocess.PIPE).stderr.read().decode('ascii').strip()
        lsdirup = subprocess.Popen(["ssh", remote_machine, "ls", os.path.dirname(exec_path[:-1])], stdout=open('/dev/null', 'w'), stderr=subprocess.PIPE).stderr.read().decode('ascii').strip()
        if "ls: cannot access" in lsdirup: # If ls on parent directory gives error
            print('Failed to create {0}\nPath {1} not present'
                .format(exec_path, os.path.dirname(exec_path[:-1])))
            print(lsdirup)
            exit(1)
        elif "ls: cannot access" not in lsdir: # If ls on the exec directory does not give error
            print(('Exec path {0} is already present in remote location {1}\n'
                'No permission to overwrite it, please delete it manually.')
                .format(exec_path, remote_machine))
            exit(1)
        print(["scp", "-r", main_exec_path, remote_machine+":"+exec_path])
        p = subprocess.Popen(
            ["scp", "-r", main_exec_path, remote_machine+":"+exec_path],
            stderr=open('/dev/null', 'w'),
            stdout=open('/dev/null', 'w'))
        p.wait()
        # Dangerous, but have to remove the local copy of the filesystem
        p = subprocess.Popen(
            ["rm", "-rf", main_exec_path],
            stderr=open('/dev/null', 'w'),
            stdout=open('/dev/null', 'w'))
        p.wait()
    elif remote_machine and hpc_shared_folder:
        exec_locations["main_remote"] = hpc_shared_folder

    return {k : exec_filesystem[k][0] for k in exec_filesystem}, exec_locations


def create_manager_scripts(cache_dir, task_folders,
        cpus_per_node, requested_nodes, batch_job_code, exec_locations,
        hpc_shared_folder=None, remote_machine=None):

    template_filename = os.path.dirname(os.path.realpath(__file__)) + '/template_manager.txt'

    if remote_machine and not hpc_shared_folder:
        loc_exe_dir = cache_dir + batch_job_code + "_tmp_task_dir/"
        if not os.path.exists(loc_exe_dir):
            os.mkdir(loc_exe_dir)
        rem_exe_dir = exec_locations["main_remote"] + exec_locations["exe_dir"]
    elif remote_machine and hpc_shared_folder:
        loc_exe_dir = exec_locations["main_local"] + exec_locations["exe_dir"]
        rem_exe_dir = hpc_shared_folder + exec_locations["exe_dir"]
    elif not remote_machine:
        this_exe_path = exec_locations["main_local"] + exec_locations["exe_dir"]
        loc_exe_dir = rem_exe_dir = this_exe_path

    key_list = sorted([k for k in task_folders])
    # General method for dividing len(key_list) elements in requested_nodes
    # lists. Ex: 6 jobs in 4 processors = [2,2,1,1]
    tasks_per_node_list = distribute_items_in_fixed_length_list(
        requested_nodes, 
        len(key_list)
    )
    if remote_machine and min(tasks_per_node_list) < 10*cpus_per_node:
        grand = 10*cpus_per_node / max(tasks_per_node_list)
        rqn = (
            int(requested_nodes / grand)
            + max(tasks_per_node_list) - min(tasks_per_node_list)
        )
        requested_nodes = max(1, rqn)
        tasks_per_node_list = distribute_items_in_fixed_length_list(
            requested_nodes, 
            len(key_list)
        )
    print("Actual number of nodes used", requested_nodes)

    # Each manager script handles one node
    sublists_per_job = []
    for jobid in range(requested_nodes):
        # Define script variables
        tasks_per_node = tasks_per_node_list[jobid]
        ik = sum(tasks_per_node_list[:jobid])
        tfname = 'taskfile_{0}{1}'.format(batch_job_code, str(jobid).zfill(3))
        taskfilename = rem_exe_dir + '{0}.txt'.format(tfname)
        outpath = rem_exe_dir + '{0}.out.txt'.format(tfname)
        errpath = rem_exe_dir + '{0}.err.txt'.format(tfname)
        partition = 'norm' if requested_nodes < 10 else 'multinode'

        # Compile using the template (so far, only supports SLURM)
        with open(template_filename) as tempf:
            text = tempf.read()
        text = (
            text.replace('<jobd>', batch_job_code)
            .replace('<jobid>', str(jobid).zfill(3))
            .replace('<cpuspertask>', str(cpus_per_node))
            .replace('<outpath>', outpath)
            .replace('<errpath>', errpath)
            .replace('<taskfile>', taskfilename)
            .replace('<exedir>', rem_exe_dir)
            .replace('<partition>', partition)
        )
        manager_filename = (
            loc_exe_dir 
            + 'manager_{0}{1}.slurm'.format(batch_job_code, str(jobid).zfill(3))
        )
        with open(manager_filename, 'w') as mf:
            mf.write(text)
        subprocess.call(["chmod", "777", manager_filename])
        taskfilename = (
            loc_exe_dir
            + 'taskfile_{0}{1}.txt'.format(batch_job_code, str(jobid).zfill(3))
        )
        with open(taskfilename, 'w') as tf:
            tf.write(''.join(
                [(x + '\t' + task_folders[x] + '\n') 
                for x in key_list[ik:ik+tasks_per_node]]
            ))
        identifier = (remote_machine, rem_exe_dir, batch_job_code, jobid)
        sublists_per_job.append((identifier, key_list[ik:ik+tasks_per_node]))

    rmhidden_cmd = ["rm", loc_exe_dir+".*"]
    if remote_machine and not hpc_shared_folder:
        rmhidden_cmd = ["ssh", remote_machine] + rmhidden_cmd
        filestocopy = [x for x in glob.glob(loc_exe_dir+"/*")]
        p = subprocess.Popen(
            ["scp"] + filestocopy + [remote_machine+":"+rem_exe_dir],
            stderr=open('/dev/null', 'w'),
            stdout=open('/dev/null', 'w'),
        )
#        print('EXEDIRLOC', loc_exe_dir)
#        print(["scp"] + filestocopy + [remote_machine+":"+rem_exe_dir])
        p.wait()
        p = subprocess.Popen(
            ["rm", "-rf", loc_exe_dir],
            stderr=open('/dev/null', 'w'),
            stdout=open('/dev/null', 'w')
        )
        p.wait()
    p = subprocess.Popen(
        rmhidden_cmd, 
        stderr=open('/dev/null', 'w'),
        stdout=open('/dev/null', 'w')
    )
    p.wait()

    # List of job identifiers (exe dir, job descr, job id)
    # and key sublists for each manager script
    return sublists_per_job


def remote_job_control(batch_job_code, exe_dir_path, sublists_per_job,
        waiting_time, remote_machine, hpc_shared_folder):
    # There must be a passwordless connection between the two machines: to
    # achieve it, ssh-agent and then ssh-add.
    # If the two machines share a folder - and thus there is no need to ssh to
    # the remote machine just for checking - it must be stated here

    if remote_machine:
        for ij, sublist_per_job in enumerate(sublists_per_job):
            identifier, _ = sublist_per_job
            _, rem_exe_dir, _, _ = identifier
            p = subprocess.Popen(
                [
                    "ssh", 
                    remote_machine, 
                    "sbatch", 
                    rem_exe_dir + 'manager_{0}{1}.slurm'
                        .format(batch_job_code, str(ij).zfill(3))
                ],
                stderr=open('/dev/null', 'w'),
                stdout=open('/dev/null', 'w')
            )
    else:
        # If no hpc, there is only one node, i.e. one manager
        rem_exe_dir = sublists_per_job[0][0][1]
        mname = 'manager_{0}{1}.slurm'.format(batch_job_code, str(0).zfill(3))
        print("nohup", rem_exe_dir + mname)
        p = subprocess.Popen(
            ["nohup", rem_exe_dir + mname], 
            stderr=open('/dev/null', 'w'), 
            stdout=open('/dev/null', 'w')
        )

    is_over = False
    while not is_over:
        time.sleep(waiting_time)
        is_over = True
        for ij, sublist_per_job in enumerate(sublists_per_job):
            identifier, _ = sublist_per_job
            _, rem_exe_dir, _, _ = identifier

            macname = "{0}/.manager_activity_check_{1}{2}".format(
                rem_exe_dir, 
                batch_job_code, 
                str(ij).zfill(3)
            )
            if remote_machine and not hpc_shared_folder:
                manager_cmd = ["ssh", remote_machine, "ls", "-latrh", macname]
                chk_time_cmd = "ssh " + remote_machine + " echo $(date +%H:%M)"
            elif remote_machine:
                manager_cmd = ["ls", "-latrh", macname]
                chk_time_cmd = "ssh " + remote_machine + " echo $(date +%H:%M)"
            else:
                manager_cmd = ["ls", "-latrh", macname]
                chk_time_cmd = "echo $(date +%H:%M)"
            touch_time_txt = subprocess.Popen(
                manager_cmd, 
                stderr=open('/dev/null', 'w'), 
                stdout=subprocess.PIPE
            ).stdout.read().decode('ascii')
            print("ls", "-latrh", "{0}/.manager_activity_check_{1}{2}".format(exe_dir_path, batch_job_code, str(ij).zfill(3)))
            print("ANSWER", touch_time_txt)
            if touch_time_txt.strip():
                local_time_l = subprocess.Popen(
                    chk_time_cmd, 
                    stderr=open('/dev/null', 'w'), 
                    stdout=subprocess.PIPE, 
                    shell=True
                ).stdout.read().decode('ascii').split()[0].split(":")
                touch_time_l = touch_time_txt.split()[7].split(":")
                touch_time = int(touch_time_l[0])*60 + int(touch_time_l[1])
                local_time = int(local_time_l[0])*60 + int(local_time_l[1])
                coeff = (local_time - touch_time)%(24*60)
                is_active = False if coeff > 1 else True
                print(touch_time, local_time)
                if is_active:
                    is_over = False
                    print("Job", ij, "running")
                else:
                    print("Job", ij, "ended")
            else:
                is_over = False
                print("Job", ij, "pending")
    print("Jobs are over")


def gather_results(cache_dir, job_data, batch_job_code, task_folders,
        main_exec_path, exec_locations, sublists_per_job, log_dir, 
        remote_machine=None):
    devnull = open('/dev/null', 'w')

    # The database is now connected with local machine
    if remote_machine:
        output_local_dir = cache_dir + "/" + batch_job_code + "_output_tmp/"
        main_exec_dir = output_local_dir
        scp_path = remote_machine+":"+main_exec_path+batch_job_code+"/"
        scp_cmd = ["scp", "-r", scp_path, output_local_dir]
        print(scp_cmd)
        p = subprocess.Popen(scp_cmd, stderr=devnull, stdout=devnull)
        p.wait()
        
        ssh_cmd = ["ssh", remote_machine, "rm", "-rf", main_exec_path]
        print(ssh_cmd)
#        p = subprocess.Popen(ssh_cmd, stderr=devnull, stdout=devnull)
#        p.wait()
    else:
        main_exec_dir = main_exec_path

    
    ft = "/" if main_exec_dir[0] == "/" else ""
    main_exec_dir = ft + "/".join([x for x in main_exec_dir.split("/") if x]) + "/"

    reschedule = set()
    for ij, sublist in enumerate(sublists_per_job):
        tfname = "taskfile_{0}{1}.*".format(batch_job_code, str(ij).zfill(3))
        task_filename = main_exec_dir + exec_locations["exe_dir"] + tfname
        print("mv", task_filename, log_dir)
        p = subprocess.Popen(
            ["mv", task_filename, log_dir], 
            stderr=devnull, 
            stdout=devnull
        )
        p.wait()
        sname = "status_{0}{1}".format(batch_job_code, str(ij).zfill(3))
        status_filename = main_exec_dir + exec_locations["exe_dir"] + sname
        grep_pending_cmd = ["grep", "'pending'", status_filename]
        if remote_machine:
            grep_pending_cmd = ["ssh", remote_machine] + grep_pending_cmd
        txtlines = subprocess.Popen(
            grep_pending_cmd, 
            stderr=open('/dev/null', 'w'), 
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
    completed_with_error = set()
    output_paths = {}
    for jd in job_data:
        if jd['unique_code'] in reschedule:
            continue
        for output in jd['outputs']:
            tfuc = task_folders[jd['unique_code']]
            slashlist = re.sub("/(/+)", "/", tfuc).split("/")
            (
                relative_env_folder, 
                relative_batch_folder, 
                relative_task_folder
            ) = [x+"/" for x in slashlist][-4:-1]
            output_batch_dir = jd['output_dir'] + relative_batch_folder
            output_task_dir = (jd['output_dir'] + relative_batch_folder 
                + relative_task_folder)
            if not os.path.exists(output_batch_dir):
                os.mkdir(output_batch_dir)
            if not os.path.exists(output_task_dir):
                os.mkdir(output_task_dir)
            output_path = (main_exec_dir
                + relative_batch_folder + relative_task_folder + output)
            print(output_path,  os.path.exists(output_path))
            if os.path.exists(output_path): 
                print("mv", output_path, output_task_dir)
                p = subprocess.Popen(
                    ["mv", output_path, output_task_dir], 
                    stderr=devnull, 
                    stdout=devnull
                )
                p.wait()
                output_paths[jd['unique_code']] = output_task_dir
            else:
                completed_with_error.add(jd['unique_code'])
    p = subprocess.Popen(
        ["rm", "-rf", main_exec_dir+"/*"], 
        stderr=devnull, 
        stdout=devnull
    )
    p.wait()
    output_logfilename = jd['output_dir'] + "output.log"
    with open(output_logfilename, "w") as of:
        for jid in sorted([k['unique_code'] for k in job_data]):
            if jid in output_paths:
                status = "present"
                path = output_paths[jid]
            elif jid in completed_with_error:
                status = "error"
                path = "-"
            else:
                status = "missing"
                path = "-"
            of.write("{0}\t{1}\n".format(status, path))

    output_d = { k['unique_code'] : k 
        for k in job_data if k['unique_code'] in reschedule }

    return output_d, output_paths, completed_with_error


def highly_parallel_job_manager(options, exec_filename,
        batch_job_code, output_dir, force_redo=False):
    this_name = highly_parallel_job_manager.__name__

    # Creates cache dir
    cache_dir = output_dir + ".cache/" 
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
    os.mkdir(cache_dir)
    cache_dir = os.path.realpath(cache_dir) + '/'

    # Creates log dir
    log_dir = output_dir + "logs/"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.mkdir(log_dir)

    # Read exec file and compile job_data
    job_data = []
    with open(exec_filename) as exec_file:
        for line in exec_file:
            if line.startswith("c"):
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
            elif line.startswith("i"):
                fields = line.split()
                jd['clean_env_inps'] = fields[1:]
                print('clean_env_inps', fields[1:], line, exec_filename)
            elif line.startswith("s"):  # shared inputs must be declared in "s" line as file.txt:/path/of/file.txt, where file.txt is a filename appearing in the "c" line
                fields = line.split()
                jd['shared_inps'] = {k : p 
                    for (k,p) in [x.split(":") for x in fields[1:]]}
#                print("LETTURA SHARED", jd['shared_inps'])
            elif line.startswith("o"):
                fields = line.split()
                jd['outputs'] = fields[1:]
                jd['output_dir'] = output_dir
                jd['unique_code'] = batch_job_code + fields[0][1:-1]
                jd['log_filename'] = (log_dir + batch_job_code + 
                    fields[0][1:-1] + '_log.txt')
                if os.path.exists(jd['log_filename']):
                    if force_redo:
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
            main_exec_path = options['local_shared_dir']
            print("ONE", options)
            hpc_shared_folder = options['hpc_exec_dir']
        else:
            print("TWO", options)
            main_exec_path = options['hpc_exec_dir']
            hpc_shared_folder = None
    else:
        remote_exec_path = False
        remote_machine = None
        hpc_shared_folder = None
        requested_nodes = 1
        cpus_per_node = options['number_of_processors']
        main_exec_path = cache_dir + batch_job_code + "_tmp_exec_path/"
        if not os.path.exists(main_exec_path):
            os.mkdir(main_exec_path)


    completed_with_error = set()
    output_paths = {}
    while job_data:
        # Create the hosting file system
        task_folders, exec_locations = generate_exec_filesystem(
            cache_dir, 
            job_data, 
            main_exec_path,
            batch_job_code,
            hpc_shared_folder=hpc_shared_folder, 
            remote_machine=remote_machine
        )

        # Create local manager script that does the mpiq job, launchable on each node. It checks the situation regularly each 10 secs.
        sublists_per_job = create_manager_scripts(
            cache_dir, 
            task_folders, 
            cpus_per_node, 
            requested_nodes, 
            batch_job_code, 
            exec_locations, 
            hpc_shared_folder=hpc_shared_folder, 
            remote_machine=remote_machine
        )

        # Create extrenal master script that checks out from time to time (each 5 mins or so)
        # This step is over only when no process is active anymore
        lenlist = [len(x[1]) for x in sublists_per_job]
        waiting_time = min(600, 10*(1+len(job_data)//min(lenlist)))
        remote_job_control(
            batch_job_code, 
            main_exec_path + exec_locations["exe_dir"], 
            sublists_per_job, 
            waiting_time, 
            remote_machine,
            hpc_shared_folder
        )

        # Collect results, update job_data (only processes that remained pending on running are written in job_data again
        job_data, outp, witherr = gather_results(
            cache_dir, 
            job_data, 
            batch_job_code, 
            task_folders, 
            main_exec_path, 
            exec_locations, 
            sublists_per_job, 
            log_dir,
            remote_machine=remote_machine
        )
        completed_with_error |= witherr
        for x in outp:
            output_paths[x] = outp[x]


def define_options():
    options = {}
    options['TYPES'] = {}
    options['run_on_hpc'], options['TYPES']['run_on_hpc'] = False, bool
    options['host_name'], options['TYPES']['host_name'] = None, str
    options['requested_nodes'], options['TYPES']['requested_nodes'] = None, int
    options['cpus_per_node'], options['TYPES']['cpus_per_node'] = None, int
    options['local_shared_dir'], options['TYPES']['local_shared_dir'] = None, str
    options['hpc_exec_dir'], options['TYPES']['hpc_exec_dir'] = None, str
    options['number_of_processors'], options['TYPES']['number_of_processors'] = 1, int
    return options


def launch(indir=None, outdir=None, code=None, spcins=None, shdins=None,
        outs=None, cmd=None, optf=None):

    devnull = open("/dev/null", "w")
    options = define_options()

    # Check args
    if not (indir and outdir and code and spcins and outs and cmd and optf):
        print(("ERROR (launch): Please specify all the arguments. Only shared"
            "inputs are optional"))
        exit(1)

    # Parse option file
    if not os.path.exists(optf):
        print("ERROR (launch): The specified parameter file was not found.")
        print("                Path: {0}".format(optf))
        exit(1)
    else:
        with open(optf) as f:
            for l in f:
                # Remove comments
                if "#" in l:
                    line = l[:l.index("#")].strip()
                else:
                    line = l.strip()
                # Do not parse empty lines
                if not line:
                    continue
                fields = line.split()
                if fields[0] not in options:
                    print(("ERROR (launch): {0} ".format(fields[0]),
                        "is not a valid argument"))
                    exit(1)
                # If argument has no value, default value will be chosen
                if len(fields) < 2:
                    continue
                field0, field1 = fields[0], " ".join(fields[1:])
                deftype = options['TYPES'][field0]
                if deftype == bool:
                    if field1 == "False":
                        options[field0] = False
                    elif field1 == "True":
                        options[field0] = True
                    else:
                        raise ValueError
                else:
                    options[field0] = options['TYPES'][field0](field1)
                    if field0[-4:] == "_dir" and options[field0][-1] != "/":
                        options[field0] += "/"

    print("indir", indir)
    # Compile set of IDs basing on the files in indir
    id_list = []
    for spcin in spcins:
        spc_format = spcin.replace("<id>", "*")
        inp_list = [os.path.basename(x) for x in glob.glob(indir+spc_format)]
        id_place = spc_format.index("*")
        for inp in inp_list:
            suffix_len = len(spc_format)-id_place-1
            idx = inp[id_place:-suffix_len]
            if idx not in id_list:
                id_list.append(idx)

    # If all indexes are integer numbers, it will sort them correctly
    idx_are_int = True
    for idx in id_list:
        try:
            tryidx = int(idx)
        except:
            idx_are_int = False
        if not idx_are_int:
            break
    if idx_are_int:
        id_list = [str(x) for x in sorted([int(y) for y in id_list])]
    else:
        id_list = sorted(id_list)
    print("listofids", id_list)

    # Create the execution file used by the job manager
    exec_filename = outdir + code + ".exec_file.txt"
    create_exec_file(
        id_list, 
        cmd,
        indir,
        outdir, 
        outs, 
        exec_filename, 
        shared_inputs=shdins, 
        inputs_for_clean_environment=spcins
    )

    # Main routine
    highly_parallel_job_manager(options, exec_filename, code, outdir, force_redo=False)
