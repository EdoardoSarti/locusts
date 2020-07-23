# Name: 
# Language: python3
# Libraries:
# Description: 
# Author: Edoardo Sarti
# Date: 

from manager_supporting_functions import *

def create_exec_file(str_set, command_template, output_filename_templates, exec_filename, shared_inputs=[], inputs_for_clean_environment=[]):
	with open(exec_filename, "w") as exec_file:
		for ip, pdbi in enumerate(sorted(list(str_set))):
			exec_file.write('c'+str(ip).zfill(6)+":\t"+command_template.replace("<pdbi>", pdbi)+'\n')
			if inputs_for_clean_environment:
				exec_file.write('i'+str(ip).zfill(6)+":\t"+''.join([x.replace("<pdbi>", pdbi)+" " for x in inputs_for_clean_environment])+'\n')
			if shared_inputs:
				exec_file.write('s'+str(ip).zfill(6)+":\t"+''.join([x.replace("<pdbi>", pdbi)+" " for x in shared_inputs])+'\n')
			exec_file.write('o'+str(ip).zfill(6)+":\t"+''.join([x.replace("<pdbi>", pdbi)+" " for x in output_filename_templates])+'\n')


def generate_exec_filesystem(locations, job_data, exec_path, hpc_shared_folder=None, remote_machine=None):
	# If remote_exec_path == None, exe_dir must be a shared folder between the hpc and this machine
	# Otherwise, the folder is prepared here, then moved in the correct remote location, then deleted from here

	if remote_machine and not hpc_shared_folder:
		# A temporary local path is created
		main_exec_path = locations['FSYSPATH']['cache'] + "/" + batch_job_code + "_tmp_exec_path/"
		print("mkdir", main_exec_path)
		os.mkdir(main_exec_path)
	else:
		# The real location is used
		main_exec_path = exec_path

	exec_locations = {"main_local" : main_exec_path, "main_remote" : "", "shared" : "shared/", "exe_dir" : "exec_folder/"}			
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
		batchno = jdi//10000
		batch_folder = main_clean_folder + 'batch_' + str(batchno) + '/'
		job_folder = batch_folder + 'task_' + jd['unique_code'] + '/'
		if remote_machine and hpc_shared_folder:
			rem_jf = hpc_shared_folder + batch_job_code + '/' + 'batch_' + str(batchno) + '/' + 'task_' + jd['unique_code'] + '/'
		else:
			rem_jf = job_folder
		exec_filesystem[jd['unique_code']] = [rem_jf, {}]
		if not os.path.exists(job_folder):
			os.mkdir(job_folder)
		for fpath in jd['clean_env_inps']:
			destpath = job_folder + os.path.basename(fpath)
			shutil.copyfile(fpath, destpath)
		for skey in jd['shared_inps']:
			print("shared", skey)
			if skey not in shared:
				batchisp = (len(shared) + 1)//10000
				batch_folder = shared_path + 'batch_' + str(batchisp) + '/'
				if not os.path.exists(batch_folder):
					os.mkdir(batch_folder)
				destpath = batch_folder + skey
				shutil.copyfile(jd['shared_inps'][skey], destpath)	# fills the shared directory
				if remote_machine and hpc_shared_folder:
					destpath = hpc_shared_folder + exec_locations["shared"] + 'batch_' + str(batchisp) + '/' + skey
				shared[skey] = destpath
			else:
				destpath = shared[skey]
			exec_filesystem[jd['unique_code']][1]["<shared>"+skey] = destpath

		new_command = beautify_bash_oneliner(jd['command'], replacements=exec_filesystem[jd['unique_code']][1])
		task_filename = job_folder + "task.sh"
		with open(task_filename, "w") as tf:
			tf.write(new_command)
		subprocess.call(["chmod", "777", task_filename])

	if remote_machine and not hpc_shared_folder:
		exec_locations["main_remote"] = exec_path
		exec_locations["main_local"] = ''
		lsfile = subprocess.Popen(["ssh", remote_machine, "ls", exec_path], stderr=open('/dev/null', 'w'), stdout=subprocess.PIPE).stdout.read().decode('ascii').strip()
		if lsfile:
			print_log('CRITICAL', this_name, 'Exec path {0} is already present in remote location {1}. No permission to overwrite it, please delete it manually.'.format(exec_path, remote_machine))

		print(["scp", "-r", main_exec_path, remote_machine+":"+exec_path])
		p = subprocess.Popen(["scp", "-r", main_exec_path, remote_machine+":"+exec_path], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
		p.wait()
		# Dangerous, but have to remove the local copy of the filesystem
		p = subprocess.Popen(["rm", "-rf", main_exec_path], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
		p.wait()
	elif remote_machine and hpc_shared_folder:
		exec_locations["main_remote"] = hpc_shared_folder


	return {k : exec_filesystem[k][0] for k in exec_filesystem}, exec_locations


def create_manager_scripts(locations, task_folders, cpus_per_node, requested_nodes, batch_job_code, exec_locations, hpc_shared_folder=None, remote_machine=None):
	if remote_machine and not hpc_shared_folder:
		loc_exe_dir = locations['FSYSPATH']['cache'] + batch_job_code + "_tmp_task_dir/"
		os.mkdir(loc_exe_dir)
		rem_exe_dir = exec_locations["main_remote"] + exec_locations["exe_dir"]
	elif remote_machine and hpc_shared_folder:
		loc_exe_dir = exec_locations["main_local"] + exec_locations["exe_dir"]
		rem_exe_dir = hpc_shared_folder + exec_locations["exe_dir"]
	elif not remote_machine:
		loc_exe_dir = rem_exe_dir = exec_locations["main_local"] + exec_locations["exe_dir"]

	key_list = sorted([k for k in task_folders])
	# General method for dividing len(key_list) elements in requested_nodes lists. Ex: 6 jobs in 4 processors = [2,2,1,1]
	tasks_per_node_list = distribute_items_in_fixed_length_list(requested_nodes, len(key_list))
	if remote_machine and min(tasks_per_node_list) < 10*cpus_per_node:
		requested_nodes = max(1, int(requested_nodes / (10*cpus_per_node / max(tasks_per_node_list))) + max(tasks_per_node_list) - min(tasks_per_node_list))
		tasks_per_node_list = distribute_items_in_fixed_length_list(requested_nodes, len(key_list))
	print("Actual number of nodes used", requested_nodes)

	# Each manager script handles one node
	sublists_per_job = []
	for jobid in range(requested_nodes):
		# Define script variables
		tasks_per_node = tasks_per_node_list[jobid]
		ik = sum(tasks_per_node_list[:jobid])
		taskfilename = rem_exe_dir + 'taskfile_{0}{1}.txt'.format(batch_job_code, str(jobid).zfill(3))
		outpath = rem_exe_dir + 'taskfile_{0}{1}.out.txt'.format(batch_job_code, str(jobid).zfill(3))
		errpath = rem_exe_dir + 'taskfile_{0}{1}.err.txt'.format(batch_job_code, str(jobid).zfill(3))
		partition = 'norm' if requested_nodes < 10 else 'multinode'

		# Compile using the template
		with open(locations['SYSFILES']['slurmtemplate']) as slurmtempf:
			text = slurmtempf.read()
		text = text.replace('<jobd>', batch_job_code).replace('<jobid>', str(jobid).zfill(3)).replace('<cpuspertask>', str(cpus_per_node)).replace('<outpath>', outpath).replace('<errpath>', errpath).replace('<taskfile>', taskfilename).replace('<exedir>', rem_exe_dir).replace('<partition>', partition)
		manager_filename = loc_exe_dir + 'manager_{0}{1}.slurm'.format(batch_job_code, str(jobid).zfill(3))
		with open(manager_filename, 'w') as mf:
			mf.write(text)
		subprocess.call(["chmod", "777", manager_filename])
		taskfilename = loc_exe_dir + 'taskfile_{0}{1}.txt'.format(batch_job_code, str(jobid).zfill(3))
		with open(taskfilename, 'w') as tf:
			tf.write(''.join([x+'\t'+task_folders[x]+'\n' for x in key_list[ik:ik+tasks_per_node]]))
		identifier = (remote_machine, rem_exe_dir, batch_job_code, jobid)
		sublists_per_job.append((identifier, key_list[ik:ik+tasks_per_node]))

	rmhidden_cmd = ["rm", loc_exe_dir+".*"]
	if remote_machine and not hpc_shared_folder:
		rmhidden_cmd = ["ssh", remote_machine] + rmhidden_cmd
		p = subprocess.Popen(["scp", loc_exe_dir+"*", remote_machine+":"+rem_exe_dir], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
		p.wait()
		p = subprocess.Popen(["rm", "-rf", loc_exe_dir], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
		p.wait()
	p = subprocess.Popen(rmhidden_cmd, stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
	p.wait()

	return sublists_per_job	# List of job identifiers (exe dir, job descr, job id) and key sublists for each manager script


def remote_job_control(batch_job_code, exe_dir_path, sublists_per_job, waiting_time, remote_machine):
	# There must be a passwordless connection between the two machines: to achieve it, ssh-agent and then ssh-add
	# If the	 two machines share a folder - and thus there is no need to ssh to the remote machine just for checking - it must be stated here

	if remote_machine:
		for ij, sublist_per_job in enumerate(sublists_per_job):
			identifier, _ = sublist_per_job
			_, rem_exe_dir, _, _ = identifier
			p = subprocess.Popen(["ssh", remote_machine, "sbatch", rem_exe_dir + 'manager_{0}{1}.slurm'.format(batch_job_code, str(ij).zfill(3))], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
	else:
		# If no hpc, there is only one node, i.e. one manager
		rem_exe_dir = sublists_per_job[0][0][1]
		print("nohup", rem_exe_dir + 'manager_{0}{1}.slurm'.format(batch_job_code, str(0).zfill(3)))
		p = subprocess.Popen(["nohup", rem_exe_dir + 'manager_{0}{1}.slurm'.format(batch_job_code, str(0).zfill(3))], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))

	is_over = False
	while not is_over:
		time.sleep(waiting_time)
		is_over = True
		for ij, sublist_per_job in enumerate(sublists_per_job):
			identifier, _ = sublist_per_job
			_, rem_exe_dir, _, _ = identifier
			
			if remote_machine and not hpc_shared_folder:
				manager_activity_cmd = ["ssh", remote_machine, "ls", "-latrh", "{0}/.manager_activity_check_{1}{2}".format(rem_exe_dir, batch_job_code, str(ij).zfill(3))]
				check_local_time_cmd = "ssh " + remote_machine + " echo $(date +%H:%M)"
			elif remote_machine:
				manager_activity_cmd = ["ls", "-latrh", "{0}/.manager_activity_check_{1}{2}".format(exe_dir_path, batch_job_code, str(ij).zfill(3))]
				check_local_time_cmd = "ssh " + remote_machine + " echo $(date +%H:%M)"
			else:
				manager_activity_cmd = ["ls", "-latrh", "{0}/.manager_activity_check_{1}{2}".format(exe_dir_path, batch_job_code, str(ij).zfill(3))]
				check_local_time_cmd = "echo $(date +%H:%M)"
			touch_time_txt = subprocess.Popen(manager_activity_cmd, stderr=open('/dev/null', 'w'), stdout=subprocess.PIPE).stdout.read().decode('ascii')
			print("ls", "-latrh", "{0}/.manager_activity_check_{1}{2}".format(exe_dir_path, batch_job_code, str(ij).zfill(3)))
			print("ANSWER", touch_time_txt)
			if touch_time_txt.strip():
				local_time_l = subprocess.Popen(check_local_time_cmd, stderr=open('/dev/null', 'w'), stdout=subprocess.PIPE, shell=True).stdout.read().decode('ascii').split()[0].split(":")
				touch_time_l = touch_time_txt.split()[7].split(":")
				touch_time = int(touch_time_l[0])*60 + int(touch_time_l[1])
				local_time = int(local_time_l[0])*60 + int(local_time_l[1])
				is_active = False if (local_time - touch_time)%(24*60) > 1 else True	
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


def gather_results(job_data, batch_job_code, task_folders, main_exec_path, exec_locations, sublists_per_job, log_dir, remote_machine=None):
	# The database is now connected with local machine
	if remote_machine:
		output_local_dir = locations['FSYSPATH']['cache'] + "/" + batch_job_code + "_output_tmp/"
		main_exec_dir = output_local_dir
		p = subprocess.Popen(["scp", remote_machine+":"+main_exec_path+batch_job_code+"/", output_local_dir], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
		p.wait()
		p = subprocess.Popen(["ssh", remote_machine, "rm", "-rf", main_exec_path], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
		p.wait()
	else:
		main_exec_dir = main_exec_path

	reschedule = set()
	for ij, sublist in enumerate(sublists_per_job):
		task_filename = main_exec_dir + exec_locations["exe_dir"] + "taskfile_{0}{1}.*".format(batch_job_code, str(ij).zfill(3))
		print("mv", task_filename, log_dir)
		p = subprocess.Popen(["mv", task_filename, log_dir], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
		p.wait()
		status_filename = main_exec_dir + exec_locations["exe_dir"] + "status_{0}{1}".format(batch_job_code, str(ij).zfill(3))
		grep_pending_cmd = ["grep", "'pending'", status_filename]
		if remote_machine:
			grep_pending_cmd = ["ssh", remote_machine] + grep_pending_cmd
		txtlines = subprocess.Popen(grep_pending_cmd, stderr=open('/dev/null', 'w'), stdout=subprocess.PIPE).stdout.readlines()
		for line in txtlines:
			internal_id, task_id, status, task_dirpath = line.decode('ascii').split()
			if status in ['running', 'pending']:
				reschedule.add(task_id)
	completed_with_error = set()
	output_paths = {}
	for jd in job_data:
		if jd['unique_code'] in reschedule:
			continue
		for output in jd['outputs']:
			relative_env_folder, relative_batch_folder, relative_task_folder = [x+"/" for x in re.sub("/(/+)", "/", task_folders[jd['unique_code']]).split("/")][-4:-1]
			output_batch_dir = jd['output_dir']+relative_batch_folder
			output_task_dir = jd['output_dir']+relative_batch_folder+relative_task_folder
			if not os.path.exists(output_batch_dir):
				os.mkdir(output_batch_dir)
			if not os.path.exists(output_task_dir):
				os.mkdir(output_task_dir)
			output_path = main_exec_dir+relative_env_folder+relative_batch_folder+relative_task_folder+output
			print(output_path,  os.path.exists(output_path))
			if os.path.exists(output_path): 
				print("mv", output_path, output_task_dir)
				p = subprocess.Popen(["mv", output_path, output_task_dir], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
				p.wait()
				output_paths[jd['unique_code']] = output_task_dir
			else:
				completed_with_error.add(jd['unique_code'])
	p = subprocess.Popen(["rm", "-rf", main_exec_dir+"/*"], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
	p.wait()
	output_lofilename = jd['output_dir'] + "output.log"
	with open(output_lofilename, "w") as of:
		for jid in sorted([k['unique_code'] for k in job_data.keys()]):
			if jid in output_paths:
				status = "present"
				path = output_paths[jid]
			elif jid in completed_with_error:
				status = "error"
				path = "-"
			else:
				status = "missing"
				path = "-"
			of.write("{0}\t{1}".format(status, path))

	return { k['unique_code'] : k for k in job_data if k['unique_code'] in reschedule }, output_paths, completed_with_error


def highly_parallel_jobs_manager(options, locations, exec_filename, batch_job_code, output_dir, force_redo=False):
	this_name = highly_parallel_jobs_manager.__name__

	# Read exec file and compile job_data
	job_data = []
	with open(exec_filename) as exec_file:
#		jd = {'command' : '', 'outputs' : [], 'success' : None, 'issues' : [], 'unique_code' : '', 'log_filename' : '', 'clean_env_inps' : []}
		for line in exec_file:
			if line.startswith("c"):
				jd = {'command' : '', 'outputs' : [], 'output_dir' : '', 'success' : None, 'issues' : [], 'unique_code' : '', 'log_filename' : '', 'clean_env_inps' : [], 'shared_inps' : []}
				jd['command'] = line[8:].strip()
#				for w in jd['command'].split():
#					if w.startswith("<shared>"):
#						jd['shared_inps'].append(w[8:])		# shared inputs can only be present in command line as "command <shared>file.txt", otherwise files have to be copied in the clean environment and are thus listed in the "i" input line.
			elif line.startswith("i"):
				fields = line.split()
				jd['clean_env_inps'] = fields[1:]
			elif line.startswith("s"):	# shared inputs must be declared in "s" line as file.txt:/path/of/file.txt, where file.txt is a filename appearing in the "c" line
				fields = line.split()
				jd['shared_inps'] = {k : p for (k,p) in [x.split(":") for x in fields[1:]]}
#				print("LETTURA SHARED", jd['shared_inps'])
			elif line.startswith("o"):
				fields = line.split()
				jd['outputs'] = fields[1:]
				jd['output_dir'] = output_dir
				jd['unique_code'] = batch_job_code + fields[0][1:-1]
				jd['log_filename'] = locations['FSYSPATH']['logs'] + batch_job_code + fields[0][1:-1] + '_log.txt'
				if os.path.exists(jd['log_filename']):
					if force_redo:
						os.remove(jd['log_filename'])
						job_data.append(jd)
				else:
					job_data.append(jd)

	if options['RUN'][('hpc', 'run_on_hpc')]:
		# Copy files in separate location (for hpc the option 'exec location' must be set to '/data/biowulf/sartie/'
		remote_machine = options['RUN'][('host', 'host_name')]
		requested_nodes = options['RUN'][('nodes', 'requested_nodes')]
		cpus_per_node = options['RUN'][('cpn', 'cpus_per_node')]
		if options['RUN'][('hpclsf', 'local_shared_folder')]:
			main_exec_path = options['RUN'][('hpclsf', 'local_shared_folder')]
			hpc_shared_folder = options['RUN'][('hpcdir', 'hpc_exec_path')]
		else:
			main_exec_path = options['RUN'][('hpcdir', 'hpc_exec_path')]
			hpc_shared_folder = None
	else:
		remote_exec_path = False
		remote_machine = None
		hpc_shared_folder = None
		requested_nodes = 1
		cpus_per_node = options['RUN'][('np', 'number_of_processors')]
		main_exec_path = locations['FSYSPATH']['cache']	+ batch_job_code + "_tmp_exec_path/"
		if os.path.exists(main_exec_path):
			pass
		os.mkdir(main_exec_path)


	completed_with_error = set()
	output_paths = {}
	while job_data:
		# Create the hosting file system
		task_folders, exec_locations = generate_exec_filesystem(locations, job_data, main_exec_path, hpc_shared_folder=hpc_shared_folder, remote_machine=remote_machine)

		# Create local manager script that does the mpiq job, launchable on each node. It checks the situation regularly each 10 secs.
		sublists_per_job = create_manager_scripts(locations, task_folders, cpus_per_node, requested_nodes, batch_job_code, exec_locations, hpc_shared_folder=hpc_shared_folder, remote_machine=remote_machine)
			
		# Create extrenal master script that checks out from time to time (each 5 mins or so)
		# This step is over only when no process is active anymore
		waiting_time = min(600, 10*(1+len(job_data)//min([len(x[1]) for x in sublists_per_job])))
		remote_job_control(batch_job_code, main_exec_path+exec_locations["exe_dir"], sublists_per_job, waiting_time, remote_machine)

		# Collect results, update job_data (only processes that remained pending on running are written in job_data again
		job_data, outp, witherr = gather_results(job_data, batch_job_code, task_folders, main_exec_path, exec_locations, sublists_per_job, locations['FSYSPATH']['logs'])
		completed_with_error |= witherr
		for x in outp:
			output_paths[x] = outp[x]


def run_PPM(options, locations, str_set, str_data, skip_PPM=False):
	this_name = run_PPM.__name__

	if not skip_PPM:
		# Prepare input files
		for pdbi in str_set:
			inpfname = locations['FSYSPATH']['cache'] + pdbi + '.inp'
			with open(inpfname, 'w') as inpf:
				inpf.write(" 0 in  {0}_temp.pdb\n".format(pdbi))
			print(inpfname, os.path.exists(inpfname))


		# Write temporary coord PDB files
		for pdbi in str_set:
			tmp_filename = locations['FSYSPATH']['cache'] + pdbi + '_temp.pdb'
			cif_filename = str_data[pdbi]['present_cache']
			if not cif_filename:
				print_log('CRITICAL', this_name, 'Structure {0} has no designated present cache file'.format(pdbi))
			c_dict_pdbi = retrieve_coords_from_CIF(cif_filename)
			write_ENC(str_data[pdbi], c_dict_pdbi, tmp_filename, locations['SYSFILES']['ignoredpasscodes'])
			print(tmp_filename, os.path.exists(tmp_filename))

		# Inputs for clean environment
		if options['PATHS'][('singularity', 'singularity_path')] and options['PATHS'][('container', 'container_path')]:
			cmd = options['PATHS'][('singularity', 'singularity_path')] + ' exec ' + options['PATHS'][('container', 'container_path')] + ' cp  /EncoMPASS/ppm/res.lib ' + locations['FSYSPATH']['cache']
		else:
			cmd = 'cp ' + os.path.dirname(options['PATHS'][('', 'ppm_reslib_path')]) + '/res.lib ' + locations['FSYSPATH']['cache']
		p = subprocess.Popen(cmd.split(), stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
		p.wait() 
		inputs_for_clean_environment = [locations['FSYSPATH']['cache'] + '<pdbi>_temp.pdb', locations['FSYSPATH']['cache'] + '/res.lib', locations['FSYSPATH']['cache'] + '<pdbi>.inp']
		exec_filename = locations['FSYSPATH']['cache'] + 'PPM_exec_instructions.txt'
#		command_template = '{0} < {1} > {2}'.format("".join([x+" " for x in options['PATHS'][('hpcppm', 'hpc_ppm_path')].split()[1:]), '<pdbi>.inp', '<pdbi>_log.out')
		command_template = '{0} < {1} > {2}'.format(options['PATHS'][('hpcppm', 'hpc_ppm_path')], '<pdbi>.inp', '<pdbi>_log.out')
		output_filename_templates = ['<pdbi>_temp_datapar1', '<pdbi>_temp_datasub1', '<pdbi>_tempout.pdb']
		output_dir = locations['FSYSPATH']['cache'] + 'output_PPM/'
		if os.path.exists(output_dir):
			subprocess.Popen(["rm", "-rf", output_dir], stderr=open('/dev/null', 'w'), stdout=open('/dev/null', 'w'))
		os.mkdir(output_dir)
		print("PPM", command_template, exec_filename)
		create_exec_file(str_set, command_template, output_filename_templates, exec_filename, inputs_for_clean_environment=inputs_for_clean_environment)
		print(exec_filename, os.path.exists(exec_filename))
		highly_parallel_jobs_manager(options, locations, exec_filename, 'PPM', output_dir)
	else:
		output_dir = locations['FSYSPATH']['cache'] + 'output_PPM/'
		output_filename_templates = ['<pdbi>_temp_datapar1', '<pdbi>_temp_datasub1', '<pdbi>_tempout.pdb']


	output_paths = {}
	with open(output_dir + "output.log") as of:
		for line in of:
			fields = line.split()
			output_paths[fields[0]] = (fields[1], fields[2])

	# Analysis
	for pdbi in str_set:
		failed = False
		for outfname in output_filename_templates:
			if output_paths[outfname] != "present":
				failed = True
				break
		if failed:
			str_data[pdbi]['PASSPORT'].append(passport_entry(this_name+'_fppm', pdbi, "During the analysis with the PPM software, the process has failed because of unexpected structural or format inconsistencies. This entry is scheduled for deletion"))
			str_data[pdbi]['status'].append('condemned')
			continue

		# Output PDB file
		PPM_dict = parse_PDB_coords(output_paths['<pdbi>_tempout.pdb'.replace('<pdbi>', pdbi)][1], with_UNK=options['PARAMETERS'][('', 'with_UNK')])
		if PPM_dict and 'COORDS' in PPM_dict and PPM_dict['COORDS']:
			str_data[pdbi]['PASSPORT'].append(passport_entry(this_name+'_ppmok', pdbi, "The software PPM has successfully completed the analysis of this structure."))
		else:
			str_data[pdbi]['PASSPORT'].append(passport_entry(this_name+'_ppmerr', pdbi, "The software PPM could not complete the analysis for this structure. This entry is scheduled for deletion"))
			str_data[pdbi]['status'].append('condemned')
			continue
		str_data[pdbi]['present_cache'] = locations['FSYSPATH']['TMPcoords'] + pdbi + '_fromPPM.cif'
		write_mmCIF(str_data[pdbi], PPM_dict, str_data[pdbi]['present_cache'])
		write_ENC(str_data[pdbi], PPM_dict, str_data[pdbi]['ENCOMPASS']['coordinate_file'], locations['SYSFILES']['ignoredpasscodes'])
		if not PPM_dict['DUMMIES']:
			print("ERROR: NO DUM ATOMS")
			exit(1)
		str_data[pdbi]['ENCOMPASS']['lim_sup'] = abs(PPM_dict['DUMMIES'][0][1][2])
		str_data[pdbi]['ENCOMPASS']['lim_inf'] = -str_data[pdbi]['ENCOMPASS']['lim_sup']


		# Output segments
		with open(output_paths['<pdbi>_temp_datasub1'.replace('<pdbi>', pdbi)][1]) as datasub_file:
			for line in datasub_file:
				if not line.strip():
					continue
				subud = FixedDict(str_data[pdbi]['FROM_OPM']['subunits'].get_fdict_template())
				macrofields = line.split(';')
				protein_letter = macrofields[1]
				subud['tilt'] = macrofields[2]
				subud['segment'] = []
				for seg in macrofields[3].split('(')[1:]:
					n = seg.split('-')
					if len(n) == 2:
						n1, n2 = seg.split('-')
						n1 = int(''.join(n1.split()))
						n2 = int(''.join(n2.split(')')[0].split()))
					elif len(n) == 1:
						n1, n2 = n[0][:n[0].index(')')].split()
					else:
						print_log('CRITICAL', this_name, 'Structure {0} has shown inconsistencies in the definition of TM domains'.format(pdbi))
					subud['segment'].append(tuple([n1, n2]))
				str_data[pdbi]['FROM_OPM']['subunits'].new(protein_letter)
				str_data[pdbi]['FROM_OPM']['subunits'][protein_letter] = subud
			if not str_data[pdbi]['FROM_OPM']['subunits']:
				str_data[pdbi]['PASSPORT'].append(passport_entry(this_name+'_notm', pdbi, "The software PPM has not found any transmembrane domain inside this structure. This entry is scheduled for deletion"))
				str_data[pdbi]['status'].append('condemned')
				continue

	# This runs on all structures because it has to assign the TM regions
	for pdbi in str_data:
		# Topological elements
		topological_elements = define_TM_regions(options, locations, str_data[pdbi]['ENCOMPASS']['coordinate_file'], str_data[pdbi], pdbi, c_dict={}, cache_coord_filename='', span=None)
		for c in topological_elements:
			str_data[pdbi]['ENCOMPASS']['TMTEs'].new(c)
			for te in topological_elements[c]:
				segt = str_data[pdbi]['ENCOMPASS']['TMTEs'][c]['segments'].get_fdict_template()
				segt['ss_type'] = te[1][0]
				segt['residues_num_type3'] = te[1][1:]
				segt['TE_score'] = te[0]
				str_data[pdbi]['ENCOMPASS']['TMTEs'][c]['segments'].append(FixedDict(segt))
			if c in str_data[pdbi]['FROM_OPM']['subunits']:
				str_data[pdbi]['ENCOMPASS']['TMTEs'][c]['OPM_confirmed'] = True
			else:
				str_data[pdbi]['ENCOMPASS']['TMTEs'][c]['OPM_confirmed'] = False
		if not str_data[pdbi]['ENCOMPASS']['TMTEs']:
			str_data[pdbi]['PASSPORT'].append(passport_entry(this_name+'_note', pdbi, "No Topological Elements have been found in the transmembrane subunits of this structure. Its insertion in the lipid bilayer has not been confirmed : this entry is scheduled for deletion"))
			str_data[pdbi]['status'].append('condemned')
			continue

	write_checkpoint(str_data, locations['FSYSPATH']['cache'] + 'str_data_3.pkl')

	return str_data


def test_managers():
	my_input_dir = ""

	with open(locations['FSYSPATH']['cache'] +'/res.lib', "w") as f:
		f.write("cicciput\n")
	str_set = set()
	for i in range(4):
		iz = "NUM" + str(i).zfill(3)
		str_set.add(iz)
		with open(locations['FSYSPATH']['cache'] +'<pdbi>_temp.pdb'.replace('<pdbi>', iz), "w") as f:
			f.write("cicciput {0}\n".format(iz))

	batch_job_code = "TRY"
	inputs_for_clean_environment = [locations['FSYSPATH']['cache'] + '<pdbi>_temp.pdb']
	shared_inputs = ['res.lib:'+locations['FSYSPATH']['cache'] + 'res.lib']
	command_template = 'sleep 10; ls -lrth {0} {1} > {2}; cat {0} {1} > {3}'.format('<pdbi>_temp.pdb', '<shared>res.lib', '<pdbi>_log.out', '<pdbi>_temp_datapar1')
	output_filename_templates = ['<pdbi>_log.out', '<pdbi>_temp_datapar1']
	output_dir = locations['FSYSPATH']['cache'] + "boh_out/"
	os.mkdir(output_dir)
	exec_filename = locations['FSYSPATH']['cache'] + batch_job_code + '_exec_instructions.txt'
	create_exec_file(str_set, command_template, output_filename_templates, exec_filename, shared_inputs=shared_inputs, inputs_for_clean_environment=inputs_for_clean_environment)

	print(highly_parallel_jobs_manager(options, locations, exec_filename, batch_job_code, output_dir, force_redo=False))


if __name__ == "__main__":
	test_managers()
