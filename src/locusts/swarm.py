from locusts.manager import *

def define_options():
    """Initializes the options data structure with its default values.
    Each value is also associated with a fixed type"""

    options = {}
    options['TYPES'] = {}
    options['run_on_hpc'], options['TYPES']['run_on_hpc'] = False, bool
    options['host_name'], options['TYPES']['host_name'] = None, str
    options['requested_nodes'], options['TYPES']['requested_nodes'] = None, int
    options['cpus_per_node'], options['TYPES']['cpus_per_node'] = None, int
    options['local_shared_dir'], options['TYPES']['local_shared_dir'] = None, str
    options['hpc_exec_dir'], options['TYPES']['hpc_exec_dir'] = None, str
    options['number_of_processors'], options['TYPES']['number_of_processors'] = 1, int
    options['force_redo'], options['TYPES']['force_redo'] = False, bool
    options['singularity'], options['TYPES']['singularity'] = None, str
    options['singularity_container'], options['TYPES']['singularity_container'] = None, str
    options['singularity_modload'], options['TYPES']['singularity_modload'] = None, str
    options['data_transfer_protocol'], options['TYPES']['data_transfer_protocol'] = os.path.dirname(os.path.realpath(__file__)) + "/data_transfer_protocol.sh", str
    options['email_address'], options['TYPES']['email_address'] = "", str
    options['nodewise_scratch_folder'], options['TYPES']['nodewise_scratch_folder'] = "", str
    options['nodewise_scratch_memory'], options['TYPES']['nodewise_scratch_memory'] = "", str
    options['walltime'], options['TYPES']['walltime'] = "24:00:00", str
    options['extra_outer_statements'], options['TYPES']['extra_outer_statements'] = "", str
    options['partition'], options['TYPES']['partition'] = "", str
    options['exclusive'], options['TYPES']['exclusive'] = False, bool

    return options


def parse_parameter_file(parf):
    """Parses the parameter file and compiles the options dictionary"""

    if not os.path.exists(parf):
        print("ERROR (launch): The specified parameter file was not found.")
        print("                Path: {0}".format(parf))
        exit(1)

    # Initialize the options dictionary
    options = define_options()

    with open(parf) as f:
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
                print(("ERROR (launch): {0} "
                    "is not a valid argument").format(fields[0]))
                exit(1)

            # If argument has no value, default value will be chosen
            if len(fields) < 2:
                continue

            # Fill option dictionary
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

    return options


def compile_id_list(indir, spcins):
    """Compile set of IDs basing on the files in input directory
    presenting the correct format"""

    # Retrieve list of IDs from files in indir
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

    return id_list


def create_exec_file(env_type, command_template, exec_filename, data):
    def replace_args(string, arglist):
        nargs = len(arglist)
        for i in range(nargs):
            string = string.replace("<arg{0}>".format(i), arglist[i])
        return string

    if env_type == "locusts":
        (
            id_list,
            indir,
            outdir,
            output_filename_templates,
            shared_inputs,
            inputs_for_clean_environment
        ) = data 

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

    elif env_type == "locusts-arg":
        (
            args,
            indir,
            outdir,
            output_filename_templates,
            shared_inputs,
            inputs_for_clean_environment
        ) = data 

        with open(exec_filename, "w") as exec_file:
            for ias, arglst in enumerate(args):
                command = replace_args(command_template, arglst)
                exec_file.write(('c{0}:\t{1}\n'.format(str(ias).zfill(6), command))) 
                if inputs_for_clean_environment:
                    inls_list = []
                    for x in inputs_for_clean_environment:
                        inls_list.append(indir + replace_args(x, arglst))
                    inls = ' '.join(inls_list)
                    exec_file.write('i{0}:\t{1}\n'.format(str(ias).zfill(6), inls))
                if shared_inputs:
                    shls_list = []
                    for x in shared_inputs:
                        shls_list.append(replace_args(x, arglst).replace(":", ":"+indir))
                    shls = ' '.join(shls_list)
                    exec_file.write('s{0}:\t{1}\n'.format(str(ias).zfill(6), shls))
                ols_list = []
                for x in output_filename_templates:
                    ols_list.append(replace_args(x, arglst))
                ols = ' '.join(ols_list)
                exec_file.write('o{0}:\t{1}\n'.format(str(ias).zfill(6), ols))
        
    elif env_type == "custom":
        args = data
        with open(exec_filename, "w") as exec_file:
            for ias, arglst in enumerate(args):
                command = replace_args(command_template, arglst)
                exec_file.write(('c{0}:\t{1}\n'.format(str(ias).zfill(6), command)))
                
    else:
        print(("ERROR (create_exec_file): allowed environment types:\n"
            "locusts - please provide input and output dirs and templates\n"
            "custom - please provide env path and filesystem specification"))
        exit(1)


def launch(indir=None, outdir=None, code=None, spcins=None, shdins=None, locdir=None,
        outs=None, cmd=None, args=None, envroot=None, envfs=None, parf=None):

    # Check 3 compulsory args
    if not (code and cmd and parf):
        print(("ERROR (launch): Please specify all compulsory arguments:\n"
            "code # Identifier\ncmd # Command or command file\n"
            "parf # Parameter file\n"))
        exit(1)
    # Check 4 args of Locusts Env
    if (indir and outdir and spcins and outs):
        execfile_dir = reduceslash(outdir + "/")
        if not args:
            print("Running Locusts Environment")
            env_type = "locusts"
            id_list = compile_id_list(indir, spcins)
            data = (
                id_list,
                indir,
                outdir,
                outs,
                shdins,
                spcins
            )
        # Check additional "args" arg
        else:
            print("Running Locusts Environment (with arguments)")
            env_type = "locusts-arg"
            data = (
                args,
                indir,
                outdir,
                outs,
                shdins,
                spcins
            )
    # Check 3 args of Custom Env
    elif (args and envroot and envfs and locdir):
        env_type = "custom"
        execfile_dir = reduceslash(locdir + "/")
        env_instr_filename = envfs
#        parse_fs_tree(envfs, envroot)
        data = args
        print("Running Custom Environment")
        print("Warning: Locusts will not perform quality checks on runs")
        
    else:
        print(("ERROR (launch): Please specify one of the two sets of"
            "arguments:\nindir # Input directory\noutdir # Output directory\n"
            "spcins # Specific inputs\nouts # Expected outputs\n\nor\n\n"
            "args # Argument list\nenvroot # Environment root directory\n"
            "envf # Environment filesystem specification file\nlocdir # Local "
            "path to store caches and build the filesystem"))
        exit(1)

    # Parse parameter file and compiles options data structure
    options = parse_parameter_file(parf)

    # Create the execution file used by the job manager
    exec_filename = execfile_dir + code + ".exec_file.txt"
    create_exec_file(
        env_type, 
        cmd, 
        exec_filename, 
        data
    )

    # Main routine
    if env_type in ["locusts", "locusts-arg"]:
        highly_parallel_job_manager(options, exec_filename, code, outdir)
    elif env_type == "custom":
        highly_parallel_job_manager(options, exec_filename, code, locdir, env_root_dir=envroot, env_instr=env_instr_filename)
