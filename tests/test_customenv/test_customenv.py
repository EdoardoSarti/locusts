import locusts.swarm

def test_managers():
    batch_job_code = "MngTest"  # A unique identifier of your choice
    parameter_file = "tests/test_customenv/test_customenv.par"
    env_root = "tests/test_customenv/environment/"
    env_fs_tree = "tests/test_customenv/fs_tree.fst"
    argument_list = [[str(10*x)+"-"+str(10*(x+1)-1)]
        for x in range(20)]
    command_template = 'bash important_script.sh <arg0>'
    local_dir = 'tests/test_customenv/my_local_dir/'

    locusts.swarm.launch(
        code=batch_job_code,
        cmd=command_template,
        args=argument_list,
        envroot=env_root,
        envfs=env_fs_tree,
        parf=parameter_file,
        locdir=local_dir
    )


if __name__ == "__main__":
	test_managers()

