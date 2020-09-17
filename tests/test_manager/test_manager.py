import locusts.manager

def test_managers():
    my_input_dir = "tests/test_manager/my_input_dir/"  # The path of the directory containing the inputs
    my_output_dir = "tests/test_manager/my_output_dir/"  # The path of the directory that will store the outputs
    batch_job_code = "MngTest"  # A unique identifier of your choice
    specific_inputs = ['inputfile_<id>.txt', 'secondinputfile_<id>.txt']
    shared_inputs = ['sf1:sharedfile.txt']
    outputs = ['ls_output_<id>.txt', 'cat_output_<id>.txt']
    parameter_file = "tests/test_manager/test_manager.par"
    command_template = ('sleep 1;'
                        'ls -lrth {0} {1} {2} > {3};'
                        'cat {0} {1} {2} > {4}'
        ).format('inputfile_<id>.txt',
        'secondinputfile_<id>.txt',
        '<shared>sf1',
        'ls_output_<id>.txt',
        'cat_output_<id>.txt')

    locusts.manager.launch(
        indir=my_input_dir,
        outdir=my_output_dir,
        code=batch_job_code,
        spcins=specific_inputs,
        shdins=shared_inputs,
        outs=outputs,
        cmd=command_template,
        optf=parameter_file
    )


if __name__ == "__main__":
	test_managers()

