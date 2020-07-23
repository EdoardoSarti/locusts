# Name: 
# Language: python3
# Libraries:
# Description: 
# Author: Edoardo Sarti
# Date: 

from manager_supporting_functions import *

def test_manager():
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
