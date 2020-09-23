#WORKDIR : all_scripts/

all_scripts :
	important_script.sh
	helper_scripts : * 
important_files : *.txt
	! big_file.txt
other_files_1 :
	class_A : **
		! useless_folder/
		! useful_folder/very_big_file.txt
	class_B : **
