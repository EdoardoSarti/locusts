# Locusts

Locusts is a Python package for distributing many small jobs on a system (which can be your machine or a remote HPC running SLURM).


## Installation

Locusts package is currently part of the [PyPI](https://test.pypi.org) Test archive.
In order to install it, type

`python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps locusts`

Note: PyPI Test is not a permanent archive. Expect this installation procedure to change over time.


## How it works

Locusts is thought for whom has to run a **huge amount of small, independent jobs** and has problems with the most used schedulers which will scatter the jobs over over too many nodes, or queue them indefinitely.
Moreover, this package provides a **safe, clean environment for each job instance**, and keeps and collects notable inputs and outputs.
In short, locusts creates a minimal filesystem where it prepares one environment for each job it has to execute. The runs are directed by a manager bash script, which schedules them and reports its stauts and the one of the jobs to the main locusts routine, which will always be run locally. Finally, it checks for a set of compulsory output files and compiles a list of success and failures.

### Modes

Locusts can help you distributing your jobs when you are facing one of these three situations:
* You want to run everything on your local machine (**local mode**)
* You want to submit jobs to a HPC (**remote mode**)
* You want to submit jobs to a HPC which shares a directory with your local machine (**remote-shared mode**)

### Environments

Once you give locusts the set of input to consider and the command to execute, it creates the Generic Environment, a minimal filesystem composed of three folders:
* An **execution folder**, where the main manager scripts will be placed and executed and where execution cache files will keep them updated on the progress of the single jobs
* A **work folder**, where the specific inputs of each job are considered and where outputs are addressed
* A **shared folder**, where common inputs have to be placed in case a group of different jobs wants to use them

Basing on this architecture, Locusts provides two types of environments the user can choose from depending on her needs:

#### Default Locusts Environment
![Locusts Default](./locusts-img/Locusts.001.jpeg)

If the user only needs to process a (possibly huge) amount of files and get another (still huge) amount of output files in return, this environment is the optimal choice: it allows for minimal data transfer and disk space usage while each of the parallel runs will run in a protected sub-environment. The desired output files and the corresponding logs will then be collected and put in a folder designated by the user

#### Custom Environment
![Locusts Custom](./locusts-img/Locusts.003.jpeg)
The user could nonetheless want to parallelize a program or a code having more complex effects than taking in a bunch of input files  and returning some outputs: for example, a program displacing files around a filesystem will not be able to run in the Default Locusts Environment. In these situations, the program needs to have access to a whole environment rather than to a set of input files. 



Starting from this common base, there are two different environments that can be used:
* The default Locusts Environment consists in having one folder corresponding to each set of files for running one instance of the command
* The Custom Environment lets the user employ any other filesystem 

## Tutorial

### Example 1: Running a script requiring input/output management (Default Environment)
You can find this example in the directory `tests/test_manager/`
In `tests/test_manager/my_input_dir/` you will find 101 pairs of input files: `inputfile\_\#.txt` and `secondinputfile\_\#.txt`, where 0 <= \# <= 100. Additionally, you will also find a single file named `sharedfile.txt`.
The aim here is executing this small script over the 101 sets of inputs:
`
sleep 1;
ls -lrth <inputfile> <secondinputfile> <sharedfile> > <outputfile>;
cat <inputfile> <secondinputfile> <sharedfile> > <secondoutputfile>
`
For each pair, the script takes in `inputfile\_\#.txt`, `secondinputfile\_\#.txt` (both vary from instance to instance) and `sharedfile.txt` (which instead remains always the same), and returns `ls\_output\_\#.txt` and `cat\_output\_\#.txt`. In order to mimick a longer process, the script is artificially made to last at least one second.

The file `tests/test_manager/test_manager.py` gives you an example (and also a template) of how ou can submit a job on Locusts.
The function you want to call is `locusts.swarm.launch`, which takes several arguments.
Before describing them, let's look at the strategy used by Locusts: in essence, you give Locusts a template of the command you want to execute, and the you tell Locusts where to look for files to execute that template with. In our case, the template is:
`
sleep 1;
ls -lrth inputfile_<id>.txt secondinputfile_<id>.txt <shared>sf1 > ls_output_<id>.txt;
cat inputfile_<id>.txt secondinputfile_<id>.txt <shared>sf1 > cat_output_<id>.txt
`
Notice there are two handles that Locusts will know how to replace: `<id>` and `<shared>`. The `<id>` handle is there to specify the variable part of a filename (in our case, an integer in the [0,100] interval). The `<shared>` tag tells locust


* `indir` takes the location (absolute path or relative from where you are calling the script) of the directory containing all your input files
* `outdir` takes the location (absolute path or relative from where you are calling the script) of the directory where you want to collect your results
* `code` takes a unique codename for the job you want to launch
* `spcins` takes a list containing the template names for the 
        shdins=shared_inputs,
        outs=outputs,
        cmd=command_template,
        parf=parameter_file

### Example 2: Running a script requiring input/output management (Default Environment)
You will find the material 
