import os
import subprocess


def beautify_bash_oneliner(command, replacements={}):
	# parse command, by dividing it in lines at each ";" which is not inside "", '' or ()
	cmd_lines = []
	jumpit = False
	nl = ""
	for c in command:
		if c != ";":
			nl += c
		elif c == ";" and not jumpit:
			cmd_lines.append(nl)
			nl = ""
		if c in ["(", '"', "'"]:
			jumpit = True
		elif c in [")", "'", '"'] and jumpit:
			jumpit = False
	if nl.strip():
		cmd_lines.append(nl)
	
	# transcribe command on the task.sh file
	new_command = ""
	ntabs = 0
	for cmd_line in cmd_lines:
		new_line = "\t"*ntabs
		fields = cmd_line.split()
		for fi, f in enumerate(fields):
			if f in replacements:
				new_line += replacements[f] + " "
			elif f in ["do", "then"]:
				new_line += "\n" + "\t"*ntabs + f
				ntabs += 1
				if fi != len(fields)-1:
					new_line += "\n" + "\t"*ntabs
			elif f in ["done", "fi"]:
				ntabs -= 1
				new_line += "\n" + "\t"*ntabs + f + " "
			else:
				new_line += f + " "
		if new_line.strip():
			new_command += new_line[:-1]
		new_command += '\n'
	return new_command


def distribute_items_in_fixed_length_list(n_items, list_length):
	return [((list_length-1)//n_items)+1]*(((list_length-1)%n_items)+1) + [list_length//n_items]*(n_items - list_length%n_items)

