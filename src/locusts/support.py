import os
import re
import gc
import sys
import glob
import time
import shutil
import datetime
import subprocess

global DEBUG
DEBUG = True if "--locusts-debug" in sys.argv else False

def string_decode_element(element, is_immutable=False, permissive=False):
    """WARNING: Cannot decode following types: decimal, complex, range, bytes,
    bytearrary, and any mutable or immutable user-defined class or
    non-standard library class (e.g. np.array())"""
    this_name = string_decode_element.__name__

    nop_err = ()
    element = element.strip()
    if string_is_properstring(element):
        if len(element) == 2:
            return ""
        else:
            return element[1:-1]
    elif element == "None":
        return None
    elif element == "False":
        return False
    elif element == "True":
        return True
    elif string_is_int(element):
        return int(element)
    elif string_is_float(element):
        return float(element)
    elif ((len(element) > 1 and element[0] == "(" and element[-1] == ")")
            or (len(element) > 6 and element[:6] == "tuple("
            and element[-1] == ")")):
        return string_decode_list(element, is_tuple=True)
    elif (len(element) > 10 and element[:10] == "frozenset("
            and element[-1] == ")"):
        return string_decode_list(element, is_fset=True)
    elif not is_immutable:
        if ((len(element) > 1 and element[0] == "{" and element[-1] == "}"
                and string_isnot_dict(element)) or (len(element) > 4
                and element[:4] == "set(" and element[-1] == ")")):
            return string_decode_list(element, is_set=True)
        elif ((len(element) > 1 and element[0] == "[" and element[-1] == "]")
                or (len(element) > 5 and element[:5] == "list("
                and element[-1] == ")")):
            return string_decode_list(element)
        elif ((len(element) > 1 and element[0] == "{" and element[-1] == "}")
                or (len(element) > 5 and element[:5] == "dict("
                and element[-1] == ")")):
            return string_decode_dict(element)
        elif permissive:
            return element
        else:
            nop_err = ('CRITICAL', this_name, 
                "could not process element {0}".format(element))
    elif permissive:
        return element
    else:
        nop_err = ('CRITICAL', this_name,
            "could not process immutable element {0}".format(element))

    if nop_err:
        print(nop_err)
        exit(1)


def beautify_bash_oneliner(command, replacements={}):
    """Makes a one-line bash command more immediate by adding indentation.
    It also can replace tags with a specific string"""

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


def distribute_items_in_fixed_length_list(list_length, n_items, min_in_list=None):
    base = n_items // list_length
    if min_in_list is not None:
        if base < min_in_list:
            list_length = max(1, n_items // min_in_list) # if n_items < min_in_list, reserve a node anyway
            base = n_items // list_length
    plus = n_items - base*list_length
    reslist = [base]*list_length
    for i in range(plus):
        reslist[i] += 1
    return reslist


def reduceslash(string):
    while string != string.replace("//", "/"):
        string = string.replace("//", "/")
    return string.strip()


def get_obj_size(obj):
    marked = {id(obj)}
    obj_q = [obj]
    sz = 0

    while obj_q:
        sz += sum(map(sys.getsizeof, obj_q))

        # Lookup all the object referred to by the object in obj_q.
        # See: https://docs.python.org/3.7/library/gc.html#gc.get_referents
        all_refr = ((id(o), o) for o in gc.get_referents(*obj_q))

        # Filter object that are already marked.
        # Using dict notation will prevent repeated objects.
        new_refr = {o_id: o for o_id, o in all_refr if o_id not in marked and not isinstance(o, type)}

        # The new obj_q will be the ones that were not marked,
        # and we will update marked with their ids so we will
        # not traverse them again.
        obj_q = new_refr.values()
        marked.update(new_refr.keys())

    return sz


if __name__ == "__main__":
    requested_nodes = 300
    cpus_per_node = 16
    min_stack_per_core = 1
    n_taskid_list = 3
    print("requested_nodes", requested_nodes)
    print("cpus_per_node", cpus_per_node)
    print("min_stack_per_core", min_stack_per_core)
    print("n_taskid_list", n_taskid_list)

    tasks_per_node_list = distribute_items_in_fixed_length_list(requested_nodes, n_taskid_list)
    print("new tasks_per_node_list", tasks_per_node_list)
    tasks_per_node_list = distribute_items_in_fixed_length_list(requested_nodes, n_taskid_list, min_in_list=min_stack_per_core*cpus_per_node)
    print("new tasks_per_node_list", tasks_per_node_list)
