import os
import re
import sys
import glob
import time
import shutil
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


def distribute_items_in_fixed_length_list(n_items, list_length):
    factor_1 = ((list_length - 1) % n_items) + 1
    factor_2 = n_items - (list_length % n_items)
    list_1 = [((list_length - 1) // n_items) + 1]
    list_2 = [list_length // n_items]
    return list_1 * factor_1 + list_2 * factor_2


def reduceslash(string):
    while string != string.replace("//", "/"):
        string = string.replace("//", "/")
    return string.strip()
