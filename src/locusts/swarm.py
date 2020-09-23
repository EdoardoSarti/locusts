from locusts.manager import *
from locusts.environment import *

def launch(indir=None, outdir=None, code=None, spcins=None, shdins=None,
        outs=None, cmd=None, optf=None):

    devnull = open("/dev/null", "w")
    options = define_options()

    # Check args
    if not (indir and outdir and code and spcins and outs and cmd and optf):
        print(("ERROR (launch): Please specify all the arguments. Only shared"
            "inputs are optional"))
        exit(1)

    # Parse option file
    if not os.path.exists(optf):
        print("ERROR (launch): The specified parameter file was not found.")
        print("                Path: {0}".format(optf))
        exit(1)
    else:
        with open(optf) as f:
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
                    print(("ERROR (launch): {0} ".format(fields[0]),
                        "is not a valid argument"))
                    exit(1)
                # If argument has no value, default value will be chosen
                if len(fields) < 2:
                    continue
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
