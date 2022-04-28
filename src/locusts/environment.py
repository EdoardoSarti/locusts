from locusts.support import *

def find_empty_folders(dirs, files):
    """Finds all folders that do not contain any file nor subfolder"""
    nonemptyfolders = set()
    for f in files:
        nonemptyfolders.add(os.path.dirname(f) + "/")
    for d in dirs:
        nonemptyfolders.add(os.path.dirname(d[:-1]) + "/")
    return [d for d in dirs if d not in nonemptyfolders]


def parse_fs_tree(fst_path, env_root):
    def synterr(fn, iline):
        print("Error: in {0}\nsyntax error at line {1}".format(fn, iline))
        exit(1)

    if not os.path.exists(fst_path):
        print("Error: filesystem tree file {0} not found".format(fst_path))
        exit(1)


    dirs, files = [], []
    exceptions, filexceptions = [], []
    with open(fst_path) as fst:
        il = 0
        prespath = ""
        workdir = None
        depth = 0
        is_dir = False
        for line in fst:
            il += 1

            # Parse work directory
            if line.startswith("#WORKDIR"):
                if ":" not in line:
                    synterr(fst_path, il)
                else:
                    workdir = line.split(":")[1].strip()

            # Delete other comments
            if "#" in line:
                tline = line[:line.index("#")]
            else:
                tline = line
            if not tline.strip():
                continue

            # Exit if it is not a tsv file
            if line.startswith(" ") and line.strip():
                print("ERROR: the file {0} must be tab-indented, NOT space-indented (line {1})".format(fst_path, il))
                exit(1)

            # Count how many prepending tabs there are 
            #  and update the depth level
            # NOTICE: is_dir here refers to previous record
            cases = tline.split("\t")
            ntabs = 0
            for c in cases:
                if c:
                    break
                ntabs += 1
            if ntabs > depth + 1:
                synterr(fst_path, il)
            elif ntabs == depth + 1 and not is_dir:
                synterr(fst_path, il)
            depth = ntabs

            # Parse basename and selection (if is_dir == True)
            fields = tline.split(":")
            basename = fields[0].strip()
            if len(fields) == 1:
                is_dir = False
                lt = ""
            elif len(fields) == 2:
                is_dir = True
                lt = "/"
                selection = fields[1].strip()
            else:
                synterr(fst_path, il)

            # Update present path: cut to present depth
            prespath = "/".join([d for d in prespath.split("/")][:depth])

            # "!!" : only delete files, maintain filesystem
            if tline.strip().startswith("!!"):
                kd = reduceslash(prespath + "/" + tline.split("!")[2].strip())
                filexceptions.append(kd)
                continue
            # "!" : deletes all (NOTICE: !folder/** also deletes folder/)
            elif tline.strip().startswith("!"):
                kd = reduceslash(prespath + "/" + tline.split("!")[1].strip())
                exceptions.append(kd)
                continue

            # Looks for dir and file paths
            if is_dir:
                if selection == "**":
                    recursive = True
                else:
                    recursive = False
                globpath = env_root + "/" + prespath + "/" + basename + "/" + selection
            else:
                recursive = False
                globpath = env_root + "/" + prespath + "/" + basename
            globpath = reduceslash(globpath)
            # Look for dirs
            newdirs = glob.glob(reduceslash(globpath + "/"), recursive=recursive)
            # If dir with no selection, add the dir (if it isn't already present)
            if is_dir and selection:
                addglobpath = reduceslash(env_root + "/" + prespath + "/" + basename + "/")
                for d in glob.glob(addglobpath, recursive=False):
                    if d not in newdirs:
                        newdirs.append(d)
            if recursive:
                dirs += newdirs 
#                print("DIRS ADDED:", newdirs)
            elif is_dir: # and not recursive...
                this_dir = env_root + "/" + prespath + "/" + basename + '/'
                if this_dir not in dirs:
                    dirs.append(this_dir)
#                    print("DIRS ADDED:", this_dir)

            # Look for files (== all - dirs)
            newfiles = [] 
            newall = glob.glob(globpath, recursive=recursive)
            # Be sure to remove also the empty dirs that might have been read along with the files, e.g. if the selection was "*"
            for x in newall:
                if x[-1] != "/" and reduceslash(x+"/") not in dirs and reduceslash(x+"/") not in newdirs:
#                if reduceslash(x+"/") not in dirs and reduceslash(x+"/") not in newdirs:
                    newfiles.append(x)
#            print("FILES ADDED:", newfiles)
            files += newfiles
#            print(line)
#            print(globpath)
#            print(newall)
#            print("FILES ADDED:", newfiles)

            # If a dir has been read, present path must be updated (enters that dir)
            if is_dir:
                prespath = reduceslash("/".join([x for x in [prespath, basename] if x]))
                

    # Record folders that are empty
    emptyfolders = find_empty_folders(dirs, files)

    # List all files not to be traced
    efiles = []
    keepdirs = []
    totexdirs = []
    for epath in exceptions + filexceptions:
        globpath = env_root + "/" + epath
        if "**" in epath:
            recursive = True
        else:
            recursive = False
        # find all mentioned dirs
        exdirs = glob.glob(globpath + "/", recursive=recursive)
        exdirs = [reduceslash(x) for x in exdirs]
        # only dirs in exceptions will ne deleted
        if epath in filexceptions:
            keepdirs += exdirs
        else:
            totexdirs += exdirs
        # Files are deleted regardless
        exall = glob.glob(globpath, recursive=recursive)
        exfiles = []
        for x in exall:
            if x[-1] != "/" and x+"/" not in exdirs:
                exfiles.append(x)
        efiles += exfiles

    # Update file list
    new_files = []
    for f in files:
        if f not in efiles:
            new_files.append(f)

    # Record empty folders after having deleted exception files
    new_emptyfolders = set(find_empty_folders(dirs, new_files))

    # Update dir list
#    print("KEEPDIRS:", keepdirs)
    new_dirs = []
    for d in dirs:
        # If !! says it should be kept, do it
        if d in keepdirs:
            new_dirs.append(d)
            continue 
        # If ! says it should be deleted, do it
        elif d in totexdirs:
            continue
        # If no clear decision has been made, the folder will be deleted
        #  only if it became empty when files were deleted
        if d in new_emptyfolders:
            if d in emptyfolders:
                new_dirs.append(d)
            else:
                continue
        else:
            new_dirs.append(d)

    instructions = [] 
    for d in new_dirs:
        rd = "/".join([x for x in d.replace(env_root, "").split("/") if x])
#        print(d, d.replace(env_root, "").split("/"), rd)
        instructions.append("<mkdir> <runtime_envroot_mkdir>/{0}".format(rd))
    for f in new_files:
        rf = "/".join([x for x in f.replace(env_root, "").split("/") if x])
        instructions.append("<copy> <build_envroot>/{0} <runtime_envroot_cp>/{1}".format(rf, os.path.dirname(rf)))

    return workdir, instructions


if __name__ == "__main__":
    parse_fs_tree(os.path.realpath(os.getcwd())+"/tests/test_importenv/fs_tree.fst", os.path.realpath(os.getcwd())+"/tests/test_importenv/environment")
