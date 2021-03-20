import os
import pathlib

fn_mtime = {}

for folderName, subfolders, filenames in os.walk('.'):
    print(folderName)
    for filename in filenames:
        if filename.endswith('.pdf'):
            fname = pathlib.Path(filename)
            # print(fname.stat().st_mtime)
            fn_mtime[filename] = fname.stat().st_mtime

sorted_fn_mtime = {k: v for k, v in sorted(fn_mtime.items(), key=lambda item: item[1])}

print(os.getcwd())
# print(list(sorted_fn_mtime.keys())[0])
