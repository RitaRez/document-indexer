import os

notice_files = []
print("../output/shards/")
for address, dirs, files in os.walk("../output/shards/"):
    for name in files:
        notice_files.append(name)
            
print(notice_files)
