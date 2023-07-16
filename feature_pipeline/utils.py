import json
import os

def save_json(data,file_path,save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir,exist_ok=True)
    data_path=os.path.join(save_dir,file_path)
    with open(data_path,"w") as write:
        json.dump(data,write)


def load_json(file_name,load_dir):
    data_path=os.path.join(load_dir,file_name)
    if not os.path.exists(data_path):
        raise FileNotFoundError("data doesnot exist")
    with open(data_path,"r") as read:
        return json.load(read)