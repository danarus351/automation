import os
from glob import glob


def run_sync(fold_lis, path, destination):
    for folder in fold_lis:
        dest_folder = folder.replace(path, destination)
        rsync_cmd = f"rsync -rtlvP {folder} {dest_folder} \n"
        screen_name = f'{path.split("/")[2]}_sync_{dest_folder.split("/")[-1].replace("-","_")}'
        new_screen_cmd = "screen -dmS {} {}".format(screen_name, rsync_cmd)
        os.system(new_screen_cmd)




if __name__ == "__main__":
    fpath = '<Source path>'
    fold_lis = glob(f'{fpath}*')
    destination = '<destination path>'
    run_sync(fold_lis, fpath, destination)