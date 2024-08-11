import os
import subprocess
import shutil
import zipfile


tarball = "dist/mens_t20i_data_collector-0.1.tar.gz"
extract_to = "layer/python/lib/python3.8/site-packages"
layer_path = "layer"

def run_command(command):
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        raise Exception(f"Command failed: {command}")

def create_package():
    run_command("python setup.py sdist")
    os.makedirs(extract_to, exist_ok=True)
    run_command(f"tar -xzf {tarball} --strip-components=2 -C {extract_to}")
    zipf = zipfile.ZipFile('new.zip', 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(layer_path):
        for file in files:
            zipf.write(
                os.path.join(root, file),
                os.path.relpath(os.path.join(root, file), layer_path)
            )
    zipf.close()
    shutil.rmtree(layer_path)

if __name__ == "__main__":
    create_package()
