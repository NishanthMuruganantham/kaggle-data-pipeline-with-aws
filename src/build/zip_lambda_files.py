import os
import shutil
import zipfile


LAMBDA_HANDLER_FILES = [
    'src/mens_t20i_data_collector/lambdas/download_from_cricsheet/download_from_cricsheet_lambda_function.py',
]

def zip_lambda_handler_files(lambda_files, output_folder, temp_folder):
    os.makedirs(output_folder, exist_ok=True)

    for file_path in lambda_files:
        file_name = os.path.basename(file_path)
        zip_file_name = f"{os.path.splitext(file_name)[0]}.zip"
        zip_file_path = os.path.join(output_folder, zip_file_name)

        os.makedirs(temp_folder, exist_ok=True)
        shutil.copy(file_path, os.path.join(temp_folder, file_name))

        with zipfile.ZipFile(zip_file_path, 'w') as zipf:
            zipf.write(os.path.join(temp_folder, file_name), arcname=file_name)

        shutil.rmtree(temp_folder)
        print(f"File '{file_name}' zipped and placed in the '{output_folder}' folder successfully.")

def build_packages():
    output_folder = './output'
    temp_folder = 'temp_zip'

    zip_lambda_handler_files(LAMBDA_HANDLER_FILES, output_folder, temp_folder)

if __name__ == "__main__":
    build_packages()
