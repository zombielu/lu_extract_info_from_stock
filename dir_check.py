import os
from datetime import datetime

input_directory = '/Users/weilinwu/Desktop/input'
output_directory = '/Users/weilinwu/Desktop/output'

for root, dirs, files in os.walk(input_directory):
    files = [f for f in files if not f.startswith('.')]
    for file_name in files:

        # Get the full path of the current file
        input_file_path = os.path.join(root, file_name)

        # Extract year, month, and day from the directory structure
        relative_path = os.path.relpath(input_file_path, input_directory)
        print(input_file_path, input_directory)
        print(relative_path)
        year, month, day, x = relative_path.split(os.path.sep)

        # Create output directory if it doesn't exist
        output_dir = os.path.join(output_directory, year, month, day)
        os.makedirs(output_dir, exist_ok=True)

        # Determine output file path
        output_file_path = os.path.join(output_dir, file_name)

        # Here you can perform your processing and save the result to output_file_path
        # For example, copying the file:
        # shutil.copy(input_file_path, output_file_path)

        # print(f"Processed: {file_name}, Year: {year}, Month: {month}, Day: {day}, {x}")
        print(output_file_path)

    # for root, _, files in os.walk(parquet_dir_path):
    #     files = [f for f in files if not f.startswith('.')]
    #     for file_name in files:
    #         input_file_path = os.path.join(root, file_name)
    #         relative_path = os.path.relpath(input_file_path, parquet_dir_path)
    #         year, month, day, _ = relative_path.split(os.path.sep)
    #         output_dir = os.path.join(output_base_dir, year, month, day)
    #         os.makedirs(output_dir, exist_ok=True)
    #         timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    #         output_file_path = os.path.join(output_dir, f'{timestamp}.parquet')

