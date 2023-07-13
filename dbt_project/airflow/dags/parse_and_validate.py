import csv

def process_blobs(tggr_file,CONTAINER_CLIENT,ti=None,**context):
    print("trigger_file: ", tggr_file)
    print("container_client: ", CONTAINER_CLIENT)
    ctrl_files = []
    #control_file_path = f"{tggr_file.split('/')[0]}/{(tggr_file.split("/")[1])}"
    source = tggr_file.split('/')[0]
    division = tggr_file.split("/")[1]
    pattern = f"{division.upper()}.CF"
    control_file_path = f"{source}/{division}"

    # blob_list = CONTAINER_CLIENT.list_blobs(control_file_path)

    blob_list = [blob.name for blob in CONTAINER_CLIENT.list_blobs() if blob.name.startswith(control_file_path) 
                and blob.name.endswith(pattern)]

    print("LIST OF CONTROL FILES AT LOCATION: ", blob_list)

    # matched_ctrl_files = []
    # for blob in blob_list:
    #     if blob.name.endswith(pattern):
    #         matched_ctrl_files.append(blob.name)
    #     print(matched_ctrl_files)
    ti.xcom_push(key="ctrl_files_list",value=blob_list)
    parse_ctrl_files(tggr_file,blob_list, CONTAINER_CLIENT)



def parse_ctrl_files(tggr_file,control_file_list, CONTAINER_CLIENT,ti=None, **context):
    data_files_list = []
    source = tggr_file.split('/')[0]
    division = tggr_file.split("/")[1]
    data_file_path = f"{source}/{division}"
    #control_file_list = ti.xcom_pull(key="ctrl_files_list",task_ids="get_the_control_files")
    for file in control_file_list:
        print(file)
        blob_client = CONTAINER_CLIENT.get_blob_client(file)
        blob_data = blob_client.download_blob().readall().decode('utf-8')
        print(blob_data)
        reader = csv.reader(blob_data.splitlines(), delimiter="|")
        for row in reader:
            data_files_dict={'file_path': f"{data_file_path}/{row[1]}", 'expected_count': row[3]}
            data_files_list.append(data_files_dict)
    print('LIST OF DATA FILES: ', data_files_list)
    validate_data_files(CONTAINER_CLIENT,data_files_list,tggr_file,control_file_list)


def validate_data_files(CONTAINER_CLIENT,data_files_list,tggr_file,control_file_list):
    print("validating datafiles")
    all_files_present= True
    all_matched_records = True
# Iterate through the data files
    for data_file in data_files_list:
        file_path = data_file['file_path']
        expected_count = data_file['expected_count']
        print(f"Processing {file_path} has {expected_count} records")
        print(type(expected_count))

        # Get a reference to the data file
        blob_client = CONTAINER_CLIENT.get_blob_client(file_path)
        
        # Check if the data file exists in the container
        if blob_client.exists():
            # Download the data file content
            file_data = blob_client.download_blob().readall().decode('utf-8')
            
            # Count the number of records in the data file
            actual_count = int(len(file_data.split('\n'))) - 2 
            print(type(actual_count))
            # Compare the actual count with the expected count
            if int(actual_count) == int(expected_count):
                print(f"Data file '{file_path}' is present. Expected record count:{expected_count} matching with Actual Count: {actual_count}")
            else:
                all_matched_records = False
                print(f"Data file '{file_path}' is present. Expected record count of {expected_count} not matching Actual Count: {actual_count}")
        else:
            all_files_present = False
            print(f"Data file '{file_path}' does not exist in the container.")
            

# Move files to appropriate folders
    for file_dict in data_files_list:
        file_name =file_dict["file_path"]
        source_blob_client = CONTAINER_CLIENT.get_blob_client(file_name)
        if source_blob_client.exists():
            file = f"{file_name}_{CURRENT_DATE}"
            destination_folder = 'external' if (all_files_present and  all_matched_records) else 'failed'
            destination_blob_client = CONTAINER_CLIENT.get_blob_client(f"{destination_folder}/{file}")
            destination_blob_client.start_copy_from_url(source_blob_client.url)
            source_blob_client.delete_blob()
    for ctrl_file_name in control_file_list:
        print("Moving CONTROLS file to archive folder")
        source_blob_client = CONTAINER_CLIENT.get_blob_client(ctrl_file_name)
        file = f"{ctrl_file_name}_{CURRENT_DATE}"
        destination_folder = 'archived'
        destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
        destination_blob_client.start_copy_from_url(source_blob_client.url)
        source_blob_client.delete_blob()
    print("Moving TRIGGER file to archive folder")
    source_blob_client = CONTAINER_CLIENT.get_blob_client(tggr_file)
    file = f"{tggr_file}_{CURRENT_DATE}"
    destination_folder = 'archived'
    destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
    destination_blob_client.start_copy_from_url(source_blob_client.url)
    source_blob_client.delete_blob()
    if all_files_present==False: 
        raise FileNotFoundError("Some file is missing...moving the files to failed folder.")
    elif all_matched_records==False:
        raise ValueError("Records does not match...moving the files to failed folder.")
    
