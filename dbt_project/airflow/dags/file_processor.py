

import csv

class FileProcessor():
    def __init__(self, trigger_file, CONTAINER_CLIENT, CURRENT_DATE):
        self.trigger_file = trigger_file
        self.CONTAINER_CLIENT = CONTAINER_CLIENT
        self.source = trigger_file.split("/")[0]
        self.division = trigger_file.split("/")[1]
        self.root_file_path = f"{self.source}/{self.division}"
        self.CURRENT_DATE = CURRENT_DATE

    def get_control_files(self):
        control_file_pattern = f"{self.division.upper()}.CF"
        control_file_list = [blob.name 
                            for blob in self.CONTAINER_CLIENT.list_blobs() 
                            if blob.name.startswith(self.root_file_path) and blob.name.endswith(control_file_pattern)]


        return control_file_list


    def parse_control_files(self, control_file_list):
        data_files_list = []
        for file in control_file_list:
            print(file)
            blob_client = self.CONTAINER_CLIENT.get_blob_client(file)
            blob_data = blob_client.download_blob().readall().decode('utf-8')
            print(blob_data)
            reader = csv.reader(blob_data.splitlines(), delimiter="|")
            for row in reader:
                data_files_dict={'file_name': f"{row[1]}", 'expected_count': row[3]}
                data_files_list.append(data_files_dict)

        return data_files_list

    def move_files_to_folder(self, file_name, destination_folder):
        source_blob_client = self.CONTAINER_CLIENT.get_blob_client(f"{self.root_file_path}/{file_name}")
        if source_blob_client.exists():
            file = f"{file_name}_{self.CURRENT_DATE}"
            destination_blob_client = self.CONTAINER_CLIENT.get_blob_client(f"{self.root_file_path}/{destination_folder}/{file}")
            destination_blob_client.start_copy_from_url(source_blob_client.url)
            source_blob_client.delete_blob()
    
    def validate_data_files(self, data_files_list, control_files_list):
        all_files_present= True
        all_matched_records = True
        # Iterate through the data files
        for data_file in data_files_list:
            file_name = data_file['file_name']
            expected_count = data_file['expected_count']
            print(f"Processing {file_name} has {expected_count} records")
            print(type(expected_count))
            # Get a reference to the data file
            blob_client = self.CONTAINER_CLIENT.get_blob_client(f"{self.root_file_path}/{file_name}")
            
            # Check if the data file exists in the container
            if blob_client.exists():
                # Download the data file content
                file_data = blob_client.download_blob().readall().decode('utf-8')
                
                # Count the number of records in the data file
                actual_count = int(len(file_data.split('\n'))) - 2 
                print(type(actual_count))
                # Compare the actual count with the expected count
                if int(actual_count) == int(expected_count):
                    print(f"Data file '{file_name}' is present. Expected record count:{expected_count} matching with Actual Count: {actual_count}")
                else:
                    all_matched_records = False
                    print(f"Data file '{file_name}' is present. Expected record count of {expected_count} not matching Actual Count: {actual_count}")
            else:
                all_files_present = False
                print(f"Data file '{file_name}' does not exist in the container.")


        # Move files to appropriate folders
        for file_dict in data_files_list:
            file_name =file_dict["file_name"]

            if all_files_present and all_matched_records:
                destination_folder = 'external'
            else:
                destination_folder = "failed"

            self.move_files_to_folder(file_name, destination_folder)

        for ctrl_file_name in control_files_list:
            print("Moving CONTROLS file to archive folder")
            ctrl_file_name = ctrl_file_name.split("/")[-1]
            destination_folder = 'archived'
            self.move_files_to_folder(ctrl_file_name, destination_folder)

        print("Moving TRIGGER file to archive folder")

        destination_folder = 'archived'
        trigger_file = self.trigger_file.split("/")[-1]
        self.move_files_to_folder(trigger_file, destination_folder)

        if all_files_present==False: 
            raise FileNotFoundError("Some file is missing...moving the files to failed folder.")
        elif all_matched_records==False:
            raise ValueError("Records does not match...moving the files to failed folder.")


    def process_files(self):
        control_files_list = self.get_control_files()
        print("LIST OF CONTROL FILES:", control_files_list)
        data_files_list = self.parse_control_files(control_files_list)
        print("LIST OF DATA FILES:", data_files_list)
        self.validate_data_files(data_files_list, control_files_list)

