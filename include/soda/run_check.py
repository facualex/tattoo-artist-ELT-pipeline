def run_check(data_source_name,
              check_name,
              check_file_path,
              yaml_configuration_file_path):
    from os import path

    if not path.exists(yaml_configuration_file_path):
        raise ValueError(f"The configuration file path: {yaml_configuration_file_path} does not exist. Make sure that your configuration file is actually located there.")

    if not path.exists(check_file_path):
        raise ValueError(f"The check file path: {check_file_path} does not exist. Make sure that your check file is actually located there.")

    from soda.scan import Scan
    from datetime import datetime

    scan = Scan() # loading the function
    scan.set_data_source_name(data_source_name) # initialising the datasource name

    # Loading the configuration file
    scan.add_configuration_yaml_file(
        file_path=yaml_configuration_file_path
    )

    # Adding scan date variable to label the scan date
    scan.add_variables({ "date": datetime.now().strftime('%d-%m-%Y') })

    # Loading the check yaml file
    scan.add_sodacl_yaml_file(check_file_path)

    # Executing the scan
    scan.execute()

    # Setting logs to verbose mode
    scan.set_verbose(True)

    # Inspect the scan result
    scan.get_scan_results()

    # Check for errors in the scan results and raise an error if errors are detected
    if scan.has_check_fails():
        raise ValueError(f"Errors were detected in the data quality check task: {check_name}. Failed checks: {scan.get_checks_fail_text()}")