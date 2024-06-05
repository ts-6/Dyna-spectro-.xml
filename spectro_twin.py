import xml.etree.ElementTree as Et
import pyodbc
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
import traceback
import re
import logging

logging.basicConfig(filename='logfile.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        elif event.src_path.lower().endswith(".xml"):
            print(f"\nNew .xml file created: {event.src_path}")
            process_xml_file(event.src_path)


def processed_cusname(cus_name_text):
    # Remove non-alphanumeric characters and convert to uppercase
    cust_name = re.sub(r'[^A-Z0-9.]', '', cus_name_text.upper())

    # Handle specific cases
    if cust_name == 'SIGMA.' or cust_name == ' SIGMA.':
        return 'SIGMA'

    elif cust_name == 'AV' or cust_name == 'AVK':
        return 'AVK'

    elif cust_name == 'TECSO' or cust_name == 'TECSO IMPORT' or cust_name == 'TESCO IMPORT' or cust_name == 'TESCO':
        return 'TECSO'

    elif (cust_name == 'SCHWING-1' or cust_name == 'SCHWING - 1' or cust_name == 'SCHWING -1' or cust_name == 'SCHWING- 1' or cust_name == 'SCHWING'
          or cust_name == 'SCHWING-01' or cust_name == 'SCHWING - 01' or cust_name == 'SCHWING -01' or cust_name == 'SCHWING- 01'
          or cust_name == 'SCHWING 01' or cust_name == 'SCHWING01' or cust_name == 'SCHWING02' or cust_name == 'SCHWING 02'
          or cust_name == 'SCHWING-2' or cust_name == 'SCHWING - 2' or cust_name == 'SCHWING -2' or cust_name == 'SCHWING- 2'
          or cust_name == 'SCHWING-02' or cust_name == 'SCHWING - 02' or cust_name == 'SCHWING -02' or cust_name == 'SCHWING- 02'):
        return 'SCHWING'

    elif (cust_name == 'WILO-1' or cust_name == 'WILO- 1' or cust_name == 'WILO -1' or cust_name == 'WILO - 1' or cust_name == 'WILO' or
          cust_name == 'WILLO' or cust_name == 'WILO01' or cust_name == 'WILO 01' or cust_name == 'WILO 02' or cust_name == 'WILO02' or
          cust_name == 'WILLO01' or cust_name == 'WILLO 01' or cust_name == 'WILLO 02' or cust_name == 'WILLO02' or
          cust_name == 'WILLO-1' or cust_name == 'WILLO- 1' or cust_name == 'WILLO -1' or cust_name == 'WILLO - 1' or
          cust_name == 'WILO-2' or cust_name == 'WILO- 2' or cust_name == 'WILO -2' or cust_name == 'WILO - 2' or
          cust_name == 'WILLO-2' or cust_name == 'WILLO- 2' or cust_name == 'WILLO -2' or cust_name == 'WILLO - 2'):
        return 'WILO'
    elif (cust_name == 'GALVIN ENGG ' or cust_name == 'GALVIN ENGG' or cust_name == 'GALVIN' or cust_name == 'GALVIN ' or
          cust_name == ' GALVIN ENGG' or cust_name == ' GALVIN' or cust_name == ' GALVIN ENGG ' or cust_name == ' GALVIN '):
        return 'GALVIN'

    else:
        cust_name = cus_name_text
    # If no specific case matches, return the cleaned grade
    return cust_name


def process_grade(grade):
    # Remove non-alphanumeric characters and convert to uppercase
    cleaned_grade = re.sub(r'[^A-Z0-9]', '', grade.upper())

    # Handle specific cases
    if cleaned_grade == '654512' or cleaned_grade == '6541512' or cleaned_grade == '6545132':
        return '65-45-12'
    elif cleaned_grade == '604210':
        return '60-42-10'
    elif cleaned_grade == '5007' or cleaned_grade == 'ENGJS500-7' or cleaned_grade == 'ENGJS5007' or cleaned_grade == 'ENGJS500':
        return '500-7'
    elif cleaned_grade == '40012':
        return '400-12'
    elif cleaned_grade == '40015':
        return '400-15'
    elif cleaned_grade == '45010':
        return '450-10'
    elif cleaned_grade == '42010':
        return '420-10'
    elif cleaned_grade == '200' or cleaned_grade == 'FG200':
        return 'FG-200'
    elif cleaned_grade == '210' or cleaned_grade == 'FG210':
        return 'FG-210'
    elif cleaned_grade == '250' or cleaned_grade == 'FG250' or cleaned_grade == 'FG-250' or cleaned_grade == 'ENGJL250':
        return 'EN-GJL-250'
    elif cleaned_grade == '260' or cleaned_grade == 'FG260':
        return 'FG-260'
    elif cleaned_grade == '300' or cleaned_grade == 'FG300' or cleaned_grade == 'FG-300' or cleaned_grade == 'ENGJL300':
        return 'EN-GJL-300'

    # If no specific case matches, return the cleaned grade
    return cleaned_grade


# Process XML files
def process_xml_file(xml_file_path):
    try:
        # Parse the XML file
        tree = Et.parse(xml_file_path)
        root = tree.getroot()

        total_replicate_count = 0  # Initialize the count of all replicates
        deleted_replicate_count = 0  # Initialize the count of deleted replicates
        non_deleted_replicate_count = 0  # Initialize the count of non-deleted replicates

        for replicate in root.findall('.//MeasurementReplicate'):
            total_replicate_count += 1  # Increment total count for every replicate

            # Check if IsDeleted attribute is present and set to 'Yes'
            is_deleted = replicate.get('IsDeleted')
            if is_deleted and is_deleted.lower() == 'yes':
                deleted_replicate_count += 1  # Increment count of deleted replicates
            else:
                non_deleted_replicate_count += 1  # Increment count of non-deleted replicates

        # Extract data from SampleResult and SampleIDs
        sample_result = root.find('SampleResult')
        item_name = sample_result.get('Name')

        sample_ids = sample_result.find('SampleIDs')
        heat_no_element = sample_ids.find(".//SampleID[IDName='Heat No']/IDValue")
        date_element = sample_ids.find(".//SampleID[IDName='Date']/IDValue")
        grade_element = sample_ids.find(".//SampleID[IDName='Grade']/IDValue")
        furnace_num_element = sample_ids.find(".//SampleID[IDName='Furnace No.']/IDValue")
        cus_name_element = sample_ids.find(".//SampleID[IDName='Customer Name']/IDValue")

        heat_number = heat_no_element.text.strip() if heat_no_element is not None else "N/A"
        date = date_element.text.strip() if date_element is not None and date_element.text is not None else None
        date_formats = ['%d.%m.%Y', '%d-%m-%Y', '%d/%m/%Y']

        position = None
        dummy_heat = None
        if heat_number.startswith('B'):
            position = 'Bath'
            heat_number = heat_number.strip() if heat_number is not None else "N/A"
            dummy_heat = re.sub(r'^B-', '', heat_number)

        elif heat_number.startswith('H'):
            position = 'Final'
            dummy_heat = heat_number

        formatted_date = None
        for format_pattern in date_formats:
            try:
                formatted_date = datetime.strptime(date, format_pattern).strftime('%Y-%m-%d')
                break  # Exit the loop if conversion succeeds
            except ValueError:
                pass  # Continue to the next format pattern if the current one fails

        if formatted_date is None:
            print(f"Error converting date: {date}. Skipping file: {xml_file_path}")
            return

        grade = grade_element.text.strip() if grade_element is not None else "N/A"
        furnace_num = furnace_num_element.text.strip() if furnace_num_element is not None else "N/A"
        cus_name_text = cus_name_element.text.strip() if cus_name_element is not None else "N/A"

        # Process the grade
        grade_txt = process_grade(grade)
        cus_name_txt = processed_cusname(cus_name_text)
        print(f"State:", position, "\nHeat No:", heat_number, "\nDummy Heat:", dummy_heat)
        print(f"\nTotal Spark Count: {total_replicate_count}, Reported Count: {non_deleted_replicate_count}, "
              f"Deleted count: {deleted_replicate_count}")
        print(f"\nItem Name: {item_name}, Date: {formatted_date}, Grade: {grade_txt}, "
              f"Furnace Num: {furnace_num}, Customer Name: {cus_name_txt}")

        # Extract data from Elements
        element_results = {}
        for replicate in root.findall('.//MeasurementReplicate'):
            # Skip processing if IsDeleted="Yes"
            if replicate.get('IsDeleted') == 'Yes':
                print(f"MeasurementReplicate has IsDeleted='Yes'. Skipping MeasurementReplicate.")
                continue

            # Iterate through Elements in each MeasurementReplicate
            for element in replicate.findall('.//Element'):
                element_name = element.get('ElementName')
                result_value = float(element.find('.//ResultValue').text)

                # If element_name not in dictionary, add it with an empty list
                if element_name not in element_results:
                    element_results[element_name] = []

                # Append the result value to the list for the current replicate
                element_results[element_name].append(result_value)

            # Print results for each element and calculate average
        database_dicts = {}  # Dictionary to store formatted averages
        for element_name, results in element_results.items():
            print(f"\nResults for Element {element_name}:")

            if results:
                average_value = sum(results) / len(results)
                formatted_average = "{:.3f}".format(average_value)
                print(f"{results}, \nAverage: {formatted_average}")  # Mean value

                # Store formatted average in the dictionary
                database_dicts[element_name] = formatted_average

        # Insert values into the database using the dictionary
        insert_into_database(heat_number, formatted_date, item_name, furnace_num, cus_name_txt, grade_txt,
                             database_dicts, total_replicate_count, deleted_replicate_count, non_deleted_replicate_count, position, dummy_heat)

        return True  # Add a return statement here

    except Exception as ex:
        logging.error(f"An error occurred while processing XML file: {str(ex)}")
        print(f"An error occurred while processing XML file: {str(ex)}")
        traceback.print_exc()
        return False  # Add a return statement here


# Insert data into the Microsoft SQL Server database
def insert_into_database(heat_number, formatted_date, item_name, furnace_num, cus_name_txt, grade_txt, database_dicts,
                         total_replicate_count, deleted_replicate_count, non_deleted_replicate_count, position, dummy_heat):
    conn = None
    try:
        # Connect to the database 192.168.1.5 'SERVER=45.116.2.234;'
        connection_string = 'DRIVER={SQL Server};' \
                            'SERVER=45.116.2.234;' \
                            'DATABASE=Dyna_Himcast;' \
                            'PORT=1433;' \
                            'UID=sa;' \
                            'PWD=knvhn;' \
                            'TrustServerCertificate=yes;'

        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Construct the column names and placeholders for parameters
        columns = ', '.join(f'[{col}]' for col in database_dicts.keys())
        placeholders = ', '.join('?' for _ in database_dicts)

        # Insert data into the Spectro table
        query = (f"INSERT INTO Spectro (heat_number, date, item_name, furnace_num, customer_name, grade, {columns}, "
                 f"total_sparks, deleted_spark, reported_spark, state, dummy_heat) "
                 f"VALUES (?, ?, ?, ?, ?, ?, {placeholders}, ?, ?, ?, ?, ?)")
        params = ([heat_number, formatted_date, item_name, furnace_num, cus_name_txt, grade_txt] +
                  list(database_dicts.values()) + [total_replicate_count, deleted_replicate_count, non_deleted_replicate_count, position, dummy_heat])
        cursor.execute(query, params)
        conn.commit()

    except pyodbc.Error as ex:
        logging.error(f"Error inserting data into the database: {ex}")
        print(f"Error inserting data into the database: {ex}")
        traceback.print_exc()
    except Exception as ex:
        logging.error(f"An error occurred: {str(ex)}")
        print(f"An error occurred: {str(ex)}")
        traceback.print_exc()
    finally:
        if conn is not None:
            conn.close()
            print(f"Database insertion done")
            logging.info("Database insertion done")


if __name__ == "__main__":
    print("Script is running... watching for new xml files...")

    observer = None

    try:
        # Set up watchdog
        folder_to_watch = r"C:\Users\PEPE\Desktop\Spectro"
        # r"C:\Spectro Smart Studio\Sample Results" r"C:\Users\PEPE\Desktop\Spectro" # Change to the folder you want to monitor

        event_handler = MyHandler()
        observer = Observer()
        observer.schedule(event_handler, path=folder_to_watch, recursive=False)
        observer.start()

        while True:
            pass

    except KeyboardInterrupt:
        observer.stop()
        print("Script terminated by user.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        print(f"An unexpected error occurred: {str(e)}")
        traceback.print_exc()

    finally:
        if observer is not None:
            observer.join()
