import glob
import pandas as pd
import xml.etree.ElementTree
import datetime
import json
import logging
import os

# Setup logging configuration
logging.basicConfig(
    filename='operations.txt',   # Name of the log file
    level=logging.INFO,          # Log level (INFO, DEBUG, etc.)
    format='%(asctime)s - %(message)s', # Log format with timestamp
    datefmt='%Y-%m-%d %H:%M:%S'  # Date and time format
)

def ExtractDataFromCSV(csv_files):
  data_frame = pd.DataFrame() 
  for file in csv_files:
    # Load the CSV into a Pandas DataFrame
    # data_frame = pd.read_csv(file)
    # print(data_frame)
    logging.info(f"contents of the {file} is appended to list")
    data_frame = pd.concat([data_frame,  pd.read_csv(file)], ignore_index=True)
  print(data_frame)
  return data_frame

def ExtractDataFromJSON(json_files): 
  data_frame = pd.DataFrame()
  for file in json_files:
    # Load the json into a Pandas DataFrame
    # json_data = pd.read_json(file, lines=True)
    # print(json_data)
    logging.info(f"contents of the {file} is appended to list")
    data_frame = pd.concat([data_frame,  pd.read_json(file,lines=True)], ignore_index=True)
  print(data_frame)
  return data_frame

def ExtractDataFromXML(xml_files): 
  data_frame = pd.DataFrame()
  for file in xml_files:
    # Load the json into a Pandas DataFrame
    # xml_data = pd.read_xml(file)
    # print(xml_data)
    logging.info(f"contents of the {file} is appended to list")
    data_frame = pd.concat([data_frame,  pd.read_xml(file)], ignore_index=True)
  print(data_frame)
  return data_frame

def ExtractData(file_path):
  main_data_frame = pd.DataFrame()
  try:
    # csv files are flat structure where things are represented in rows and columns
    all_csv_files = glob.glob(f"{file_path}/*.csv")
    print(all_csv_files)
    logging.info("Checks for all csv files in the directory")
    logging.info(f"list of csv files {all_csv_files}")
    main_data_frame = ExtractDataFromCSV(all_csv_files)
    print(f"main function print {main_data_frame}")

    # json structure is heirarchial and not always flat, might require special handling in some cases
    # default structure for json [....] bur one can also use the lines are delimiter
    # to read such files lines=TRUE should be set
    all_json_files = glob.glob(f"{file_path}/*.json")
    print(all_json_files)
    logging.info("Checks for all json files in the directory")
    logging.info(f"list of json files {all_json_files}")
    json_data_frame = ExtractDataFromJSON(all_json_files)
    main_data_frame = pd.concat([main_data_frame, json_data_frame], ignore_index=True)

    # straight forward method
    all_xml_files = glob.glob(f"{file_path}/*.xml")
    print(all_xml_files)
    logging.info("Checks for all xml files in the directory")
    logging.info(f"list of xml files {all_xml_files}")
    xml_data_frame = ExtractDataFromXML(all_xml_files)
    main_data_frame = pd.concat([main_data_frame, xml_data_frame], ignore_index=True)
 
  except Exception as e:
    logging.info(f"Error occurred: {e}")
  
  return main_data_frame


current_directory = os.getcwd()
print(f"current directory - {current_directory}")
df = ExtractData(current_directory)
print(df)
#convert the entire height column from inches to meters
df["height"] = df["height"] * 0.0254
logging.info(f"heights are converted from inches to meter")

#convert the entire weight column from pounds to kgs
df["weight"] = df["weight"] * 0.453592
logging.info(f"weights are converted from pounds to kgs")

print(df)

df.to_csv('finalData.csv', index=False)
logging.info(f"A new csv file created with content from various appended and converted")
