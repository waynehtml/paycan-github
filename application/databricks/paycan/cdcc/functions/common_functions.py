# Databricks notebook source
import zipfile
import os

# COMMAND ----------

def path_does_exist(path : str) -> bool:
    try:
        dbutils.fs.ls(path)
    except Exception as e:
        return False
    return True

# COMMAND ----------

def decompress_zip_file(zip_file_path : str, zip_file_extract_to : str):

    if not zipfile.is_zipfile(zip_file_path):
        raise ValueError(f"The file {zip_file_path} is not a valid ZIP archive.")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(zip_file_extract_to)
        #return zip_ref.namelist()