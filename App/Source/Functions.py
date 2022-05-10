import math
import os
import re
import shutil
import time
from Config import *


def trim(string: str) -> str:
    """
    Remove any special character and multiple spaces to make the life easier

    :param string: self explanatory
    :return: the result
    """

    string = re.sub(r"[.|,?!;:\-\"@#$%^&*()_=+{}<>\\/\n\t\[\]`~]", " ", string)

    string = re.sub(r" +", " ", string)

    return string.lower()


def is_illegal(string: str) -> bool:
    """
    Check if the word can be the name of a file because some words are exclusive to Widows :)

    :param string: self explanatory
    :return: the result
    """

    if string.upper() in ["COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
                          "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
                          "CON", "PRN", "AUX", "NUL", ""]:
        return True

    return False


def create_folders(output_path: str) -> None:
    """
    Create the folders used in this program

    :param output_path: self explanatory
    :return: None
    """

    map_path = os.path.join(output_path, "map")
    merge_path = os.path.join(output_path, "merge")
    reduce_path = os.path.join(output_path, "reduce")

    # Delete the folder (where the result of mapping will be stored) if exists and create an empty one
    if os.path.exists(map_path):
        shutil.rmtree(map_path)
    os.makedirs(map_path)

    for rank in MAP_PROCESSES:
        # Folder where the process will store the data
        process_directory = os.path.join(map_path, str(rank))

        # Delete the folder (where the result of mapping will be stored) if exists and create an empty one
        if os.path.exists(process_directory):
            shutil.rmtree(process_directory)
        os.makedirs(process_directory)

    # Delete the folder (where the result of merging will be stored) if exists and create an empty one
    if os.path.exists(merge_path):
        shutil.rmtree(merge_path)
    os.makedirs(merge_path)

    # Delete the folder (where the result of reducing will be stored) if exists and create an empty one
    if os.path.exists(reduce_path):
        shutil.rmtree(reduce_path)
    os.makedirs(reduce_path)


def split_files(path: str, number: int) -> list[list[str]]:
    """
    Split a set of files into chunks

    :param path: self explanatory
    :param number: self explanatory
    :return: files' names
    """

    # Get a list with the name of the files in the given folder
    input_files = os.listdir(path)

    # split the files into chunks (sub arrays)
    split_input_files = [[] for _ in range(number)]

    # Is like an array that is transformed into a matrix, kind of
    for i in range(math.ceil(len(input_files) / number)):
        for j in range(number):

            # This try is used in case the number of files is not divisible by the given number of sub arrays (number)
            try:
                split_input_files[j].append(input_files[i * number + j])
            except:
                continue

    return split_input_files


def map_files(in_path: str, output_path: str, files: list[str], rank: int) -> None:
    """
    Mapping part of MapReduce

    :param in_path: self explanatory
    :param output_path: self explanatory
    :param files: the files that will be mapped
    :param rank: the rank of the process
    :return: None
    """

    start = time.time()

    map_path = os.path.join(output_path, "map")

    # Folder where the process will store the data
    process_directory = os.path.join(map_path, str(rank))

    # For each given file
    for file in files:
        try:
            with open(os.path.join(in_path, file), "r", encoding='utf-8-sig', errors='ignore') as read_file:

                # Read the file line by line
                content = read_file.readlines()

                #  For each line in the file
                for line in content:

                    # Remove any special character and multiple spaces
                    line = trim(line)

                    # Split the line into words
                    words = line.split(" ")

                    # For each word in the line
                    for word in words:

                        # Some words are reserved to Windows :)
                        if is_illegal(word):
                            continue

                        with open(os.path.join(process_directory, f"{word}.txt"), "a+") as write_file:
                            write_file.write(f"<{file[:-4]}:1> ")

        except Exception as e:
            print(e)

        print(f"    The process [ {rank} ] processed the file [ {file} ] in {round(time.time() - start, 3)} seconds")


def merge_files(output_path: str) -> None:
    """
    Merge all files (words) after every process ends in mapping phase

    :param output_path: self explanatory
    :return: None
    """

    map_path = os.path.join(output_path, "map")
    merge_path = os.path.join(output_path, "merge")

    # Get a list with the name of the files in the given folder
    directories = os.listdir(map_path)

    # For each directory in mapping folder
    for directory in directories:
        try:
            # Get the files from this directory
            files = os.listdir(os.path.join(map_path, directory))

            # For every file in the folder
            for file in files:

                # The math where the file is in map folder
                map_file_path = os.path.join(map_path, directory, file)

                # The path where the file will be in merge folder
                merge_file_path = os.path.join(merge_path, file)

                # If the file (word.txt) already exists in merging folder append the content to the existing file
                if os.path.isfile(merge_file_path):
                    with open(map_file_path, "r", encoding='utf-8-sig', errors='ignore') as read_file:
                        with open(merge_file_path, "a", encoding='utf-8-sig', errors='ignore') as write_file:
                            write_file.write(read_file.read())

                # If the file (word.txt) doesn't exists in merging folder copy the file
                else:
                    shutil.copyfile(map_file_path, merge_file_path)

        except Exception as e:
            print(e)


def reduce_files(output_path: str, files: list[str], rank: int) -> None:
    """
    Reduce part of MapReduce

    :param output_path: self explanatory
    :param files: the files that will be reduced
    :param rank: the rank of the process
    :return: None
    """

    start = time.time()

    merge_path = os.path.join(output_path, "merge")
    reduce_path = os.path.join(output_path, "reduce")

    for file in files:
        try:
            # The path where the file is in merge folder
            merge_file_path = os.path.join(merge_path, file)

            # The path where the file will be in reduce folder
            reduce_file_path = os.path.join(reduce_path, file)

            # Where we keep the filename and the word's occurrences in that file {filename:occurrences}
            dictionary = {}

            with open(merge_file_path, "r", encoding='utf-8-sig', errors='ignore') as read_file:

                content = read_file.read()

                content = re.sub(r"[<>]", "", content)

                data = content.split(" ")

                for value in data:

                    # To avoid this case that may appear because of the split
                    if value == "":
                        continue

                    # Get the filename
                    key = value.split(":")[0]

                    # If the filename is already added increment the value
                    if key in dictionary:
                        dictionary[key] += 1

                    # If is not add it
                    else:
                        dictionary[key] = 1

            with open(reduce_file_path, "w+", encoding='utf-8-sig', errors='ignore') as write_file:
                string = ""

                # For every filename in the dictionary
                for key in dictionary:
                    string += f"<{key}.txt:{dictionary[key]}> "

                write_file.write(string)

        except Exception as e:
            print(e)

        print(f"    The process [ {rank} ] processed the file [ {file} ] in {round(time.time() - start, 3)} seconds")
