from Functions import *


def sequential() -> None:
    """
    The application runs sequential
    To run the application: python Sequential.py

    :return: None
    """

    # Create the folders used in this program
    create_folders(OUT_SEQUENTIAL_PATH)

    # [ MAP PHASE ]
    print("\nMapping:")

    split_input_files = split_files(IN_SEQUENTIAL_PATH, len(MAP_PROCESSES))

    for files, process in zip(split_input_files, MAP_PROCESSES):
        map_files(IN_SEQUENTIAL_PATH, OUT_SEQUENTIAL_PATH, files, process)

    # [ MERGE PHASE ]
    print("\nMerging...")

    merge_files(OUT_SEQUENTIAL_PATH)

    # [ REDUCE PHASE ]
    print("\nReducing:")

    split_merged_files = split_files(os.path.join(OUT_SEQUENTIAL_PATH, "merge"), len(REDUCE_PROCESSES))

    for files, process in zip(split_merged_files, MAP_PROCESSES):
        reduce_files(OUT_SEQUENTIAL_PATH, files, process)


if __name__ == "__main__":
    sequential()
