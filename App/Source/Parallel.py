from mpi4py import MPI

from Functions import *


def parallel() -> None:
    """
    The application runs parallel
    To run the application: mpiexec -n 6 python Parallel.py

    :return: None
    """

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # Master process that will manage the work for the other processes
    if rank == MASTER_PROCESS:
        # Create the folders used in this program
        create_folders(OUT_PARALLEL_PATH)

        # [ MAP PHASE ]
        print("\nMapping:")

        split_input_files = split_files(IN_PARALLEL_PATH, len(MAP_PROCESSES))

        # Send the files to be mapped to each process
        for files, process in zip(split_input_files, MAP_PROCESSES):
            comm.send(files, dest=process)

        # [ MERGE PHASE ]
        print("\nMerging...")

        # Wait for all processes to be finished before merging. Some sort of barrier but just for some processes
        for process in MAP_PROCESSES:
            comm.recv(source=process)

        merge_files(OUT_PARALLEL_PATH)

        # [ REDUCE PHASE ]
        print("\nReducing:")

        split_merged_files = split_files(os.path.join(OUT_PARALLEL_PATH, "merge"), len(REDUCE_PROCESSES))

        # Send the files to be reduced to each process
        for files, process in zip(split_merged_files, REDUCE_PROCESSES):
            comm.send(files, dest=process)

    # Processes that will map the files
    if rank in MAP_PROCESSES:
        files = comm.recv(source=0)
        map_files(IN_PARALLEL_PATH, OUT_PARALLEL_PATH, files, rank)
        comm.send(True, dest=0)

    # Processes that will reduce the mapped files
    if rank in REDUCE_PROCESSES:
        files = comm.recv(source=0)
        reduce_files(OUT_PARALLEL_PATH, files, rank)


if __name__ == "__main__":
    parallel()
