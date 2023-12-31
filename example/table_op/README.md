# Example of Table Operations #

This example code is in purpose of demonstrating how to operate on table in UStore. To this end, the following operations are executed in order: 

1. Loading CSV data into table.
2. Updating data with predicate whose values refer to the external parameter file. This generates multiple versions (branches) of the table. 
3. Performing `diff` operation upon different versions (branches) of the table. 
4. Performing simple aggregation on one column of the table.

Meanwhile, we also benchmark the above operations and output the results to screen.

## Compilation ##

Run the following commands to compile source code:
 
    $ cd /path/to/ustore
    $ mkdir -p build && cd build && rm -rf *
    $ cmake -DENABLE_TEST=OFF ..
    $ make

The generated executable binary is `ustore_example_table_op`, which is placed under the `build/bin` directory.

## Execution ##

Run `ustore_example_table_op` with `--help` (or `-?`) to check for available command-line parameters. 

Suppose all the input data are placed in the `data` folder which resides at the same directory level as the `build` folder. Run the sample command below at the `build` folder:

    $ ./bin/ustore_example_table_op ../data/rpc_r100_a5_e3.csv -r Age_Region -v ../data/rpc_update_ref_val.txt -e Profile -a Num_Departure

The `rpc_r100_a5_e3.csv` is the input CSV file, and the `rpc_update_ref_val` file contains the update-referring values organized in a one-line-per-value manner. Each update refers to the `Age_Region` column and effects on the `Profile` column. Regarding aggregation, the `Num_Departure` column is to be aggregated.

Along with the execution, table `Test` is created and to be operated on. Each update on the table generates a new version (branch). The branch name corresponds to the input data is `master`, and the branches generated by the subsequent updates are named by `dev-XXX` where `XXX` is the update-referring value for each update. 
