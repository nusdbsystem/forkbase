# Illustrative Example of Collaborative Analytics #

As an illustrative example, we simulate the collaborative analytics as follows. 
Firstly, we generate a table which contains multiple columns and a number of records. 
Storage layout of the table is columnar, in favor of OLAP. 
The synthetic dataset are loaded into UStore under the `master` branch, which is presumably owned by an analytics coordinator. 

Secondly, we simulate the situation such that two data scientists perform different analytical tasks based on the above table. 
Both scientists work on their individual branches other than `master`. 
In particular, one scientist performs the so-called Poisson Analytics under the `poi_ana` branch, and the other scientist performs the so-called Binomial Analytics under the `bin_ana` branch. 
Both scientists store their analytical results into a new column, namely `distr`, associated with their individual branches. 

Finally, the analytics coordinator, working on the `master` branch, tries to analyze the results produced by the two data scientists and further merge the results. 
Specifically, s/he performs a _diff_ operation on the `distr` column with respect to the branches of `poi_ana` and `bin_ana` to get insights of the result differences. 
Subsequently, s/he merges the `poi_ana` and `bin_ana` branches of column `distr` into the `master` branch by averaging the `distr` columns associated with those two branches. 

Along with the above demonstration, we can see the superiority of UStore in terms of storage/data versioning and branching when supporting collaborative analytics. 

## Compilation ##

Run the following commands to compile source code:
 
    $ cd /path/to/ustore
    $ mkdir -p build && cd build && rm -rf *
    $ cmake -DENABLE_TEST=OFF -DENABLE_EXAMPLE=ON ..
    $ make

The generated executable binary is `ustore_example_ca`, which is placed under the `build/bin` directory.

## Execution ##

Run `ustore_example_ca` with `--help` (or `-?`) to check for available command-line parameters. 
For example: 

    $ ./bin/ustore_example_ca --help
    Allowed options:
      -? [ --help ]                    print usage message
      -c [ --columns ] arg (=3)        number of columns in a simple table
      -n [ --records ] arg (=10)       number of records in a simple table
      -p [ --probability ] arg (=0.01) probability used in the analytical simulation
      -i [ --iterations ] arg (=1000)  number of iterations in the analytical simulation

Specially, the program is able to detect invalid command-line parameters and abort the illegal execution early. 
For example: 

    $ ./bin/ustore_example_ca --records -5
    [ARG] Number of columns: 3
    [ERROR ARG] Number of records: [Actual] -5, [Expected] >0
    [FAILURE] Found invalid command-line option

A sample execution with default parameter settings is demonstrated as follows: 

    $ ./bin/ustore_example_ca 
    [ARG] Number of columns: 3
    [ARG] Number of records: 10
    [ARG] Probability: 0.01
    [ARG] Number of iterations: 1000

    -------------[ Loading Dataset ]-------------
    C0       @master    : [C0-0, C0-1, C0-2, C0-3, C0-4, C0-5, C0-6, C0-7, C0-8, C0-9] <String>
    C1       @master    : [C1-0, C1-1, C1-2, C1-3, C1-4, C1-5, C1-6, C1-7, C1-8, C1-9] <String>
    C2       @master    : [C2-0, C2-1, C2-2, C2-3, C2-4, C2-5, C2-6, C2-7, C2-8, C2-9] <String>
    Key      @master    : [K-0, K-1, K-2, K-3, K-4, K-5, K-6, K-7, K-8, K-9] <String>
    ---------------------------------------------

    ------------[ Poisson Analytics ]------------
    [Parameters] branch="poi_ana", lambda=0.1
    >>> Referring Columns <<<
    Key      @poi_ana   : [K-0, K-1, K-2, K-3, K-4, K-5, K-6, K-7, K-8, K-9] <String>
    C1       @poi_ana   : [C1-0, C1-1, C1-2, C1-3, C1-4, C1-5, C1-6, C1-7, C1-8, C1-9] <String>
    >>> Affected Columns <<<
    distr    @poi_ana   : [910, 88, 2, 0, 0, 0, 0, 0, 0, 0] <String>
    ---------------------------------------------

    -----------[ Binomial Analytics ]------------
    [Parameters] branch="bin_ana", p=0.01, n=10
    >>> Referring Columns <<<
    Key      @bin_ana   : [K-0, K-1, K-2, K-3, K-4, K-5, K-6, K-7, K-8, K-9] <String>
    C0       @bin_ana   : [C0-0, C0-1, C0-2, C0-3, C0-4, C0-5, C0-6, C0-7, C0-8, C0-9] <String>
    C2       @bin_ana   : [C2-0, C2-1, C2-2, C2-3, C2-4, C2-5, C2-6, C2-7, C2-8, C2-9] <String>
    >>> Affected Columns <<<
    distr    @bin_ana   : [904, 92, 4, 0, 0, 0, 0, 0, 0, 0] <String>
    ---------------------------------------------

    -------------[ Merging Results ]-------------
    [Parameters] branch="master"
    >>> Before Merging <<<
    distr    @master    : <none>
    >>> Referring Columns <<<
    Key      @master    : [K-0, K-1, K-2, K-3, K-4, K-5, K-6, K-7, K-8, K-9] <String>
    distr    @poi_ana   : [910, 88, 2, 0, 0, 0, 0, 0, 0, 0] <String>
    distr    @bin_ana   : [904, 92, 4, 0, 0, 0, 0, 0, 0, 0] <String>
    >>> After Merging <<<
    distr    @master    : [907, 90, 3, 0, 0, 0, 0, 0, 0, 0] <String>
    distr    @poi_ana   : [910, 88, 2, 0, 0, 0, 0, 0, 0, 0] <String>
    distr    @bin_ana   : [904, 92, 4, 0, 0, 0, 0, 0, 0, 0] <String>
    ---------------------------------------------

## Misc. ##

To test the worker APIs, compile source code as follows:

    $ cmake ..
    $ make

Run the test using the command below: 

    $ ./bin/test_ustore --gtest_filter=Worker.*
