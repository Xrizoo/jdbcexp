# jdbcexp
Export Data with JDBC to CSV File via Command Line

v3.2: Added support for multiple exports to a single output file (use the -a parameter).

v3.1: Minor fixes and improvements.

v3.0: Added connection parameters via command line.

Notes:

    The input SQL file accepts only one SQL statement. Therefore, you do not need to use a query terminator character, as it will result in an error.
    Special characters such as CR, TAB, and LF (\r, \t, \n) are supported by the -s parameter, even when entered without a backslash.
