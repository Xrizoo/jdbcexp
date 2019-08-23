# jdbcexp
Export CSV data from a DB through a JDBC connection with java command line app.

v3.2: Allow add multiple exports to a single output file (parameter -a)

v3.1: Some minor fixes

v3.0: Connection parameters through command line

Notes:
-The input sql file only accept one SQL instruction, so you don't have to use the end-of-query character because it returns error.
-Special characters CR, TAB and LF (\r \t \n) are accepted by -s parameter, even without backslash.
