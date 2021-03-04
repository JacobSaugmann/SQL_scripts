# SQL_scripts
Collection of scripts for SQL server

## Index analysis
Here is a script that looks at the created index and the missing index details, making a overview of usage stats, columns, index type etc.

### 04-03-2021 Added index_analysis_from_xlsx_file
This script can use the data createt in the Index_analsys script, just copt the result into excel (right click resultset copy with headers) and paste it into excel, save the file and use that location as a param for the @path_to_excel parameter in the index_analysis_from_xlsx_file.sql
###note! 
SQL ML and python has to be enabled on the server for this script to work, and the folder with the .xlsx fils has to have the rigth security settings 
run in cmd:  icacls c:\testFolder /grant *S-1-15-2-1:(OI)(CI)F /t -where testFolder is the name og the folder with the xlsx file


###Update 25-02-2020

Added memory usage of unused indexes

###Update 05-08-2019

Bugfix in density calculation, multiple equality columns on same table vas not calculated, this is fixed now.
Altered get meta age, bug (devide by Zero) when server was restarted within the last day


###Update: 30-7-2019:

Added column max length on selectativity table
Bugfix in occurance calculation

###Updated 24-06-2019:

Bugfix doublet values removed

## backup index
A script to output all the indexes as create scripts
