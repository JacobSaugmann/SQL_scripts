# SQL_scripts
Collection of scripts for SQL server

## Index analysis
Here is a script that looks at the created index and the missing index details, making a overview of usage stats, columns, index type etc.

Update 25-02-2020

Added memory usage of unused indexes
Update 05-08-2019

Bugfix in density calculation, multiple equality columns on same table vas not calculated, this is fixed now.
Altered get meta age, bug (devide by Zero) when server was restarted within the last day
Update: 30-7-2019:

 Added column max length on selectativity table
Bugfix in occurance calculation
Updated 24-06-2019:

Bugfix doublet values removed

## backup index
A script to output all the indexes as create scripts
