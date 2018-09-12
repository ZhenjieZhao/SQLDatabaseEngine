# SQLDatabaseEngine

## Description

This is a project serving as a small database engine. It can process basic sql languages and manage the dataset in the files which are divided into pages.


## Running Instruction

IDE: Eclipse
Compiler: Java 1.8
Import the project into eclipse and run DataBase.java as application, use the console to control the program.

## Program Architecture
The program has three layers: IO layer, Business layer and Infrastructure layer.
IO layer (DataBase.java) serves to communicate with user, get the input and sent output to the console.
Business layer (ExecuteCommand.java) serves to process the input.
Infrastructure layer (Page.java) serves to manage (read and write) the details of the file.

## Using Instruction

1.	SHOW TABLES
Display all tables in the data base.
2.	CREATE TABLE table_name (<column_name datatype>);
Create a table. User don’t have to define rowid, the data base automatically create the rowid column which increases automatically.
3.	INSERT INTO table_name (column_list) VALUES (value_list);
Insert into a row into the database. If the (column_list) is empty, the values will be inserted in the default order of the columns. 
4.	UPDATE table_name SET column_name = value WHERE condition;
Update a row. The length of the value of text cannot beyond original length.
5.	SELECT * FROM table_name WHERE column_name operator value;
Print all the columns of the table with the condition.
6.	DELETE FROM table_name WHERE condition;
Delete a row from the table according to the condition.
7.	DROP TABLE table_name;
Remove a table schema and all of its data.
