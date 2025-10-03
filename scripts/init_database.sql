/* 
====================================================
Create Database and Schemas
====================================================
Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
	within the databases: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution
	and ensure you have proper backups before running this script.
	*/___

Use Master;
Go

-- Drop and recreate the 'DataWarehouse' database
If Exists (Select 1 from sys.databases where name = 'DataWarehouse')
Begin
	Alter DATABASE DataWarehouse SET SINGLE_USER With rollback immediate;
	Drop DATABASE DataWarehouse;
End;
Go

-- Create the 'DataWarehouse' database
Create Database DataWarehouse;
Go

Use DataWarehouse;
Go

-- Create Schemas
Create Schema bronze;
Go

Create Schema silver;
Go

Create Schema gold;
Go

