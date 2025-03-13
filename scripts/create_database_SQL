/*
===============================================
Create Database Schemas
===============================================
Script Purpose:
	This scripts creates a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'Bronze', 'Silver', and 'Gold'. 

VARNING: 
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. 
	Proceed with caution and ensure you have proper backups before running this script.
*/

USE master;

-- Create Database DataWarehouse
CREATE Database DataWarehouse;

USE DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

--
