--This script creates database and creates schemas for the architecture medallion 

create database DataWarehouse;
go 

create schema bronze;
go
create schema silver;
go
create schema gold;
