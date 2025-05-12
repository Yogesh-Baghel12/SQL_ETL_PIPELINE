use master;

GO
create database if not exists  DataWarehouse;
GO
use DataWarehouse;
GO
create schema if not exists bronze;
GO
create schema if not exists silver;
GO
create schema if not exists gold;
