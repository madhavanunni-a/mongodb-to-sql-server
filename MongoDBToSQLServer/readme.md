# MongoDB to SQL Server Migration
	This .net 8.0 console application project shows a simple example of how to migrate data from MongoDB to SQL Server.

## Prerequisites
	1. A MongoDB instance running on localhost:27017 (The server can be changed accordingly)
	2. A SQL Server instance running on localhost (The server can be changed accordingly)
	3. Visual Studio or Visual Studio Code
	4. .Net 8.0 SDK

## How to run the project
	1. Clone the project
	2. Open the project in Visual Studio or Visual Studio Code
	3. Build the project
	4. Update the server details in the `appsettings.json` file as per need
	5. Run the project

## When to use
	1. When you want to migrate data from MongoDB to SQL Server
	2. When you want to migrate data from MongoDB to SQL Server in a scheduled manner

## Note
	For complex data types like `Array` and `Object` in MongoDB, the data is converted to JSON string and stored in SQL Server.
	In-depth data type conversion is not implemented in this project.
	