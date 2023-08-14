using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace MongoDBToSQLServer
{
    class Program
    {
        /// <summary>
        /// Load configuration from app settings
        /// </summary>
        /// <returns></returns>
        private static IConfigurationRoot LoadConfiguration()
        {
            return new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appSettings.json")
                .Build();
        }

        /// <summary>
        /// Application entry point
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            SqlConnection? sqlConnection = null;
            SqlTransaction? sqlTransaction = null;

            try
            {
                // Load configuration
                IConfigurationRoot configuration = LoadConfiguration();

                // Configure MongoDB connection
                var mongoConnectionString = configuration.GetSection("MongoDB:ConnectionString").Value;
                var mongoDatabaseName = configuration.GetSection("MongoDB:DatabaseName").Value;
                var mongoCollections = configuration.GetSection("MongoDB:Collections").Get<List<string>>();
                var mongoClient = new MongoClient(mongoConnectionString);
                var mongoDatabase = mongoClient.GetDatabase(mongoDatabaseName);

                // Configure SQL Server connection
                var sqlConnectionString = configuration.GetSection("SQLServer:ConnectionString").Value;
                sqlConnection = new SqlConnection(sqlConnectionString);
                sqlConnection.Open();

                // Begin SQL Server transaction
                sqlTransaction = sqlConnection.BeginTransaction();

                // Iterate over MongoDB collections
                foreach (var collectionName in mongoCollections)
                {
                    var mongoCollection = mongoDatabase.GetCollection<BsonDocument>(collectionName);

                    // Read data from MongoDB collection
                    var documents = mongoCollection.Find(new BsonDocument()).ToList();

                    // Get MongoDB collection schema
                    var schema = Helper.GetMongoCollectionSchema(documents);

                    // Create SQL Server table if it doesn't exist within the transaction
                    Helper.CreateTableIfNotExists(sqlConnection, collectionName, schema, sqlTransaction);

                    // Create indexes for the SQL Server table within the transaction
                    Helper.CreateIndexes(sqlConnection, collectionName, schema, mongoCollection, sqlTransaction);

                    // Import data into SQL Server table
                    //foreach (var sqlCommand in
                    //from document in documents // Serialize the document including nested arrays
                    //let serializedDocument = Helper.SerializeDocument(document) // Generate the SQL INSERT command
                    //let sqlInsertCommand = Helper.GetSqlInsertCommand(collectionName, schema, serializedDocument) // Execute the SQL INSERT command
                    //let sqlCommand = new SqlCommand(sqlInsertCommand, sqlConnection, sqlTransaction)
                    //select sqlCommand)
                    //{
                    //    sqlCommand.ExecuteNonQuery();
                    //}
                    foreach (var document in documents)
                    {
                        // Serialize the document including nested arrays
                        var serializedDocument = Helper.SerializeDocument(document);

                        // Generate the SQL INSERT command
                        var sqlInsertCommand = Helper.GetSqlInsertCommand(collectionName, schema, serializedDocument);

                        // Execute the SQL INSERT command
                        var sqlCommand = new SqlCommand(sqlInsertCommand, sqlConnection, sqlTransaction);
                        sqlCommand.ExecuteNonQuery();
                    }
                }

                // Commit the SQL Server transaction
                sqlTransaction.Commit();

                Console.WriteLine("Data imported successfully!");
            }
            catch (Exception ex)
            {
                // Rollback the SQL Server transaction on error
                sqlTransaction?.Rollback();
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
            finally
            {
                // Close SQL Server connection
                sqlConnection?.Close();
            }
        }
    }
}
