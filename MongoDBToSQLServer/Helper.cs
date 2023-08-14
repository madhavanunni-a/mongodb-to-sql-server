using Microsoft.Extensions.Primitives;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace MongoDBToSQLServer
{
    internal class Helper
    {
        /// <summary>
        /// Method to get the schema of a MongoDB collection
        /// </summary>
        /// <param name="documents"></param>
        /// <returns></returns>
        internal static Dictionary<string, Type> GetMongoCollectionSchema(List<BsonDocument> documents)
        {
            var schema = new Dictionary<string, Type>();

            foreach (var document in documents)
            {
                foreach (var element in document.Elements)
                {
                    var columnName = element.Name;
                    var columnType = element.Value.GetType();

                    if (!schema.ContainsKey(columnName))
                    {
                        schema.Add(columnName, columnType);
                    }
                }
            }
            return schema;
        }

        /// <summary>
        /// Method to create the table if it doesnt exist in the destination database
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="transaction"></param>
        internal static void CreateTableIfNotExists(SqlConnection connection, string tableName, Dictionary<string, Type> schema, SqlTransaction transaction = null)
        {
            // Check if the table exists
            var tableExists = TableExists(connection, tableName, transaction);

            // If the table doesn't exist, create it
            if (!tableExists)
            {
                // Create the SQL CREATE TABLE command
                var createTableCommand = new StringBuilder();
                createTableCommand.Append($"CREATE TABLE {tableName} (");

                // Add columns for each field in the schema
                foreach (var field in schema.Keys)
                {
                    // Check if the field is a nested array
                    var fieldType = schema[field];
                    if (fieldType.IsArray && fieldType.GetElementType() == typeof(BsonDocument))
                    {
                        createTableCommand.Append($"{field} NVARCHAR(MAX), ");
                    }
                    else
                    {
                        var sqlColumnType = GetSqlColumnType(fieldType);
                        createTableCommand.Append($"{field} {sqlColumnType}, ");
                    }
                }

                // Add the closing parentheses
                createTableCommand.Remove(createTableCommand.Length - 2, 2);
                createTableCommand.Append(')');

                // Execute the CREATE TABLE command
                using var command = new SqlCommand(createTableCommand.ToString(), connection, transaction);
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Method to check if the table exists in the destination database
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        static bool TableExists(SqlConnection connection, string tableName, SqlTransaction transaction = null)
        {
            // Create the SQL query to check if the table exists
            var query = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}'";

            // Execute the query
            using var command = new SqlCommand(query, connection);
            command.Transaction = transaction;
            var count = Convert.ToInt32(command.ExecuteScalar());
            return count > 0;
        }



        /// <summary>
        /// Method to get the SQL column type for a MongoDB field type
        /// </summary>
        /// <param name="columnType"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        internal static string GetSqlColumnType(Type columnType)
        {
            return columnType.Name switch
            {
                nameof(BsonString) or nameof(BsonNull) or nameof(BsonArray) or nameof(BsonObjectId) => "NVARCHAR(MAX)",
                nameof(BsonInt32) => "INT",
                nameof(BsonDateTime) => "DATETIME",
                nameof(BsonBoolean) => "BIT",
                nameof(BsonDouble) or nameof(BsonDecimal128) => "DECIMAL(38,18)",
                nameof(BsonInt64) => "BIGINT",
                nameof(BsonBinaryData) => "VARBINARY(MAX)",
                _ => throw new NotSupportedException($"Unsupported column type: {columnType.Name}")
            };
        }

        /// <summary>
        /// Method to create indexes on the destination table if they exist in the MongoDB collection
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="mongoCollection"></param>
        /// <param name="transaction"></param>
        internal static void CreateIndexes(SqlConnection connection, string tableName, Dictionary<string, Type> schema, IMongoCollection<BsonDocument> mongoCollection, SqlTransaction transaction = null)
        {
            // Get the list of indexes from the MongoDB collection
            var indexes = mongoCollection.Indexes.List().ToList();

            if(indexes.Count > 0 )
            {
                // Create an index for each field in the schema
                foreach (var field in schema.Keys)
                {
                    // Check if the field should be indexed
                    var indexExists = IndexExists(indexes, field);
                    if (indexExists)
                    {
                        // Create the index command
                        var indexName = $"IX_{tableName}_{field}";
                        var indexCommand = $"CREATE INDEX {indexName} ON {tableName}({field}))";

                        // Execute the index command
                        using var command = new SqlCommand(indexCommand, connection, transaction);
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        /// <summary>
        /// Method to check if an index exists in the MongoDB collection
        /// </summary>
        /// <param name="indexes"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        static bool IndexExists(List<BsonDocument> indexes, string field)
        {
            // Check if the index exists in the MongoDB collection
            return indexes.Any(i => i.Contains(field));
        }

        /// <summary>
        /// Method to serialize a MongoDB document to a BsonDocument
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        internal static BsonDocument SerializeDocument(BsonDocument document)
        {
            var serializedDocument = new BsonDocument();

            foreach (var element in document.Elements)
            {
                var field = element.Name;
                var value = element.Value;

                if (value.IsBsonArray)
                {
                    var serializedArray = value.ToJson();
                    serializedDocument[field] = serializedArray;
                }
                else if (value.IsBsonDocument)
                {
                    var serializedNestedDocument = SerializeDocument(value.AsBsonDocument);
                    serializedDocument[field] = serializedNestedDocument;
                }
                else
                {
                    serializedDocument[field] = value;
                }
            }

            return serializedDocument;
        }


        /// <summary>
        /// Method to create the sql insert command for a MongoDB document
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="document"></param>
        /// <returns></returns>
        internal static string GetSqlInsertCommand(string tableName, Dictionary<string, Type> schema, BsonDocument document)
        {
            var columns = new List<string>();
            var values = new List<string>();

            foreach (var element in document.Elements)
            {
                var columnName = element.Name;
                var columnValue = element.Value;
                columns.Add(columnName);

                if (schema.TryGetValue(columnName, out var columnType))
                {
                    var formattedValue = GetFormattedSqlValue(columnValue, columnType);
                    values.Add(formattedValue);
                }
                else
                {
                    // Handle missing column in schema (optional)
                    Console.WriteLine($"Column {columnName} not found in the extracted schema!");
                }
            }

            var sqlColumns = string.Join(",", columns);
            var sqlValues = string.Join(",", values.Select(v => $"'{v}'"));

            return $"INSERT INTO {tableName} ({sqlColumns}) VALUES ({sqlValues})";
        }

        /// <summary>
        /// Method to get the formatted SQL value for a MongoDB value
        /// </summary>
        /// <param name="value"></param>
        /// <param name="columnType"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        static string GetFormattedSqlValue(object value, Type columnType)
        {
            if (value == null || value == BsonNull.Value)
            {
                return "NULL";
            }

            switch (value)
            {
                case string stringValue:
                    return $"'{stringValue.Replace("'", "''")}'";

                case int:
                case long:
                case decimal:
                case double:
                    return value.ToString();

                case bool boolValue:
                    return boolValue ? "1" : "0";

                case DateTime dateTimeValue:
                    return $"'{dateTimeValue:yyyy-MM-dd HH:mm:ss.fff}'";

                case Guid guidValue:
                    return $"'{guidValue}'";

                case byte[] byteArrayValue:
                    return "0x" + BitConverter.ToString(byteArrayValue).Replace("-", string.Empty);

                case ObjectId objectIdValue:
                    return $"'{objectIdValue}'";

                // Handle more MongoDB data types as needed

                default:
                    //throw new NotSupportedException($"Unsupported MongoDB data type: {columnType.Name}");
                    return value.ToString();
            }
        }



    }
}
