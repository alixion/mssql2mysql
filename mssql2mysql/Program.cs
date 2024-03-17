using System;
using System.Linq;
using System.Data.Common;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Globalization;
using System.IO;
using McMaster.Extensions.CommandLineUtils;

namespace mssql2mysql
{
    //dotnet pack -o ./
    [Command(Description = "Generate mysql script from SQL Server")]
    class Program
    {
        static void Main(string[] args)
        {
            CommandLineApplication.Execute<Program>(args);
        }
        private Type StringType = typeof(string);
        private Type DateTimeType = typeof(DateTime);
        private Type BoolType = typeof(bool);

        [Option(Description = "SQL Server ConnectionString")]
        public string ConnectionString { get; set; }

        [Option(Description = "MySQL Script file name, default: dump.sql")]
        public string FileName { get; set; }

        [Option(Description = "MySQL Schema")]
        public string Schema { get; set; }
        
        [Option(Description = "Ignore tables")] public string[] IgnoreTables { get; set; }
        [Option(Description = "Table")] public string TableName { get; set; }
        [Option(Description = "With data")] public int WithData { get; set; }
        
        private int OnExecute()
        {
            if (ConnectionString == null)
            {
                return 1;
            }
            FileName = FileName ?? "dump.sql";
            using (var fileStream = new FileStream(FileName, FileMode.Create))
            {
                using (var writer = new StreamWriter(fileStream, Encoding.UTF8))
                {
                    // inserts only, if table name was specified
                    if (!string.IsNullOrEmpty(TableName) && WithData == 1)
                    {
                        writer.WriteLine("SET FOREIGN_KEY_CHECKS=0;");
                        writer.WriteLine(GetData(TableName));
                        writer.WriteLine("SET FOREIGN_KEY_CHECKS=1;");
                        GetDbConnection().Close();
                        Console.WriteLine("Save to: " + FileName);
                        return 0;
                    }
                    
                    if (Schema != null)
                    {
                        writer.WriteLine($"CREATE DATABASE  IF NOT EXISTS `{Schema}` /*!40100 DEFAULT CHARACTER SET latin1 */;");
                        writer.WriteLine($"USE `{Schema}`;");
                        writer.WriteLine();
                    }
                    
                    writer.WriteLine("SET FOREIGN_KEY_CHECKS=0;");
                    foreach (var item in GetTables().ToArray())
                    {
                        if (IgnoreTables!=null && IgnoreTables.Any(x => x == item))
                        {
                            Console.WriteLine("Skipping: " + item);
                        }
                        else
                        {
                            Console.WriteLine("Generating: " + item);
                            writer.WriteLine(GetTableSchema(item));
                        }
                        writer.WriteLine();
                        if (WithData == 1)
                        {
                            writer.WriteLine(GetData(item));
                        }
                    }
                    writer.WriteLine("SET FOREIGN_KEY_CHECKS=1;");
                }
            }
            GetDbConnection().Close();
            Console.WriteLine("Save to: " + FileName);
            return 0;
        }
        private SqlConnection SqlConnection;
        private SqlConnection GetDbConnection()
        {
            if (SqlConnection == null)
            {
                SqlConnection = new SqlConnection(ConnectionString);
                SqlConnection.Open();
            }
            return SqlConnection;
        }

        private IEnumerable<string> GetTables()
        {
            using (DbCommand dbCommand = GetDbConnection().CreateCommand())
            {
                dbCommand.CommandText = "EXEC sys.sp_tables NULL,'dbo',NULL,'''TABLE'''";
                var reader = dbCommand.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        yield return reader.GetString(2);
                    }
                }
                reader.Close();
            }
        }
        private string GetTableSchema(string tableName)
        {
            var builder = new StringBuilder();
            using (DbCommand dbCommand = GetDbConnection().CreateCommand())
            {
                dbCommand.CommandText = "EXEC sys.sp_columns '" + tableName + "';";
                var reader = dbCommand.ExecuteReader();
                if (!reader.HasRows) return "";
                
                builder.AppendLine("DROP TABLE IF EXISTS `" + tableName + "`;");
                builder.AppendLine("CREATE TABLE `" + tableName + "` (");
                var columnBuilder = new StringBuilder();
                while (reader.Read())
                {
                    if(reader.GetString(3).StartsWith("old"))
                        continue;
                    string nullable;
                    if (reader.GetInt16(10) == 1)
                    {
                        nullable = " NULL";
                    }
                    else { nullable = " NOT NULL"; }
                    var size = reader.GetInt32(7);
                    var precision = reader.GetInt32(6);
                    int? scale = null;
                    if (reader.GetValue(8) != System.DBNull.Value)
                    {
                        scale = reader.GetInt16(8);
                    }
                    var dbType = GetDataType(reader.GetString(5), size, precision, scale);
                    if (columnBuilder.Length > 0)
                    {
                        columnBuilder.AppendLine(",");
                    }
                    columnBuilder.Append($"\t`{reader.GetString(3)}` {dbType} {nullable}");
                }
                reader.Close();
                var primaryKey = GetPrimaryKey(tableName);
                if (primaryKey != null)
                {
                    columnBuilder.AppendLine(",");
                    columnBuilder.Append(primaryKey);
                }
                var foreignKey = GetForeignKey(tableName);
                if (foreignKey != null)
                {
                    columnBuilder.AppendLine(",");
                    columnBuilder.Append(foreignKey);
                }
                columnBuilder.AppendLine();
                columnBuilder.Append(");");
                builder.Append(columnBuilder.ToString());
            }
            return builder.ToString();
        }
        private string GetDataType(string type, int size, int precision, int? scale)
        {
            if (type == "nvarchar")
            {
                return $"VARCHAR({size / 2}) CHARACTER SET utf8mb4";
            }

            if (type == "varchar")
            {
                return $"VARCHAR({size}) CHARACTER SET utf8mb4";
            }
            if (type == "text" || type == "ntext")
            {
                return "LONGTEXT CHARACTER SET utf8mb4";
            }
            if (type == "char" || type == "nchar")
            {
                return $"CHAR({size}) CHARACTER SET utf8mb4";
            }
            if (type == "int" || type == "int8")
            {
                return "INT";
            }
            
            if (type=="smallint")
            {
                return "SMALLINT";
            }
            
            if (type == "datetime" || type == "smalldatetime" || type == "datetimeoffset")
            {
                return "DATETIME";
            }

            if (type == "uniqueidentifier")
                return "CHAR(13)";
            
            if (type == "image" || type == "binary" || type == "varbinary")
            {
                return "LONGBLOB";
            }
            if (type == "money" || type == "smallmoney" || type == "decimal" || type == "numeric")
            {
                return $"DECIMAL ({precision},{scale})";
            }
            if (type == "float" || type == "real")
            {
                return "FLOAT";
            }
            if (type == "bit")
            {
                return "TINYINT(1)";
            }
            if (type == "int identity")
            {
                return "INT AUTO_INCREMENT";
            }

            return "NOTFOUND"; //type.ToUpper();
        }
        private string GetPrimaryKey(string tableName)
        {
            var primaryKeys = new HashSet<string>();
            using (DbCommand dbCommand = GetDbConnection().CreateCommand())
            {
                dbCommand.CommandText = $"EXEC sys.sp_indexes_rowset '{tableName}'";
                var reader = dbCommand.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        if (reader.GetBoolean(6))
                        {
                            primaryKeys.Add($"`{reader.GetString(17)}`");
                        }
                    }
                }
                reader.Close();
            }
            if (primaryKeys.Count == 0) return null;

            return $"\tPRIMARY KEY ({string.Join(",", primaryKeys)})";
        }
        private string GetForeignKey(string tableName)
        {
            var foreignKeys = new StringBuilder();
            using (DbCommand dbCommand = GetDbConnection().CreateCommand())
            {
                dbCommand.CommandText = $"EXEC sys.sp_fkeys @fktable_name='{tableName}'";
                var reader = dbCommand.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        if (foreignKeys.Length > 0)
                        {
                            foreignKeys.AppendLine(",");
                        }
                        foreignKeys.AppendLine($"\tKEY `{reader.GetString(11)}` (`{reader.GetString(7)}`),");
                        foreignKeys.Append($"\tCONSTRAINT `{reader.GetString(11)}` FOREIGN KEY (`{reader.GetString(7)}`) REFERENCES `{reader.GetString(2)}` (`{reader.GetString(3)}`) ON DELETE NO ACTION ON UPDATE NO ACTION");
                    }
                }
                reader.Close();
            }
            if (foreignKeys.Length == 0) return null;
            return foreignKeys.ToString();
        }
        private string GetData(string tableName)
        {
            var builder = new StringBuilder();
            var columns = new List<string>();
            using (DbCommand dbCommand = GetDbConnection().CreateCommand())
            {
                dbCommand.CommandText = "EXEC sys.sp_columns '" + tableName + "';";
                var reader = dbCommand.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        if(!reader.GetString(3).StartsWith("old"))
                            columns.Add($"[{reader.GetString(3)}]");
                    }
                }
                reader.Close();
            }

            using (DbCommand dbCommand = GetDbConnection().CreateCommand())
            {
                dbCommand.CommandText = $"SELECT {string.Join(",", columns)} FROM [{tableName}];";
                var reader = dbCommand.ExecuteReader();
                if (reader.HasRows)
                {
                    builder.AppendLine($"/*!40000 ALTER TABLE `{tableName}` DISABLE KEYS */;");
                    builder.AppendLine($"INSERT INTO `{tableName}` VALUES");
                    var rowIndex = 0;
                    while (reader.Read())
                    {
                        var values = new List<string>();
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            values.Add(FormatValue(reader.GetValue(i)).ToString());
                        }
                        if (rowIndex > 0)
                        {
                            builder.AppendLine(",");
                        }
                        builder.Append($"({string.Join(',', values)})");
                        rowIndex++;
                    }
                    builder.AppendLine(";");
                    builder.AppendLine($"/*!40000 ALTER TABLE `{tableName}` ENABLE KEYS */;");
                }
                reader.Close();
            }
            return builder.ToString();
        }
        private object FormatValue(object value)
        {
            if (value == DBNull.Value)
            {
                return "NULL";
            }
            var valueType = value.GetType();
            if (valueType == StringType)
            {
                return $"'{value.ToString()!.Replace("'", "''").Replace("\"", "\\\"").Replace("\\",@"\\")}'";
            }

            if (valueType == typeof(Guid))
            {
                return $"'{((Guid)value).ToString()}'";
            }

            if (valueType == typeof(DateTimeOffset))
            {
                return
                    $"'{((DateTimeOffset)value).UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)}'";
            }
            
            if (valueType == DateTimeType)
            {
                return $"'{Convert.ToDateTime(value).ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)}'";
            }
            if (valueType == BoolType)
            {
                return Convert.ToBoolean(value) ? 1 : 0;
            }

            return value;
        }
    }
}
