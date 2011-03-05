using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Globalization;

namespace DatabaseValidator
{
    public enum Verbosity
    {
        None = 0,
        Quiet = 1,
        Normal = 2,
        Verbose = 3
    }

    public sealed class Validator
    {
        private Validator() { }

        public static bool DatabaseIsValid(string connectionString, Verbosity verbosity, bool executeObjects)
        {
            bool rc = true;
            int objectCount = 0;
            int invalidObjectCount = 0;

            using (SqlConnection db = new SqlConnection(connectionString))
            using (SqlCommand cmd = new SqlCommand(GetProcedureListSql(db, verbosity), db))
            {
                if (db.State != ConnectionState.Open) db.Open();
                using (DataSet procReader = new DataSet())
                {
                    procReader.Locale = CultureInfo.CurrentCulture;
                    procReader.Load(cmd.ExecuteReader(), LoadOption.OverwriteChanges, "main");

                    bool succeeded = false;
                    //for each object get the command text and execute the command
                    foreach (DataRow dr in procReader.Tables[0].Rows)
                    {
                        string name = (string)dr["Name"];
                        string quotedIdent = (int)dr["quoted_ident_on"] == 1 ? "ON" : "OFF";
                        string ansiNulls = (int)dr["ansi_nulls_on"] == 1 ? "ON" : "OFF";
                        string schema = (string)dr["schema"];

                        if (verbosity == Verbosity.Verbose) Console.WriteLine("Processing {0}.{1}", schema, name);

                        string text = GetProcedureText(db, schema, name);
                        if (!string.IsNullOrEmpty(text))
                        {
                            succeeded = TryCompile(db, schema, name, quotedIdent, ansiNulls, text);
                            if (succeeded && executeObjects)
                                succeeded = ExecuteIfSafe(db, schema, name, dr["type"] as string, text);
                        }

                        if (!succeeded && !string.IsNullOrEmpty(text)) invalidObjectCount++;
                        objectCount++;
                    }

                    if (verbosity != Verbosity.Quiet && verbosity != Verbosity.None)
                        Console.WriteLine("\nProcessing complete.  Objects processed: {0}\tInvalid objects found: {1}", objectCount, invalidObjectCount);
                }
                db.Close();
            }
            if (invalidObjectCount > 0) rc = false;
            return rc;
        }

        private static string[] _dangerousKeywords = { "UPDATE", "INSERT", "DELETE", "CREATE", "DROP", "EXEC", "EXECUTE" };

        private static bool ExecuteIfSafe(SqlConnection db, string schema, string name, string type, string text)
        {
            if (!IsSafe(text)) return true;

            using (SqlCommand cmd = GetSqlCommand(db, schema, name, type))
            {
                if (db.State != ConnectionState.Open) db.Open();
                try { cmd.ExecuteNonQuery(); }
                catch (SqlException ex)
                {
                    Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "{0}\t{1}\tFAILED\t{2}\tLine {3}", schema, name, ex.Message, ex.LineNumber).Replace("\r", " ").Replace("\n", " "));
                    return false;
                }
                catch (System.Data.Common.DbException ex)
                {
                    Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "{0}\t{1}\tFAILED\t{2}", schema, name, ex.Message).Replace("\r", " ").Replace("\n", " "));
                    return false;
                }
                //Debugging only
                //catch (System.Exception)
                //{ return false; }
            }
            return true;
        }

        private static SqlCommand GetSqlCommand(SqlConnection db, string schema, string name, string type)
        {
            SqlCommand target = new SqlCommand();
            target.CommandType = GetCommandType(type);
            target.Connection = db;
            target.CommandText = schema + "." + name;
            if (string.CompareOrdinal(type.Trim(), "FN") == 0)
                target.CommandText = "exec " + target.CommandText;
            else if (string.CompareOrdinal(type.Trim(), "V") == 0)
                target.CommandText = "SELECT top 1 * FROM " + target.CommandText;

            if (string.CompareOrdinal(type.Trim(), "V") != 0)
                using (SqlCommand cmd = new SqlCommand(GetCommandTextForParameters(schema, name), db))
                {
                    if (db.State != ConnectionState.Open) db.Open();
                    using (IDataReader dr = cmd.ExecuteReader())
                        while (dr.Read())
                        {
                            SqlParameter param = target.Parameters.AddWithValue(dr["Name"] as string,
                                GetFakeValueForParameter(dr["SystemType"] as string, dr["DefaultValue"]));
                            if ((bool)dr["IsOutputParameter"]) param.Direction = ParameterDirection.InputOutput;
                        }
                }
            return target;
        }

        private static object GetFakeValueForParameter(string type, object defaultValue)
        {
            if (defaultValue != DBNull.Value) return defaultValue;
            switch (type)
            {
                case "varchar":
                case "char":
                case "nvarchar":
                case "nchar":
                case "ntext":
                case "text":
                    return "D";
                case "int":
                case "bigint":
                case "bit":
                case "smallint":
                case "decimal":
                case "float":
                case "money":
                case "smallmoney":
                case "numeric":
                case "real":
                case "sql_variant":
                case "tinyint":
                    return 1;
                case "datetime":
                case "smalldatetime":
                case "timestamp":
                    return DateTime.Now;
                case "uniqueidentifier":
                    return Guid.NewGuid();
                case "binary":
                case "image":
                case "varbinary":
                case "xml":
                default:
                    return DBNull.Value;
            }
        }

        private static CommandType GetCommandType(string type)
        {
            switch (type.Trim())
            {
                case "P": return CommandType.StoredProcedure;
                case "V":
                case "FN":
                default: return CommandType.Text;
            }
        }

        static string _parameterText;
        private static string GetCommandTextForParameters(string schema, string name)
        {
            if (string.IsNullOrEmpty(_parameterText))
            {
                _parameterText = "SELECT ";
                _parameterText += "param.name AS [Name], ";
                _parameterText += "    ISNULL(baset.name, N'') AS [SystemType], ";
                _parameterText += "CAST(CASE WHEN baset.name IN (N'nchar', N'nvarchar') AND param.max_length <> -1 THEN param.max_length/2 ELSE param.max_length END AS int) AS [Length], ";
                _parameterText += "CAST(param.precision AS int) AS [NumericPrecision], ";
                _parameterText += "CAST(param.scale AS int) AS [NumericScale], ";
                _parameterText += "null AS [DefaultValue], ";
                _parameterText += "param.is_output AS [IsOutputParameter], ";
                _parameterText += "sp.object_id AS [IDText], ";
                _parameterText += "param.name AS [ParamName], ";
                _parameterText += "CAST( ";
                _parameterText += " case  ";
                _parameterText += "    when sp.is_ms_shipped = 1 then 1 ";
                _parameterText += "    when ( ";
                _parameterText += "        select  ";
                _parameterText += "            major_id  ";
                _parameterText += "        from  ";
                _parameterText += "            sys.extended_properties  ";
                _parameterText += "        where  ";
                _parameterText += "            major_id = sp.object_id and  ";
                _parameterText += "            minor_id = 0 and  ";
                _parameterText += "            class = 1 and  ";
                _parameterText += "            name = N'microsoft_database_tools_support')  ";
                _parameterText += "        is not null then 1 ";
                _parameterText += "    else 0 ";
                _parameterText += "end           ";
                _parameterText += "             AS bit) AS [ParentSysObj] ";
                _parameterText += "FROM ";
                _parameterText += "sys.all_objects AS sp ";
                _parameterText += "INNER JOIN sys.all_parameters AS param ON param.object_id=sp.object_id ";
                _parameterText += "LEFT OUTER JOIN sys.types AS baset ON baset.user_type_id = param.system_type_id and baset.user_type_id = baset.system_type_id ";
                _parameterText += "WHERE ";
            }

            string rc = _parameterText + string.Format(CultureInfo.InvariantCulture,"(sp.type = N'P' OR sp.type = N'RF' OR sp.type='PC')and(sp.name=N'{0}' and SCHEMA_NAME(sp.schema_id)=N'{1}') ", name, schema);
            rc += "ORDER BY ";
            return rc + "param.parameter_id ASC";
        }

        private static bool IsSafe(string text)
        {
            string normalizedText = text.ToUpperInvariant().Remove(0, "CREATE".Length);
            foreach (string keyword in _dangerousKeywords)
                if (normalizedText.Contains(keyword))
                    return false;
            return true;
        }

        /// <summary>
        /// Determines the correct SQL to issue to get the properties for procedures, views and functions.
        /// SQL may be different based on the version of the database, as SQL 2005 introduced seperation between 
        /// users and schemas.
        /// </summary>
        /// <param name="db">Database to check</param>
        /// <returns></returns>
        private static string GetProcedureListSql(SqlConnection db, Verbosity verbosity)
        {
            string getObjSql = "select name, OBJECTPROPERTY(id, 'ExecIsQuotedIdentOn') as quoted_ident_on, OBJECTPROPERTY(id, 'ExecIsAnsiNullsOn') as ansi_nulls_on, object_schema_name(o.id) [schema], [type] from sysobjects o where type in ('P', 'V', 'FN') and category = 0";
            string message = "Detected SQL Server version SQL 2005 or later";

            //Determine version of database
            using (SqlCommand cmd = new SqlCommand("SELECT  SERVERPROPERTY('productversion') ver", db))
            {
                if (db.State != ConnectionState.Open) db.Open();
                using (IDataReader dr = cmd.ExecuteReader())
                {
                    dr.Read();
                    // Prior to SQL 2005 (version 9), schema was owner, so we'll issue a different statement
                    if (int.Parse(((string)dr["ver"]).Remove(((string)dr["ver"]).IndexOf(".", StringComparison.CurrentCulture)), NumberStyles.Integer, CultureInfo.InvariantCulture) < 9)
                    {
                        message = "Detected SQL Server version prior to SQL 2005";
                        getObjSql = "select name, OBJECTPROPERTY(id, 'ExecIsQuotedIdentOn') as quoted_ident_on, OBJECTPROPERTY(id, 'ExecIsAnsiNullsOn') as ansi_nulls_on, user_name(o.uid) [schema], [type] from sysobjects o where type in ('P', 'V', 'FN') and category = 0";
                    }
                    dr.Close();
                }
            }
            if (verbosity != Verbosity.Quiet && verbosity != Verbosity.None) Console.WriteLine(message);
            return getObjSql;
        }

        /// <summary>
        /// Extracts the actual text of a stored procedure, view or function
        /// </summary>
        /// <param name="db">database to work on</param>
        /// <param name="schema">schema (user for versions prior to SQL 2005) for the object</param>
        /// <param name="objectName">Object name</param>
        /// <returns></returns>
        private static string GetProcedureText(SqlConnection db, string schema, string objectName)
        {
            StringBuilder sb = new StringBuilder(8000);

            //call sp_helptext to the create command
            using (SqlCommand cmd = new SqlCommand(string.Format(CultureInfo.InvariantCulture, "exec sp_helptext '{1}.{0}'", objectName, schema), db))
            {
                //loop through the command text and build up the command string
                try
                {
                    if (db.State != ConnectionState.Open) db.Open();
                    using (IDataReader textRdr = cmd.ExecuteReader())
                    {
                        while (textRdr.Read())
                            sb.Append(textRdr["text"].ToString());
                        textRdr.Close();
                    }
                }
                catch (SqlException ex)
                {
                    string errText = string.Format(CultureInfo.InvariantCulture, "{0}\t{1}\tUNREADABLE\t{2}", schema, objectName, ex.Message).Replace("\r", " ").Replace("\n", " "); ;
                    Console.WriteLine(errText);
                    sb = new StringBuilder();
                }
            }
            return sb.ToString();
        }

        /// <summary>
        /// Attempts to fake-compile a stored procedure.  By setting noexec, the database will compile the proc but not actually commit it to disk,
        /// allowing us some basic checks.  Note that this will miss many issues that are found only when running a stored procedure/view/function
        /// </summary>
        /// <param name="db">database to work on</param>
        /// <param name="schema">schema (user for versions prior to SQL 2005) for the object</param>
        /// <param name="objectName">Object name</param>
        /// <param name="quotedIdent">"ON" or "OFF" depending on which option is most appropriate</param>
        /// <param name="ansiNulls">"ON" or "OFF" depending on which option is most appropriate</param>
        /// <param name="procText">text of the procedure</param>
        private static bool TryCompile(SqlConnection cnExec, string schema, string objectName, string quotedIdent, string ansiNulls, string procText)
        {
            bool succeeded = true;
            //execute the command
            try
            {
                SqlCommand sqlCmd = new SqlCommand("set noexec on", cnExec);
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.ExecuteNonQuery();

                sqlCmd = new SqlCommand("SET QUOTED_IDENTIFIER " + quotedIdent, cnExec);
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.ExecuteNonQuery();

                sqlCmd = new SqlCommand("SET ANSI_NULLS " + ansiNulls, cnExec);
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.ExecuteNonQuery();

                sqlCmd = new SqlCommand(procText, cnExec);
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.ExecuteNonQuery();

                sqlCmd = new SqlCommand("SET QUOTED_IDENTIFIER OFF ", cnExec);
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.ExecuteNonQuery();

                sqlCmd = new SqlCommand("SET ANSI_NULLS ON", cnExec);
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.ExecuteNonQuery();

                sqlCmd = new SqlCommand("set noexec off", cnExec);
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.ExecuteNonQuery();

                sqlCmd = new SqlCommand("set parseonly off", cnExec);
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.ExecuteNonQuery();
            }
            catch (System.Data.Common.DbException ex)
            {
                succeeded = false;
                string errText = string.Format(CultureInfo.InvariantCulture, "{0}\t{1}\tFAILED\t{2}", schema, objectName, ex.Message).Replace("\r", " ").Replace("\n", " ");
                Console.WriteLine(errText);
            }
            return succeeded;
        }

    }
}
