using System;

namespace DatabaseValidator
{
    /// <summary>
    /// Summary description for Class1.
    /// </summary>
    public sealed class ValidatorConsole
    {
        private ValidatorConsole() { }

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        public static int Main(string[] args)
        {
            int rc = 0;
            bool process = true;

            foreach (string arg in args)
                if (arg.StartsWith("/?", StringComparison.CurrentCulture) || arg.StartsWith("/h", StringComparison.CurrentCultureIgnoreCase))
                {
                    ShowHelp();
                    process = false;
                }

            try
            {
                if (process) {
                    var connectionString = GetConnectionString(args);
                    Console.WriteLine("Checking data source: " + GetValueForConnectionStringKey(connectionString,"data source"));
                    Console.WriteLine("Database: " + GetValueForConnectionStringKey(connectionString, "Initial Catalog") ?? "(default for user)");
                    if (
                        !Validator.DatabaseIsValid(connectionString, GetVerbosity(args),
                                                   GetExecutionOption(args)))
                        rc = 1;
                }
            }
            catch (Exception ex)
            {
                rc = 1;
                Console.WriteLine("Error: {0}", ex.Message);
                ShowHelp();
            }

            return rc;
        }

        private static string GetValueForConnectionStringKey(string connectionString, string value) 
        {
            foreach (var part in connectionString.Split(';')) {
                var kvp = part.Split('=');
                if (kvp[0].ToLowerInvariant() == value.ToLowerInvariant())
                    return kvp[1];
            }
            return null;
        }

        private static bool GetExecutionOption(string[] args)
        {
            foreach (string arg in args)
                if (arg.StartsWith("/x", StringComparison.CurrentCultureIgnoreCase))
                    return true;
            return false;
        }

        private static Verbosity GetVerbosity(string[] args)
        {
            Verbosity rc = Verbosity.Normal;
            foreach (string arg in args)
                if (arg.StartsWith("/v:", StringComparison.CurrentCulture))
                {
                    switch (arg.Substring(3, 1).ToUpperInvariant())
                    {
                        case "Q":
                            rc = Verbosity.Quiet;
                            break;
                        case "V":
                            rc = Verbosity.Verbose;
                            break;
                        case "N":
                            rc = Verbosity.Normal;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("args", "Verbosity must be \"quiet\", \"verbose\", or \"normal\"");
                    }
                }

            return rc;
        }


        private static void ShowHelp()
        {
            Console.WriteLine("Usage: SqlValidator [/v:VERBOSITY] [/c:CONNECTIONSTRING] [/x]");
            Console.WriteLine("\tVerbosity = q (quiet - errors only), n (normal), or v (verbose)");
            Console.WriteLine("\t/x will execute objects that appear safe (no INSERT, UPDATE, or DELETE statements)");
            Console.WriteLine("\n\t SqlValidator will check stored procedures, functions, views and triggers for syntax using the SQL Server engine");
            Console.WriteLine("\t This is a basic check and does not guarantee the code will operate properly.");
            Console.WriteLine("\t For example, unless executed with the /x option, SQL Server's check does not validate that referenced tables actually exist");

        }

        private static string GetConnectionString(string[] args)
        {
            string rc = null;
            if (System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"] != null)
                rc = System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"];

            foreach (string arg in args)
                if (arg.StartsWith("/c", StringComparison.CurrentCultureIgnoreCase))
                {
                    if (arg.IndexOf(':') <= 0) throw new ArgumentOutOfRangeException("args", "No ':' found after /c switch");
                    rc = arg.Substring(arg.IndexOf(':') + 1, arg.Length - (arg.IndexOf(':') + 1));
                }

            if (rc == null) throw new ArgumentNullException("args", "No connection string found on command line or in SqlValidator.exe.config");
            return rc;
        }
    }
}
