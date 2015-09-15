using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DayCareSqlDependencyNotification
{
    public class Program : IDisposable
    {
        #region Variable declaration

        private readonly string sourceConnectionString;
        private readonly string destConnectionString;

        private readonly string notificationQuery;

        private readonly string notificationStoredProcedure;

        private SqlDependencyEx sqlDependency;

        /// <summary>
        /// SQL command.
        /// </summary>
        private SqlCommand sourceSqlCommand;

        /// <summary>
        /// SQL connection
        /// </summary>

        private const string DATABASE_NAME = "DayCareDemo";

        private const string TABLE_NAME = "Activities";

        #endregion

        # region Update Destination Db

        #endregion

        #region Constructor

        /// <summary>
        /// Prevents a default instance of the <see cref="Program"/> class from being created. 
        /// </summary>
        private Program()
        {
            this.sourceConnectionString = ConfigurationManager.ConnectionStrings["SourceDbConnection"].ConnectionString;
            this.destConnectionString = ConfigurationManager.ConnectionStrings["DestDbConnection"].ConnectionString;
            this.notificationQuery = "SELECT Guid,OUID,Id,Name,Flags,TenantID,DateCreated,DateLastModified,CreatedByID,LastModifiedBy,Version,IsDeleted,ExternalID,TagString,IsArchived FROM Activities;";
            this.notificationStoredProcedure = "uspGetSampleInformation";
        }

        #endregion

        #region Methods

        /// <summary>
        /// Main method.
        /// </summary>
        /// <param name="args">Input arguments.</param>
        public static void Main(string[] args)
        {
            var program = new Program();
            Console.WriteLine("Smartcare Sql Depedency Notification started...");
            program.Notification();


            Console.ReadLine();
            program.Dispose();
        }

        /// <summary>
        /// Dispose all used resources.
        /// </summary>
        public void Dispose()
        {
            if (null != this.sourceSqlCommand)
            {
                this.sourceSqlCommand.Dispose();
            }

            if (null != this.sqlDependency)
            {
                this.sqlDependency.Dispose();
            }

            this.sourceSqlCommand = null;

        }

        private void Notification()
        {
            while (true)
            {
                using (this.sqlDependency = new SqlDependencyEx(
                            this.sourceConnectionString,
                            DATABASE_NAME,
                            TABLE_NAME, "dbo"))
                {
                    sqlDependency.TableChanged += (o, e) =>
                    {
                        if (e.Data == null) return;

                        var insertedList = e.Data.Elements("inserted").Elements("row");//.Elements("SampleName");


                        var deletedList = e.Data.Elements("deleted").Elements("row");
                        foreach (var j in deletedList)
                        {
                            string cmd = string.Format(@"UPDATE {0} Set SORIsCurrent=0 Where Guid = '{1}' and SORIsCurrent=1;", TABLE_NAME, j.Element("Guid").Value);
                            ExecuteNonQuery(cmd, this.destConnectionString);
                            Console.WriteLine(cmd);
                            //foreach (var i in j.Nodes())
                            //    Console.WriteLine("inserted: " + i);
                        }

                        foreach (var j in insertedList)
                        {
                            string cmd = string.Format(@"INSERT INTO {0} VALUES (GETDATE(), GETDATE(), 1, '{1}', {2}, {3}, {4}, {5}, '{6}', '{7}', {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15});", TABLE_NAME, j.Element("Guid").Value,
                                j.Element("OUID") != null ? "'" + j.Element("OUID").Value + "'" : "null", j.Element("Id").Value,j.Element("Name") != null ? "'" + j.Element("Name").Value + "'" : "null",
                                j.Element("Flags") != null ? j.Element("Flags").Value : "null", j.Element("TenantID").Value, j.Element("DateCreated").Value, j.Element("DateLastModified") != null ? "'" + j.Element("DateLastModified").Value + "'" : "null",
                                j.Element("CreatedByID") != null ? "'" + j.Element("CreatedByID").Value + "'" : "null", j.Element("LastModifiedBy") != null ? "'" + j.Element("LastModifiedBy").Value + "'" : "null",
                                j.Element("Version").Value, j.Element("IsDeleted").Value, j.Element("ExternalID") != null ? "'" + j.Element("ExternalID").Value + "'" : "null",
                                j.Element("TagString") != null ? "'" + j.Element("TagString").Value + "'" : "null", j.Element("IsArchived").Value);
                            ExecuteNonQuery(cmd, this.destConnectionString);
                            Console.WriteLine(cmd);
                            //foreach (var i in j.Nodes())
                            //    Console.WriteLine("inserted: " + i);
                        }


                        Console.WriteLine("\n");

                        var wait = "temporary - just to debug above";
                    };
                    sqlDependency.Start();

                    // Wait a little bit to receive all changes.
                    Thread.Sleep(1000);
                }
            }
        }

        private static void ExecuteNonQuery(string commandText, string connectionString)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(commandText, conn))
            {
                try
                {
                    conn.Open();
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {

                    throw;
                }
            }
        }

        #endregion
    }
}
