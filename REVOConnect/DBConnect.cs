using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace REVOConnect
{
    public class DBConnect
    {
        public List<IDictionary<string, object>> Read(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = null;
            MySqlDataReader reader = null; //open the connection
            var dataItems = new List<IDictionary<string, object>>();
            try
            {
                con = new MySqlConnection(connection);
                con.Open();

                //Console.WriteLine("MySql Con OK!");
                MySqlCommand cmd = new MySqlCommand(query, con);
                reader = cmd.ExecuteReader();


                while (reader.Read())
                {

                    var record = new Dictionary<string, object>();
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        //Console.WriteLine(reader[i]);
                        var key = reader.GetName(i);
                        var value = reader[i];
                        //Console.WriteLine(value);
                        record.Add(key, value);
                    }

                    dataItems.Add(record);
                }
            }
            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //ErrorLog.WriteLog(Config._SystemName, this.GetType().Name, System.Reflection.MethodBase.GetCurrentMethod().Name, errorQuery + error.ToString());
            }
            finally
            {
                //Console.WriteLine("suspendItems OK");
                con.Close(); //safely close the connection
            }
            return dataItems;
        }
        public long InsertWithId(String query, String database)
        {
            string temp_connection = String.Format("server=localhost;database={0};userid=root;password=password;", database);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;

            try
            {
                con.Open(); // open connection.

                reader = command.ExecuteReader();
                long id = command.LastInsertedId;
                while (reader.Read())
                {
                }
                return id;
            }

            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //ErrorLog.WriteLog(Config._SystemName, this.GetType().Name, System.Reflection.MethodBase.GetCurrentMethod().Name, errorQuery + error.ToString());
                return 0;
            }
            finally
            {
                con.Close(); // close connection here.
            }
        }
        // Can be used as Update as well as Insert
        public Boolean Update(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;

            try
            {
                con.Open(); // open connection.

                reader = command.ExecuteReader();
                while (reader.Read())
                {
                }
                return true;
            }

            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //Your Log System
                return false;
            }
            finally
            {
                con.Close(); // close connection here.
            }
        }
        public static Boolean StaticUpdate(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;

            try
            {
                con.Open(); // open connection.

                reader = command.ExecuteReader();
                while (reader.Read())
                {
                }
                return true;
            }

            catch (Exception error)
            {
                return false;
            }
            finally
            {
                con.Close(); // close connection here.
            }
        }
        public DateTime SimpleReadDatetime(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;
            DateTime temp = DateTime.Now;
            try
            {
                con.Open(); // open connection.
                reader = command.ExecuteReader();

                while (reader.Read())
                {
                    temp = reader.GetDateTime(0);

                }
            }

            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //ErrorLog.WriteLog(Config._SystemName, this.GetType().Name, System.Reflection.MethodBase.GetCurrentMethod().Name, errorQuery + error.ToString());
            }
            finally
            {
                con.Close(); // close connection here.
            }

            return temp;
        }
        public String SimpleReadString(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;
            String temp = "";
            try
            {
                con.Open(); // open connection.
                reader = command.ExecuteReader();

                while (reader.Read())
                {
                    if (reader.IsDBNull(0))
                    {
                        temp = null;
                    }
                    else
                    {
                        temp = reader.GetString(0);
                    }
                }
            }

            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //Your Log System
            }
            finally
            {
                con.Close(); // close connection here.
            }

            return temp;
        }
        // SimpleRead only reads single value from MySQL database
        public long SimpleReadLong(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;
            long temp = 0;

            try
            {
                con.Open(); // open connection.
                reader = command.ExecuteReader();

                while (reader.Read())
                {
                    if (reader.IsDBNull(0))
                    {
                        temp = 0;
                    }
                    else
                    {
                        temp = reader.GetInt64(0);
                    }
                }
            }

            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //ErrorLog.WriteLog(Config._SystemName, this.GetType().Name, System.Reflection.MethodBase.GetCurrentMethod().Name, errorQuery + error.ToString());
            }
            finally
            {
                con.Close(); // close connection here.
            }

            return temp;
        }
        public Boolean CheckExist(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;
            try
            {
                con.Open(); // open connection.
                reader = command.ExecuteReader();

                if (reader.HasRows)
                {
                    return true;
                }
                else
                {
                    return false;
                }

            }

            catch
            {
                return false;
            }
            finally
            {
                con.Close(); // close connection here.
            }
        }
        public int SimpleReadInt(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;
            int temp = 0;

            try
            {
                con.Open(); // open connection.
                reader = command.ExecuteReader();

                while (reader.Read())
                {
                    if (reader.IsDBNull(0))
                    {
                        temp = 0;
                    }
                    else
                    {
                        temp = reader.GetInt32(0);
                    }
                }
            }

            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //Your Log System
            }
            finally
            {
                con.Close(); // close connection here.
            }

            return temp;
        }
        public Decimal SimpleReadDecimal(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;
            Decimal temp = 0;
            try
            {
                con.Open(); // open connection.
                reader = command.ExecuteReader();

                while (reader.Read())
                {
                    if (reader.IsDBNull(0))
                    {
                        temp = 0;
                    }
                    else
                    {
                        temp = reader.GetDecimal(0);
                    }
                }
            }

            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //Your Log System
            }
            finally
            {
                con.Close(); // close connection here.
            }

            return temp;
        }
        public int CountRow(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            MySqlDataReader reader;
            int number_of_row = 0;

            try
            {
                con.Open(); // open connection. First

                reader = command.ExecuteReader();
                while (reader.Read())
                {
                    number_of_row++;
                }
            }
            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //Your Log System
            }
            finally
            {
                con.Close(); // close connection here.
            }

            return number_of_row;
        }
        // Running Customized Query
        public string Custom(String query, String database)
        {
            string temp_connection = String.Format("server={0};database={1};userid={2};password={3};", DatabaseConfig._server,
                                     database, DatabaseConfig._username, DatabaseConfig._password);
            String connection = @temp_connection;
            MySqlConnection con = new MySqlConnection(connection);
            MySqlCommand command = new MySqlCommand(query, con);
            string result = "";

            try
            {
                con.Open(); // open connection.
                result = command.ExecuteScalar().ToString();
            }
            catch (Exception error)
            {
                String errorQuery = String.Format("[QUERY: {0} ] ", query);
                //Your Log System
            }
            finally
            {
                con.Close(); // close connection here.
            }

            return result;
        }
    }
}