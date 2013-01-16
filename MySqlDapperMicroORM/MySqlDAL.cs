using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using Dapper; 

namespace MySqlDapperMicroORM
{
    public class MySqlDAL
    {

        private string ConnectionString { get; set; }

        public MySqlDAL(string connectionString)
        {
            this.ConnectionString = connectionString; 

        }

        private void IsConnectionSet() {
            if(string.IsNullOrEmpty(this.ConnectionString)) {
                throw new Exception("Connectionstring is null or empty string!"); 
            }
        }

        private MySqlConnection Connect() 
        {
            IsConnectionSet(); 
            MySqlConnection con = new MySqlConnection(this.ConnectionString);
            return con; 
        }


        public IEnumerable<dynamic> Select(string sql)
        {
            IEnumerable<dynamic> r = null;
            using (var con = Connect())
            {
                con.Open();
                r = con.Query(sql);
                con.Close();
                con.Dispose();
            }
            return r; 
        }

        public IEnumerable<dynamic> Select(string sql, DynamicParameters param)
        {
            IEnumerable<dynamic> r = null;
            using (var con = Connect())
            {
                con.Open();
                r = con.Query(sql, param); 
                con.Close();
                con.Dispose();
            }
            return r;
        }
        

        public void Execute(string sql)
        {
            using (var con = Connect())
            {
                con.Open();
                MySqlTransaction trans = con.BeginTransaction();
                con.Execute(sql, null, trans);
                trans.Commit();
                con.Close();
                con.Dispose();
            }
        }

        public void Execute(string sql, DynamicParameters param)
        {
            using (var con = Connect())
            {
                con.Open();
                MySqlTransaction trans = con.BeginTransaction();
                con.Execute(sql, param, trans);
                trans.Commit();
                con.Close();
                con.Dispose();
            }
        }


    }
}
