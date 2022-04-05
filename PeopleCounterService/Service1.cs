using System;
using System.Text;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Threading;
using System.Data.SqlClient;
using Npgsql;
using System.IO;
using System.Timers;
using SharpCifs.Smb;

namespace PeopleCounterService
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }
         
        Thread thread1;
        Thread thread2;
        System.Timers.Timer timer;
        System.Timers.Timer timer2;
        protected override void OnStart(string[] args)
        {
            
            timer = new System.Timers.Timer();
            timer.Interval = 43200000; // 
          //  timer.Interval = 60000; // 60 seconds
            timer.Elapsed += new ElapsedEventHandler(this.OnTimer);
            timer.Start();
            thread1 = new Thread(mythread1);
            thread1.Start();
            WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Start service v1.10");
            WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Start service v1.10");


            timer2 = new System.Timers.Timer();
            timer2.Interval = 43200000; // 
            timer2.Elapsed += new ElapsedEventHandler(this.OnTimer2);
            timer2.Start();
            thread2 = new Thread(mythread2);
            thread2.Start();
            WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Start service2 v1.10");
            WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Start service2 v1.10");


        }
        static void mythread1()
        {
           

            SqlConnection conn = DBUtils.GetDBConnection();
            try
            {
                conn.Open();

                SqlCommand sqlc = new SqlCommand("SELECT [NameServer],BR FROM [PeopleCounter].[dbo].[ListServers]", conn);
                SqlDataReader r = sqlc.ExecuteReader();
                string server = "";
                int br = 0;
                WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + "Успешно прочитан список серверов из таблицы ListServers");
                while (r.Read())
                {
                    try
                    {
                        server = r.GetString(0);
                        if (server.Substring(0, 2) == "10")
                        {



                            br = r.GetInt32(1);
                            WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") +  " Начал обработку магазина: " + r.GetString(0).Trim() + " br: " + br);
                            //select timestamp from events where [id]=(SELECT max(ID) FROM events where br=87)
                            //*подклю к серверу счётчика
                            SqlCommand sqlc3;
                            SqlConnection conn3 = DBUtils.GetDBConnection();
                            conn3.Open();

                            sqlc3 = new SqlCommand("SELECT  convert(datetime,max(timestamp),120) FROM events where br=" + br, conn3);
                            SqlDataReader r3 = sqlc3.ExecuteReader();
                            r3.Read();
                            DateTime lastPassData = DateTime.Now;
                            lastPassData = lastPassData.AddDays(-30);
                            try
                            {
                                lastPassData = r3.GetDateTime(0);

                            }
                            catch
                            {

                            }

                            lastPassData = lastPassData.AddHours(-3);

                            //    lastPassData = lastPassData.Substring(0, 10);

                            string connstring = String.Format("Server={0};Port={1};" +
                            "User Id={2};Password={3};Database={4};CommandTimeout={5}",
                            server, "49998", "postgres",
                            "Axxon2.0.0", "ngp", "900");

                            NpgsqlConnection connP = new NpgsqlConnection(connstring);

                            connP.Open();
                            //  string d = (System.DateTime.Today.AddDays(-5.0)).ToString("yyyy-MM-dd");
                            string d1 = lastPassData.ToString("yyyy-MM-dd HH:mm:ss");

                            //NpgsqlCommand com = new NpgsqlCommand("SELECT *FROM public.t_json_event where type=1 and timestamp>'" + d + "' and(object_id like '%Detector.1%' or object_id like '%Detector.2%' or object_id like '%Detector.3%' or object_id like '%Detector.4%' or object_id like '%Detector.5%' or object_id like '%Detector.6%' or object_id like '%Detector.7%'or object_id like '%Detector.8%') order by object_id", connP);
                            NpgsqlCommand com = new NpgsqlCommand("SELECT *FROM public.t_json_event where type=1 and timestamp>'" + d1 + "' and(object_id like '%Detector.1%' or object_id like '%Detector.2%' or object_id like '%Detector.3%' or object_id like '%Detector.4%' or object_id like '%Detector.5%' or object_id like '%Detector.6%' or object_id like '%Detector.7%'or object_id like '%Detector.8%') order by object_id", connP);




                            NpgsqlDataReader reader;
                            reader = com.ExecuteReader();
                            WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " server: " + server + " Br:" + br + " Server connected ");
                            int x = 0;
                            while (reader.Read())

                            {
                                SqlConnection conn2 = DBUtils.GetDBConnection();
                                string id = "";
                                string time = "";
                                string object_id = "";
                                string values = "";


                                try
                                {

                                    id = reader[0].ToString();
                                    time = reader.GetDateTime(2).AddHours(3).ToString().Trim();
                                    object_id = reader.GetString(3);
                                    values = reader.GetString(4);



                                    int pos = time.IndexOf(" ");
                                    time = time.Substring(3, 3) + time.Substring(0, 3) + time.Substring(6, 5) + time.Substring(pos, time.Length - pos);

                                    int cameraid = 99;
                                    if (object_id.IndexOf("AVDetector.1") > -1)
                                    {
                                        cameraid = 1;
                                    };
                                    if (object_id.IndexOf("AVDetector.2") > -1)
                                    {
                                        cameraid = 2;
                                    };
                                    if (object_id.IndexOf("AVDetector.3") > -1)
                                    {
                                        cameraid = 3;
                                    };
                                    if (object_id.IndexOf("AVDetector.4") > -1)
                                    {
                                        cameraid = 4;
                                    };
                                    if (object_id.IndexOf("AVDetector.5") > -1)
                                    {
                                        cameraid = 5;
                                    };
                                    if (object_id.IndexOf("AVDetector.6") > -1)
                                    {
                                        cameraid = 6;
                                    };
                                    if (object_id.IndexOf("AVDetector.7") > -1)
                                    {
                                        cameraid = 7;
                                    };
                                    if (object_id.IndexOf("AVDetector.8") > -1)
                                    {
                                        cameraid = 8;
                                    };


                                    conn2.Open();
                                    SqlCommand sqlc2;
                                    if (values.ToLower().IndexOf("peoplein") > -1)
                                    {
                                        sqlc2 = new SqlCommand("if not exists(select uid from [PeopleCounter].[dbo].[Events] where uid='" + id + "') INSERT INTO [PeopleCounter].[dbo].[Events]([uid], [timestamp], [object_id], [any-values], [BR],Type,CameraID) VALUES( '" + id + "', '" + time + "', '" + object_id + "', '" + values + "', " + br + ",1," + cameraid + ")", conn2);
                                    }
                                    else
                                    {
                                        sqlc2 = new SqlCommand("if not exists(select uid from [PeopleCounter].[dbo].[Events] where uid='" + id + "') INSERT INTO [PeopleCounter].[dbo].[Events]([uid], [timestamp], [object_id], [any-values], [BR],Type,CameraID) VALUES( '" + id + "', '" + time + "', '" + object_id + "', '" + values + "', " + br + ",0," + cameraid + ")", conn2);
                                    };
                                    sqlc2.ExecuteNonQuery();
                                    //  if (Convert.ToDouble(Convert.ToInt32(x/20))== x / 20.0)
                                    //     WriteLog(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " server: " + server + " Br:" + br + " Insert People  "+x.ToString());
                                    conn2.Close();
                                    Thread.Sleep(300);
                                    x++;
                                }



                                catch (Exception e)
                                {
                                    conn2.Close();
                                    WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " server: " + server + " Br:" + br + " not insert in MSQLBASE on ivn-srv-09: " + e.Message);
                                }



                            }
                            connP.Close();

                            WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " server: " + server + " Br:" + br + " successfully connection in ivn-srv-09 and count insert records: " + x);
                            WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Закончил обработку магазина: " + r.GetString(0).Trim() + " br: " + br);


                        }
                    }
                    catch (Exception e)
                    {
                        WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " server: " + server + " Br:" + br + " not connection in PeopleCounter Shop Base: " + e.Message);
                    }
                }
                WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Обработка завершена");

            }
            catch (Exception e)
            {
                WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " not connection in MSSQL Base on ivn-srv-09 : " + e.Message);
            }





        }
        static void mythread2()
        {
           
            SqlConnection conn = DBUtils.GetDBConnection();
            conn.Open();
            try
            {
                SqlCommand sqlc = new SqlCommand("SELECT [NameServer],BR FROM [PeopleCounter].[dbo].[ListServers]", conn);
            
            SqlDataReader r = sqlc.ExecuteReader();
            string server = "";
            int br = 0;
            WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + "Успешно прочитан список серверов из таблицы ListServers");
            while (r.Read())
            {
                    
                    server = r.GetString(0).Trim();
                    br = r.GetInt32(1);


                    if (server.Substring(0, 2) != "10")
                    {                      
                        try
                        {
                            int EventEnterAll = 0;
                            int EventExitAll = 0;

                            server = server.Substring(2, server.Length - 2);
                            //WriteLog(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Zer Good " + server);
                            //  string sss = "smb://count:count@" + server + "/0101_2019-11-11";
                            string sss = "smb://count:count@" + server + "/";
                            //WriteLog(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + sss);
                            var Folder = new SmbFile(sss);



                            
                            var shares = Folder.ListFiles();
                            WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + "Folder connected " + sss+" Начал обработку магазина: "+ r.GetString(0).Trim()+" br: "+br);
                            foreach (var share in shares)
                            {
                                string FileName = share.GetName();

                                if (FileName.Substring(0, 1) == "0")
                                {
                                     EventEnterAll = 0;
                                     EventExitAll = 0;

                                    string s = "smb://count:count@" + server + "/" + FileName;

                                    var file = new SmbFile(s);

                                    //Get readable stream.
                                    var readStream = file.GetInputStream();

                                    //Create reading buffer.
                                    //    var memStream = new MemoryStream();

                                    //Get bytes.
                                    //  ((Stream)readStream).CopyTo(memStream);
                                  //  WriteLog("++++++++++++++++++++++++++++++++++++++++++++++++++++"+FileName);
                                    string DeviceNumber=FileName.Substring(2, 2);
                                    string Data = FileName.Substring(5, FileName.Length-5);
                                    var sr = new StreamReader((Stream)readStream, Encoding.UTF8);
                                    string line = String.Empty;
                                    
                                    while ((line = sr.ReadLine()) != null)
                                    {
                                        try
                                        {
                                            
                                            string Time = line.Substring(0, 5);
                                            string EventEnter = line.Substring(6, 5);
                                            string EventExit = line.Substring(12, 5);

                                            EventEnterAll = EventEnterAll+Convert.ToInt32(EventEnter);
                                            EventExitAll = EventExitAll + Convert.ToInt32(EventExit);
                                            

                                            DateTime dt = Convert.ToDateTime(Data + " " + Time);
                                            DateTime lastPassData = DateTime.Now;
                                            lastPassData = lastPassData.AddDays(-12);
                                            //WriteLog(dt.ToString()+" "+ lastPassData.ToString());
                                            if (dt > lastPassData)
                                            {
                                                SqlConnection conn2 = DBUtils.GetDBConnection();
                                                try
                                                {
                                                    
                                                    conn2.Open();
                                                    SqlCommand sqlc2 = new SqlCommand("if not exists(select uid from [PeopleCounter].[dbo].[Events] where[timestamp] = '" + Data + " " + Time + "' and Type=1 and [BR]="+br+") begin declare @d int set @d = 0 while @d < " + Convert.ToInt32(EventEnter) + " begin INSERT INTO[PeopleCounter].[dbo].[Events]([uid], [timestamp], [object_id], [any-values], [BR],Type,CameraID)	 VALUES( '---', '" + Data + " " + Time + "', '---', '---', " + br + ",1," + DeviceNumber + ")   set @d = @d + 1 end end             if not exists(select uid from [PeopleCounter].[dbo].[Events] where[timestamp] = '" + Data + " " + Time + "' and Type=0 and [BR]=" + br + ") begin declare @d1 int set @d1 = 0 while @d1 < " + Convert.ToInt32(EventExit) + " begin INSERT INTO[PeopleCounter].[dbo].[Events]([uid], [timestamp], [object_id], [any-values], [BR],Type,CameraID)	 VALUES( '---', '" + Data + " " + Time + "', '---', '---', " + br + ",0," + DeviceNumber + ")   set @d1 = @d1 + 1 end end", conn2);
                                                    sqlc2.ExecuteNonQuery();
                                                }
                                                catch (Exception x)
                                                {
                                                    WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " error insert from ftp " + sss + " " + FileName + " " + x.Message);

                                                }
                                                finally {
                                                    conn2.Close();

                                                }


                                                //WriteLog(FileName + "   Dev " + DeviceNumber + " Data " + Data + " Time " + Time + "  Enter " + EventEnter + " Exit " + EventExit + "    " + line + "  " + dt);
                                            }
                                        }
                                        catch (Exception x) {
                                            //может зря раскоментировал
                                            //WriteLog(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " error read file from ftp "+sss+" "+ FileName+" " + x.Message);

                                        }

                                    }

                                    WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Share Name = " + share.GetName()+ " EventEnterAll: "+ EventEnterAll+ " EventExitAll:"+ EventExitAll);
                                }
                               
                            }





                          
                            
                            //Console.WriteLine(Encoding.UTF8.GetString(memStream.ToArray()));
                            //Dispose readable stream.
                            //   readStream.Dispose();
                        }
                        catch (Exception e)
                        {
                            WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " error read file from ftp" + e.Message);


                        }
                        WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Закончил обработку магазина: " + r.GetString(0).Trim() + " br: " + br);



                        //Console.WriteLine(Encoding.UTF8.GetString(memStream.ToArray()));

                        //    string readText = File.ReadAllText(server);

                        //}

                    }               
            }

            WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Обработка завершена");
            }
            catch (Exception e)
            {
                WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + "thread2: not connection in MSSQL Base on ivn-srv-09 : " + e.Message);


            }

        }

        protected override void OnStop()
        {
            timer.Stop();
            timer2.Stop();

            try
            {                
                thread1.Abort();
                thread2.Abort();
            }
            catch (Exception e)
            {
                WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Threads stop error : " + e.Message);
                WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Threads stop error : " + e.Message);

            }
            WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Service1 stoped ");
            WriteLog1(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Service2 stoped ");

            WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Service1 stoped ");
            WriteLog2(System.DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + " Service2 stoped ");
        }

        public void OnTimer(object sender, ElapsedEventArgs args)
        {
            thread1 = new Thread(mythread1);
            thread1.Start();
        }
        public void OnTimer2(object sender, ElapsedEventArgs args)
        {
            
            thread2 = new Thread(mythread2);
            thread2.Start();
        }

        class DBSQLServerUtils
        {

            public static SqlConnection
                     GetDBConnection(string datasource, string database, string username, string password)
            {
                //
                // Data Source=TRAN-VMWARE\SQLEXPRESS;Initial Catalog=simplehr;Persist Security Info=True;User ID=sa;Password=12345
                //
                string connString = @"Data Source=" + datasource + ";Initial Catalog="
                            + database + ";Persist Security Info=True;User ID=" + username + ";Password=" + password;

                SqlConnection conn = new SqlConnection(connString);

                return conn;
            }


        }
        class DBUtils
        {
            public static SqlConnection GetDBConnection()
            {
                string datasource = @"10.57.0.11";

                string database = "PeopleCounter";
                string username = "sa";
                string password = "********";

                return DBSQLServerUtils.GetDBConnection(datasource, database, username, password);
            }
        }
        public static void WriteLog1(string strLog)
        {
            try
            {
                DirectoryInfo logDirInfo = null;
                FileInfo logFileInfo;


                string logFilePath = "C:\\Logs\\";
                logFilePath = logFilePath + "PeopleCounter1.log";
                logFileInfo = new FileInfo(logFilePath);
                logDirInfo = new DirectoryInfo(logFileInfo.DirectoryName);
                if (!logDirInfo.Exists) logDirInfo.Create();

                using (var file = new StreamWriter(logFilePath, true))
                {
                    file.WriteLine(strLog);
                    file.Close();
                }
            }
            catch { };
        }
        public static void WriteLog2(string strLog)
        {
            try
            {
                DirectoryInfo logDirInfo = null;
                FileInfo logFileInfo;


                string logFilePath = "C:\\Logs\\";
                logFilePath = logFilePath + "PeopleCounter2.log";
                logFileInfo = new FileInfo(logFilePath);
                logDirInfo = new DirectoryInfo(logFileInfo.DirectoryName);
                if (!logDirInfo.Exists) logDirInfo.Create();

                using (var file = new StreamWriter(logFilePath, true))
                {
                    file.WriteLine(strLog);
                    file.Close();
                }
            }
            catch { };
        }

    }
}
