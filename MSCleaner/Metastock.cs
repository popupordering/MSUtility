using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Xml.Serialization;
using System.Data;
using System.Data.Odbc;
using System.ComponentModel;

namespace MSCleaner
{
    public enum PriceErrorType
    {
        OK,
        NonTradingDay,
        HighIsNotHighest,
        LowIsNotLowest,
        MissingDay,
        OpenValueOutOfTolerance,
        CloseValueOutOfTolerance,
        HighValueOutOfTolerance,
        LowValueOutOfTolerance
    }

    public class Utilities
    {
        public static DataTable LoadCSV(string sourceFile, string columns, string whereClause)
        {
            string strConnString = String.Format("Driver={{Microsoft Text Driver (*.txt; *.csv)}};Dbq={0};Extensions=asc,csv,tab,txt;Persist Security Info=False", Path.GetDirectoryName(sourceFile));
            string sql_select = string.Format("SELECT {0} FROM {1}", columns, Path.GetFileName(sourceFile));
            try
            {
                OdbcConnection conn;
                conn = new OdbcConnection(strConnString.Trim());
                OdbcCommand cmd = new OdbcCommand(sql_select, conn);
                OdbcDataAdapter da = new OdbcDataAdapter(cmd);
                DataTable output = new DataTable();
                da.Fill(output);
                return output;
            }
            catch (Exception ex)
            {
                throw new Exception(String.Format("Error {0} while trying to load file {1} using the columns {2}. Ensure that the file exists and has the columns required", ex.Message, sourceFile, columns));
            }

        }
        /// <summary>
        /// Creates a folder on disk, or empties an existing folder
        /// </summary>
        /// <param name="destFolder"></param>
        internal static void CreateEmptyFolder(string destFolder)
        {
            if (!Directory.Exists(destFolder))
            {
                Directory.CreateDirectory(destFolder);
            }
            else
            {
                foreach (string f in Directory.GetFiles(destFolder))
                {
                    switch (f.ToUpper() )
                    {
                        case "EMASTER":
                        case "MASTER":
                            File.Delete(f);
                            break;
                        default:
                            if (f.StartsWith("F") && f.EndsWith("DAT"))
                            {
                                File.Delete(f);
                            }
                            break;
                    }
                }
            }
        }

    }


    public class Metastock
    {


        public BindingList<TickerList> Lists;
        public bool SymbolMismatch = false;
        public const string AuditFolder = "Audit";
        public const string AuditHeadings = "Date,ChangeType,Open,High,Low,Close,Volume,Source";
        internal static List<DateTime> nonTradingDays;


        public Metastock(string tickersFolder)
        {
            try
            {
                Lists = new BindingList<TickerList>();
                Lists.Add(new TickerList(tickersFolder, ""));

            }
            catch (Exception ex)
            {
                throw new Exception(String.Format("There was an error reading in the Metastock files provided: {0}", ex.Message));
            }

        }



        public Metastock(string tickersFolder, bool createNew)
        {
            try
            {
                
                Lists = new BindingList<TickerList>();
                Lists.Add(new TickerList(tickersFolder, ""));

            }
            catch (Exception ex)
            {
                throw new Exception(String.Format("There was an error reading the non-trading days file: {0}. We'll proceed without a list of the non-trading days", ex.Message));
            }

        }


        public Metastock(string tickersFolder, string nonTradingDaysFileName)
        {
            try
            {
                Lists = new BindingList<TickerList>();
                Lists.Add(new TickerList(tickersFolder, ""));

           
                Program.Log("Loading Non-trading days file from {0}", nonTradingDaysFileName);
                nonTradingDays = new List<DateTime>();

                DataTable nonTrading = Utilities.LoadCSV(nonTradingDaysFileName, "*", "");

                nonTradingDays = (from t in nonTrading.AsEnumerable()
                                  select t.Field<DateTime>(2)).ToList<DateTime>();
            }
            catch (Exception ex)
            {
                throw new Exception(String.Format("There was an error reading the non-trading days file: {0}. We'll proceed without a list of the non-trading days", ex.Message));
            }


        }

        static Metastock()
        {
            
        }

        public Metastock()
        {
            Initialise();
        }

        private void Initialise()
        {
            Lists = new BindingList<TickerList>();
            Lists.ListChanged += new ListChangedEventHandler(Lists_ListChanged);
            




        }

        void Lists_ListChanged(object sender, ListChangedEventArgs e)
        {
            if (e.ListChangedType == ListChangedType.ItemAdded)
            {
                int maxID = maxID = Lists.Max(s => s.ListID);
                Lists[e.NewIndex].ListID = maxID + 1;
            }
        }

        void Lists_AddingNew(object sender, AddingNewEventArgs e)
        {

        }






        private void Save()
        {
            System.Xml.Serialization.XmlSerializer x = new System.Xml.Serialization.XmlSerializer(typeof(Metastock));
            StreamWriter sw = new StreamWriter("AppData.xml");
            x.Serialize(sw, this);
        }





    

        /// <summary>
        /// Write all lists to the MS ticker files
        /// </summary>
        public void Commit()
        {
            foreach (TickerList l in this.Lists.Where(l => l.hasChanged == true))
            {

                l.WriteList();
            }
            this.Save();
        }
    }


    public enum ChangeOperation
    {
        Add,
        Update
    }

    public class TickerList
    {
        public int ListID;
        public string ListName;
        public string MSFolder;
        public string ListFilePath;
        public bool isNewList;
        public bool addExtraDays;
        public bool useCSVUpdates;
        public bool useCorrections;
        public DateTime? startDate;
        public DateTime? endDate;
        public bool hasChanged = false;
        internal bool hasMismatch = false;

        public BindingList<Ticker> Tickers;
        #region Constructors
        public TickerList()
        {
            Initialise();
        }

        public TickerList(DataRow row)
        {
            Initialise();
            this.MSFolder = row.Field<string>("OutputPath");
            this.ListName = row.Field<string>("ListName");
            this.ListFilePath = row.Field<string>("TickerListFile");
            this.startDate = row.Field<DateTime?>("StartDate");
            this.endDate = row.Field<DateTime?>("EndDate");
            Boolean.TryParse(row.Field<string>("UseCSVUpdates"), out this.useCSVUpdates);
            Boolean.TryParse(row.Field<string>("AddExtraDay"), out this.addExtraDays);
            Boolean.TryParse(row.Field<string>("UseCorrections"), out this.useCorrections);
        }


        public TickerList(string listName, string msFolder, string listFilePath, bool useCSVUpdates, bool useCorr, bool addDays, DateTime? fromDate, DateTime? toDate)
        {
            Initialise();
            this.ListName = listName;
            this.MSFolder = msFolder;
            this.ListFilePath = listFilePath;
            this.useCorrections = useCorr;
            this.addExtraDays = addDays;
            this.useCSVUpdates = useCSVUpdates;
            this.startDate = fromDate;
            this.endDate = toDate;
        }

        #endregion
        public List<string> Symbols
        {
            get
            {
                List<string> s = new List<string>();
                foreach (Ticker t in this.Tickers)
                {
                    s.Add(t.TickerSymbol);
                }
                return s;
            }
        }

        public void WriteCSV(string csvPath)
        {
            StreamWriter w = new StreamWriter(csvPath);
            int numberLines = 0;
            w.WriteLine("Symbol,Date,Open,High,Low,Close,Volume");
            foreach (Ticker t in this.Tickers)
            {
                foreach (Quote q in t.Prices)
                {
                    w.WriteLine(q.ToCSV(true));
                    numberLines++;
                }
            }
            w.Close();
            Program.Log("{0} lines written to file", numberLines);

        }

        public override string ToString()
        {
            return ListName;
        }

        private void Initialise()
        {
            this.Tickers = new BindingList<Ticker>();
            this.Tickers.ListChanged += new ListChangedEventHandler(Tickers_ListChanged);
        }


        void Tickers_ListChanged(object sender, ListChangedEventArgs e)
        {
            Tickers[e.NewIndex].Parent = this;
            this.hasChanged = true;
        }





        public void LoadTickers_Master()
        {
            string masterFilePath = Path.Combine(this.MSFolder, "MASTER");
            if (!File.Exists(masterFilePath))
                this.isNewList = true;

            Trace.WriteLine(String.Format("Loading MS data for list {0} from folder {1}", this.ListName, this.MSFolder));

            if (this.isNewList)
            {
                return;
            }
            BinaryReader br = null;
            Trace.Indent();
            try
            {

                br = new BinaryReader(new FileStream(masterFilePath, FileMode.Open));
                int num_files = BitConverter.ToInt16(br.ReadBytes(2), 0);
                int file_num = BitConverter.ToInt16(br.ReadBytes(2), 0);
                br.ReadBytes(49); //consume junk header data
                StringBuilder sb = new StringBuilder();
                for (int n = 1; n <= file_num; n++)
                {
                    Program.Debug("Loading data sequence {0}", n);

                    byte fileNum = br.ReadByte();                       //u_char file_num;	    /* file #, i.e., F# */
                    if (fileNum > n)
                    {
                        Program.Log("Error parsing list at {1}: Missing file found at {0}. Skipping this file", n, masterFilePath);
                        n++;
                    }
                    Program.Debug("....file number {0}", fileNum);
                    br.ReadBytes(2);                            //char file_type[2];	    /* CT file type = 0'e' (5 or 7 flds) */
                    char rec_len = br.ReadChar();               //  u_char rec_len;	    /* record length in bytes (4 x num_fields)*/
                    char num_fields = br.ReadChar();            //   u_char num_fields;	    /* number of 4-byte fields in each record*/
                    br.ReadBytes(2);                            //   char fill2[2];
                    string stockName = Conversion.GetCleanString(br.ReadBytes(16)); //char issue_name[16];    /* stock name */
                    br.ReadByte();                              //char reserved2;
                    br.ReadByte();                              // char CT_v2_8_flag;	    /* if CT ver. 2.8, 'Y'; o.w., anything else */

                    DateTime first_date = Conversion.DecodeDateFromMBF(Conversion.ConvertMbf4ToFloat(br.ReadBytes(4), 0, 3));
                    DateTime last_date = Conversion.DecodeDateFromMBF(Conversion.ConvertMbf4ToFloat(br.ReadBytes(4), 0, 3));
                    char time_frame = br.ReadChar();            //  char time_frame;	    /* data format: 'I'(IDA)/'W'/'Q'/'D'/'M'/'Y' */
                    br.ReadBytes(2);                              //* <b>intraday</b> (IDA) time base */
                    string symbol = Conversion.GetCleanString(br.ReadBytes(14)); //char symbol[14];	    /* stock symbol */
                    br.ReadByte();                              // char reserved3;	    /* <b>MetaStock</b> reserved2: must be a space */
                    char flag = br.ReadChar();		            //* ' ' or '*' for autorun */
                    br.ReadByte();                              //  char reserved4;

                    Ticker ticker = new Ticker();
                    ticker.TickerSymbol = symbol;
                    ticker.TickerName = stockName;
                    ticker.TimeFrame = time_frame.ToString();
                    ticker.fileFirstDate = first_date;
                    ticker.fileLastDate = last_date;
                    ticker.FileNumber = fileNum;
                    //     Console.WriteLine("Processing file " + fileNum);
                    ticker.LoadQuotes(this.MSFolder, n);
                    // LoadAuditData(ticker, msFolder);
                    Tickers.Add(ticker);
                }



            }
            catch (Exception ex)
            {
                throw new Exception(String.Format("There was an error reading the MS data file from the folder {0}: {1}", this.MSFolder, ex.Message));
            }
            finally
            {
                br.Close();
                // this.Lists.AddListsRow(row);
            }
        }



        /// <summary>
        /// Loads the MS data from the ticker folder
        /// </summary>
        public void LoadTickers()
        {
            string emasterFilePath = Path.Combine(this.MSFolder, "EMASTER");

            if (!File.Exists(emasterFilePath))
            {
                LoadTickers_Master();
                return;
            }
 
            Trace.WriteLine(String.Format("Loading MS data for list {0} from folder {1}", this.ListName, this.MSFolder));


            if (this.isNewList)
            {
                return;
            }
            BinaryReader br = null;
            Trace.Indent();
            try
            {
                br = new BinaryReader(new FileStream(emasterFilePath, FileMode.Open));
                int num_files = BitConverter.ToInt16(br.ReadBytes(2), 0);
                int file_num = BitConverter.ToInt16(br.ReadBytes(2), 0);
                br.ReadBytes(188); //consume junk header data
                StringBuilder sb = new StringBuilder();
                for (int n = 1; n <= file_num; n++)
                {
                    Program.Debug("Loading file number {0}", n);
                    br.ReadBytes(2); //                  char asc30[2];	    /* &quot;30&quot; */
                    byte fileNum = br.ReadByte();  //    u_char file_num;	    /* file number F# */
                    br.ReadBytes(3); //    char fill1[3];
                    char num_fields = br.ReadChar(); //u_char num_fields;	    /* number of 4-byte data fields */
                    br.ReadBytes(2); //   char fill2[2];
                    char flag = br.ReadChar();		    /* ' ' or '*' for autorun */
                    br.ReadByte(); //char fill3;
                    string symbol = Conversion.GetCleanString(br.ReadBytes(14)); //char symbol[14];	    /* stock symbol */
                    br.ReadBytes(7); //char fill4[7];
                    string stockName = Conversion.GetCleanString(br.ReadBytes(16)); //char issue_name[16];    /* stock name */
                    br.ReadBytes(12); //char fill5[12];
                    char time_frame = br.ReadChar(); //char time_frame;	    /* data format: 'D'/'W'/'M'/ etc. */
                    br.ReadBytes(3); //char fill6[3];
                    float first_date = BitConverter.ToSingle(br.ReadBytes(4), 0); // float first_date;	    /* yymmdd */
                    br.ReadBytes(4); //char fill7[4];
                    float last_date = BitConverter.ToSingle(br.ReadBytes(4), 0); // float last_date;
                    br.ReadBytes(116); //char fill8[116];

                    Ticker ticker = new Ticker();
                    ticker.TickerSymbol = symbol;
                    ticker.TickerName = stockName;
                    ticker.TimeFrame = time_frame.ToString();
                    ticker.fileFirstDate = Conversion.DecodeDateFromMBF(first_date);
                    ticker.fileLastDate = Conversion.DecodeDateFromMBF(last_date);
                    ticker.FileNumber = fileNum;
                    //     Console.WriteLine("Processing file " + fileNum);
                    ticker.LoadQuotes(this.MSFolder, n);
                    // LoadAuditData(ticker, msFolder);
                    Tickers.Add(ticker);
                }
                Trace.Unindent();
            }
            catch (Exception ex)
            {
                throw new Exception(String.Format("There was an error reading the MS data file from the folder {0}: {1}", this.MSFolder, ex.Message));
            }
            finally
            {
                br.Close();
                // this.Lists.AddListsRow(row);
            }

        }


        public TickerList(string msFolder, string listName)
        {
            Initialise();
            this.MSFolder = msFolder;

         
            this.ListName = listName;
            LoadTickers();

        }

       

        internal void LoadAuditData(Ticker ticker, string msFolder)
        {
            try
            {

                string auditPath = Path.Combine(Path.Combine(msFolder, Metastock.AuditFolder), ticker.Symbol + ".csv");
                if (File.Exists(auditPath))
                {
                    Trace.WriteLine(String.Format("Loading audit data for ticker {0} from file {1}", ticker.Symbol, auditPath));
                    DataTable auditData = Utilities.LoadCSV(auditPath, Metastock.AuditHeadings, "");



                    var auditRecs = from q in ticker.Prices.AsEnumerable()
                                    join a in auditData.AsEnumerable() on q.TickerDate equals a.Field<DateTime>("date")

                                    select new { q, a };

                    foreach (var quoteMatch in auditRecs)
                    {
                        Audit ai = new Audit();
                        ai.High = float.Parse(quoteMatch.a.Field<string>("high"));
                        ai.Low = float.Parse(quoteMatch.a.Field<string>("low"));
                        ai.Open = float.Parse(quoteMatch.a.Field<string>("open"));
                        ai.Close = float.Parse(quoteMatch.a.Field<string>("close"));
                        ai.Volume = float.Parse(quoteMatch.a.Field<string>("volume"));
                        ai.Source = quoteMatch.a.Field<string>("source");
                        ai.changeType = (ChangeOperation)Enum.Parse(typeof(ChangeOperation), quoteMatch.a.Field<string>("ChangeType"));



                    }
                }
                else
                {
                    Trace.WriteLine(String.Format("Unable to load audit data for ticker {0} from file {1}: File doesn't exist", ticker.Symbol, auditPath));
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine(String.Format("Unable to read from audit file: {0}", ex.Message));
            }

        }

        public Ticker AddTicker(string symbol, string symbolName, short fileNumber)
        {
            Ticker t = Tickers.Where(s => s.TickerSymbol == symbol).FirstOrDefault();

            if (t != null)
            {
                return t;
            }

            t = new Ticker();
            t.TickerName = symbolName;
            t.TickerSymbol = symbol;
            t.FileNumber = fileNumber;
            t.fileFirstDate = DateTime.MinValue;
            t.fileLastDate = DateTime.MaxValue;
            t.TimeFrame = "D";
            t.Parent = this;

            this.Tickers.Add(t);
            return t;

        }

        public void RemoveTicker(string symbol)
        {
            foreach (Ticker ts in Tickers.Where(t => t.TickerSymbol == symbol))
            {
                Tickers.Remove(ts);
            }

        }

        public void RemoveTickers(List<string> symbols)
        {
            //  foreach (string ts in Tickers.Where(t => symbols.Contains(t.TickerSymbol)).Select(t=>t.TickerSymbol))
            // {
            //     Tickers
            // }

        }


        public bool Validate(bool reportOnly,  ref int numberExtra,string exportCSV = "")
        {
            int extraDays = RemoveNonTradingDays(reportOnly, exportCSV);
            numberExtra += extraDays;
            if (extraDays > 0)
            {
                return true;
            }
            return false;



        }

        private void RemoveDay(DateTime d)
        {
            foreach (Ticker t in this.Tickers)
            {
                foreach (Quote q in t.Prices.Where(q => q.TickerDate == d))
                {
                    t.Prices.Remove(q);
                }
            }
        }


        public void GetMissingDays(DateTime fromDate, DateTime toDate, StreamWriter output, ref int numberMissing)
        {
            foreach (Ticker t in this.Tickers)
            {
                DateTime from = fromDate == DateTime.MinValue ? t.FirstDate : fromDate;
                DateTime to = toDate == DateTime.MaxValue ? t.LastDate : toDate;

                if (from < t.FirstDate)
                {
                    Program.Log("WARNING: Ticker {0} has a start date of {1:yyyyMMdd} and you're looking for missing data from {2:yyyyMMdd}. You're likely to get lots of missing days reported", t.TickerSymbol.Trim(), t.FirstDate, fromDate);
                }

                if (to > t.LastDate)
                {
                    Program.Log("WARNING: Ticker {0} has an end date of {1:yyyyMMdd} and you're looking for missing data to {2:yyyyMMdd}. You're likely to get lots of missing days reported", t.TickerSymbol.Trim(), t.LastDate, toDate);
                }

                while ((from = from.AddDays(1)) <= to)
                {
                    if (from.DayOfWeek != DayOfWeek.Saturday && from.DayOfWeek != DayOfWeek.Sunday && !Metastock.nonTradingDays.Contains(from))
                    {
                        if (t.Prices.Where(p => p.TickerDate == from).Count() == 0)
                        {
                            Program.Debug("Missing day : {0},{1:yyyyMMdd}", t.Symbol.Trim(), from);
                            output.WriteLine("{0},{1:yyyyMMdd}", t.Symbol.Trim(), from);
                            numberMissing++;
                        }
                    }
                }
            }                
        }

        private int RemoveNonTradingDays(bool reportOnly, string fileName = "")
        {
            int numberRemoved = 0;
            StreamWriter sw = null;
            if (reportOnly)
            {
                sw = new StreamWriter(fileName, false);
                sw.WriteLine("Symbol,Date,Open,High,Low,Close.Volume");
            }
            try
            {

                foreach (Ticker t in this.Tickers)
                {
                    List<Quote> datesToRemove = new List<Quote>();
                    foreach (Quote q in t.Prices)
                    {
                        Program.Debug("Checking date {0:dd-MMM-yyyy} in ticker {1}", q.TickerDate, t.Symbol);
                        if (q.TickerDate.DayOfWeek == DayOfWeek.Saturday || q.TickerDate.DayOfWeek == DayOfWeek.Sunday)
                        {
                            Program.Debug("Weekend date {0:dd-MMM-yyyy} found in ticker {1} in list {2}", q.TickerDate, t.TickerSymbol, this.MSFolder);
                            datesToRemove.Add(q);
                            numberRemoved++;
                        }
                        if (Metastock.nonTradingDays.Contains(q.TickerDate))
                        {
                            Program.Debug("Non-trading date {0:dd-MMM-yyyy} found in ticker {1} in list {2}", q.TickerDate, t.TickerSymbol, this.MSFolder);
                            datesToRemove.Add(q);
                            numberRemoved++;
                        }
                    }
                    if (reportOnly)
                    {
                        foreach (Quote q in datesToRemove)
                        {
                            Program.Debug("Extra day :{0},{1:yyyyMMdd},{2},{3},{4},{5},{6}", t.Symbol.Trim(), q.TickerDate, q.Open, q.High, q.Low, q.Close, q.Volume);
                            sw.WriteLine("{0},{1:yyyyMMdd},{2},{3},{4},{5},{6}", t.Symbol.Trim(), q.TickerDate, q.Open, q.High, q.Low, q.Close, q.Volume);
                        }


                    }
                    else
                    {
                        foreach (Quote q in datesToRemove)
                        {
                            t.Prices.Remove(q);
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                throw ex;

            }
            if (reportOnly)
            {
                sw.Close();
            }

            return numberRemoved;
        }



       

     


        internal void WriteList()
        {


            Utilities.CreateEmptyFolder(this.MSFolder);

            WriteMaster();
            WriteEMaster();
            WriteTickers();
            // WriteAudit();
        }

        private void WriteAudit()
        {
            string auditPath = Path.Combine(this.MSFolder, Metastock.AuditFolder);
            if (!Directory.Exists(auditPath)) Directory.CreateDirectory(auditPath);
            foreach (Ticker t in this.Tickers)
            {

                string auditFile = Path.Combine(auditPath, t.Symbol + ".csv");

                if (File.Exists(auditFile))
                {
                    File.Delete(auditFile);
                }
                StreamWriter sw = new StreamWriter(auditFile);
                sw.WriteLine(Metastock.AuditHeadings);
                foreach (Quote q in t.Prices)
                {
                    foreach (Audit a in q.AuditTrail)
                    {
                        sw.WriteLine(String.Format("{0:yyyy-MM-dd},{1},{2},{3},{4},{5},{6},{7}", q.TickerDate, a.changeType.ToString(),
                            a.Open, a.High, a.Low, a.Close, a.Volume, a.Source));
                    }
                }
                sw.Close();
                sw = null;
            }
        }

        private void WriteEMaster()
        {
            string filePath = Path.Combine(MSFolder, "EMASTER");
            if (File.Exists(filePath))
                File.Delete(filePath);

            BinaryWriter bw = new BinaryWriter(new FileStream(filePath, FileMode.CreateNew));

            byte[] buffer = new byte[192];
            Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes(Tickers.Count), 0);
            Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes(Tickers.Count), 2);
            // write header record
            bw.Write(buffer);

            byte fileNum = 1;
            foreach (Ticker row in this.Tickers)
            {
                buffer = new byte[192];
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes("66"), 0);
                Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes(fileNum), 2);
                // fill1 (3)
                Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes((short)7), 6);
                // fill2 (2)
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes("#"), 9);
                // fill3 (1)
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes(row.TickerSymbol), 11);
                // fill4(7)
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes(row.TickerName), 32); //char issue_name[16];    /* stock name */
                // fill5(12)
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes(row.TimeFrame), 60);
                // fill6(3)
                Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes(Conversion.ConvertDateToFloat(row.FirstDate)), 64);//  float first_date;	    /* yymmdd */
                // fill7(4)
                Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes(Conversion.ConvertDateToFloat(row.LastDate)), 72);//    float last_date;
                bw.Write(buffer);
                fileNum++;
            }
            bw.Close();
            bw = null;
        }

        private void WriteMaster()
        {
            Trace.WriteLine("");

            string filePath = Path.Combine(MSFolder, "MASTER");
            if (File.Exists(filePath))
                File.Delete(filePath);

            BinaryWriter bw = new BinaryWriter(new FileStream(filePath, FileMode.CreateNew));

            byte[] buffer = new byte[53];
            Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes(Tickers.Count), 0);
            Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes(Tickers.Count), 2);
            // write header record
            bw.Write(buffer);

            byte fileNum = 1;
            foreach (Ticker row in Tickers)
            {
                buffer = new byte[53];
                Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes(row.FileNumber), 0);//u_char file_num;	    /* file #, i.e., F# */
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes("e"), 1); //char file_type[2];	    /* CT file type = 0'e' (5 or 7 flds) */
                Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes((short)28), 3);  //  u_char rec_len;	    /* record length in bytes (4 x num_fields)*/
                Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes((short)7), 4);   //   u_char num_fields;	    /* number of 4-byte fields in each record*/
                // char reserved1[2];	    /*  in the data file */
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes(row.TickerName), 7); //char issue_name[16];    /* stock name */
                //char reserved2;
                // char CT_v2_8_flag;	    /* if CT ver. 2.8, 'Y'; o.w., anything else */
                Conversion.WriteToBuffer(ref buffer, Conversion.ConvertDateToBytes(row.FirstDate), 25);//  float first_date;	    /* yymmdd */
                Conversion.WriteToBuffer(ref buffer, Conversion.ConvertDateToBytes(row.LastDate), 29);//    float last_date;
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes(row.TimeFrame), 33);  //  char time_frame;	    /* data format: 'I'(IDA)/'W'/'Q'/'D'/'M'/'Y' */
                //      u_short ida_time;	    /* <b>intraday</b> (IDA) time base */
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes(row.TickerSymbol), 36); // char symbol[14];	    /* stock symbol */
                // char reserved3;	    /* <b>MetaStock</b> reserved2: must be a space */
                Conversion.WriteToBuffer(ref buffer, Encoding.ASCII.GetBytes("*"), 51);  /* ' ' or '*' for autorun */
                //  char reserved4;
                bw.Write(buffer);
                fileNum++;
            }
            bw.Close();
            bw = null;
        }

        private void WriteTickers()
        {
            foreach (Ticker row in Tickers)
            {
                string filePath = Path.Combine(MSFolder, String.Format("F{0}.DAT", row.FileNumber));
                row.WriteToFile(filePath);
            }
        }

     

    }

    public class Ticker
    {
        public string TickerSymbol;
        internal TickerList Parent;
        public string TickerName;
        public string TimeFrame = "D";
        public short FileNumber;

        public DateTime fileFirstDate;
        public DateTime fileLastDate;


        #region Constructors

        public Ticker(string tickerSymbol, short fileNum)
        {
            Initialise();
            this.TickerSymbol = tickerSymbol;
            this.TickerName = tickerSymbol;
            this.FileNumber = fileNum;
        }
        public Ticker()
        {
            Initialise();
        }

        #endregion

        #region Methods
        private void Initialise()
        {
            this.Prices = new BindingList<Quote>();
            this.Prices.ListChanged += new ListChangedEventHandler(Prices_ListChanged);
        }

        internal void ClearPrices()
        {
            Initialise();
        }

        void Prices_ListChanged(object sender, ListChangedEventArgs e)
        {
            this.Prices[e.NewIndex].Parent = this;
        }

        public override string ToString()
        {
            if (TickerName == string.Empty)
            {
                return TickerSymbol;
            }
            else
            {
                return String.Format("{0} ({1})", TickerSymbol, TickerName);
            }
        }
        #endregion

        #region Properties

        public string Symbol
        {
            get
            {
                return TickerSymbol;
            }
        }
        public string Name
        {
            get
            {
                return TickerName;
            }
        }

        public DateTime FirstDate
        {
            get
            {
                if (Prices.Count > 0)
                {
                    return Prices.Min(p => p.TickerDate);
                }
                else
                {
                    return DateTime.MinValue;
                }
            }
        }
        public DateTime LastDate
        {
            get
            {
                if (Prices.Count > 0)
                {
                    return Prices.Max(p => p.TickerDate);
                }
                else
                {
                    return DateTime.MinValue;
                }
            }
        }
        #endregion


        /// <summary>
        /// Add the price entry after performing basic validation.
        /// </summary>
        /// <param name="q"></param>
        public void AddQuote(Quote q)
        {
            if (q.ValidationResult != PriceErrorType.NonTradingDay)
            {
              
                    this.Prices.Add(q);
            }


        }
        public BindingList<Quote> Prices;

   
        private static float GetDifference(float a, float b)
        {
            if (a > b)
            {
                return (a / b) - 1;
            }
            else
            {
                return (b / a) - 1;
            }
        }

        public bool AddExtraDay()
        {
            try
            {
                Quote lastDay = this.Prices.Where(p => p.TickerDate == this.LastDate).FirstOrDefault();
                if (lastDay == null)
                {
                    Program.Log("Ticker {0} has no data so cannot add extra day", this.TickerSymbol);
                    return false;
                }
                Quote newDay = new Quote();
                newDay.Parent = this;
                newDay.TickerDate = GetNextTradingDay(lastDay.TickerDate);
                newDay.High = lastDay.High;
                newDay.Low = lastDay.Low;
                newDay.Open = lastDay.Open;
                newDay.Close = lastDay.Close;
                newDay.Volume = lastDay.Volume;

                Program.Log("Adding extra day {0} to ticker {1}", newDay.TickerDate, this.TickerName);
                this.AddQuote(newDay);
                return true;
            }
            catch (Exception ex)
            {
                Program.Log("There was an error adding the extra day after {0} to ticker {1}: {2}", this.LastDate, this.TickerSymbol, ex.Message);
                return false;
            }

        }


        public DateTime GetNextTradingDay(DateTime basisDate)
        {
            DateTime nextDay = basisDate;
            do
            {
                nextDay = nextDay.AddDays(1);
            } while (!IsTradingDay(nextDay));
            return nextDay;

        }


        public bool IsTradingDay(DateTime d)
        {
            if (d.DayOfWeek == DayOfWeek.Saturday || d.DayOfWeek == DayOfWeek.Sunday)
            {
                return false;
            }
            if (Metastock.nonTradingDays == null)
            {
                return true;
            }
            else
            {
                return !Metastock.nonTradingDays.Contains(d);
            }
        }

        public Quote AddQuote(DateTime date, float open, float high, float low, float close, float volume)
        {
            Quote q = new Quote(this, date, open, high, low, close, volume, "");
            this.Prices.Add(q);
            return q;
        }

        public void AddOrUpdateQuote(Quote q, out bool isNew)
        {
            Quote existing = this.Prices.Where(p => p.TickerDate == q.TickerDate).FirstOrDefault();
            if (existing == null)
            {
                this.Prices.Add(q);
                isNew = true;
            }
            else
            {
                existing = q;
                isNew = false;
            }
        }
        public Quote AddOrUpdateQuote(DateTime date, float open, float high, float low, float close, float volume, out bool isNew)
        {
            Quote q = this.Prices.Where(p => p.TickerDate == date).FirstOrDefault();
            if (q == null)
            {
                Program.Debug("\t\tAdding new date {0:yyyyMMdd} to ticker {1}", date, this.Symbol);
                q = new Quote(this, date, open, high, low, close, volume, "");
                this.Prices.Add(q);
                isNew = true;
            }
            else
            {
                Program.Debug("\t\tDate {0:yyyyMMdd} of ticker {1} exists. Updating values...", date, this.Symbol);
                q.High = high;
                q.Low = low;
                q.Open = open;
                q.Close = close;
                q.Volume = volume;
                isNew = false;
            }
            return q;
        }

   
        #region Metastock
        internal void WriteToFile(string filePath)
        {

            if (File.Exists(filePath))
                File.Delete(filePath);

            BinaryWriter bw = new BinaryWriter(new FileStream(filePath, FileMode.CreateNew));

            byte[] buffer = new byte[28];
            // max_recs
            Conversion.WriteToBuffer(ref buffer, BitConverter.GetBytes(Prices.Count + 1), 2);
            // 24 byte filler

            bw.Write(buffer);

            foreach (Quote qr in Prices)
            {
                buffer = new byte[28];
                Conversion.WriteToBuffer(ref buffer, Conversion.ConvertDateToBytes(qr.TickerDate), 0);
                Conversion.WriteToBuffer(ref buffer, Conversion.ConvertFloatToMbf4(qr.Open), 4);
                Conversion.WriteToBuffer(ref buffer, Conversion.ConvertFloatToMbf4(qr.High), 8);
                Conversion.WriteToBuffer(ref buffer, Conversion.ConvertFloatToMbf4(qr.Low), 12);
                Conversion.WriteToBuffer(ref buffer, Conversion.ConvertFloatToMbf4(qr.Close), 16);
                Conversion.WriteToBuffer(ref buffer, Conversion.ConvertFloatToMbf4(qr.Volume), 20);
                Conversion.WriteToBuffer(ref buffer, Conversion.ConvertFloatToMbf4(0), 24);
                bw.Write(buffer);
            }
            bw.Close();
            bw = null;






        }
        internal void LoadQuotes(string sourceFolder, int fileNumber)
        {
            string sourceFile = Path.Combine(sourceFolder, String.Format("F{0}.DAT", fileNumber));
            if (!File.Exists(sourceFile))
                throw new Exception(String.Format("Ticker file {0} does not exist", sourceFile));

            BinaryReader br = new BinaryReader(new FileStream(sourceFile, FileMode.Open));

            short maxRecs = BitConverter.ToInt16(br.ReadBytes(2), 0);
            short lastRec = BitConverter.ToInt16(br.ReadBytes(2), 0);
            br.ReadBytes(24); // remainder of header record
            int recLength = 28;

            for (int n = 0; n < lastRec - 1; n++)
            {
                byte[] buff = br.ReadBytes(recLength);
                Quote q = new Quote();

                q.TickerDate = Conversion.DecodeDateFromMBF(Conversion.ConvertMbf4ToFloat(buff, 0, 3));
                q.Open = Conversion.ConvertMbf4ToFloat(buff, 4, 7);
                q.High = Conversion.ConvertMbf4ToFloat(buff, 8, 11);
                q.Low = Conversion.ConvertMbf4ToFloat(buff, 12, 15);
                q.Close = Conversion.ConvertMbf4ToFloat(buff, 16, 19);
                q.Volume = Conversion.ConvertMbf4ToFloat(buff, 20, 23);
                Prices.Add(q);
                // rec[n].op_int = ConvertMbf4ToFloat(buff, 24, 27);
                // Console.WriteLine("{0:yyyyMMdd} - O={1}, H={2}, L={3}, C={4}, V={5}", rec[n].date, rec[n].open, rec[n].high, rec[n].low, rec[n].close, rec[n].volume);
            }
            br.Close();


        }
        #endregion


    }

    public class Quote
    {


        internal Ticker Parent;
        [XmlAttribute]
        public DateTime TickerDate { get; set; }
        [XmlAttribute]
        public Single Open { get; set; }
        [XmlAttribute]
        public Single High { get; set; }
        [XmlAttribute]
        public Single Low { get; set; }
        [XmlAttribute]
        public Single Close { get; set; }
        [XmlAttribute]
        public Single Volume { get; set; }
        [XmlAttribute]
        public PriceErrorType ValidationResult { get; set; }

        public string ToCSV(bool includeSymbol)
        {
            if (includeSymbol)
            {
                return String.Format("{6},{0:yyyyMMdd},{1},{2},{3},{4},{5}", TickerDate, Open, High, Low, Close, Volume, Parent.Symbol.Trim());
            }
            else 
            {
                return String.Format("{0:yyyyMMdd},{1},{2},{3},{4},{5}", TickerDate, Open, High, Low, Close, Volume);
            }
        }

        public override string ToString()
        {
           return String.Format("{0}-{1:yyyyMMdd}", Parent.TickerSymbol, TickerDate);
        }

        public List<Audit> AuditTrail;
        #region
        public Quote() { }

        public Quote(Ticker parent, DateTime date, Single open, Single high, Single low, Single close, Single volume, string source)
        {
            this.Parent = parent;
            TickerDate = date;
            Open = open;
            Close = close;
            Low = low;
            High = high;
            Volume = volume;

         //   Validate();

            AuditTrail = new List<Audit>();
            AuditTrail.Add(new Audit(this, ChangeOperation.Add, open, high, low, close, volume, source));
        }
        #endregion

  
        public void Update(Ticker parent, DateTime date, Single open, Single high, Single low, Single close, Single volume, string source)
        {
            this.Parent = parent;
            TickerDate = date;
            Open = open;
            Close = close;
            Low = low;
            High = high;
            Volume = volume;

         //   Validate();

            AuditTrail = new List<Audit>();
            AuditTrail.Add(new Audit(this, ChangeOperation.Update, open, high, low, close, volume, source));
        }

  
    }

    public class Audit
    {
        internal Quote Parent;
        [XmlAttribute]
        public ChangeOperation changeType;
        [XmlAttribute]
        public string Source;
        [XmlAttribute]
        public Single Open;
        [XmlAttribute]
        public Single High;
        [XmlAttribute]
        public Single Low;
        [XmlAttribute]
        public Single Close;
        [XmlAttribute]
        public Single Volume;

        public Audit() { }
        public Audit(Quote parent, ChangeOperation type, Single open, Single high, Single low, Single close, Single volume, string source)
        {
            this.Parent = parent;
            this.Open = open;
            this.Close = close;
            this.Low = low;
            this.High = high;
            this.Volume = volume;
            this.changeType = type;
            this.Source = source;
        }

    }

    internal class Conversion
    {
        #region Conversion Routines
        internal static DateTime DecodeDateFromMBF(float MBFDate)
        {
            if (MBFDate == 0)
                return DateTime.MinValue;
            string date = MBFDate.ToString();
            int len = date.Length;
            int day = int.Parse(date.Substring(len - 2));
            int month = int.Parse(date.Substring(len - 4, 2));
            int year = int.Parse(date.Substring(0, len - 4)) + 1900;
            return new DateTime(year, month, day);
        }

        internal static byte[] ConvertDateToBytes(DateTime date)
        {
            return ConvertFloatToMbf4(ConvertDateToFloat(date));
        }

        internal static Single ConvertDateToFloat(DateTime date)
        {
            string tempDate = "";
            if (date.Year < 1900 || date.Year > 2999)
                tempDate = "0000000";
            else
                tempDate = String.Format("{0:000}{1:00}{2:00}", date.Year - 1900, date.Month, date.Day);
            return Single.Parse(tempDate);
        }

        internal static void WriteToBuffer(ref byte[] buffer, byte[] newValue, int startIndex)
        {
            for (int n = 0; n < newValue.Length; n++)
                buffer[startIndex + n] = newValue[n];

        }

        internal static byte[] ConvertFloatToMbf4(float s)
        {
            if (s == 0.0f)
            {
                return new byte[4];
            }

            if (Single.IsNaN(s))
                throw new ArgumentException(
                  "Cannot convert a NaN to MBF format");
            if (Single.IsInfinity(s))
                throw new ArgumentException(
                  "Cannot convert an infinity to MBF format");

            byte[] single = BitConverter.GetBytes(s);
            UInt32 temp = BitConverter.ToUInt32(single, 0);
            temp = (((temp & 0x7F800000) << 1) + 0x02000000) |
              ((temp & 0x80000000) >> 8) |
              (temp & 0x007FFFFF);

            return BitConverter.GetBytes(temp);
        }

        public static float ConvertMbf4ToFloat(byte[] buffer, int Start, int End)
        {
            int len = End - Start + 1;
            byte[] buff = new byte[len];
            for (int n = 0; n < len; n++)
                buff[n] = buffer[Start + n];

            if (buff[0] == 0 && buff[1] == 0 && buff[2] == 0 && buff[3] == 2)
                return 0.0f;

            return ConvertMbf4ToFloat(buff);

        }

        public static float ConvertMbf4ToFloat(byte[] mbf)
        {
            if ((mbf == null) || (mbf.Length != 4))
                throw new ArgumentException("Invalid MBF array");

            if (mbf[3] == 0) return 0.0f;
            if (mbf[3] <= 2)
                throw new ArgumentException(
                  "Underflow when converting from MBF to single");

            UInt32 temp = BitConverter.ToUInt32(mbf, 0);
            temp = (((temp - 0x02000000) & 0xFF000000) >> 1) |
              ((temp & 0x00800000) << 8) |
              (temp & 0x007FFFFF);
            byte[] single = BitConverter.GetBytes(temp);
            return BitConverter.ToSingle(single, 0);
        }

        static public string GetCleanString(byte[] buff)
        {
            string s = String.Empty;
            for (int n = 0; (n < buff.Length) && (buff[n] != 0); n++)
                s += Encoding.ASCII.GetString(buff, n, 1);

            return s;


        }

        static public string CleanString(string s)
        {
            if (s != null && s.Length > 0)
            {
                StringBuilder sb = new StringBuilder(s.Length);
                foreach (char c in s)
                {
                    sb.Append(Char.IsControl(c) ? ' ' : c);
                }
                s = sb.ToString();
            }
            return s;
        }

        #endregion
    }

}
