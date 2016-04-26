using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Xml.Serialization;
using System.Data;
using System.ComponentModel;

namespace PricesConvert
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

    public class Metastock
    {
        public BindingList<TickerList> Lists;
        public bool SymbolMismatch = false;
        public const string AuditFolder = "Audit";
        public const string AuditHeadings = "Date,ChangeType,Open,High,Low,Close,Volume,Source";
        internal static List<DateTime> nonTradingDays;

        static Metastock(string filePath)
        {
            try
            {
                Trace.WriteLine("Loading Non-trading days file from {0}", filePath);
                nonTradingDays = new List<DateTime>();

                DataTable nonTrading = Utilities.LoadCSV(filePath, "*", "");

                nonTradingDays = (from t in nonTrading.AsEnumerable()
                                  select t.Field<DateTime>(3)).ToList<DateTime>();
            }
            catch (Exception ex)
            {
                Trace.WriteLine("There was an error reading the non-trading days file: {0}. We'll proceed without a list of the non-trading days", ex.Message);
            }
        }

        public Metastock() {
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



        public Metastock(string configFilePath, bool createEmptyLists)
        {
            Initialise();
            if (!this.LoadConfigFile(configFilePath, createEmptyLists))
            {
                SymbolMismatch = true;
            }
        }

        public static Metastock Load(string configFilePath)
        {
           // string appData = MySettings.App.PersistentFile;
            Metastock m;
          //  if (File.Exists(appData))
         //   {
          //      System.Xml.Serialization.XmlSerializer x = new System.Xml.Serialization.XmlSerializer(typeof(Metastock));
          //      StreamReader sr = new StreamReader(appData);
         //       m = (Metastock)x.Deserialize(sr);
         //   }
         //   else
         //   {
                m = new Metastock();
                m.LoadConfigFile(configFilePath, false);

          //  }
            return m;
        }


        private void Save()
        {
            System.Xml.Serialization.XmlSerializer x = new System.Xml.Serialization.XmlSerializer(typeof(Metastock));
            StreamWriter sw = new StreamWriter("AppData.xml");
            x.Serialize(sw, this);
        }



      

        public bool LoadConfigFile(string configFilePath, bool createEmptyLists)
        {
            this.Lists = ListProcessing.LoadTickerLists(ListLoadBehaviour.LoadBothAndCompare);
            Lists.ListChanged += new ListChangedEventHandler(Lists_ListChanged);

  
            return true;
        }

        /// <summary>
        /// Write all lists to the MS ticker files
        /// </summary>
        public void Commit()
        {
            foreach (TickerList l in this.Lists.Where(l=>l.hasChanged == true))
            {
                
                l.WriteList(false);
            }
            this.Save();
        }
    }


    public enum ChangeOperation
    {
        Add,
        Update
    }

    struct TickerAndName
    {
        internal string Symbol;
        internal string Name;
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
        public bool hasChanged=false;
        internal bool hasMismatch = false;
        public bool isCompositeList = false;
        public string compositeSource;
        internal CompositeTickerList compositeList;

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
        public TickerList(string msFolder, string listName, string listFilePath)
        {
            Initialise();
            this.MSFolder = msFolder;
            this.ListName = listName;
            this.ListFilePath = listFilePath;
            this.LoadTickerList();

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

        public void ErrorCheck(float dayTolerance)
        {
            foreach (Ticker t in this.Tickers) t.ErrorCheck(dayTolerance);
        }


        public void LoadTickerList()
        {
            Initialise();
            List<TickerAndName> symbols = Utilities.LoadTickerAndNamesFromCSV(this.ListFilePath);
            short fileNum = 1;

            foreach (TickerAndName s in symbols)
            {
                this.Tickers.Add(new Ticker(s.Symbol ,s.Name,  fileNum++));
            }

        }

        public Quote GetDayValue(string symbol, DateTime day)
        {
            Ticker tl = this.Tickers.Where(t => t.TickerSymbol == symbol).FirstOrDefault();
            if (tl == null) return null;
            return tl.Prices.Where(p => p.TickerDate == day).FirstOrDefault();
        }


        public int  CreatePeriodData(int numberDays, string destFolder, ref string resultMessage)
        {
            Trace.WriteLine(String.Format("Creating period data list from list name {0} using period {1} to destination folder {2}", this.ListName, numberDays, destFolder));
            try
            {
                List<DateTime> listDates = new List<DateTime>();
                foreach (Ticker t in this.Tickers) listDates.AddRange(t.TickerDates);
                List<DateTime> distinctDates = listDates.Select(t => t.Date).Distinct().ToList();

                if (distinctDates.Count < numberDays)
                {
                    resultMessage = String.Format("The period selected ({0}) is longer than the amount of days in the list ({1})!", numberDays, distinctDates.Count);
                    return 2;
                }
                TickerList periodList = new TickerList();
                periodList.ListName = this.ListName + "_Period";
                periodList.MSFolder = destFolder;

                foreach (Ticker t in this.Tickers)
                {
                    Ticker periodTicker = new Ticker(t.Name, t.FileNumber);
                    float open = 0;
                    float close = 0;
                    float high = 0;
                    float low = 0;
                    float vol = 0;
                    int dayCounter = 0;
                    int actualDayCounter = 0;
                    foreach (DateTime d in distinctDates)
                    {
                        Quote q = t.Prices.Where(p => p.TickerDate == d).FirstOrDefault();
                        if (q != null)
                        {
                            Trace.WriteLine(String.Format("Day counter {0}; Actual day counter {1}. Date {2}, O={3}, H={4}, L={5}, C={6}, V={7}", dayCounter, actualDayCounter, d, q.Open, q.High, q.Low, q.Close, q.Volume));
                            if (actualDayCounter == 0)
                            {
                                open = q.Open;
                                low = q.Low;
                                close = 0;
                                high = q.High;
                                vol = q.Volume;
                            }
                            else
                            {
                                low = (q.Low < low ? q.Low : low);
                                high = (q.High > high ? q.High : high);
                                vol += q.Volume;
                                close = q.Close;
                                // Program.Debug(".....new low = {0}; new high = {1}; updated volume = {2}", low, high, vol);
                            }
                            actualDayCounter++;
                        }
                        else
                        {
                            Program.Debug("No data found in ticker {0} for date {1}", t.Name, d);
                        }
                        dayCounter++;

                        if (dayCounter == numberDays)
                        {
                            Program.Debug("Writing period data. Date {0}, O={1}, H={2}, L={3}, C={4}, V={5}", d, open, high, low, close, vol);
                            periodTicker.AddQuote(new Quote(periodTicker, d, open, high, low, close, vol, "Period"));
                            dayCounter = 0;
                            actualDayCounter = 0;
                            open = 0;
                            high = 0;
                            low = 0;
                            close = 0;
                            vol = 0;
                        }
                    }
                    periodList.Tickers.Add(periodTicker);
                }
                periodList.WriteList(false);
                resultMessage = "Files create successfully";
                return 1;
            }
            catch (Exception ex)
            {
                Log("There was an error processing the period data. Error: {0} \n Stack Trace: {1}", ex.Message, ex.StackTrace);
                resultMessage = "There was an error processing the files. Please see the logs for details";
                return -1;
            }

        }

        static void Log(string s, params object[] args)
        {

        }

        static void Debug(string s, params object[] args)
        {

        }

        /// <summary>
        /// Loads the MS data from the ticker folder
        /// </summary>
        public void LoadTickers()
        {
             string emasterFilePath = Path.Combine(this.MSFolder, "EMASTER");
             if (!Directory.Exists(this.MSFolder))
            {
                 // Ticker lists are not supported for composite tickers

                if (!isCompositeList) 
                {
                    this.isNewList = true;
                    LoadTickerList();
                }
            }
            else
            {
                if (!File.Exists(emasterFilePath))
                    this.isNewList = true;
            }

            Log("Loading MS data for list {0} from folder {1}", this.ListName , this.MSFolder);


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
                    Debug("Loading file number {0}", n);
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
                    ticker.TickerSymbol  = symbol;
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
                Log("There was an error reading the MS data file from the folder {0}: {1}", this.MSFolder, ex.Message);
                throw ex;
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

        public TickerList(string msFolder, string listName, bool isComposite)
        {
            Initialise();
            this.MSFolder = msFolder;
            this.ListName = listName;
            this.isCompositeList = isComposite;
            LoadTickers();
    

        }
        

        internal bool AddExtraDay()
        {
            Log("Adding extra day to all tickers on the list {0}", this.ListName);
            Trace.Indent();
            bool success = true;
            foreach (Ticker t in this.Tickers)
            {
                if (!t.AddExtraDay())
                {
                    success = false;
                }
            }
            Trace.Unindent();
            return success;
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
                    Log("Unable to load audit data for ticker {0} from file {1}: File doesn't exist", ticker.Symbol, auditPath);
                }
            }
            catch (Exception ex)
            {
                Log("Unable to read from audit file: {0}", ex.Message);
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
            t.fileLastDate  = DateTime.MaxValue;
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


        public void Validate()
        {
            RemoveNonTradingDays();



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

        private void RemoveNonTradingDays()
        {
            foreach (DateTime d in NonTradingDays.nonTradingDays)
            {
                this.RemoveDay(d);
            }
        }



        public void ApplyCorrections(string correctionsFilePath)
        {
           //string filePath = MySettings.GetPath(MySettings.User.CorrectionsFile);
            if (!File.Exists(correctionsFilePath))
            {
                Log("No corrections file provided. Skipping the running of corrections on this list");
                return;
            }
            TickerCorrections tc = TickerCorrections.LoadCSV(correctionsFilePath);

            var matches = from correct in tc.Corrections.AsEnumerable()
                          join t in Tickers.AsEnumerable() on correct.PriceData.Symbol equals t.TickerSymbol
                          where correct.PriceData.TickerDate >= t.FirstDate && correct.PriceData.TickerDate <= t.LastDate
                          select new { correct, t };
            if (matches.Count() == 0)
            {
                Log("There are no corrections records that are applicable for the current list");
            }
            foreach (var c in matches)
            {
                c.t.AddCorrection(c.correct);
            }


        }

        public bool AddPriceUpdates(List<Price> updatedPrices, string source = "")
        {
            Log("Applying updates to the database from source {0}", source);
            Trace.Indent();

            try
            {
                var results = from table1 in updatedPrices.AsEnumerable()
                              join table2 in this.Tickers.AsEnumerable() on (string)table1.Symbol equals (string)table2.TickerSymbol 
                              select new
                              {
                                  symbol = (string)table1.Symbol,
                                  ticker = table2
                              };
                foreach (var item in results)
                {
                    List<Price> tickerPrices =
                      new List<Price>(
                        from pricesRow in updatedPrices.AsEnumerable()
                        where pricesRow.Symbol == item.symbol
                        orderby pricesRow.TickerDate
                        select pricesRow);

                    if (tickerPrices.Count > 0) item.ticker.AddPrices(tickerPrices);

                }
                return true;
            }
            catch (Exception ex)
            {
                Log( "Error in AddPrices (applying updates):" + ex.Message);
                Log( ex.StackTrace);
                return false;
            }
            finally
            {
                Trace.Unindent();
            }



        }




        internal void WriteList(bool isCorrection)
        {

            if (this.addExtraDays && !isCorrection) this.AddExtraDay();

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
                    foreach(Audit a in q.AuditTrail)
                    {
                    sw.WriteLine(String.Format("{0:yyyy-MM-dd},{1},{2},{3},{4},{5},{6},{7}", q.TickerDate, a.changeType.ToString(),  
                        a.Open, a.High, a.Low, a.Close, a.Volume, a.Source));
                    }
                }
                sw.Close();
                sw=null;
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

        internal void WriteToCSV(string destFolder)
        {
            try
            {
                Utilities.CreateEmptyFolder(destFolder);
                foreach (Ticker t in this.Tickers) t.SaveToCSV(destFolder);
            }
            catch (Exception ex)
            {
                throw new Exception(String.Format("Unable to write list {0} to disk at location {1}", this.ListName, destFolder), ex);
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

        public Ticker(string tickerSymbol, string tickerName, short fileNum)
        {
            Initialise();
            this.TickerSymbol = tickerSymbol;
            this.TickerName = tickerName;
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
        internal List<DateTime> TickerDates
        {
            get
            {
                List<DateTime> ret = new List<DateTime>();
                foreach (Quote q in this.Prices) ret.Add(q.TickerDate);
                return ret;
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

        public void ErrorCheck(float dayTolerance)
        {
            for (int i = 1; i < this.Prices.Count; i++)
            {
                Quote q1 = this.Prices[i-1];
                Quote q2 = this.Prices[i];

                q1.Validate();

                if (GetDifference(q1.Open, q2.Open) > dayTolerance)
                {
                    q2.ValidationResult = PriceErrorType.OpenValueOutOfTolerance;
                }
                if (GetDifference(q1.Close, q2.Close) > dayTolerance)
                {
                    q2.ValidationResult = PriceErrorType.CloseValueOutOfTolerance;
                }
                if (GetDifference(q1.High, q2.High) > dayTolerance)
                {
                    q2.ValidationResult = PriceErrorType.HighValueOutOfTolerance;
                }
                if (GetDifference(q1.Low, q2.Low) > dayTolerance)
                {
                    q2.ValidationResult = PriceErrorType.LowValueOutOfTolerance;
                }
            }
        }

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
                    Log("Ticker {0} has no data so cannot add extra day", this.TickerSymbol);
                    return false;
                }
                Quote newDay = new Quote();
                newDay.Parent = this;
                newDay.TickerDate = Utilities.GetNextTradingDay(lastDay.TickerDate);
                newDay.High = lastDay.High;
                newDay.Low = lastDay.Low;
                newDay.Open = lastDay.Open;
                newDay.Close = lastDay.Close;
                newDay.Volume = lastDay.Volume;

                Log("Adding extra day {0} to ticker {1}", newDay.TickerDate, this.TickerName);
                this.AddQuote(newDay);
                return true;
            }
            catch (Exception ex)
            {
                Log("There was an error adding the extra day after {0} to ticker {1}: {2}", this.LastDate, this.TickerSymbol, ex.Message);
                return false;
            }

        }

        public Quote AddQuote(DateTime date, float open, float high, float low, float close, float volume)
        {
            Quote q = new Quote(this, date, open, high, low, close, volume, "");
            this.Prices.Add(q);
            return q;
        }

        public void AddPrices(List<Price> prices, string source = "")
        {
            // get the list ID
            //short listID = this.Lists.Where(l => l.ListName == listName).First().ListID;

            Trace.Indent();
            try
            {

                foreach (Price p in prices)
                {

                    Debug("List {6}, Symbol {7} :Adding date {0:yyyyMMdd}, O={1}, H={2}, L={3}, C={4}, V={5}", p.TickerDate, p.Open, p.High, p.Low, p.Close, p.Volume, this.Parent.ListName, this.TickerSymbol);
                    
                    Quote qr = this.Prices.Where(pr => pr.TickerDate == p.TickerDate).FirstOrDefault();

                    // QuotesRow qr = this.Quotes.FindByDateTickerID(row.date, tr.TickerID);
                    // AuditLogRow log = this.AuditLog.NewAuditLogRow();
                    if (qr == null)
                    {
                        qr = new Quote(this, p.TickerDate, p.Open, p.High, p.Low, p.Close, p.Volume, source);
                        this.AddQuote(qr);
                    }
                    else
                    {
                        Debug( "Existing date");
                        qr.Update(this, p.TickerDate, p.Open, p.High, p.Low, p.Close, p.Volume, source);
                    }
                }

            }
            catch (Exception e)
            {
                Trace.WriteLine("Error in AddPrices: " + e.Message);
                Debug( e.StackTrace);
            }
            finally
            {
                Trace.Unindent();
            }



        }

        public void AddCorrection(TickerCorrections.TickerCorrection tc)
        {
            Log("Applying correction for ticker {0} date {1}: {2}", tc.PriceData.Symbol, tc.PriceData.TickerDate, tc.Type);
            Quote qr = this.Prices.Where(p => p.TickerDate == tc.PriceData.TickerDate).FirstOrDefault();
            switch (tc.Type)
            {
                case CorrectionType.AddDate:
                    if (qr != null)
                    {
                        Program.Log("Date {0} already exists. Updating existing data...", tc.PriceData.TickerDate);
                        qr.Update(this, tc.PriceData.TickerDate, tc.PriceData.Open, tc.PriceData.High, tc.PriceData.Low, tc.PriceData.Close, tc.PriceData.Volume, "Corrections");
                    }
                    else
                    {
                        qr = new Quote(this, tc.PriceData.TickerDate, tc.PriceData.Open, tc.PriceData.High, tc.PriceData.Low, tc.PriceData.Close, tc.PriceData.Volume, "Corrections");
                        this.Prices.Add(qr);
                    }
                    break;
                case CorrectionType.CorrectPrices:
                    if (qr != null)
                    {
                        qr.Update(this, tc.PriceData.TickerDate, tc.PriceData.Open, tc.PriceData.High, tc.PriceData.Low, tc.PriceData.Close, tc.PriceData.Volume, "Corrections");
                    }
                    else
                    {
                        Program.Log("Date {0} does not exist. Adding date from corrections...", tc.PriceData.TickerDate);
                        qr = new Quote(this, tc.PriceData.TickerDate, tc.PriceData.Open, tc.PriceData.High, tc.PriceData.Low, tc.PriceData.Close, tc.PriceData.Volume, "Corrections");
                        this.Prices.Add(qr);
                    }
                    break;
                case CorrectionType.RemoveDate:
                    if (qr == null)
                    {
                        Program.Log("Date {0} does not exist. Doing nothing", tc.PriceData.TickerDate);
                    }
                    else
                    {
                        this.Prices.Remove(qr);
                    }
                    break;
            }


       




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

                foreach (Quote qr in Prices.OrderBy(p=> p.TickerDate))
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
        internal void SaveToCSV(string folderPath)
        {
            string filePath = Utilities.GetCSVFileName(this.TickerSymbol, folderPath);
            Log("Writing ticker {0} to file {1}", this.TickerSymbol, filePath);
            StreamWriter sw = new StreamWriter(filePath);
            string columns = MySettings.App.CSVColumns;
            sw.WriteLine(columns);
            List<string> cols = columns.Split(',').ToList<string>();
            foreach (Quote q in this.Prices.OrderBy(p => p.TickerDate)) sw.WriteLine(q.CSVOutput(cols));
            sw.Flush();
            sw.Close();
        }

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

            Validate();

            AuditTrail = new List<Audit>();
            AuditTrail.Add(new Audit(this, ChangeOperation.Add, open, high, low, close, volume, source));
        }
        #endregion

        public PriceErrorType Validate()
        {

            if (this.TickerDate.DayOfWeek == DayOfWeek.Saturday || this.TickerDate.DayOfWeek == DayOfWeek.Sunday)
            {
                Program.Log("Attempting to add a weekend date of {0} to the ticker {1}.", this.Parent.TickerSymbol, this.TickerDate);
                ValidationResult = PriceErrorType.NonTradingDay;
            }

            if (this.High < this.Low || this.High < this.Open || this.High < this.Close)
            {
                Program.Log("Ticker {0} on {1} has a High value ({2}) that is lower than either the Open ({3}), Low ({4}) or Close ({5})", this.Parent.TickerSymbol, this.TickerDate, this.High, this.Open, this.Low, this.Close);

                ValidationResult = PriceErrorType.HighIsNotHighest;
            }

            if (this.Low > this.High || this.Low > this.Open || this.Low > this.Close)
            {
                Program.Log("Ticker {0} on {1} has a Low value ({2}) that is higher than either the Open ({3}), High ({4}) or Close ({5})", this.Parent.TickerSymbol, this.TickerDate, this.Low, this.Open, this.High, this.Close);
                ValidationResult = PriceErrorType.LowIsNotLowest;
            }

            if (Metastock.nonTradingDays.Contains(this.TickerDate))
            {
                Program.Log("Ticker {0}: Date {1} is a non-trading day. Skipping this day.", this.Parent.TickerSymbol, this.TickerDate);
                ValidationResult = PriceErrorType.NonTradingDay;
            }

            if (ValidationResult != PriceErrorType.OK)
            {
                Errors.AddError(this.Parent.Symbol, this, ValidationResult);
            }

            return ValidationResult;

        }

        public void Update(Ticker parent, DateTime date, Single open, Single high, Single low, Single close, Single volume, string source)
        {
            this.Parent = parent;
            TickerDate = date;
            Open = open;
            Close = close;
            Low = low;
            High = high;
            Volume = volume;

            Validate();

            AuditTrail = new List<Audit>();
            AuditTrail.Add(new Audit(this, ChangeOperation.Update, open, high, low, close, volume, source));
        }

        internal string CSVOutput(List<string> colNames)
        {
            string output = string.Empty;

            foreach (string s in colNames)
            {
                switch (s.ToUpper())
                {
                    case "DATE":
                    case "TICKERDATE":
                        output += this.TickerDate.ToString(MySettings.App.CSVDateFormat);
                        break;
                    case "OPEN":
                    case "O":
                        output += this.Open.ToString();
                        break;
                    case "HIGH":
                    case "H":
                        output += this.High.ToString();
                        break;
                    case "CLOSE":
                    case "C":
                        output += this.Close.ToString();
                        break;
                    case "LOW":
                    case "L":
                        output += this.Low.ToString();
                        break;
                    case "VOLUME":
                    case "VOL":
                    case "V":
                        output += this.Volume.ToString();
                        break;
                    case "SYMBOL":
                    case "TICKER":
                    case "EPIC":
                        output += this.Parent.TickerSymbol;
                        break;
                }
                output += ",";
            }
            // knock off the trailing comma
            output = output.Substring(0, output.Length - 1);
            return output;
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
