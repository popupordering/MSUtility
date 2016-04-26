using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Globalization;

namespace PricesConvert
{
    public class Price
    {
        public string Symbol;
        public DateTime TickerDate;
        public Single Open;
        public Single Close;
        public Single High;
        public Single Low;
        public Single Volume;
        public Dictionary<string, string> ExtendedProperties = new Dictionary<string,string>();
        List<string> csvColumns = MySettings.App.CorrectionsColumns.Split(',').ToList();
        internal bool _parseError = false;
        public Price(){}

        public Price(string _symbol, DateTime _date, Single _open, Single _close, Single _high, Single _low, Single _volume)
        {
            Symbol = _symbol;
            TickerDate = _date;
            Open = _open;
            Close = _close;
            High = _high;
            Low = _low;
            Volume = _volume;
        }
        public string ToCSV()
        {
            try
            {
                string dateStr = String.Format("{0:" + MySettings.App.CSVDateFormat + "}", this.TickerDate);
                List<string> output = new List<string>();
                foreach (string s in csvColumns)
                {
                    switch (s.ToUpper())
                    {
                        case "OPEN":
                            output.Add(this.Open.ToString());
                            break;
                        case "CLOSE":
                            output.Add(this.Close.ToString());
                            break;
                        case "HIGH":
                            output.Add(this.High.ToString());
                            break;
                        case "LOW":
                            output.Add(this.Low.ToString());
                            break;
                        case "VOLUME":
                        case "VOL":
                            output.Add(this.Volume.ToString());
                            break;
                        case "DATE":
                        case "TICKERDATE":
                            output.Add(dateStr);
                            break;
                        case "SYMBOL":
                        case "TICKER":
                            output.Add(this.Symbol);
                            break;
                        case "TYPE":
                            output.Add(this.ExtendedProperties["Type"]);
                            break;
                    }
                }
                string line = String.Join(",", output.ToArray());
                Program.Debug("Writing corrections line '{0}' to file", line);
                return line;
            }
            catch (Exception ex)
            {
                throw new Exception(String.Format("Unable to write the price data for symbol {0} date {1} to the corrections file", this.Symbol, this.TickerDate), ex);
            }
        }


        public Price(string line, List<string> colNames)
        {
            string[] arr = line.Split(',');
           
            Program.Log("Parsing corrections file record {0}", line);
            ExtendedProperties = new Dictionary<string, string>();
            foreach (string s in colNames)
            {
                int ix = colNames.IndexOf(s);
                switch (s)
                {
                    case "Date":
                    case "TickerDate":
                        if (!DateTime.TryParseExact(arr[ix], MySettings.App.CSVDateFormat, new CultureInfo("en-US"), DateTimeStyles.None, out this.TickerDate))
                        {
                            // date not optional for this class. Throw error if there is a dodgy one
                            throw new Exception(String.Format("Unable to parse date {0} using the date format string {1}", arr[ix], MySettings.App.CSVDateFormat));
                        }
                        break;
                    case "Open":
                        if (!float.TryParse(arr[ix], out this.Open)) _parseError = true;
                        break;
                    case "High":
                        if (!float.TryParse(arr[ix], out this.High)) _parseError = true;
                        break;
                    case "Low":
                        if (!float.TryParse(arr[ix], out  this.Low)) _parseError = true;
                        break;
                    case "Volume":
                        if (!float.TryParse(arr[ix], out  this.Volume)) _parseError = true;
                        break;
                    case "Symbol":
                        this.Symbol = arr[ix];
                        break;
                    case "Close":
                        if (!float.TryParse(arr[ix], out  this.Close)) _parseError = true;
                        break;
                    default:
                        ExtendedProperties.Add(s, arr[ix]);
                        break;
                }
            }

            
        }
    }


    class DB
    {
        


        static OleDbConnection dbPrices;

        static DB()
        {
            string dbFolder = Path.GetDirectoryName(MySettings.GetPath(MySettings.User.PricesLocation));
            dbPrices = new OleDbConnection("Provider=vfpoledb;Data Source=" + dbFolder + ";");
        }

        internal static List<Price> GetPrices(string symbol, DateTime? fromDate, DateTime? toDate, bool useCSV)
        {
            if (useCSV)
            {
                return GetCSVPrices(symbol, fromDate, toDate);
            }
            else
            {
                return GetPricesFromDBF(symbol, fromDate, toDate);
            }
        }

        internal static List<Price> GetCSVPrices(string symbol, DateTime? fromDate, DateTime? toDate)
        {
            string columns = "Symbol, Date,Open,High,Low,Close,Volume";
           // string where = 
            string sourceFile = Path.Combine(MySettings.GetPath(MySettings.User.CSVUpdatesFolder), String.Format("{0}.csv", symbol));
            if (!File.Exists(sourceFile))
            {
                Program.Log("No CSV update file found at {0}", sourceFile);
                return new List<Price>();
            }
            return ConvertPricesDatatable(Utilities.LoadCSV(sourceFile, columns,""), "Symbol");
        }

        internal static List<Price> ConvertPricesDatatable(DataTable dt, string symbolFieldName)
        {
            List<Price> output = new List<Price>();
            foreach (DataRow dr in dt.Rows)
            {
                Price p = new Price();
                p.Symbol = dr.Field<string>(symbolFieldName);
                p.TickerDate = dr.Field<DateTime>("date");
                p.Close = dr.Field<float>("close");
                p.Open = dr.Field<float>("open");
                p.High = dr.Field<float>("high");
                p.Low = dr.Field<float>("low");
                p.Volume = dr.Field<int>("volume");
                output.Add(p);
            }
            return output;
       }

        internal static List<Price> ConvertPricesDatatable(PricesDBF.PricesDataTable dt)
        {
            List<Price> output = new List<Price>();
            foreach (DataRow dr in dt.Rows)
            {
                Price p = new Price();
                p.TickerDate = dr.Field<DateTime>("date");
                p.Symbol = dr.Field<string>("epic");
                p.Close = dr.Field<float>("close");
                p.Open = dr.Field<float>("open");
                p.High = dr.Field<float>("high");
                p.Low = dr.Field<float>("low");
                p.Volume = (int)dr.Field<Single>("volume");
                output.Add(p);
            }
            return output;
        }



        internal static List<Price> GetPrices(string symbol, DateTime? fromDate, DateTime? toDate)
        {
   
            return GetPricesFromDBF(symbol, fromDate, toDate);
        }

        internal static List<Price> GetPricesFromDBF(string symbol, DateTime? fromDate, DateTime? toDate)
        {

            string dbName = Path.GetFileName(MySettings.GetPath(MySettings.User.PricesLocation));
            List<string> symbols = new List<string>();
            symbols.Add(symbol);
            string sql = Utilities.GenerateSQL(dbName, symbols, fromDate, toDate);
            Program.Debug("Attempting to extract data from DBF using sql {0}", sql);
            OleDbCommand cmd = new OleDbCommand(sql, dbPrices);
            PricesDBF.PricesDataTable results = new PricesDBF.PricesDataTable();

            OleDbDataAdapter da = new OleDbDataAdapter(cmd);

            da.Fill(results);

            return ConvertPricesDatatable(results);

        }

        internal static List<Price> GetAllUpdates(string sourceFolder, string sourceFile)
        {
            string sql = Utilities.GenerateSQL(sourceFile);
            OleDbConnection dbConn = new OleDbConnection("Provider=vfpoledb;Data Source=" + sourceFolder + ";");

            OleDbCommand cmd = new OleDbCommand(sql, dbConn);
            PricesDBF.PricesDataTable results = new PricesDBF.PricesDataTable();

            OleDbDataAdapter da = new OleDbDataAdapter(cmd);

            da.Fill(results);
            return DB.ConvertPricesDatatable(results);
        }
        
        internal static List<Price> GetAllUpdates(string sourceFolder, string sourceFile, List<string> symbols, DateTime? fromDate, DateTime? toDate)
        {
            const int chunkSize=30;
            PricesDBF.PricesDataTable results = new PricesDBF.PricesDataTable();
            int symbolCount = symbols.Count;
            int chunkCount = 0;
            do
            {
                
                List<string> tmpSymbols = new List<string>();
                if (symbolCount > chunkSize)
                {
                    tmpSymbols.AddRange(symbols.GetRange(chunkCount * chunkSize, chunkSize));
                }
                else
                {
                    tmpSymbols.AddRange(symbols.GetRange(chunkCount * chunkSize, symbolCount));
                }

                string sql = Utilities.GenerateSQL(sourceFile, tmpSymbols, fromDate, toDate);
                OleDbConnection dbConn = new OleDbConnection("Provider=vfpoledb;Data Source=" + sourceFolder + ";");
                OleDbCommand cmd = new OleDbCommand(sql, dbConn);
                OleDbDataAdapter da = new OleDbDataAdapter(cmd);
                PricesDBF.PricesDataTable tmpRes = new PricesDBF.PricesDataTable();
                da.Fill(tmpRes);
                symbolCount -= chunkSize;
                chunkCount++;
                    results.Merge(tmpRes);

            } while (symbolCount > 0);




            Console.WriteLine("Found {0} rows from {1}", results.Count, sourceFile);
            
            return DB.ConvertPricesDatatable(results);
        }

    }
}
