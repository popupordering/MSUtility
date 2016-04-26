using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;

namespace MSCleaner
{
    enum appMode
    {
        NotSelected,
        CleanData,
        AddExtraDay,
        ExportCreate,
        Export,
        Import,
        ImportCreate,
        ImportAppend,
        ExportAppend,
        MissingDaysReport,
        AddedDaysReport


    }


    class Program
    {

        static bool isDebug = false;
        static appMode applicationMode = appMode.NotSelected;
        static string csvPath = "";
        static DateTime fromDate = DateTime.MinValue;
        static DateTime toDate = DateTime.MaxValue;
        static string currentFolder = "";
        static StreamWriter logFile = null;

        static void Main(string[] args)
        {
            currentFolder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            string tickerFolder = "";
            string nonTradingDaysFile = Path.Combine(currentFolder, "NonTradingDays.csv");
            bool showCalling = false;
            if (args.Length == 0) showCalling = true;
            logFile = new StreamWriter(Path.Combine(currentFolder, "MSUtility.log"));
            foreach (string s in args)
            {
                if (s.Contains(':'))
                {
                    string[] prm = s.Split(':');
                    switch (prm[0].ToUpper())
                    {
                        case "/T":
                        case "-T":
                            if (prm.Length > 2)
                            {
                                tickerFolder = prm[1] + ":" + prm[2];
                            }
                            else
                            {
                                tickerFolder = prm[1];
                            }


                            break;
                        case "/N":
                        case "-N":
                            nonTradingDaysFile = prm[1];
                            break;
                        case "/CSV":
                        case "-CSV":
                            if (prm.Length > 2)
                            {
                                csvPath = prm[1] + ":" + prm[2];
                            }
                            else
                            {
                                csvPath = prm[1];
                            }
                            break;
                        case "/MODE":
                        case "-MODE":
                            if (!Enum.TryParse(prm[1], true, out applicationMode))
                            {
                                Program.Log("Unknown MODE selection. Must be Import, ExportCreate, ExportAppend, AddExtraDay, CleanData, MissingDaysReport, AddedDaysReport");
                                Environment.Exit(-3);
                            }
                            break;
                        case "/FROM":
                        case "-FROM":
                            if (!DateTime.TryParseExact(prm[1], "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out fromDate))
                            {
                                Program.Log("{0} is an invalid FROM date. Please use the format YYYYMMDD. For example, 1st October 2014 would be rendered as /FROM:20141001", prm[1]);
                                Environment.Exit(-1);
                            }
                            break;
                        case "/TO":
                        case "-TO":
                            if (!DateTime.TryParseExact(prm[1], "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out toDate))
                            {
                                Program.Log("{0} is an invalid TO date. Please use the format YYYYMMDD. For example, 1st October 2014 would be rendered as /TO:20141001", prm[1]);
                                Environment.Exit(-1);
                            }
                            break;
                        default:
                            Program.Log("Unknown parameter '{0}'", s);
                            showCalling = true;
                            break;
                    }
                }
                else
                {
                    switch (s.ToUpper())
                    {
                        case "/?":
                        case "-?":
                            ShowCalling();
                            return;
                        case "/D":
                        case "-D":
                            isDebug = true;
                            break;
                        case "/A":
                        case "-A":
                            applicationMode = appMode.AddExtraDay;
                            break;
                        default:
                            Program.Log("Unknown parameter '{0}'", s);
                            showCalling = true;
                            break;
                    }

                }
            }
            if (showCalling)
            {
                ShowCalling();
            }

            Program.Log("Application running in mode {0}", applicationMode);

            if (tickerFolder == "")
            {
                Program.Log("No ticker folder provided. Please define the source/destination tickers folder using the /T parameter. E.g. /T:C:\\MyTickersFolder");
                Environment.Exit(-2);
            }

 
            try{
                Metastock m;
                switch (applicationMode)
                {
                    case appMode.CleanData:
                        try
                        {
                            if (nonTradingDaysFile == "")
                            {
                                Program.Log("CleanData mode selected but no non-trading days file provided. Can't do anything");
                                Environment.Exit(-4);
                            }
                            Program.Log("Running MSCleaner over tickers in folder {0} using the non-trading days file {1}", tickerFolder, nonTradingDaysFile);
                            m = new Metastock(tickerFolder, nonTradingDaysFile);
                            foreach (TickerList tl in m.Lists)
                            {
                                Program.Log("Cleaning list at {0}", tl.MSFolder);
                                int numberExtra = 0;
                                if (tl.Validate(false, ref numberExtra))
                                {
                                    Program.Log("Re-saving data....");
                                    tl.WriteList();
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Program.Log("!!!ERROR: Clean Data failed. Error message is {0} !!!", ex.Message);
                            Program.Log(ex.StackTrace);
                        }
                        break;
                    case appMode.AddExtraDay:
                        try
                        {
                            if (nonTradingDaysFile == "")
                            {
                                Program.Log("AddExtraDay selected but no Non-Trading Days file provided. We'll be unable to verify if the extra day is a trading day");
                                m = new Metastock(tickerFolder);
                            }
                            else
                            {
                                m = new Metastock(tickerFolder, nonTradingDaysFile);
                            }
                            foreach (TickerList tl in m.Lists)
                            {
                                foreach (Ticker t in tl.Tickers)
                                {
                                    t.AddExtraDay();
                                }
                                Program.Log("Re-saving data....");
                                tl.WriteList();
                            }
                        }
                        catch (Exception ex)
                        {
                            Program.Log("!!!ERROR: AddExtraDay failed. Error message is {0} !!!", ex.Message);
                            Program.Log(ex.StackTrace);
                        }
                        break;
                    case appMode.ExportCreate:
                    case appMode.Export:
                        ExportData(tickerFolder,csvPath, false, fromDate, toDate);
                        break;
                    case appMode.ExportAppend:
                        ExportData(tickerFolder, csvPath, true,  fromDate, toDate);
                        break;

                    case appMode.ImportCreate:
                        ImportData(tickerFolder, csvPath, false );
                        break;
                    case appMode.ImportAppend:
                    case appMode.Import:
                        ImportData(tickerFolder, csvPath, true );
                        break;

                    case appMode.AddedDaysReport:
                        try
                        {
                            int numberExtra = 0;
                            if (nonTradingDaysFile == "")
                            {
                                Program.Log("AddedDaysReport mode selected but no non-trading days file provided. Will only be able to report on weekends");
                                m = new Metastock(tickerFolder);
                            }
                            else
                            {
                                Program.Log("Running AddedDaysReport over tickers in folder {0} using the non-trading days file {1}", tickerFolder, nonTradingDaysFile);
                                m = new Metastock(tickerFolder, nonTradingDaysFile);
                            }
                            foreach (TickerList tl in m.Lists)
                            {
                                Program.Log("Reporting on list at {0}, exporting Added Days to {1}", tl.MSFolder, csvPath);
                                tl.Validate(true, ref numberExtra, csvPath);
                            }
                            Program.Log("Number of extra days: {0}. Detailed information provided in CSV file at {1}", numberExtra, csvPath);
                        }
                        catch (Exception ex)
                        {
                            Program.Log("!!!ERROR: AddedDaysReport failed. Error message is {0} !!!", ex.Message);
                            Program.Log(ex.StackTrace);
                        }
                        break;
                    case appMode.MissingDaysReport:
                        try
                        {
                            if (nonTradingDaysFile == "")
                            {
                                Program.Log("MissingDaysReport selected but no Non-Trading Days file provided. We may output valid holidays as missing data");
                                m = new Metastock(tickerFolder);
                            }
                            else
                            {
                                m = new Metastock(tickerFolder, nonTradingDaysFile);
                            }

                            if (fromDate == DateTime.MinValue || toDate == DateTime.MaxValue)
                            {
                                Program.Log("No /FROM or /TO date provided as a range to validate. Will default to the start/end dates of each ticker");
                            }
                            StreamWriter sw = new StreamWriter(csvPath, false);
                            sw.WriteLine("Symbol,Date");
                            int numberMissing = 0;
                            try
                            {
                                foreach (TickerList tl in m.Lists)
                                {
                                    tl.GetMissingDays(fromDate, toDate, sw, ref numberMissing);
                                }
                            }
                            catch (Exception ex)
                            {
                                throw ex;

                            }
                            Program.Log("Number of missing days found in list: {0}. Detail will be found in CSV file at {1}", numberMissing, csvPath);
                            sw.Close();

                        }
                        catch (Exception ex)
                        {
                            Program.Log("!!!ERROR: Added days report failed. Error message is {0} !!!", ex.Message);
                            Program.Log(ex.StackTrace);
                        }
                        break;
        

                    default:
                        Program.Log("No -MODE selected. Nothing to do!");
                        ShowCalling();
                        break;
                }

            } catch (Exception ex)
            {
                Program.Log("!!!ERROR: Process failed. Error message is:  {0} !!!", ex.Message);
                Program.Log(ex.StackTrace);
            }
            logFile.Close();

        }

        static void ExportData(string tickerFolder, string csvPath, bool append, DateTime fromDate, DateTime toDate)
        {
            try
            {
                Metastock m = null;
                if (csvPath == "")
                {
                    Program.Log("No export CSV file path provided. Please define the export file using the /CSV parameter. E.g. /CSV:C:\\MyExportFile.csv");
                    Environment.Exit(-5);
                }
                else
                {
                    if (Path.GetDirectoryName(csvPath) == "")
                    {
                        csvPath = Path.Combine(currentFolder, csvPath);
                    }
                }

                TickerList outputList = new TickerList();
                if (File.Exists(csvPath))
                {
                    if (append)
                    {
                        int tickersAdded = 0;
                        int pricesAdded = 0;
                        int pricesUpdated = 0;
                        int numberErrors = 0;
                        Program.Log("Loading existing CSV file from {0} so we can append to it", csvPath);
                        ImportCSV(csvPath, ref tickersAdded, ref pricesAdded, ref pricesUpdated, ref numberErrors, ref outputList);
                    }
                    else
                    {
                        Program.Log("Export file already exists and you haven't selected the ExportAppend option. File will be overwritten");
                    }

                }

                m = new Metastock(tickerFolder);
                
                Program.Log("About to export tickers from folder {0} from date {1:yyyyMMdd} to {2:yyyyMMdd}", tickerFolder, fromDate, toDate);
                int numberNew = 0;
                foreach (Ticker t in m.Lists[0].Tickers)
                {
                    Ticker outputTicker = outputList.Tickers.Where(tl => tl.TickerSymbol == t.TickerSymbol).FirstOrDefault();
                    if (outputTicker == null)
                    {
                        outputTicker = new Ticker(t.TickerSymbol, (short)(outputList.Tickers.Count + 1));
                        outputList.Tickers.Add(outputTicker);
                    }
                    Program.Debug("\tExporting symbol {0}", t.Symbol);
                    foreach (Quote q in t.Prices.Where(p => p.TickerDate >= fromDate && p.TickerDate <= toDate))
                    {
                        bool isNew = false;
                        outputTicker.AddOrUpdateQuote(q, out isNew);
                        if (isNew)
                        {
                            numberNew++;
                        }
                        else
                        {
                            Program.Debug("Date {0:yyyyMMdd} already exists so we've replaced it", q.TickerDate);
                        }
                    }
                }
                if (append)
                {
                    Program.Log("A total of {0} new price records will be written to the file", numberNew);
                }
                outputList.WriteCSV(csvPath);
                Program.Log("Finished exporting list");
            }
            catch (Exception ex)
            {
                Program.Log("!!!ERROR: Export failed. Error message is {0} !!!", ex.Message);
                Program.Log(ex.StackTrace);
            }

        }


        static void ImportData(string tickerFolder, string csvPath, bool append)
        {
            try
            {
                Metastock m = null;
                if (csvPath == "")
                {
                    Program.Log("No import CSV file path provided. Please define the export file using the /CSV parameter. E.g. /CSV:C:\\MyExportFile.csv");
                    Environment.Exit(-5);
                }
                else
                {
                    if (Path.GetDirectoryName(csvPath) == "")
                    {
                        csvPath = Path.Combine(currentFolder, csvPath);
                    }
                }
                if (fromDate != DateTime.MinValue || toDate != DateTime.MaxValue)
                {
                    Program.Log("WARNING: Date range will be ignored for Import operation. By default all data in the CSV will be imported");
                }

                if (!File.Exists(csvPath))
                {
                    Program.Log("Cannot find CSV file at {0}", csvPath);
                    Environment.Exit(-6);
                }

                if (append && !File.Exists(Path.Combine(tickerFolder, "EMASTER")))
                {
                    Program.Log("Ticker folder doesn't exist and ImportCreate not selected. Defaulting to ImportCreate");
                    append = false;
                }

                if (!append && Directory.Exists(tickerFolder))
                {
                    Program.Log("Deleting existing tickers folder {0} before importing", tickerFolder);
                    Directory.Delete(tickerFolder,true);
                }

                m = new Metastock(tickerFolder, !append);
                int tickersAdded = 0;
                int pricesAdded = 0;
                int pricesUpdated = 0;
                int numberErrors = 0;

                TickerList l = m.Lists[0];
                ImportCSV(csvPath, ref tickersAdded, ref pricesAdded, ref pricesUpdated, ref numberErrors, ref l);

                Program.Log("Import stats:");
                Program.Log("\tTickers added: {0}", tickersAdded);
                Program.Log("\tPrice records added: {0}", pricesAdded);
                Program.Log("\tPrice records updated: {0}", pricesUpdated);
                Program.Log("\tNumber of errors: {0}", numberErrors);
                Program.Log("Writing updated list to {0}", m.Lists[0].MSFolder);
                m.Lists[0].WriteList();
            }
            catch (Exception ex)
            {
                Program.Log("!!!ERROR: Import failed. Error message is {0} !!!", ex.Message);
                Program.Log(ex.StackTrace);
            }
        }



        static private void ImportCSV(string csvPath, ref int tickersAdded, ref int pricesAdded, ref int pricesUpdated, ref int numberErrors, ref TickerList l)
        {
            Program.Log("Importing CSV file {0} into memory", csvPath);
            StreamReader r = new StreamReader(csvPath);
            string line = r.ReadLine(); // ignore the first line of headers
            while (!r.EndOfStream)
            {
                line = r.ReadLine();
                string[] fields = line.Split(',');
                if (fields.Length == 7)
                {
                    Ticker t = l.Tickers.Where(s => s.Symbol.Trim() == fields[0]).FirstOrDefault();
                    if (t == null)
                    {
                        Program.Debug("\tLoading the ticker {0}", fields[0]);
                        t = new Ticker(fields[0], (short)(l.Tickers.Count + 1));
                        l.Tickers.Add(t);
                        tickersAdded++;
                    }
                    DateTime tickerDate = DateTime.MinValue;
                    if (DateTime.TryParseExact(fields[1], "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out tickerDate))
                    {
                        bool isNew = false;
                        t.AddOrUpdateQuote(tickerDate, float.Parse(fields[2]), float.Parse(fields[3]), float.Parse(fields[4]), float.Parse(fields[5]), float.Parse(fields[6]), out isNew);
                        if (isNew)
                        {
                            pricesAdded++;
                        }
                        else
                        {
                            pricesUpdated++;
                        }
                    }
                    else
                    {
                        Program.Log("\tInvalid date value ({0}) for ticker {1} from line '{3}'. Skipping line", fields[1], fields[0], line);
                        numberErrors++;
                    }
                }
                else
                {
                    Program.Log("\tUnable to parse line {0} as a CSV string. Expecting 6 fields and only can find {1}", line, fields.Length);
                    numberErrors++;
                }
            }
            
            return;

        }
        

        static private void ShowCalling()
        {
            Console.WriteLine("MSUtility application for working with Metastock data files");

            Console.WriteLine("Calling arguments:");
            Console.WriteLine("\tMSUtility.exe -T:<TickersFolder> ");
            Console.WriteLine("\t              -MODE:{CleanData|AddExtraDay|Export|Import|MissingDaysReport|AddedDaysReport}");
            Console.WriteLine("\t              Where: ");
            Console.WriteLine("\t                ExportAppend | ExportCreate: Exports price data to a single CSV file. ");
            Console.WriteLine("\t                   Parameters: -CSV:<Path to destination CSV file (file will be overwritten if it exists)");   
            Console.WriteLine("\t                               [-FROM:<Start of date range (inclusive)> ]");
            Console.WriteLine("\t                               [-TO:<End of date range (inclusive)> ]");
            Console.WriteLine("\t                                 NB: Dates to be provided in the format YYYYMMDD");
            Console.WriteLine("\t                                 NB: ExportCreate will create a new output file, ExportAppend will append to an existing file");
            Console.WriteLine("\t                ImportCreate | ImportAppend: Imports price data from a single CSV file");
            Console.WriteLine("\t                   Parameters: -CSV:<Path to source CSV file");
            Console.WriteLine("\t                                 NB: ImportCreate will create a new set of MS files in the location specified, ImportAppend will append to existing files");
            Console.WriteLine("\t                AddExtraDay: Appends an extra day onto each ticker with the same OHLCV data as the previous day");
            Console.WriteLine("\t                   Parameters: [-N:<NonTradingDaysFile>] (if this is not provided then the extra day cannot be properly validated) ");
            Console.WriteLine("\t                CleanData: Cleans out non-trading days from the ticker list");
            Console.WriteLine("\t                   Parameters: -N:<NonTradingDaysFile> ");
            Console.WriteLine("\t                MissingDaysReport: Creates a CSV of all the missing days in the list provided");
            Console.WriteLine("\t                AddedDaysReport: Creates a CSV of all the extra days in the list provided");
            Console.WriteLine("\t                   Parameters: -CSV:<Path to destination CSV file (file will be overwritten if it exists)");
            Console.WriteLine("\t                               [-FROM:<Start of date range (inclusive)> ]");
            Console.WriteLine("\t                               [-TO:<End of date range (inclusive)> ]");
            Console.WriteLine("\t                               [-N:<NonTradingDaysFile>. Defaults to NonTradingDays.csv in the application folder ]");
            Console.WriteLine("\t              [-D (debug)] [/?]");


        }

        static internal void Log(string s, params object[] args)
        {
            Console.WriteLine(String.Format(s, args));
            logFile.WriteLine(DateTime.Now.ToLongTimeString() + " - " + String.Format(s, args));
        }

        static internal void Debug(string s, params object[] args)
        {
            if (isDebug) Log("--> " + s, args);
            if (isDebug) logFile.WriteLine(DateTime.Now.ToLongTimeString() + " ------> " + String.Format(s, args));
        }
    }
}
