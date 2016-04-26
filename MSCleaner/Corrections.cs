using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using System.IO;
using System.Diagnostics;
using System.Data;


namespace PricesConvert
{
    public enum CorrectionType
    {
        RemoveDate,
        AddDate,
        CorrectPrices
    }

    [XmlRoot]
    public class TickerCorrections
    {
       // private const string CSVHeadings = "Symbol,Date,Type,Open,High,Low,Close,Vol";

        private static TickerCorrections _instance;

        public static TickerCorrections Instance
        {
            get
            {
                if (_instance == null)
                {
                    string filePath = MySettings.GetPath(MySettings.User.CorrectionsFile);
                    if (File.Exists(filePath))
                    {
                        _instance = LoadCSV(filePath);
                    }
                    else
                    {
                        _instance = new TickerCorrections();
                    }
                }
                return _instance;
            }
        }

        public static TickerCorrections LoadCSV(string filePath)
        {
            TickerCorrections tc = new TickerCorrections();

            if (File.Exists(filePath))
            {
                Program.Log("Loading corrections file from {0}", filePath);
                StreamReader sr = new StreamReader(filePath);
                List<string> columns = MySettings.App.CorrectionsColumns.Split(',').ToList();
                string line = sr.ReadLine(); // read in the headers

                try
                {
                    
                    while (!sr.EndOfStream)
                    {
                        line = sr.ReadLine();
                        if (line.Trim().Length == 0) break;
                        Price p = new Price(line, columns);
                        tc.Add(new TickerCorrection(p));
                    }
                }
                catch (Exception ex)
                {
                    Program.Debug("Error parsing string: {0}", ex.Message);
                    throw new Exception(String.Format("Unable to load corrections file because of error {0}. Please see log file for more information.", ex.Message));
                }
                finally
                {
                    sr.Close();
                    sr = null;
                    Trace.Unindent();
                }
            }
            else
            {
                throw new Exception(String.Format("The corrections file specified ({0}) does not exist or you do not have access to read it", filePath));
            }
            return tc;


        }

        public static void Save()
        {
            Program.Log("Attempting to save the corrections file");
            try
            {
                string tmpFile = Path.GetTempFileName();
                StreamWriter sw = new StreamWriter(tmpFile);
                sw.WriteLine(MySettings.App.CorrectionsColumns);
                foreach (TickerCorrection tc in _instance.Corrections)
                {
                    sw.WriteLine(tc.ToCSV());
                }
                sw.Close();
                Program.Log("Corrections file written. Replacing the current version");
                File.Delete(MySettings.GetPath(MySettings.User.CorrectionsFile));
                File.Move(tmpFile, MySettings.GetPath(MySettings.User.CorrectionsFile));

            }
            catch (Exception ex)
            {
                Program.Log("There was an error writing the corrections file: {0}", ex.Message);
                throw ex;
            }

        }

        public static void DeleteDay(DateTime tickerDate, string symbol)
        {
            CorrectDay(tickerDate, symbol, 0, 0, 0, 0, 0, CorrectionType.RemoveDate);
        }

        public static void CorrectDay(DateTime tickerDate, string symbol, Single open, Single high, Single low, Single close, Single vol, CorrectionType corrType)
        {
            TickerCorrections correctionsList = Instance;
            TickerCorrection existing = correctionsList.Corrections.Where(c => c.PriceData.Symbol == symbol && c.PriceData.TickerDate == tickerDate).FirstOrDefault();
            if (existing == null)
            {
                Program.Debug("Adding new correction item for ticker {0} on date {1}", symbol, tickerDate);
                correctionsList.Add(new TickerCorrection(symbol, tickerDate, corrType, open, high, low, close, vol));
            }
            else
            {
                Program.Debug("Updating existing correction item for ticker {0} on date {1}", symbol, tickerDate);
                existing.PriceData.Open = open;
                existing.PriceData.Close = close;
                existing.PriceData.High = high;
                existing.PriceData.Low = low;
                existing.PriceData.Volume = vol;
            }
        
         
        }

        

        public static void CreateTest()
        {
            List<TickerCorrection> c = new List<TickerCorrection>();
            c.Add(new TickerCorrection("sdfsd", DateTime.Parse("12-May-2011"), CorrectionType.AddDate, 123, 150, 110, 135, 34545));
            System.Xml.Serialization.XmlSerializer x = new System.Xml.Serialization.XmlSerializer(c.GetType());
            StreamWriter sw = new StreamWriter("C:\\Corrections.xml");
            x.Serialize(sw, c);

        }
        public List<TickerCorrection> Corrections = new List<TickerCorrection>();

        internal void Add(TickerCorrection c)
        {
            Corrections.Add(c);
        }

        public class TickerCorrection
        {
            public TickerCorrection() { }

            public TickerCorrection(Price p, CorrectionType corrType)
            {
                PriceData = p;
                Type = corrType;
            }

            public string ToCSV()
            {
                return this.PriceData.ToCSV();
            }
       
            public TickerCorrection(Price p)
            {
                PriceData = p;
                string tmpType = "";
                if (p.ExtendedProperties.TryGetValue("Type", out tmpType))
                {
                    Type =(CorrectionType)Enum.Parse(typeof(CorrectionType), tmpType);
                }
            }

            public TickerCorrection(string symbol, DateTime date, CorrectionType type, Single open, Single high, Single low, Single close, Single vol)
            {
                this.PriceData = new Price();

                this.PriceData.Symbol = symbol;
                this.PriceData.TickerDate = date;
                this.Type = type;
                this.PriceData.ExtendedProperties.Add("Type", type.ToString());
                this.PriceData.Open = open;
                this.PriceData.High = high;
                this.PriceData.Low = low;
                this.PriceData.Close = close;
                this.PriceData.Volume = vol;
            }

            public Price PriceData;
            public CorrectionType Type { get; set; }

        }
    }



}
