using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.IO;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Diagnostics;
using System.Windows.Forms;

namespace PricesConvert
{
    class Utilities
    {
        public static DialogResult MsgBox(string line, MessageBoxButtons btns)
        {
            return MessageBox.Show(line, Application.ProductName, btns);
        }

        public static string GetCSVFileName(string symbolName, string folder)
        {
            return Path.Combine(folder, symbolName.Replace(".", "").Trim() + ".csv");
        }

        public static DialogResult MsgBox(string line)
        {
            return MessageBox.Show(line, Application.ProductName);
        }

        public static DialogResult MsgBox(string line, MessageBoxButtons btns, MessageBoxIcon icon)
        {
            return MessageBox.Show(line, Application.ProductName, btns, icon);
        }

        internal static string GetLocalPath(string objectName)
        {
           return Path.Combine(Path.GetDirectoryName(Application.ExecutablePath), objectName);

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
                    File.Delete(f);
                }
            }
        }

        public static List<string> ParseCSVLine(string rawData, char delimiter)
        {
            List<string> ret = new List<string>();
            bool inQuotes = false;

            string curr = string.Empty;
            char[] s = rawData.ToCharArray();
            for (int i = 0; i < rawData.Length; i++)
            {
                switch (s[i])
                {
                    case '"': inQuotes = !inQuotes;
                        break;
                    case ',': if (!inQuotes)
                        {
                            ret.Add(curr);
                            curr = String.Empty;

                        }
                        else
                        {
                            curr += s[i];
                        }
                        break;
                    default:
                        curr += s[i];
                        break;
                }
            }
            ret.Add(curr);
            return ret;



        }



        public static string OpenFolder(string defaultVal, string caption)
        {
            FolderBrowserDialog fld = new FolderBrowserDialog();
            fld.Description = caption;
            fld.SelectedPath = defaultVal;
            if (fld.ShowDialog() == DialogResult.OK)
            {
                defaultVal = fld.SelectedPath;
            }
            return defaultVal;
        }

        public static DateTime GetNextTradingDay(DateTime basisDate)
        {
            DateTime nextDay = basisDate;
            do
            {
                nextDay = nextDay.AddDays(1);
            } while (!Utilities.IsTradingDay(nextDay));
            return nextDay;
            
        }

        public static bool IsTradingDay(DateTime d)
        {
            if (d.DayOfWeek == DayOfWeek.Saturday || d.DayOfWeek == DayOfWeek.Sunday)
            {
                return false;
            }
            
            return NonTradingDays.IsTradingDay(d);
        }

        public static string OpenFile(string filter, string defaultExt, string defaultVal, string windowTitle)
        {
            OpenFileDialog openFile = new OpenFileDialog();
            openFile.DefaultExt = defaultExt;
            // The Filter property requires a search string after the pipe ( | )
            openFile.Filter = filter;
            openFile.Title = windowTitle;
            if (defaultVal != "")
            {
                openFile.InitialDirectory = Path.GetDirectoryName(defaultVal);
            }
            openFile.Multiselect = false;
            if (openFile.ShowDialog() == DialogResult.OK)
            {
                if (openFile.FileNames.Length > 0)
                {
                    return openFile.FileName;
                }
            }
            return defaultVal;
        }

        public static string GenerateSQL(string sourceFile)
        {
            return GenerateSQL(sourceFile, new List<string>(), DateTime.MinValue, DateTime.MaxValue);
        }

        public static string GenerateSQL(string sourceFile, List<string> symbols, DateTime? fromDate, DateTime? toDate)
        {
            string sql = "SELECT epic, date,  CAST(open AS NUMERIC(11,3)) AS open, CAST(close AS NUMERIC(11,3)) AS close, CAST(high AS NUMERIC(11,3)) AS high, CAST(low AS NUMERIC(11,3)) AS low, vol AS volume FROM " + sourceFile;
            List<string> filters = new List<string>();

            string symbolFilter = "";
            for (int n = 0; n < symbols.Count; n++)
            {
                if (n > 0)
                    symbolFilter += " OR ";
                symbolFilter += String.Format("epic = '{0}'", symbols[n]);
            }

            if (symbolFilter.Length > 0)  filters.Add(String.Format("({0})", symbolFilter));
//            if (fromDate.HasValue)             filters.Add(String.Format("date >= CTOD('{0:MM/dd/yyyy}')", fromDate));
 //           if (toDate.HasValue) filters.Add(String.Format("date <= CTOD('{0:MM/dd/yyyy}')", toDate));

            if (fromDate.HasValue) filters.Add(String.Format("date >= DATE({0:yyyy,MM,dd})", fromDate));
            if (toDate.HasValue) filters.Add(String.Format("date <= DATE({0:yyyy,MM,dd})", toDate));

            for (int n = 0; n < filters.Count; n++)
            {
                if (n == 0)
                    sql += " WHERE ";
                else
                    sql += " AND ";

                sql += filters[n];

            }
            return sql;
        }

        public static List<string> LoadTickersFromCSV(string sourceFile)
        {
            List<TickerAndName> tnList = LoadTickerAndNamesFromCSV(sourceFile);
            return tnList.Select(s => s.Symbol).ToList();
        }

        public static List<TickerAndName> LoadTickerAndNamesFromCSV(string sourceFile)
        {
            List<TickerAndName> ret = new List<TickerAndName>();
            if (!File.Exists(sourceFile))
            {
                Trace.WriteLine(String.Format("The file {0} does not exist", sourceFile));
                return ret;
            }

            StreamReader sr = new StreamReader(sourceFile);
            while (!sr.EndOfStream)
            {
                string[] line = sr.ReadLine().Trim().Split(',');
                TickerAndName tn = new TickerAndName();
                tn.Symbol = Unquote(line[0]);
                if (line.Length > 1)
                {
                    tn.Name = Unquote(line[1]);
                }
                else
                {
                    tn.Name = tn.Symbol;
                }
                ret.Add(tn);
            }
            return ret;
        }

        private static string Unquote(string s)
        {
            if (s.StartsWith("\"") && s.EndsWith("\""))
            {
                return s.Substring(1, s.Length - 2);
            }
            return s;
        }

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


    }

    class MyListener : TextWriterTraceListener
    {
        public MyListener(string fileName)
            : base(fileName)
        {
        }
           
         protected override void WriteIndent() {
           
            }

        // Custom implementation
        public override void WriteLine(string message)
        {
            base.WriteLine(string.Format("{0:yyyy-MM-dd HH:mm:ss} - {1}{2}", DateTime.Now, new String(' ', this.IndentLevel * this.IndentSize), message));
        }

        
    }

}
