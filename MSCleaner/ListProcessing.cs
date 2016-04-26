using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using System.ComponentModel;
using System.Diagnostics;


namespace PricesConvert
{
    public enum ListLoadBehaviour
    {
        /// <summary>
        /// Load the list only with no tickers
        /// </summary>
        LoadListsOnly,
        /// <summary>
        /// Load the tickers list from the CSV file
        /// </summary>
        LoadTickersFromCSV,
        /// <summary>
        /// Load the ticker data from the MS files
        /// </summary>
        LoadTickerData,
        /// <summary>
        /// Load the MS files and the ticker list CSV and check for mismatches
        /// </summary>
        LoadBothAndCompare

    }

    class ListProcessing
    {
        const string cstrListMasterHeader = "ListName,OutputPath,TickerListFile,UseCSVUpdates,AddExtraDay,UseCorrections,StartDate,EndDate";

        public static string CreateBlankList()
        {
            string listsPath = MySettings.GetPath(MySettings.User.ListsFile);
            StreamWriter sw = new StreamWriter(listsPath);
            sw.WriteLine(cstrListMasterHeader);
            sw.Close();
            return listsPath;
        }

         public static bool SaveList(ref TickerList selected, string listName, string newName, string outputPath, string tickersPath, bool useCSVUpdates, bool useCorrections, bool addExtraDays, DateTime? fromDate, DateTime? toDate)
        {
            BindingList<TickerList> configuredLists = LoadTickerLists(ListLoadBehaviour.LoadListsOnly);
            TickerList origList = configuredLists.Where(l => l.ListName == listName).First();
            if (selected == null)
            {
                TickerList newList = new TickerList(newName, outputPath, tickersPath, useCSVUpdates, useCorrections, addExtraDays, fromDate, toDate);
                configuredLists.Add(newList);
            }
            else
            {
                selected.ListName = newName;
                selected.MSFolder = outputPath;
                selected.startDate = fromDate;
                selected.endDate = toDate;
                selected.ListFilePath = tickersPath;
                selected.useCSVUpdates = useCSVUpdates;
                selected.useCorrections = useCorrections;
                selected.addExtraDays = addExtraDays;
                //origList = selected;
            }
            configuredLists.Remove(origList);
            configuredLists.Add(selected);
            return SaveListMaster(configuredLists);
        }


         public static bool  SaveListMaster(BindingList<TickerList> myLists)
         {
             string tmpPath = Path.GetTempFileName();
             StreamWriter sw = new StreamWriter(tmpPath);
             sw.WriteLine(cstrListMasterHeader);
             foreach (TickerList l in myLists)
             {
                 sw.WriteLine(String.Format("{0},{1},{2},{3},{4},{5},{6:dd-MMM-yyyy},{7:dd-MMM-yyyy}", l.ListName, l.MSFolder, l.ListFilePath, l.useCSVUpdates, l.addExtraDays, l.useCorrections, l.startDate, l.endDate));
             }
             sw.Close();
             try
             {
                 File.Delete(MySettings.GetPath(MySettings.User.ListsFile));
                 File.Move(tmpPath, MySettings.GetPath(MySettings.User.ListsFile));
                 return true;
             }
             catch (Exception ex)
             {
                 Utilities.MsgBox("Unable to save List Master file! The reason is: " + ex.Message);
                 return false;
             }

         }

         public static BindingList<TickerList> LoadTickerLists(ListLoadBehaviour loadBehaviour)
         {
             BindingList<TickerList> ret = new BindingList<TickerList>();
             Program.Log("Loading lists master file");
             DataTable lists = Utilities.LoadCSV(MySettings.GetPath(MySettings.User.ListsFile), cstrListMasterHeader, "");
             foreach (DataRow r in lists.Rows)
             {
                 TickerList tl = new TickerList(r);
                 switch (loadBehaviour)
                 {
                     case ListLoadBehaviour.LoadTickerData:
                         tl.LoadTickers();
                         break;
                     case ListLoadBehaviour.LoadTickersFromCSV:
                         tl.LoadTickerList();
                         break;
                     case ListLoadBehaviour.LoadBothAndCompare:
                         tl.LoadTickers();
                         List<string> listTickers = tl.Tickers.Select(t => t.TickerSymbol).ToList();
                         List<string> fileTickers = Utilities.LoadTickersFromCSV(tl.ListFilePath);
                         
                         
                         if (!listTickers.All(x => fileTickers.Contains(x)) || listTickers.Count != fileTickers.Count )
                         {
                             // one or more of the tickers has changed. 
                             tl.hasMismatch = true;
                             tl.LoadTickerList();
                         }
                         break;
                 }
                 ret.Add(tl);
             }
             return ret;
         }



         internal static bool RunFullRefresh(BindingList<TickerList> lists, ToolStripStatusLabel lbl, ToolStripProgressBar prgBar, Form f)
         {
             List<string> tickerSymbols = new List<string>();
             Program.Log(String.Format("Running full refresh"));
             decimal dayTolerance = MySettings.User.DailyTolerance;
             Program.Log("Iterating through all tickers");
             Trace.Indent();
             try
             {
                 int totalTickers = lists.SelectMany(l => l.Tickers).Count();
                 prgBar.Visible = true;
                 prgBar.Value = 0;
                 prgBar.Maximum = totalTickers;

                 foreach (TickerList l in lists)
                 {
                     l.hasChanged = true;
                     Program.Log("Processing ticker list {0}", l.ListName);
                     foreach (Ticker t in l.Tickers)
                     {
                         t.ClearPrices();
                         lbl.Text = String.Format("Processing ticker {0}", t.TickerSymbol);
                         prgBar.Value++;
                         f.Refresh();
                         
                         Program.Log("Getting prices for ticker " + t.TickerSymbol);
                         List<Price> prices = DB.GetPrices(t.TickerSymbol, l.startDate, l.endDate, l.useCSVUpdates);
                         Program.Log("Adding price data to dataset");


                         t.AddPrices(prices, MySettings.GetPath(MySettings.User.PricesLocation));
                     }

                     if (l.useCorrections)
                     {
                         Program.Log("List {0} is configured to apply corrections. Doing it now", l.ListName);
                         l.ApplyCorrections();
                     }
                     else
                     {
                         Program.Log("List {0} is NOT configured to apply corrections. Skipping", l.ListName);
                     }
                     l.ErrorCheck((float)dayTolerance);
                     l.WriteList(false);
                 }

             }
             catch (Exception e)
             {
                 MessageBox.Show(String.Format("There was an error running the refresh: {0}. Please see the log for more details.", e.Message));
                 Program.Log("Error running full refresh: " + e.Message);
                 Program.Debug(e.StackTrace);
                 return false;
             }
             finally
             {
                 prgBar.Visible = false;
                 Trace.Unindent();
                 
             }
             lbl.Text = "";
             prgBar.Visible = false;
             Errors.WriteToFile(true);
             return true;
         }

         internal static bool RunUpdates(BindingList<TickerList> lists, ToolStripStatusLabel lbl, ToolStripProgressBar prgBar, Form f, out int numberUpdates)
         {


             string sourceFolder = MySettings.GetPath(MySettings.User.UpdatesFolder);
             bool recursive = MySettings.App.RecurseUpdatesFolder;
             List<Price> results = new List<Price>();
             List<string> updateFiles = new List<string>();

             decimal dayTolerance = MySettings.User.DailyTolerance;

             string pricesDbf = MySettings.App.UpdatesDBFName;
             ICSharpCode.SharpZipLib.Zip.FastZip zipFile = new ICSharpCode.SharpZipLib.Zip.FastZip();
             string tempFolder = System.IO.Path.GetTempPath();
             string tempFile = Path.Combine(tempFolder, pricesDbf);

             Trace.WriteLine(String.Format("Running updates from {0}, recursive = {1}", sourceFolder, recursive));
             GetUpdateFiles(sourceFolder, recursive, ref updateFiles);
             Program.Log("Number of update files found = {0}", updateFiles.Count);
             prgBar.Maximum = updateFiles.Count * lists.Count;
             prgBar.Value = 0;

             numberUpdates = 0;

             foreach (TickerList tl in lists)
             {
                 List<string> symbols = tl.Symbols;
                 Program.Log("Running updates for list {0} between dates {1} and {2}", tl.ListName, tl.startDate, tl.endDate);
                 Trace.Indent();
                 foreach (string filePath in updateFiles)
                 {
                     Program.Log(String.Format("Processing file {0}", filePath));
                     prgBar.Value++;
                     lbl.Text = "Processing file " + Path.GetFileName(filePath);
                     f.Refresh();
                     zipFile.ExtractZip(filePath, tempFolder, pricesDbf);
                     results = DB.GetAllUpdates(tempFolder, pricesDbf, symbols,tl.startDate, tl.endDate);
                     numberUpdates += results.Count;
                     if (results.Count > 0)
                     {
                         tl.AddPriceUpdates(results, filePath);
                     }
                 }
                 Trace.Unindent();
                 if (tl.useCorrections)
                 {
                     Program.Log("List {0} is configured to apply corrections. Doing it now", tl.ListName);
                     tl.ApplyCorrections();
                 }
                 else
                 {
                     Program.Log("List {0} is NOT configured to apply corrections. Skipping", tl.ListName);
                 }
                 tl.hasChanged =  (numberUpdates > 0);
                 tl.ErrorCheck((float)dayTolerance);
                 
                 tl.WriteList(true);
             }
             lbl.Text = "";
             prgBar.Visible = false;
             Errors.WriteToFile(false);
             return true;
         }


         /// <summary>
         /// Gets a list of update EXE files from the updates folder
         /// </summary>
         /// <param name="sourceFolder"></param>
         /// <param name="isRecursive"></param>
         /// <param name="files"></param>
         private static void GetUpdateFiles(string sourceFolder, bool isRecursive, ref List<string> files)
         {
             if (isRecursive)
                 foreach (string folder in Directory.GetDirectories(sourceFolder))
                     GetUpdateFiles(folder, isRecursive, ref files);

             foreach (string file in Directory.GetFiles(sourceFolder, "IE*.EXE"))
             {
                 files.Add(file);
             }

         }







    }
}
