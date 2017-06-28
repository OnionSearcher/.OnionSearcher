using System;
using System.Collections.Specialized;
using System.IO;
using System.Threading;
using GetPretyOnionName.Properties;
using WebSearcherCommon;

namespace GetPretyOnionName
{
    class Program
    {

        private static readonly string pathHostname = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TorExpertBundle\\Data\\hostname");
        private static readonly string pathPrivate_key = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TorExpertBundle\\Data\\private_key");
        private static readonly StringCollection searched = Settings.Default.Searched;

        static void Main(string[] args)
        {
            try
            {
                bool found = false;
                while (true)
                {
                    TorManager.StartAsync(new CancellationToken()).Wait();

                    do
                    {
                        Thread.Sleep(50);
                    } while (!File.Exists(pathHostname) && !File.Exists(pathPrivate_key));

                    TorManager.Stop();
                    Thread.Sleep(10); // limit FileNotFoundException

                    try
                    {
                        string hostname = File.ReadAllText(pathHostname).TrimEnd();
                        Console.WriteLine(hostname);

                        foreach (string str in searched)
                        {
                            if (hostname.StartsWith(str))
                                found = true;
                        }

                        if (!found)
                        {
                            File.Delete(pathHostname);
                            File.Delete(pathPrivate_key);
                        }
                        else
                        {
                            Console.WriteLine(File.ReadAllText(pathPrivate_key).TrimEnd());
                            Console.ReadLine(); // pause
                            break;
                        }
                    }
                    catch (FileNotFoundException)
                    {
                        Console.WriteLine("FileNotFoundException");
                        if (File.Exists(pathHostname)) File.Delete(pathHostname);
                        if (File.Exists(pathPrivate_key)) File.Delete(pathPrivate_key);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException().ToString());
                Console.ReadLine(); // pause
            }

        }

    }
}
