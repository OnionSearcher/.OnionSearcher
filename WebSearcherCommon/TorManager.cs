using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace WebSearcherCommon
{
    /// <summary>
    /// Called from the Role, not in the same space than the IIS process, don't mix theses call (not the same BaseDirectory)
    /// </summary>
    public static class TorManager
    {

        internal static void OutputHandler(object sender, DataReceivedEventArgs e)
        {
            if (!String.IsNullOrWhiteSpace(e.Data))
            {

                if (e.Data.Contains("[warn]") && !e.Data.Contains(" is relative"))
                {
                    Trace.TraceWarning("TorManager : " + e.Data);
#if DEBUG
                    if (Debugger.IsAttached) { Debugger.Break(); } // sometime Tor stay up between debug session, remeber to kill him if required
#endif
                }
                else if (e.Data.Contains("[err]"))
                {
                    Trace.TraceError("TorManager : " + e.Data);
#if DEBUG
                    if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                }
                else
                {
                    if (e.Data.Contains("Tor has successfully opened a circuit."))
                        hasStarted = true;
                    Trace.TraceInformation("TorManager : " + e.Data);
                }
            }
        }
        internal static void ErrorOutputHandler(object sender, DataReceivedEventArgs e)
        {
            if (!String.IsNullOrWhiteSpace(e.Data))
            {
                Trace.TraceError("TorManager : " + e.Data);
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        static private Process torProcess;
        private static bool hasStarted;

        public static bool Start()
        {
            try
            {
                Trace.TraceInformation("TorManager.Start");
                hasStarted = false;

                // sometime Tor is not well killed (at last in dev mode)
                foreach (var process in Process.GetProcessesByName("tor"))
                {
                    process.Kill();
                }

                torProcess = new Process()
                {
                    StartInfo = new ProcessStartInfo
                    {
                        WorkingDirectory = AppDomain.CurrentDomain.BaseDirectory, // changing that doesn't seems to work well with azure emulator
                        FileName = "TorExpertBundle\\Tor\\tor.exe", // le WorkingDirectory ne marchaint pas trop en fait...
                        Arguments = "--defaults-torrc \"" + Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "torrc-defaults") + "\" -f \"" + Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "torrc") + "\" --ignore-missing-torrc", // full path not mandatory but avoid a warning...
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        CreateNoWindow = true
                    }
                };

                torProcess.OutputDataReceived += new DataReceivedEventHandler(OutputHandler);
                torProcess.ErrorDataReceived += new DataReceivedEventHandler(ErrorOutputHandler);

                torProcess.Start();
                torProcess.PriorityClass = ProcessPriorityClass.AboveNormal;

                torProcess.BeginOutputReadLine();
                torProcess.BeginErrorReadLine();
            }
            catch (Exception ex)
            {
                Trace.TraceError("TorManager.Start Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                //return false;
            }
            return true;
        }


        public static async Task WaitStartedAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && ! hasStarted)
            {
                await Task.Delay(100, cancellationToken);
            }
        }

        public static void Stop()
        {
            try
            {
                if (torProcess != null)
                {
                    torProcess.Close();
                }
                torProcess = null;
            }
            catch (Exception ex)
            {
                Trace.TraceError("TorManager.Stop Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        public static void TraceHostname()
        {
            try
            {
                string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TorExpertBundle\\Data\\hostname");
                if (File.Exists(path))
                {
                    string hostname = File.ReadAllText(path).TrimEnd();
                    Trace.TraceWarning("TorManager.TraceHostname : " + hostname); // check ouput console in debug emulator mode
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("TorManager.TraceHostname Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }
        
        public static bool IsProcessOk()
        {
            return torProcess != null && torProcess.Responding && !torProcess.HasExited;
        }

        public static bool IsTor(this Uri uri)
        {
            return uri != null
                    && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps)
                    && uri.DnsSafeHost.EndsWith(".onion") && uri.DnsSafeHost.Length == 22;
        }

    }
}