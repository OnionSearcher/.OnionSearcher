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
    public class RotManager : IDisposable
    {
        public const int Rotrc0Port = 12345;// TBD
        private static readonly object gcLock = new object();

        //public static async Task WaitFreePort(int port, CancellationToken cancellationToken)
        //{
        //    int wait = 0;
        //    while (IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners().Any(x => x.Port == port) && !cancellationToken.IsCancellationRequested && wait < 3000)
        //    {
        //        await Task.Delay(100, cancellationToken);
        //        wait++;
        //    }
        //    if (wait == 3000)
        //        throw new MustRecycleException("Port " + port + " not free after 30 sec");
        //}

        /// <summary>
        ///  Need to be done by task KillRot.cmd on real Azure Cloud : Runtime executionContext="elevated not enouth or don t work
        /// </summary>
        public static void TryKillTorIfRequired()
        {
            try
            {
                // sometime Tor is not well killed (at last in dev mode)
                foreach (var oldProcess in Process.GetProcessesByName("rot"))
                    oldProcess.Kill(); //permission issue on Azure may occur
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("RotManager.killTorIfRequired Exception : " + ex.GetBaseException().Message);  // No right usualy, simple message to keep.
            }
        }
        
        private bool hasStarted = false;

        private void OutputHandler(object sender, DataReceivedEventArgs e)
        {

            if (!String.IsNullOrWhiteSpace(e.Data))
            {

                if (e.Data.Contains("[err]"))
                {
                    Trace.TraceError("RotManager : " + e.Data);
                    // TOFIX Trace don't work on WebRole, use it if require : StorageManager.Contact("RotManager : " + e.Data);
#if DEBUG
                    if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                }
                else
                {
                    if (e.Data.Contains("Tor has successfully opened a circuit."))
                        hasStarted = true;
                    Trace.TraceInformation("RotManager : " + e.Data);
                    // TOFIX Trace don't work on WebRole, use it if require : StorageManager.Contact("RotManager : " + e.Data);
                }
            }
        }

        private void ErrorOutputHandler(object sender, DataReceivedEventArgs e)
        {
            if (!String.IsNullOrWhiteSpace(e.Data))
            {
                Trace.TraceError("RotManager : " + e.Data);
                // TOFIX Trace don't work on WebRole, use it if require : StorageManager.Contact("RotManager : " + e.Data);
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }
        
        private Process process;
        private static readonly string basePath = AppDomain.CurrentDomain.BaseDirectory;
        public RotManager(int i)
        {
#if !DEBUG      // else DEBUG will publish and take the production .onion !
            if (File.Exists(Path.Combine(basePath, @"ExpertBundle\hostname"))) // copy keyfiles, need to be writen else the azure cloud service won't have the right for rewrite the file (don't kwow why Tor rewrite the same file...)
            {
                File.Copy(Path.Combine(basePath, @"ExpertBundle\hostname"), Path.Combine(basePath, @"ExpertBundle\Data\hostname"));
                File.Copy(Path.Combine(basePath, @"ExpertBundle\private_key"), Path.Combine(basePath, @"ExpertBundle\Data\private_key"));
            }
#endif

            process = new Process()
            {
                StartInfo = new ProcessStartInfo
                {
                    WorkingDirectory = basePath, // changing that doesn't seems to work well with azure emulator
                    FileName = @"ExpertBundle\Rot\rot.exe",
                    Arguments = "-f \"" + Path.Combine(basePath, @"ExpertBundle\Data\rotrc" + i.ToString()) + "\" --defaults-torrc \"" + Path.Combine(basePath, @"ExpertBundle\Data\rotrc-defaults") + "\"", // full path not mandatory but avoid a warning...
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                }
            };
            process.OutputDataReceived += new DataReceivedEventHandler(OutputHandler);
            process.ErrorDataReceived += new DataReceivedEventHandler(ErrorOutputHandler);
            process.Start();
            // after start
            process.PriorityClass = ProcessPriorityClass.AboveNormal;
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
        }
  
        public async Task WaitStartAsync(CancellationToken cancellationToken)
        {
            // WaitStartedAsync
            while (!cancellationToken.IsCancellationRequested && !hasStarted)
            {
                await Task.Delay(100, cancellationToken);
            }
        }

        public bool IsProcessOk()
        {
            return process != null && !process.HasExited && process.Responding;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (process != null)
                    {
                        if (!process.HasExited)
                        {
                            try
                            {
                                process.Kill();
                                while (!process.HasExited)
                                {
                                    Task.Delay(200).Wait();
                                }
                            }
                            catch (Exception ex)
                            {
                                Trace.TraceWarning("RotManager.Dispose Exception : " + ex.GetBaseException().ToString());
                            }
                        }
                        process.Dispose();
                        process = null;
                    }
                }
                disposedValue = true;
            }
        }
        
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
        
    }
}