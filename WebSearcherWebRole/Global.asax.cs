using System;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Hosting;
using System.Web.Mvc;
using System.Web.Routing;
using WebSearcherCommon;

namespace WebSearcherWebRole
{

    public class WebSearcherApplication : HttpApplication
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private Task dequeueBackground;

        public static bool IsRetailBuild()
        {
#if DEBUG
            return false;
#else
			return true;
#endif
        }

        private static string version;
        public static string GetVersion()
        {
            if (version == null)
            {
                version = Assembly.GetExecutingAssembly().GetName().Version.ToString(3);
            }
            return version;
        }

        protected void Application_Start()
        {
#if DEBUG
            Trace.TraceInformation("WebSearcherApplication.Application_Start v" + GetVersion() + " on " + Environment.OSVersion.ToString());
#else
            Trace.TraceInformation("WebSearcherApplication.Application_Start v"+ GetVersion() + " on " + Environment.OSVersion.ToString());
#endif

            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            AreaRegistration.RegisterAllAreas();
            RouteConfig.RegisterRoutes(RouteTable.Routes);

            MvcHandler.DisableMvcResponseHeader = true;

            dequeueBackground = Task.Run(() =>
            {
                try
                {
                    StorageManager.DequeueAsync(cancellationTokenSource.Token).Wait(cancellationTokenSource.Token);
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    Trace.TraceError("StorageManager.DequeueAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                    if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                }
                Trace.TraceInformation("StorageManager.DequeueAsync finish");
            }, cancellationTokenSource.Token);
        }

        protected void Application_PreSendRequestHeaders()
        {
            Response.Headers.Remove("Server"); // event statics content thanks to runAllManagedModulesForAllRequests
            // Microsoft-HTTPAPI/2.0 Error 400 Bad Request - Invalid Hostname may still occure
        }

        protected void Application_Error(Object sender, EventArgs e)
        {
            Trace.TraceError("WebSearcherApplication.Application_Error : " + Server.GetLastError().GetBaseException().ToString());
#if DEBUG
            if (Debugger.IsAttached && !(Server.GetLastError() is HttpException)) { Debugger.Break(); }
#endif
        }

        private void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            if (e.ExceptionObject is Exception ex)
            {
                Trace.TraceError("WebSearcherApplication.CurrentDomain_UnhandledException : " + ex.GetBaseException().Message, ex);
            }
            else
            {
                Trace.TraceError("WebSearcherApplication.CurrentDomain_UnhandledException : NULL");
            }
#if DEBUG
            if (Debugger.IsAttached) { Debugger.Break(); }
#endif
        }

        protected void Application_Stop()
        {
            Trace.TraceWarning("WebSearcherApplication.Application_Stop : " + HostingEnvironment.ShutdownReason);
            cancellationTokenSource.Cancel();

            if (dequeueBackground != null)
            {
                if (!dequeueBackground.IsCompleted || !dequeueBackground.IsCanceled || !dequeueBackground.IsFaulted)
                    try
                    {
                        dequeueBackground.Wait(100, cancellationTokenSource.Token);
                    }
                    catch (OperationCanceledException) { }
                    catch (AggregateException) { }
                try
                {
                    dequeueBackground.Dispose();
                }
                catch (InvalidOperationException) { }
                dequeueBackground = null;
            }
        }

    }
}
