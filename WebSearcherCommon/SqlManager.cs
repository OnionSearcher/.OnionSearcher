using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace WebSearcherCommon
{
    /// <summary>
    /// Called from the Role, not in the same space than the IIS process, don't mix theses call (not the same BaseDirectory)
    /// </summary>
    public class SqlManager : IDisposable
    {

        #region Common

        public const int MaxSqlIndex = 450;
        public const int MaxSqlText = 1 * 1024 * 1024;

        public SqlManager() { }

        private SqlConnection conn;
        private void CheckOpen()
        {
            if (conn == null)
                conn = new SqlConnection(ConfigurationManager.ConnectionStrings["sqlDb"].ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open(); // todo catch connection timeout et looper !
        }
        private async Task CheckOpenAsync(CancellationToken cancellationToken)
        {
            if (conn == null)
                conn = new SqlConnection(ConfigurationManager.ConnectionStrings["sqlDb"].ConnectionString);
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync(cancellationToken); // todo catch connection timeout et looper !
        }

        #endregion Common

        #region WebSearcherWebRole

        public SearchResultEntity GetSearchResult(string keywords, short page, bool isFull)
        {
            CheckOpen();

            SearchResultEntity ret = new SearchResultEntity()
            {
                Results = new List<PageResultEntity>()
            };
            using (DataSet ds = new DataSet())
            {
                using (SqlCommand cmd = new SqlCommand("Search2", conn))
                {
                    cmd.Parameters.Add("@Keywords", SqlDbType.NVarChar, 64).Value = keywords;
                    cmd.Parameters.Add("@Page", SqlDbType.SmallInt).Value = page - 1; // 0 based
                    cmd.Parameters.Add("@Full", SqlDbType.SmallInt).Value = isFull; // 0 based
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                    {
                        da.Fill(ds);
                    }
                }

                ret.ResultsTotalNb = (int)ds.Tables[0].Rows[0][0];
                foreach (DataRow dr in ds.Tables[1].Rows)
                {
                    PageResultEntity pe = new PageResultEntity()
                    {
                        UrlClick = "/?url=" + WebUtility.UrlEncode(dr[0].ToString()),
                        Url = dr[0].ToString(),
                        Title = dr[1].ToString(),
                        InnerText = dr[2].ToString(),
                        CrawleError = (dr[3] as short?) > 0,
                        DaySinceLastCrawle = (int)dr[4],
                        HourSinceLastCrawle = (int)dr[5],
                        HiddenServiceMain = dr[6] as string
                    };
                    if (pe.Title.Length > 70)
                    {
                        pe.TitleToolTip = pe.Title;
                        pe.Title = pe.Title.Substring(0, 67).Replace("\"", "&quot;") + "...";
                    }
                    if (pe.HiddenServiceMain != null) // We keep it only on root HD in DB, not needed for display
                    {
                        pe.HiddenServiceMain = pe.HiddenServiceMain.Substring(0, pe.Url.Length - 1);
                        pe.HiddenServiceMainClick = "/?url=" + WebUtility.UrlEncode(pe.HiddenServiceMain);
                    }
                    if (pe.Url.EndsWith("/")) // We keep it only on root HD in DB, not needed for display
                    {
                        pe.Url = pe.Url.Substring(0, pe.Url.Length - 1);
                    }
                    else if (pe.Url.Length > 115) // 120 may cause linebreak with tag
                    {
                        pe.UrlToolTip = pe.Url;
                        pe.Url = pe.Url.Substring(0, 117).Replace("\"", "&quot;") + "...";
                    }
                    ret.Results.Add(pe);

                }
            }
            return ret;
        }

        public void PostContactMessage(string message)
        {
            CheckOpen();

            using (SqlCommand cmd = new SqlCommand("INSERT INTO ContactMessages VALUES (SYSUTCDATETIME(), @msg)", conn))
            {
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Add("@msg", SqlDbType.NVarChar, 4000).Value = message;
                cmd.ExecuteNonQuery();
            }
        }

        public HashSet<string> GetStopWords()
        {
            CheckOpen();

            using (SqlCommand cmd = new SqlCommand("SELECT stopword FROM sys.fulltext_stopwords WHERE language_id=1033 AND stoplist_id=(SELECT stoplist_id FROM sys.fulltext_stoplists)", conn))
            {
                cmd.CommandType = CommandType.Text;

                HashSet<string> ret = new HashSet<string>();
                using (SqlDataReader reader = cmd.ExecuteReaderAsync().Result)
                {
                    while (reader.Read())
                        ret.Add(reader.GetSqlValue(0).ToString());
                }
                return ret;
            }
        }
        
        public void CrawleRequestEnqueue(string url)
        {
            CheckOpen();
            
            using (SqlCommand cmd = new SqlCommand("CrawleRequestEnqueue", conn))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@url", SqlDbType.NVarChar, 450).Value = url;
                cmd.Parameters.Add("@prio", SqlDbType.SmallInt).Value = 1;

                cmd.ExecuteNonQuery();
            }
        }

        #endregion WebSearcherWebRole

        #region WebSearcherWorkerRole
        /*
        public async Task<bool> CheckIfCanCrawlePageAsync(string url, string hiddenService, CancellationToken cancellationToken)
        {
            if (url == null) throw new ArgumentNullException("url");
            if (url.Length > MaxSqlIndex) url = url.Substring(0, MaxSqlIndex);

            await CheckOpenAsync(cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand("CanCrawle", conn))
                {
                    cmd.Parameters.Add("@Url", SqlDbType.NVarChar, 450).Value = url;
                    cmd.Parameters.Add("@HiddenService", SqlDbType.NVarChar, 37).Value = hiddenService;
                    cmd.CommandType = CommandType.StoredProcedure;

                    SqlParameter outputRet = new SqlParameter("@ret", SqlDbType.SmallInt)
                    {
                        Direction = ParameterDirection.Output
                    };
                    cmd.Parameters.Add(outputRet);

                    await cmd.ExecuteNonQueryAsync(cancellationToken);

                    short? ret = outputRet.Value as short?;
                    return ret.HasValue ? (ret.Value == 1) : false;
                }
            else
                return false;
        }
        */

        public async Task CrawleRequestEnqueueAsync(string url, short prio, CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand("CrawleRequestEnqueue", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@url", SqlDbType.NVarChar, 450).Value = url;
                    cmd.Parameters.Add("@prio", SqlDbType.SmallInt).Value = prio;

                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
        }

        public async Task<string> CrawleRequestDequeueAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand("CrawleRequestDequeue", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    SqlParameter outputRet = new SqlParameter("@Url", SqlDbType.NVarChar, 450)
                    {
                        Direction = ParameterDirection.Output
                    };
                    cmd.Parameters.Add(outputRet);

                    await cmd.ExecuteNonQueryAsync(cancellationToken);

                    return outputRet.Value as string;
                }
            else
                return null;
        }


        public async Task UrlPurge(string url, CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            if (!cancellationToken.IsCancellationRequested && !string.IsNullOrWhiteSpace(url))
                using (SqlCommand cmd = new SqlCommand("DELETE FROM Pages WHERE Url=@str", conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Add("@str", SqlDbType.NVarChar, 450).Value = url;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
        }

        public async Task PageInsertOrUpdateOk(PageEntity page, CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);
            if (page == null) throw new ArgumentNullException("page");
            if (page.HiddenService.Length > MaxSqlIndex) page.HiddenService = page.HiddenService.Substring(0, MaxSqlIndex);
            if (page.Url.Length > MaxSqlIndex) page.Url = page.Url.Substring(0, MaxSqlIndex);
            if (page.Title.Length > MaxSqlIndex) page.Title = page.Title.Substring(0, MaxSqlIndex);
            if (page.Heading.Length > MaxSqlIndex) page.Title = page.Heading.Substring(0, MaxSqlIndex);
            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand(@"MERGE Pages AS target USING (SELECT @HiddenService,@Url,@Title,@LastCrawle,@Heading) AS source (HiddenService,Url,Title,LastCrawle,Heading) ON (target.Url = source.Url)
WHEN MATCHED THEN UPDATE SET HiddenService=source.HiddenService,Url=source.Url,Title=source.Title,LastCrawle=source.LastCrawle,CrawleError=NULL,Heading=source.Heading
WHEN NOT MATCHED THEN INSERT (HiddenService,Url,Title,FirstCrawle,LastCrawle,Heading) VALUES (source.HiddenService,source.Url,source.Title,source.LastCrawle,source.LastCrawle,source.Heading);", conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Add("@HiddenService", SqlDbType.NVarChar, 37).Value = page.HiddenService;
                    cmd.Parameters.Add("@Url", SqlDbType.NVarChar, 450).Value = page.Url;
                    cmd.Parameters.Add("@Title", SqlDbType.NVarChar, 450).Value = page.Title;
                    cmd.Parameters.Add("@LastCrawle", SqlDbType.DateTime2).Value = page.LastCrawle;
                    cmd.Parameters.Add("@Heading", SqlDbType.NVarChar, 450).Value = page.Heading;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }

            if (page.OuterHdLinks.Count > 0 && !cancellationToken.IsCancellationRequested)
            {
                string sqlSelect = "SELECT @f,'" + string.Join("' UNION SELECT @f,'", page.OuterHdLinks) + "'"; // check SQL Injection before !
                using (SqlCommand cmd = new SqlCommand(@"MERGE HiddenServiceLinks AS target USING (" + sqlSelect + @") AS source (HiddenService,HiddenServiceTarget) ON (target.HiddenService=source.HiddenService AND target.HiddenServiceTarget=source.HiddenServiceTarget)
WHEN NOT MATCHED THEN INSERT (HiddenService,HiddenServiceTarget) VALUES (source.HiddenService,source.HiddenServiceTarget);", conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Add("@f", SqlDbType.NVarChar, 37).Value = page.HiddenService;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }

            if (page.InnerText.Length > MaxSqlText) page.InnerText = page.InnerText.Substring(0, MaxSqlText);
            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand("UPDATE Pages SET InnerText=@InnerText WHERE Url=@Url", conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Add("@Url", SqlDbType.NVarChar, 450).Value = page.Url;
                    cmd.Parameters.Add("@InnerText", SqlDbType.NVarChar).Value = page.InnerText;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
        }

        public async Task PageInsertOrUpdateKo(PageEntity page, CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);
            if (page == null) throw new ArgumentNullException("page");
            if (page.Url.Length > MaxSqlIndex) page.Url = page.Url.Substring(0, MaxSqlIndex);

            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand(@"MERGE Pages AS target USING (SELECT @url,@LastCrawle,@HiddenService) AS source (Url,LastCrawle,HiddenService) ON (target.Url = source.Url)
WHEN MATCHED THEN UPDATE SET CrawleError=COALESCE(CrawleError,0)+1,LastCrawle=source.LastCrawle,Rank=CASE WHEN Rank IS NOT NULL THEN Rank/2.0 ELSE 0.0 END,RankDate=SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT (Url,LastCrawle,FirstCrawle,HiddenService,CrawleError,Rank,RankDate) VALUES (source.Url,source.LastCrawle,source.LastCrawle,source.HiddenService,1,0.0,SYSUTCDATETIME());", conn)) // rank at 0 a new or half it if previously OK
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Add("@HiddenService", SqlDbType.NVarChar, 37).Value = page.HiddenService;
                    cmd.Parameters.Add("@Url", SqlDbType.NVarChar, 450).Value = page.Url;
                    cmd.Parameters.Add("@LastCrawle", SqlDbType.DateTime2).Value = page.LastCrawle;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
        }

        #endregion WebSearcherWorkerRole

        #region WebSearcherManagerRole

        public async Task<IEnumerable<string>> GetHiddenServicesToCrawleAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand("SELECT * FROM HiddenServicesToCrawle", conn))
                {
                    cmd.CommandType = CommandType.Text;

                    List<string> ret = new List<string>();
                    using (SqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {
                        while (reader.Read())
                        {
                            ret.Add(reader.GetString(0));
                        }
                    }
                    return ret;
                }
            else
                return null;
        }

        public async Task<IEnumerable<string>> GetPagesToCrawleAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand("SELECT * FROM PagesToCrawle", conn))
                {
                    cmd.CommandType = CommandType.Text;

                    List<string> ret = new List<string>();
                    using (SqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {
                        while (reader.Read())
                        {
                            ret.Add(reader.GetString(0));
                        }
                    }
                    return ret;
                }
            else
                return null;
        }

        public async Task<int> ComputePerfPagesAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            using (SqlCommand cmd = new SqlCommand("SELECT COUNT(1) FROM Pages WITH (NOLOCK)", conn))
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = 0;
                return (int)await cmd.ExecuteScalarAsync(cancellationToken);
            }
        }
        public async Task<int> ComputePerfPagesOkAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            using (SqlCommand cmd = new SqlCommand("SELECT COUNT(1) FROM Pages WITH (NOLOCK) WHERE CrawleError IS NULL", conn))
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = 0;
                return (int)await cmd.ExecuteScalarAsync(cancellationToken);
            }
        }
        public async Task<int> ComputePerfHiddenServicesAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            using (SqlCommand cmd = new SqlCommand("SELECT COUNT(1) FROM HiddenServices WITH (NOLOCK)", conn))
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = 0;
                return (int)await cmd.ExecuteScalarAsync(cancellationToken);
            }
        }
        public async Task<int> ComputePerfHiddenServicesOkAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            using (SqlCommand cmd = new SqlCommand("SELECT COUNT(1) FROM Pages WITH (NOLOCK) WHERE HiddenService=Url AND CrawleError IS NULL", conn))
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = 0;
                return (int)await cmd.ExecuteScalarAsync(cancellationToken);
            }
        }

        #endregion WebSearcherManagerRole

        #region IDisposable Support
        private bool disposedValue = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (conn != null)
                    {
                        conn.Dispose();
                        conn = null;
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