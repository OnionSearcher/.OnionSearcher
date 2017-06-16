using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
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
        private const int maxSqlIndex = 450;
        private const int maxSqlText = 1 * 1024 * 1024;
        
        public SqlManager()
        {
        }

        private SqlConnection conn;
        private void CheckOpen()
        {
            if (conn == null)
            {
                conn = new SqlConnection(ConfigurationManager.ConnectionStrings["sqlDb"].ConnectionString);
            }
            if (conn.State != ConnectionState.Open)
            {
                conn.Open(); // todo catch connection timeout et looper !
            }
        }
        private async Task CheckOpenAsync(CancellationToken cancellationToken)
        {
            if (conn == null)
            {
                conn = new SqlConnection(ConfigurationManager.ConnectionStrings["sqlDb"].ConnectionString);
            }
            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync(cancellationToken); // todo catch connection timeout et looper !
            }
        }

#if SAVEHTMLRAW
        private SqlConnection connHisto;
        private async Task CheckHistoOpenAsync(CancellationToken cancellationToken)
        {
            if (connHisto == null)
            {
                connHisto = new SqlConnection(ConfigurationManager.ConnectionStrings["sqlHistoDb"].ConnectionString);
            }
            if (connHisto.State != ConnectionState.Open)
            {
                await connHisto.OpenAsync(cancellationToken); // todo catch connection timeout et looper !
            }
        }
#endif

        public async Task<bool> CheckIfCanCrawlePageAsync(string url, CancellationToken cancellationToken)
        {
            if (url == null) throw new ArgumentNullException("url");
            if (url.Length > maxSqlIndex) url = url.Substring(0, maxSqlIndex);

            await CheckOpenAsync(cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand("CanCrawle", conn))
                {
                    cmd.Parameters.Add("@Url", SqlDbType.NVarChar).Value = url;
                    cmd.Parameters.Add("@HiddenService", SqlDbType.NVarChar).Value = PageEntity.GetHiddenService(url);
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

        #region WebSearcherWorkerRole

        public async Task PageInsertOrUpdateOk(PageEntity page, CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);
            if (page == null) throw new ArgumentNullException("page");
            if (page.HiddenService.Length > maxSqlIndex) page.HiddenService = page.HiddenService.Substring(0, maxSqlIndex);
            if (page.Url.Length > maxSqlIndex) page.Url = page.Url.Substring(0, maxSqlIndex);
            if (page.Title.Length > maxSqlIndex) page.Title = page.Title.Substring(0, maxSqlIndex);
            if (page.InnerText.Length > maxSqlText) page.InnerText = page.InnerText.Substring(0, maxSqlText);
            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand(@"MERGE Pages AS target USING (SELECT @HiddenService,@Url,@Title,@InnerText,@LastCrawle,@InnerLinks,@OuterLinks) AS source (HiddenService,Url,Title,InnerText,LastCrawle,InnerLinks,OuterLinks) ON (target.Url = source.Url)
WHEN MATCHED THEN UPDATE SET HiddenService=source.HiddenService,Url=source.Url,Title=source.Title,InnerText=source.InnerText,LastCrawle=source.LastCrawle,CrawleError=NULL,InnerLinks=source.InnerLinks,OuterLinks=source.OuterLinks
WHEN NOT MATCHED THEN INSERT (HiddenService,Url,Title,InnerText,FirstCrawle,LastCrawle,InnerLinks,OuterLinks) VALUES (source.HiddenService,source.Url,source.Title,source.InnerText,source.LastCrawle,source.LastCrawle,source.InnerLinks,source.OuterLinks);", conn))
                {
                    cmd.Parameters.Add("@HiddenService", SqlDbType.NVarChar).Value = page.HiddenService;
                    cmd.Parameters.Add("@Url", SqlDbType.NVarChar).Value = page.Url;
                    cmd.Parameters.Add("@Title", SqlDbType.NVarChar).Value = page.Title;
                    cmd.Parameters.Add("@InnerText", SqlDbType.NVarChar).Value = page.InnerText;
                    cmd.Parameters.Add("@LastCrawle", SqlDbType.DateTimeOffset).Value = page.LastCrawle;
                    cmd.Parameters.Add("@InnerLinks", SqlDbType.NVarChar).Value = string.Join("\r\n", page.InnerLinks);
                    cmd.Parameters.Add("@OuterLinks", SqlDbType.NVarChar).Value = string.Join("\r\n", page.OuterLinks);
                    cmd.CommandType = CommandType.Text;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }

#if SAVEHTMLRAW
            if (page.HtmlRaw != null && page.HtmlRaw.Length > maxSqlText) page.HtmlRaw = page.HtmlRaw.Substring(0, maxSqlText);
            try
            {
                await CheckHistoOpenAsync(cancellationToken);
                if (!cancellationToken.IsCancellationRequested)
                    using (SqlCommand cmd = new SqlCommand(@"MERGE HistoPages AS target USING (SELECT @Url,@HtmlRaw) AS source (Url,HtmlRaw) ON (target.Url = source.Url)
WHEN MATCHED THEN UPDATE SET Url=source.Url,HtmlRaw=source.HtmlRaw
WHEN NOT MATCHED THEN INSERT (Url,HtmlRaw) VALUES (source.Url,source.HtmlRaw);", connHisto))
                    {
                        cmd.Parameters.Add("@Url", SqlDbType.NVarChar).Value = page.Url;
                        cmd.Parameters.Add("@HtmlRaw", SqlDbType.NVarChar).Value = page.HtmlRaw;
                        cmd.CommandType = CommandType.Text;
                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }
            }
            catch (SqlException ex) // error on Histo dump is not reported as a KO.
            {
                Trace.TraceWarning("SqlManager.PageInsertOrUpdateOk HistoPages SqlException : " + ex.GetBaseException().ToString());
            }
#endif
        }

        public async Task PageInsertOrUpdateKo(PageEntity page, CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);
            if (page == null) throw new ArgumentNullException("page");
            if (page.Url.Length > maxSqlIndex) page.Url = page.Url.Substring(0, maxSqlIndex);

            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand(@"MERGE Pages AS target USING (SELECT @url,@LastCrawle,@HiddenService) AS source (Url,LastCrawle,HiddenService) ON (target.Url = source.Url)
WHEN MATCHED THEN UPDATE SET CrawleError=COALESCE(CrawleError,0)+1,LastCrawle=source.LastCrawle,Rank=CASE WHEN Rank IS NOT NULL THEN Rank/2.0 ELSE 0.0 END
WHEN NOT MATCHED THEN INSERT (Url,LastCrawle,FirstCrawle,HiddenService,CrawleError,Rank) VALUES (source.Url,source.LastCrawle,source.LastCrawle,source.HiddenService,1,0.0);", conn))
                {
                    cmd.Parameters.Add("@HiddenService", SqlDbType.NVarChar).Value = page.HiddenService;
                    cmd.Parameters.Add("@Url", SqlDbType.NVarChar).Value = page.Url;
                    cmd.Parameters.Add("@LastCrawle", SqlDbType.DateTimeOffset).Value = page.LastCrawle;
                    cmd.CommandType = CommandType.Text;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
        }

        public async Task<IEnumerable<string>> GetHiddenServicesListAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
                using (SqlCommand cmd = new SqlCommand("SELECT HiddenService, COUNT(1) from Pages WITH (NOLOCK) GROUP BY HiddenService ORDER BY 2 ASC", conn))
                {
                    cmd.CommandType = CommandType.Text;

                    List<string> ret = new List<string>();
                    using (SqlDataReader reader = await cmd.ExecuteReaderAsync())
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

        #endregion WebSearcherWorkerRole

        #region WebSearcherWebRole

        public SearchResultEntity GetSearchResult(string keywords, int page)
        {
            CheckOpen();

            SearchResultEntity ret = new SearchResultEntity()
            {
                Results = new List<PageResultEntity>()
            };
            using (DataSet ds = new DataSet())
            {
                using (SqlCommand cmd = new SqlCommand("Search", conn))
                {
                    cmd.Parameters.Add("@Keywords", SqlDbType.NVarChar, 64).Value = keywords;
                    cmd.Parameters.Add("@Page", SqlDbType.Int).Value = page - 1; // 0 based
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
                        HourSinceLastCrawle = (int)dr[5]
                    };
                    if (pe.Title.Length > 70)
                    {
                        pe.TitleToolTip = pe.Title;
                        pe.Title = pe.Title.Substring(0, 67).Replace("\"", "&quot;") + "...";
                    }
                    if (pe.Url.Length > 120)
                    {
                        pe.UrlToolTip = pe.Url;
                        pe.Url = pe.Url.Substring(0, 117).Replace("\"", "&quot;") + "...";
                    }
                    ret.Results.Add(pe);

                }
            }
            return ret;
        }

        #endregion WebSearcherWebRole

        #region WebSearcherWorkerRole2

        public async Task ComputeIndexedPagesAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);
            
            using (SqlCommand cmd = new SqlCommand("ComputeIndexedPagesTask", conn))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        public async Task<bool> UpdateHiddenServicesRankAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            using (SqlCommand cmd = new SqlCommand("UpdateHiddenServicesRankTask", conn))
            {
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
        }

        public async Task<bool> UpdatePageRankAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            using (SqlCommand cmd = new SqlCommand("UpdatePageRankTask", conn))
            {
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
        }

        public async Task<bool> PagesPurgeAsync(CancellationToken cancellationToken)
        {
            await CheckOpenAsync(cancellationToken);

            using (SqlCommand cmd = new SqlCommand("PagesPurgeTask", conn))
            {
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
        }
        
        #endregion WebSearcherWorkerRole2

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
#if SAVEHTMLRAW
                    if (connHisto != null)
                    {
                        connHisto.Dispose();
                        connHisto = null;
                    }
#endif
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