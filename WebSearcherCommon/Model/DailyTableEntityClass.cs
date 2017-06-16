using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace WebSearcherCommon
{

    public class DailyTableEntityClass : TableEntity
    {
        public DailyTableEntityClass() { }

        public DailyTableEntityClass(string message)
        {
            this.Timestamp = DateTimeOffset.UtcNow;
            this.PartitionKey = this.Timestamp.ToString("yyyyMMdd");
            this.RowKey = Guid.NewGuid().ToString(); // not used but mandatory

            this.Message = message;
        }

        public string Message { get; set; }

        public override string ToString()
        {
            return Message;
        }
    }

}
