namespace StatPurchaseOrderProcessing.Excel
{
    using CsvHelper;
    using CsvHelper.Configuration;
    using StatPurchaseOrderProcessing.S3;
    using System.Globalization;

    public static class ExcelUtility
    {
        public static List<Claim> GetClaimsData(string filePath)
        {
            var config = new CsvConfiguration(CultureInfo.CurrentCulture) { Delimiter = "~" };

            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, config))
            {
                csv.Context.RegisterClassMap<ClaimMap>();
                return csv.GetRecords<Claim>().ToList();
            }
        }
    }
}