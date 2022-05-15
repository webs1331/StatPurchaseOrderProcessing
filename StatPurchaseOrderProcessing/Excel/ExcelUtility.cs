namespace StatPurchaseOrderProcessing.Excel
{
    using CsvHelper;
    using CsvHelper.Configuration;
    using System.Globalization;

    public static class ExcelUtility
    {
        public static List<T> ReadMappedData<T, TMap>(string filePath, string delimeter) 
            where T: class 
            where TMap : ClassMap<T>
        {
            var config = new CsvConfiguration(CultureInfo.CurrentCulture) { Delimiter = delimeter };

            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, config))
            {
                csv.Context.RegisterClassMap<TMap>();
                return csv.GetRecords<T>().ToList();
            }
        }

        public static List<T> ReadData<T>(StreamReader reader)
        {
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                return csv.GetRecords<T>().ToList();
        }

        public static void WriteData<T>(IEnumerable<T> data, string writePath)
        {
            using (var writer = new StreamWriter(writePath))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                csv.WriteRecords(data);
        }
    }
}