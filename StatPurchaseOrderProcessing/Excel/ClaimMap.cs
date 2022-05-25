namespace StatPurchaseOrderProcessing.Excel
{
    using CsvHelper.Configuration;
    using S3;

    internal sealed class ClaimMap : ClassMap<Claim>
    {
        internal ClaimMap()
        {
            Map(x => x.PurchaseOrderNumber).Name("PO Number");
            Map(x => x.FileNames).Name("Attachment List");
        }
    }
}