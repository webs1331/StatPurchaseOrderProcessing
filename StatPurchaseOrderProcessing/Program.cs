namespace StatPurchaseOrderProcessing
{
    using System.Text;
    using S3;

    internal class Program
    {
        static async Task<int> Main(string[] args)
        {
            try
            {
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

                await S3Utility.Process();

                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error encountered. Message:'{0}'", e.Message);
                return 1;
            }
            finally
            {
                S3Utility.CleanTempDirectory();
            }
        }
    }
}