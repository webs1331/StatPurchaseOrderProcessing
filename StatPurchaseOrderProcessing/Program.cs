namespace StatPurchaseOrderProcessing
{
    using S3;

    internal class Program
    {
        public static async Task<int> Main()
        {
            try
            {
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