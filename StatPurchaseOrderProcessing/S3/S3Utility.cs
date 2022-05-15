namespace StatPurchaseOrderProcessing.S3
{
    using Amazon.Runtime;
    using Amazon.S3;
    using Amazon.S3.Model;
    using CsvHelper;
    using StatPurchaseOrderProcessing.Excel;
    using System.Data;
    using System.Globalization;
    using System.IO.Compression;
    using System.Net;

    public static class S3Utility
    {
        private static readonly string BucketName = "";
        private static readonly string AccessKeyId = "";
        private static readonly string Secret = "";
        private static readonly string TempDirectory = "TempFileProcessing";
        private static readonly string MetadataKey = "StatPurchaseOrderProcessingMetadata.csv";
        private static readonly string MetaDataLocalPath = $"{TempDirectory}\\{MetadataKey}";

        private static readonly AmazonS3Client S3Client = new AmazonS3Client(GetCredentials(), Amazon.RegionEndpoint.USEast2);

        internal async static Task Process()
        {
            var results = await S3Client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = BucketName
            });

            List<MetaData> metaDatas = await GetProcessingMetadata();

            results.S3Objects.RemoveAll(o => metaDatas.Any(md => md.ZipFileName == o.Key) || Path.GetExtension(o.Key) != ".zip");

            foreach (var s3Object in results.S3Objects)
            {
                var response = await S3Client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = s3Object.BucketName,
                    Key = s3Object.Key
                });

                var localFilePath = Path.Combine(TempDirectory, response.Key);
                var directoryName = Path.GetFileNameWithoutExtension(localFilePath);
                var extractDestination = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, TempDirectory, directoryName);

                await response.WriteResponseStreamToFileAsync(localFilePath, false, default(CancellationToken));

                ZipFile.ExtractToDirectory(localFilePath, extractDestination);

                var mappingFile = Directory.GetFiles(extractDestination)
                                            .First(x => Path.GetExtension(x) == ".csv");

                var claimsData = ExcelUtility.GetClaimsData(mappingFile);

                foreach (var claim in claimsData)
                {
                    if (string.IsNullOrEmpty(claim.FileNames) || string.IsNullOrEmpty(claim.PurchaseOrderNumber))
                        continue;

                    var attachments = claim.FileNames
                        .Split(",")
                        .Select(x => Path.GetFileName(x))
                        .ToArray();

                    var attachmentsFlattened = string.Empty;
                    foreach (var attachmentName in attachments)
                    {
                        var localPath = $"{extractDestination}\\{attachmentName.Replace("/", @"\")}";

                        if (await UploadProcessedFile(localPath, attachmentName, claim.PurchaseOrderNumber))
                            attachmentsFlattened += $",{attachmentName}";
                    }

                    metaDatas.Add(new MetaData
                    {
                        ExtractDate = DateTime.Now,
                        ZipFileName = response.Key,
                        ExtractedFileNamesFlattened = attachmentsFlattened
                    });
                }
            }

            await UpdateMetaData(metaDatas);

            return;
        }

        internal static void CleanTempDirectory()
        {
            var di = new DirectoryInfo(TempDirectory);

            foreach (var file in di.GetFiles())
                File.Delete(file.FullName);

            foreach (var directory in di.GetDirectories())
                directory.Delete(recursive: true);
        }

        private static async Task<bool> UploadProcessedFile(string localFilePath, string fileName, string poNumber)
        {
            if (!File.Exists(localFilePath))
                return false;

            await S3Client.PutObjectAsync(new PutObjectRequest
            {
                FilePath = localFilePath,
                BucketName = BucketName,
                Key = $"by-po/{poNumber}/{fileName}",
                ContentType = "application/pdf"
            });

            return true;
        }

        private static async Task<List<MetaData>> GetProcessingMetadata()
        {
            try
            {
                var response = await S3Client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = BucketName,
                    Key = MetadataKey
                });

                return await GetMetaDataAsync();
            }
            catch (AmazonS3Exception e)
            {
                if (e.ErrorCode == "NoSuchKey")
                {
                    await CreateMetadata();
                    return await GetProcessingMetadata();
                }
                else
                {
                    throw;
                }
            }
        }

        private static async Task CreateMetadata()
        {
            var response = await S3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = BucketName,
                Key = MetadataKey,
                ContentBody = string.Empty
            });

            if (response.HttpStatusCode != HttpStatusCode.OK)
                throw new Exception("Failed to create metadata");
        }

        private static async Task UpdateMetaData(List<MetaData> metaDatas)
        {
            using (var writer = new StreamWriter(MetaDataLocalPath))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                csv.WriteRecords(metaDatas);

            using (var stream = new FileStream(MetaDataLocalPath, FileMode.Open))
            {
                var response = await S3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = BucketName,
                    Key = MetadataKey,
                    InputStream = stream,
                    ContentType = "application/csv"
                });

                if (response.HttpStatusCode != HttpStatusCode.OK)
                    throw new Exception("Failed to upload metadata.");
            }
        }

        private static async Task<List<MetaData>> GetMetaDataAsync()
        {
            var request = new GetObjectRequest
            {
                BucketName = BucketName,
                Key = MetadataKey
            };

            using (var response = await S3Client.GetObjectAsync(request))
            using (var reader = new StreamReader(response.ResponseStream))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                return csv.GetRecords<MetaData>().ToList();
        }

        private static AWSCredentials GetCredentials()
            => new BasicAWSCredentials(AccessKeyId, Secret);
    }
}