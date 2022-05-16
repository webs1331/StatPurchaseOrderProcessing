namespace StatPurchaseOrderProcessing.S3
{
    using Amazon.Runtime;
    using Amazon.S3;
    using Amazon.S3.Model;
    using StatPurchaseOrderProcessing.Excel;
    using System.Data;
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
            var metaDatas = await GetProcessingMetadataAsync();
            var zipFiles = await GetZipFileListObjects(metaDatas);

            foreach (var s3Object in zipFiles)
            {
                var response = await S3Client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = s3Object.BucketName,
                    Key = s3Object.Key
                });

                var zipLocalFilePath = Path.Combine(TempDirectory, response.Key);
                var zipDirectoryName = Path.GetFileNameWithoutExtension(zipLocalFilePath);
                var extractDestination = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, TempDirectory, zipDirectoryName);

                await response.WriteResponseStreamToFileAsync(zipLocalFilePath, false, default);

                ZipFile.ExtractToDirectory(zipLocalFilePath, extractDestination);

                var claimsFilePath = Directory.GetFiles(extractDestination).First(x => Path.GetExtension(x) == ".csv");
                var claimsData = ExcelUtility.ReadMappedData<Claim, ClaimMap>(claimsFilePath, "~");

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
                        var localPath = $"{extractDestination}\\{attachmentName}";

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
                CleanTempDirectory();
            }

            if (zipFiles.Count > 0)
                await UpdateMetaData(metaDatas);
        }

        internal static void CleanTempDirectory()
        {
            var di = new DirectoryInfo(TempDirectory);

            foreach (var file in di.GetFiles())
                File.Delete(file.FullName);

            foreach (var directory in di.GetDirectories())
                directory.Delete(recursive: true);
        }

        private static async Task<List<S3Object>> GetZipFileListObjects(List<MetaData> currentMetaDatas)
        {
            var listRequest = new ListObjectsV2Request { BucketName = BucketName };
            var listResponse = new ListObjectsV2Response();
            var zipFiles = new List<S3Object>();

            do
            {
                listResponse = await S3Client.ListObjectsV2Async(listRequest);

                listResponse.S3Objects.RemoveAll(o => currentMetaDatas.Any(md => md.ZipFileName == o.Key) || Path.GetExtension(o.Key) != ".zip");

                zipFiles.AddRange(listResponse.S3Objects);

                listRequest.ContinuationToken = listResponse.NextContinuationToken;
            }
            while (listResponse.IsTruncated);

            return zipFiles;
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

        private static async Task<List<MetaData>> GetProcessingMetadataAsync()
        {
            try
            {
                var response = await S3Client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = BucketName,
                    Key = MetadataKey
                });

                return ExcelUtility.ReadData<MetaData>(response.ResponseStream);
            }
            catch (AmazonS3Exception e)
            {
                if (e.ErrorCode == "NoSuchKey")
                {
                    await CreateMetadata();
                    return await GetProcessingMetadataAsync();
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
            ExcelUtility.WriteData(metaDatas, MetaDataLocalPath);

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

        private static AWSCredentials GetCredentials()
            => new BasicAWSCredentials(AccessKeyId, Secret);
    }
}