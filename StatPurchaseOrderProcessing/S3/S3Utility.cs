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
        private const string BucketName = "TODO INITIALIZE";
        private const string AccessKeyId = "TODO INITIALIZE";
        private const string Secret = "TODO INITIALIZE";
        private const string TempDirectory = "TempFileProcessing";
        private const string MetadataKey = "StatPurchaseOrderProcessingMetadata.csv";
        private const string MetaDataLocalPath = $"{TempDirectory}\\{MetadataKey}";

        private static readonly AmazonS3Client S3Client;

        static S3Utility()
        {
            S3Client = new AmazonS3Client(GetCredentials(), Amazon.RegionEndpoint.USEast2);
        }

        internal static async Task Process()
        {
            var metaData = await GetProcessingMetadata();
            var zipFiles = await GetZipFileListObjects(metaData);

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

                var claimsFilePath = Directory.GetFiles(extractDestination).Single(x => Path.GetExtension(x) == ".csv");
                var claimsData = ExcelUtility.ReadMappedData<Claim, ClaimMap>(claimsFilePath, "~");

                foreach (var claim in claimsData)
                {
                    if (string.IsNullOrEmpty(claim.FileNames) || string.IsNullOrEmpty(claim.PurchaseOrderNumber))
                        continue;

                    var attachments = claim.FileNames
                        .Split(",")
                        .Select(Path.GetFileName)
                        .ToArray();

                    var attachmentsFlattened = string.Empty;
                    foreach (var attachmentName in attachments)
                    {
                        var localPath = $"{extractDestination}\\{attachmentName}";

                        if (attachmentName != null && await UploadProcessedFile(localPath, attachmentName, claim.PurchaseOrderNumber))
                            attachmentsFlattened += $",{attachmentName}";
                    }

                    metaData.Add(new MetaData
                    {
                        ExtractDate = DateTime.Now,
                        ZipFileName = response.Key,
                        ExtractedFileNamesFlattened = attachmentsFlattened
                    });
                }
                CleanTempDirectory();
            }

            if (zipFiles.Count > 0)
                await UpdateMetaData(metaData);
        }

        internal static void CleanTempDirectory()
        {
            var di = new DirectoryInfo(TempDirectory);

            foreach (var file in di.GetFiles())
                File.Delete(file.FullName);

            foreach (var directory in di.GetDirectories())
                directory.Delete(recursive: true);
        }

        private static async Task<List<S3Object>> GetZipFileListObjects(List<MetaData> currentMetaData)
        {
            var listRequest = new ListObjectsV2Request { BucketName = BucketName };
            ListObjectsV2Response listResponse;
            var zipFiles = new List<S3Object>();

            do
            {
                listResponse = await S3Client.ListObjectsV2Async(listRequest);

                listResponse.S3Objects.RemoveAll(o => currentMetaData.Any(md => md.ZipFileName == o.Key) || Path.GetExtension(o.Key) != ".zip");

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

        private static async Task<List<MetaData>> GetProcessingMetadata()
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
                if (e.ErrorCode != "NoSuchKey") throw;

                await CreateMetadata();
                return await GetProcessingMetadata();
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

        private static async Task UpdateMetaData(IEnumerable<MetaData> metaData)
        {
            ExcelUtility.WriteData(metaData, MetaDataLocalPath);

            await using var stream = new FileStream(MetaDataLocalPath, FileMode.Open);

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

        private static AWSCredentials GetCredentials()
            => new BasicAWSCredentials(AccessKeyId, Secret);
    }
}