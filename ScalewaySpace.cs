using System;
using RestSharp;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon;
using Amazon.Runtime;
using System.Threading.Tasks;
using System.Globalization;
using System.Collections.Generic;
using System.IO;

namespace ScalewaySpaces
{
	public class ScalewaySpace
	{
		private const string bucketName = "launchers";
		private const string s3AccessKey = "SCW91QGYBQWS58H96YVT";
		private const string s3SecretKey = "ebabea8c-3493-4270-9827-bd0e3cd4e0f0";

		private readonly string s3RegionURL = "https://s3.fr-par.scw.cloud";
		private readonly string s3Region = "fr-par";

		private IAmazonS3 client;
		private AmazonS3Config config;

		public ScalewaySpace()
		{

			config = new AmazonS3Config()
			{
				ServiceURL = s3RegionURL,
				AuthenticationRegion = s3Region
			};
			AWSConfigsS3.UseSignatureVersion4 = true;
			client = new AmazonS3Client(s3AccessKey, s3SecretKey, config);
			try
			{
				ListBucketsResponse response = client.ListBucketsAsync().Result;
				foreach (S3Bucket b in response.Buckets)
				{
					Console.WriteLine(b.BucketName);
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e.ToString());
			}
		}

		public List<S3Object> GetObjects(string path)
		{
			ListObjectsV2Request request = new ListObjectsV2Request()
			{
				BucketName = bucketName,
				StartAfter = path
			};
			ListObjectsV2Response response = client.ListObjectsV2Async(request).Result;
			return response.S3Objects;
		}

		public DeleteObjectResponse DeleteObject(string path)
		{
			DeleteObjectRequest request = new DeleteObjectRequest()
			{
				BucketName = bucketName,
				Key = path
			};
			DeleteObjectResponse response = client.DeleteObjectAsync(request).Result;
			return response;
		}

		public PutObjectResponse CreateDirectory(string path)
		{
			PutObjectRequest request = new PutObjectRequest
			{
				BucketName = bucketName,
				Key = path
			};
			PutObjectResponse response = client.PutObjectAsync(request).Result;
			
			return response;
		}

		public Stream StreamFileFromS3(string path)
		{
			GetObjectRequest request = new GetObjectRequest()
			{
				Key = path,
				BucketName = bucketName
			};
			GetObjectResponse response = client.GetObjectAsync(request).Result;
			return response.ResponseStream;
		}

		public long UploadObject(string path, Stream dataStream)
		{
			PutObjectRequest request = new PutObjectRequest()
			{
				BucketName = bucketName,
				Key = path,
				InputStream = dataStream
			};
			PutObjectResponse response = client.PutObjectAsync(request).Result;
			return response.ContentLength;
		}

		public InitiateMultipartUploadResponse InitMultipartUpload(string path)
		{
			InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest
			{
				BucketName = bucketName,
				Key = path,
			};
			InitiateMultipartUploadResponse response = client.InitiateMultipartUploadAsync(request).Result;
			return response;
		}

		public UploadPartResponse UploadPart(string path, string uploadId, int partNumber, long partSize, MemoryStream input, bool lastPart)
		{
			input.Position = 0;
			UploadPartRequest request = new UploadPartRequest
			{
				BucketName = bucketName,
				Key = path,
				UploadId = uploadId,
				PartNumber = partNumber,
				PartSize = partSize,
				InputStream = input,
				IsLastPart = lastPart
			};
			UploadPartResponse response = client.UploadPartAsync(request).Result;
			return response;
		}

		public CompleteMultipartUploadResponse CompleteMultipartUpload(string path, string uploadId, List<UploadPartResponse> parts)
		{
			CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest
			{
				BucketName = bucketName,
				Key = path,
				UploadId = uploadId
			};
			request.AddPartETags(parts);
			var req = new ListPartsRequest
			{
				BucketName = bucketName,
				Key = path,
				UploadId = uploadId
			};
			var res = client.ListPartsAsync(req).Result;
			CompleteMultipartUploadResponse response = client.CompleteMultipartUploadAsync(request).Result;
			return response;
		}

		public bool RenameObject(string path, string newPath)
		{
			CopyObjectRequest request = new CopyObjectRequest
			{
				SourceBucket = bucketName,
				DestinationBucket = bucketName,
				SourceKey = path,
				DestinationKey = newPath
			};
			CopyObjectResponse response = client.CopyObjectAsync(request).Result;
			GetObjectMetadataRequest req = new GetObjectMetadataRequest
			{
				BucketName = bucketName,
				Key = path
			};
			GetObjectMetadataResponse resp = client.GetObjectMetadataAsync(req).Result;
			
			if (resp.ETag == response.ETag)
			{
				DeleteObject(path);
				return true;
			}
			else
			{
				return false;
			}
		}

		public bool ObjectExists(string path)
		{
			try
			{
				GetObjectMetadataRequest request = new GetObjectMetadataRequest()
				{
					BucketName = bucketName,
					Key = path
				};
				GetObjectMetadataResponse response = client.GetObjectMetadataAsync(request).Result;
				return true;
			}
			catch (AmazonS3Exception e)
			{
				if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
				{
					return false;
				}
				return false;
			}
		}
	}
}
