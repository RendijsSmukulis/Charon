using System;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Charon
{
    public class Function
    {
        private IAmazonS3 s3Client;

        private IS3PathFilter filter;
        
        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            this.s3Client = new AmazonS3Client();
            this.filter = new AlwaysProcessS3PathFilter();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            this.s3Client = s3Client;
            this.filter = new AlwaysProcessS3PathFilter();
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt">The S3 event.</param>
        /// <param name="context">The Lambda context.</param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            if (string.IsNullOrEmpty(s3Event?.Bucket?.Name) || string.IsNullOrEmpty(s3Event.Object?.Key))
            {
                context.Logger.LogLine("Ignoring null event, null bucket name or null key name");
                return null;
            }

            var bucket = s3Event.Bucket.Name;
            var key = s3Event.Object.Key;

            // Filter out any buckets and/or keys that should not be copied to Redhshift
            if (!this.filter.ShouldProcess(bucket, key))
            {
                context.Logger.LogLine($"Ignoring bucket '{bucket}', key '{key}' since it was filtered out.");
                return null;
            }

            context.Logger.LogLine($"Processing bucket '{bucket}', key '{key}'.");

            try
            {
                var response = await this.s3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                context.Logger.LogLine($"Object info: Last modified: {response.LastModified}");

                return response.Headers.ContentType;
            }
            catch (Exception e)
            {
                context.Logger.LogLine($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }
    }
}
