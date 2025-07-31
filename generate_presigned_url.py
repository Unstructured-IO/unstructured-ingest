#!/usr/bin/env python3
"""
Helper script to generate presigned URLs for testing.
This shows how users would generate presigned URLs to use with the ambient credentials feature.
"""

import boto3
import sys
from botocore.exceptions import ClientError, NoCredentialsError

def generate_presigned_list_url(bucket_name, prefix="", expiration=3600):
    """Generate a presigned URL for listing S3 bucket contents."""
    try:
        s3_client = boto3.client('s3')
        
        # Generate presigned URL for ListObjectsV2
        presigned_url = s3_client.generate_presigned_url(
            'list_objects_v2',
            Params={
                'Bucket': bucket_name,
                'Prefix': prefix,
                'MaxKeys': 1000
            },
            ExpiresIn=expiration
        )
        
        return presigned_url
        
    except NoCredentialsError:
        print("AWS credentials not found. Please configure your AWS credentials:")
        print("   - AWS CLI: aws configure")
        print("   - Environment: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        print("   - IAM role (if running on EC2)")
        return None
        
    except ClientError as e:
        print(f"AWS error: {e}")
        return None
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def test_presigned_url(presigned_url):
    """Test if a presigned URL works."""
    import requests
    
    print(f"\nTesting presigned URL...")
    try:
        response = requests.head(presigned_url, timeout=30)
        if response.status_code in [200, 403]:
            print("Presigned URL is accessible")
            return True
        else:
            print(f"Presigned URL returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"Error testing presigned URL: {e}")
        return False

def main():
    """Generate and test a presigned URL."""
    print("S3 Presigned URL Generator for Ambient Credentials Testing")
    print("=" * 60)
    
    if len(sys.argv) < 2:
        print("Usage: python generate_presigned_url.py <bucket_name> [prefix] [expiration_seconds]")
        print("\nExample:")
        print("  python generate_presigned_url.py my-test-bucket")
        print("  python generate_presigned_url.py my-test-bucket documents/ 7200")
        return 1
    
    bucket_name = sys.argv[1]
    prefix = sys.argv[2] if len(sys.argv) > 2 else ""
    expiration = int(sys.argv[3]) if len(sys.argv) > 3 else 3600
    
    print(f"Generating presigned URL for:")
    print(f"  Bucket: {bucket_name}")
    print(f"  Prefix: '{prefix}' (empty = all objects)")
    print(f"  Expiration: {expiration} seconds ({expiration/3600:.1f} hours)")
    
    presigned_url = generate_presigned_list_url(bucket_name, prefix, expiration)
    
    if not presigned_url:
        return 1
    
    print(f"\nGenerated presigned URL:")
    print(f"{presigned_url}")
    
    # Test the URL
    if test_presigned_url(presigned_url):
        print(f"\nSUCCESS! You can use this presigned URL for testing:")
        print(f"\nFor the test scripts:")
        print(f"export TEST_PRESIGNED_URL='{presigned_url}'")
        print(f"export TEST_ROLE_ARN='arn:aws:iam::YOUR_ACCOUNT:role/YOUR_ROLE'")
        print(f"python test_real_aws.py")
        
        print(f"\nFor CLI usage:")
        print(f"--use-ambient-credentials \\")
        print(f"--presigned-url '{presigned_url}' \\")
        print(f"--role-arn 'arn:aws:iam::YOUR_ACCOUNT:role/YOUR_ROLE'")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())