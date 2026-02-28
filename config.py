# config.py
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from functools import lru_cache
import sys
import logging

logger = logging.getLogger(__name__)

@lru_cache(maxsize=1)
def get_boto_session():
    """Get cached boto3 session - uses IAM role on EC2, profile locally"""
    try:
        # Try EC2 IAM role first (no profile needed)
        session = boto3.Session(region_name='ap-south-1')
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        logger.info("AWS Session: %s", identity['Arn'])
        return session
    except NoCredentialsError:
        # Fallback to profile for local development
        try:
            session = boto3.Session(profile_name='Absc', region_name='ap-south-1')
            sts = session.client('sts')
            identity = sts.get_caller_identity()
            logger.info("AWS Session (profile): %s", identity['Arn'])
            return session
        except NoCredentialsError:
            logger.error("No AWS credentials found")
            logger.error("EC2: Attach IAM role with SSM/S3/SQS permissions")
            logger.error("Local: Run 'aws sso login --profile Absc'")
            sys.exit(1)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ExpiredToken':
            logger.error("AWS session expired - run: aws sso login --profile Absc")
        else:
            logger.error("AWS auth failed: %s", e)
        sys.exit(1)

class SSMConfig:
    """Manages configuration from AWS SSM Parameter Store"""
    
    def __init__(self, region='ap-south-1'):
        self.ssm_client = get_boto_session().client('ssm', region_name=region)
    
    @lru_cache(maxsize=20)
    def get_parameter(self, param_name, decrypt=True):
        """Get parameter from SSM (cached)"""
        try:
            response = self.ssm_client.get_parameter(
                Name=param_name,
                WithDecryption=decrypt
            )
            return response['Parameter']['Value']
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                logger.warning("Parameter not found: %s", param_name)
            else:
                logger.error("Error fetching %s: %s", param_name, e)
            return None
    
    def set_parameter(self, param_name, value):
        """Update parameter in SSM"""
        try:
            self.ssm_client.put_parameter(
                Name=param_name,
                Value=value,
                Type='SecureString',
                Overwrite=True
            )
            self.get_parameter.cache_clear()
            logger.info("Updated: %s", param_name)
            return True
        except ClientError as e:
            logger.error("Error setting %s: %s", param_name, e)
            return False
    
    # Upstox Configuration
    @property
    def API_KEY(self):
        """
        Property that returns the Upstox API key from SSM Parameter Store.

        Returns:
            str: The Upstox API key
        """
        return self.get_parameter('upstox_api_key')
    
    @property
    def API_SECRET(self):
        """
        Property that returns the Upstox API secret from SSM Parameter Store.

        Returns:
            str: The Upstox API secret
        """
        return self.get_parameter('upstox_api_secret')
    
    @property
    def REDIRECT_URI(self):
        """
        Property that returns the redirect URI from SSM Parameter Store.

        Returns:
            str: The redirect URI
        """
        return self.get_parameter('redirect_uri', decrypt=False)
    
    @property
    def CODE(self):  
        """
        Property that returns the Upstox authorization code from SSM Parameter Store.

        Returns:
            str: The Upstox authorization code
        """
        return self.get_parameter('upstox_auth_code')
    
    @property
    def ACCESS_TOKEN(self):
        """
        Property that returns the Upstox access token from SSM Parameter Store.

        Returns:
            str: The Upstox access token
        """
        return self.get_parameter('upstox_access_token')
    
    @property
    def NIFTY_SPOT(self):
        """
        Property that returns the current Nifty spot price from SSM Parameter Store.

        Returns:
            float: The current Nifty spot price
        """
        return self.get_parameter('nifty_spot')
    
    def save_access_token(self, token):
        """
        Saves the Upstox access token to SSM Parameter Store.

        Args:
            token (str): The Upstox access token to save

        Returns:
            bool: True if the token save is successful, False otherwise
        """
        return self.set_parameter('upstox_access_token', token)
    
    # Neon PostgreSQL Configuration
    @property
    def NEON_CONNECTION_STRING_CALL(self):
        """Get Neon connection string for CALL options"""
        return self.get_parameter('/neon_connection_string/call')
    
    @property
    def NEON_CONNECTION_STRING_PUT(self):
        """Get Neon connection string for PUT options"""
        return self.get_parameter('/neon_connection_string/put')

    def save_nifty_spot(self, spot_price):
        """
        Saves the current Nifty spot price to SSM Parameter Store.

        Args:
            spot_price (float): The current Nifty spot price to save

        Returns:
            bool: True if the spot price save is successful, False otherwise
        """
        return self.set_parameter('nifty_spot', str(spot_price))

# Singleton instance
config = SSMConfig()