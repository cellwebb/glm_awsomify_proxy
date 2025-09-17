import os
import json
import asyncio
import aiohttp
from aiohttp import web
import logging
from typing import Dict, Any

from api_key_manager import ApiKeyManager


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Target API host
TARGET_API_HOST = "https://api.cerebras.ai/v1/"

# Error codes that trigger key rotation
ROTATE_KEY_ERROR_CODES = {429, 500}


class ProxyServer:
    """
    A proxy server that forwards requests to the Cerebras API
    with round-robin API key rotation.
    """
    def __init__(self, api_key_manager: ApiKeyManager):
        self.api_key_manager = api_key_manager
        self.app = web.Application()
        # Add the catch-all route
        self.app.router.add_route("*", "/{path:.*}", self.proxy_handler)
        
    async def proxy_handler(self, request: web.Request) -> web.Response:
        """
        Handles all incoming requests, forwards them to the target API,
        and returns the response. Implements retry logic with key rotation.
        """
        path = request.match_info["path"]
        
        # Avoid /v1/v1 duplication if the request path already includes v1/
        if path.startswith("v1/"):
            path = path[3:]  # Remove the "v1/" prefix
        
        target_url = f"{TARGET_API_HOST}{path}"
        
        # Get all headers except Authorization and Host
        headers = {key: value for key, value in request.headers.items()
                   if key.lower() not in ('authorization', 'host')}
        headers["User-Agent"] = "Cerebras-Proxy/1.0" # Add a User-Agent header
        
        # Number of keys determines the number of retries
        key_count = self.api_key_manager.get_key_count()
        logger.info(f"Attempting request to {target_url} with up to {key_count} retries.")
        
        for attempt in range(key_count):
            # Get the current API key (rotates internally)
            api_key = await self.api_key_manager.get_next_key()
            headers["Authorization"] = f"Bearer {api_key}"
            
            try:
                # Use aiohttp client to make request
                async with aiohttp.ClientSession() as session:
                    # Prepare the request based on the method
                    method = request.method
                    if method in ("GET", "HEAD", "OPTIONS"):
                        async with session.request(method, target_url, headers=headers) as resp:
                            # Stream the response body back to the client
                            body = await resp.read()
                            # Create a new response with the target API's status and headers
                            response = web.Response(
                                status=resp.status,
                                body=body,
                                headers={key: value for key, value in resp.headers.items()
                                         if key.lower() not in ('content-length', 'transfer-encoding')}
                            )
                            # If the status is a rotation-triggering error, continue to retry
                            if resp.status in ROTATE_KEY_ERROR_CODES:
                                logger.info(f"Request failed with status {resp.status}, rotating key...")
                                continue
                            else:
                                # Success or non-retryable error
                                logger.info(f"Request completed with status {resp.status}")
                                return response
                    else:
                        # For methods with bodies (POST, PUT, PATCH, DELETE)
                        # Stream the request body to the target API
                        async with session.request(method, target_url, headers=headers, 
                                                   data=request.content) as resp:
                            # Stream the response body back to the client
                            body = await resp.read()
                            response = web.Response(
                                status=resp.status,
                                body=body,
                                headers={key: value for key, value in resp.headers.items()
                                         if key.lower() not in ('content-length', 'transfer-encoding')}
                            )
                            if resp.status in ROTATE_KEY_ERROR_CODES:
                                logger.info(f"Request failed with status {resp.status}, rotating key...")
                                continue
                            else:
                                logger.info(f"Request completed with status {resp.status}")
                                return response
                            
            except aiohttp.ClientError as e:
                logger.error(f"Client error on attempt {attempt + 1}: {e}")
                # For client errors, we'll also rotate the key
                continue
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                # Return a 500 error to the client for unexpected issues
                return web.Response(status=500, text=f"Proxy error: {e}")
        
        # If we get here, all attempts failed
        logger.error("All API key attempts failed.")
        return web.Response(status=503, text="Service unavailable: All API keys exhausted.")
        
    async def run(self, host: str = "0.0.0.0", port: int = 8080):
        """
        Starts the proxy server using the existing event loop.
        """
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        # Keep the server running
        while True:
            await asyncio.sleep(3600)  # Sleep for an hour, effectively running forever


# Example startup logic to load API keys from environment variable
# This would normally be handled by a configuration management system
# but is included here for demonstration purposes
async def main():
    """
    Main entry point for the proxy server.
    """
    logger.info("Starting main() function...")
    
    # Get the API keys from the environment variable
    api_keys_json = os.environ.get("CEREBRAS_API_KEYS", "{}")
    logger.info(f"Retrieved API keys JSON: {repr(api_keys_json)}")
    
    try:
        api_keys: Dict[str, str] = json.loads(api_keys_json)
        logger.info(f"Successfully parsed {len(api_keys)} API keys")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON for API keys: {e}")
        logger.error(f"Raw API keys value: {repr(api_keys_json)}")
        return

    # Create the API key manager
    api_key_manager = ApiKeyManager(api_keys)
    logger.info("Created API key manager successfully")

    # Create and run the proxy server
    proxy = ProxyServer(api_key_manager)
    logger.info("About to call proxy.run() with proper event loop integration")
    await proxy.run()


if __name__ == "__main__":
    try:
        # Try to get the current event loop
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # If no loop is running, create a new one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Run the main function in the appropriate event loop
    loop.run_until_complete(main())