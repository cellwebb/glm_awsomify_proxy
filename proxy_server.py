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
        # Add status endpoint
        self.app.router.add_get("/_status", self.status_handler)
        # Add the catch-all route (must be last)
        self.app.router.add_route("*", "/{path:.*}", self.proxy_handler)

    async def status_handler(self, request: web.Request) -> web.Response:
        """
        Returns the current status of all API keys.
        """
        status = await self.api_key_manager.get_status()
        return web.json_response(status)

    async def proxy_handler(self, request: web.Request) -> web.Response:
        """
        Handles all incoming requests, forwards them to the target API,
        and returns the response. Implements smart retry logic with key rotation.
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

        logger.info(f"Processing request to {target_url}")

        # Retry with automatic key rotation
        max_retries = self.api_key_manager.get_key_count() * 2  # Allow multiple passes through all keys

        for attempt in range(max_retries):
            # Get the current API key (will wait if all are rate-limited)
            api_key = await self.api_key_manager.get_current_key()
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

                            # Handle rate limiting
                            if resp.status == 429:
                                logger.warning(f"Rate limited (429), marking key and switching...")
                                await self.api_key_manager.mark_key_rate_limited(api_key)
                                continue
                            elif resp.status == 500:
                                logger.warning(f"Server error (500), trying next key...")
                                await self.api_key_manager.mark_key_rate_limited(api_key)
                                continue
                            else:
                                # Success or non-retryable error
                                if resp.status < 400:
                                    await self.api_key_manager.mark_key_success(api_key)
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

                            # Handle rate limiting
                            if resp.status == 429:
                                logger.warning(f"Rate limited (429), marking key and switching...")
                                await self.api_key_manager.mark_key_rate_limited(api_key)
                                continue
                            elif resp.status == 500:
                                logger.warning(f"Server error (500), trying next key...")
                                await self.api_key_manager.mark_key_rate_limited(api_key)
                                continue
                            else:
                                # Success or non-retryable error
                                if resp.status < 400:
                                    await self.api_key_manager.mark_key_success(api_key)
                                logger.info(f"Request completed with status {resp.status}")
                                return response

            except aiohttp.ClientError as e:
                logger.error(f"Client error on attempt {attempt + 1}: {e}")
                # For client errors, try the next key
                await self.api_key_manager.mark_key_rate_limited(api_key)
                continue
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                # Return a 500 error to the client for unexpected issues
                return web.Response(status=500, text=f"Proxy error: {e}")

        # If we get here, all attempts failed
        logger.error("Maximum retry attempts exceeded.")
        return web.Response(status=503, text="Service unavailable: Maximum retries exceeded.")
        
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

    # Get optional cooldown configuration
    cooldown_seconds = int(os.environ.get("CEREBRAS_COOLDOWN", "60"))
    logger.info(f"Cooldown period set to {cooldown_seconds} seconds")

    # Create the API key manager
    api_key_manager = ApiKeyManager(api_keys, cooldown_seconds=cooldown_seconds)
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