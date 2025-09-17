import asyncio
import collections
from typing import Dict, List


class ApiKeyManager:
    """
    Manages API keys for the proxy server.
    """
    def __init__(self, keys: Dict[str, str]):
        """
        Initializes the API key manager with a dictionary of keys.
        
        Args:
            keys: A dictionary where the values are the API keys.
        """
        # Store keys in a deque for efficient rotation
        self._keys: collections.deque = collections.deque(keys.values())
        # Lock to ensure thread-safe access to the keys
        self._lock: asyncio.Lock = asyncio.Lock()
        
        if not self._keys:
            raise ValueError("No API keys provided.")
            
    async def get_next_key(self) -> str:
        """
        Gets the next API key in rotation. This is thread-safe.
        
        Returns:
            The next API key in the rotation.
        """
        async with self._lock:
            # Rotate the deque to move to the next key
            self._keys.rotate(-1)
            # Return the current key (which is now at the end after rotation)
            return self._keys[-1]
            
    def get_key_count(self) -> int:
        """
        Gets the total number of API keys.
        
        Returns:
            The number of API keys.
        """
        return len(self._keys)