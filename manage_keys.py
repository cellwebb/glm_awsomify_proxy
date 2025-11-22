#!/usr/bin/env python3
"""
Command-line tool for managing incoming API keys.
"""
import argparse
import sys
from incoming_key_manager import IncomingKeyManager
from datetime import datetime


def format_timestamp(ts_str):
    """Format ISO timestamp to readable format."""
    if not ts_str:
        return "Never"
    try:
        dt = datetime.fromisoformat(ts_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except:
        return ts_str


def cmd_add(args):
    """Add a new API key."""
    manager = IncomingKeyManager(args.db)
    api_key = manager.generate_api_key(args.name)
    print(f"\n✓ API Key created successfully!")
    print(f"  Name: {args.name}")
    print(f"  Key:  {api_key}")
    print(f"\nStore this key securely - it won't be shown again!\n")


def cmd_list(args):
    """List all API keys."""
    manager = IncomingKeyManager(args.db)
    keys = manager.list_api_keys()

    if not keys:
        print("\nNo API keys found.\n")
        return

    print("\n" + "=" * 120)
    print(f"{'ID':<4} {'Name':<20} {'API Key':<45} {'Status':<10} {'Requests':<10} {'Last Used':<25}")
    print("=" * 120)

    for key in keys:
        status = "REVOKED" if key['revoked'] else "ACTIVE"
        api_key_display = key['api_key'][:40] + "..." if len(key['api_key']) > 40 else key['api_key']
        last_used = format_timestamp(key['last_used_at'])

        print(f"{key['id']:<4} {key['name']:<20} {api_key_display:<45} {status:<10} {key['request_count']:<10} {last_used:<25}")

    print("=" * 120)

    stats = manager.get_stats()
    print(f"\nTotal: {stats['total']} | Active: {stats['active']} | Revoked: {stats['revoked']}\n")


def cmd_revoke(args):
    """Revoke an API key."""
    manager = IncomingKeyManager(args.db)

    if manager.revoke_api_key(args.key):
        print(f"\n✓ API Key revoked successfully: {args.key[:20]}...\n")
    else:
        print(f"\n✗ Failed to revoke key (not found or already revoked): {args.key[:20]}...\n")
        sys.exit(1)


def cmd_stats(args):
    """Show statistics."""
    manager = IncomingKeyManager(args.db)
    stats = manager.get_stats()

    print("\n" + "=" * 40)
    print("API Key Statistics")
    print("=" * 40)
    print(f"Total Keys:   {stats['total']}")
    print(f"Active Keys:  {stats['active']}")
    print(f"Revoked Keys: {stats['revoked']}")
    print("=" * 40 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Manage incoming API keys for Cerebras Proxy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Add a new API key
  python manage_keys.py add "Client Name"

  # List all API keys
  python manage_keys.py list

  # Revoke an API key
  python manage_keys.py revoke sk-abc123...

  # Show statistics
  python manage_keys.py stats
        """
    )

    parser.add_argument('--db', default='./data/incoming_keys.db',
                       help='Path to SQLite database (default: ./data/incoming_keys.db)')

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    subparsers.required = True

    # Add command
    parser_add = subparsers.add_parser('add', help='Add a new API key')
    parser_add.add_argument('name', help='Descriptive name for the API key')
    parser_add.set_defaults(func=cmd_add)

    # List command
    parser_list = subparsers.add_parser('list', help='List all API keys')
    parser_list.set_defaults(func=cmd_list)

    # Revoke command
    parser_revoke = subparsers.add_parser('revoke', help='Revoke an API key')
    parser_revoke.add_argument('key', help='API key to revoke')
    parser_revoke.set_defaults(func=cmd_revoke)

    # Stats command
    parser_stats = subparsers.add_parser('stats', help='Show statistics')
    parser_stats.set_defaults(func=cmd_stats)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
