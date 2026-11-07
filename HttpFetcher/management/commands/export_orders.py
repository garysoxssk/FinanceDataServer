import asyncio
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from HttpFetcher.orders import export_orders_csv


class Command(BaseCommand):
    help = "Export local dummy orders to a sorted CSV without making HTTP requests."

    def add_arguments(self, parser):
        parser.add_argument("--output", type=Path, default=settings.BASE_DIR / "exports" / "orders.csv")
        parser.add_argument("--limit", type=int, default=100)
        parser.add_argument("--since")
        parser.add_argument("--max-retries", type=int, default=3)

    def handle(self, *args, **options):
        try:
            count = asyncio.run(export_orders_csv(
                options["output"], limit=options["limit"], since=options["since"],
                max_retries=options["max_retries"], dummy=True,
            ))
        except (ValueError, OSError) as error:
            raise CommandError(str(error)) from error
        self.stdout.write(self.style.SUCCESS(f"Saved {count} dummy orders to {options['output'].resolve()}"))
