from django.conf import settings
from django.shortcuts import render

from .orders import export_orders_csv


async def data_viewer(req):
    output_path = settings.BASE_DIR / "exports" / "orders.csv"
    count = await export_orders_csv(output_path, dummy=True)
    return render(req, "data_viewer.html", {"output_path": output_path, "order_count": count})
