from django.db import models

# Create your models here.
class OrderModel(models.Model):
    # id (string), created_at (UTC ISO 8601), amount_cents (int), customer_id (string), customer_email (string)
    id = models.CharField(max_length=255, primary_key=True)
    created_at = models.DateTimeField()
    amount_cents = models.IntegerField()
    customer_id = models.CharField(max_length=255)
    customer_email = models.CharField(max_length=255)

    def __str__(self):
        return f'ID: {self.id}, Amount: {self.amount_cents}, Customer ID: {self.customer_id}, Customer Email: {self.customer_email}, Created At: {self.created_at}'