# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-08-10 19:10
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('grapher_admin', '0005_auto_20170720_1410'),
    ]

    operations = [
        migrations.AddField(
            model_name='entity',
            name='additional_info',
            field=models.CharField(max_length=255, null=True),
        ),
    ]
