# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-02-16 01:16
from __future__ import unicode_literals

from django.db import migrations
import django_mysql.models


class Migration(migrations.Migration):

    dependencies = [
        ('grapher_admin', '0032_auto_20180216_0050'),
    ]

    operations = [
        migrations.AddField(
            model_name='variable',
            name='display',
            field=django_mysql.models.JSONField(default=dict),
        ),
    ]