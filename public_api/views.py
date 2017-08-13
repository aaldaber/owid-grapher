import copy
import datetime
from dateutil import parser
import json
import re
import csv
import glob
import os
import subprocess
import shlex
import time
from unidecode import unidecode
from io import StringIO
from urllib.parse import urlparse
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.views import login as loginview
from django.db.models import Q
from django.db import connection
from django.http import HttpRequest, HttpResponseRedirect, HttpResponse, HttpResponseNotFound, JsonResponse, QueryDict, StreamingHttpResponse
from django.urls import reverse
from django.utils import timezone
from django.utils.crypto import get_random_string
from grapher_admin.models import Chart, Variable, User, UserInvitation, Logo, ChartSlugRedirect, ChartDimension, Dataset, Setting, DatasetCategory, DatasetSubcategory, Entity, Source, VariableType, DataValue, License
from owid_grapher.views import get_query_string, get_query_as_dict
from django.views.decorators.csrf import csrf_exempt
import random
from django.views.decorators.clickjacking import xframe_options_exempt
from django.shortcuts import render


def get_all_categories(request: HttpRequest):
    category_names = list(DatasetCategory.objects.all().values('id', 'name'))
    return JsonResponse(category_names, safe=False)


def get_subcategories(request: HttpRequest):
    request_dict = get_query_as_dict(request)
    if request_dict.get('category'):
        try:
            category = DatasetCategory.objects.get(pk=int(request_dict.get('category')[0]))
        except:
            return JsonResponse([], safe=False)
        subcategory_names = list(DatasetSubcategory.objects.filter(fk_dst_cat_id=category).values('id', 'name'))
        return JsonResponse(subcategory_names, safe=False)
    else:
        return JsonResponse([], safe=False)


def get_variables(request: HttpRequest):
    request_dict = get_query_as_dict(request)
    if request_dict.get('subcategory'):
        try:
            subcategory = DatasetSubcategory.objects.get(pk=int(request_dict.get('subcategory')[0]))
        except:
            return JsonResponse([], safe=False)
        datasets = Dataset.objects.filter(fk_dst_subcat_id=subcategory)
        variable_names = list(Variable.objects.filter(fk_dst_id__in=datasets).values('id', 'name'))
        return JsonResponse(variable_names, safe=False)
    else:
        return JsonResponse([], safe=False)


def get_years(request: HttpRequest):
    request_dict = get_query_as_dict(request)
    if request_dict.get('variable'):
        try:
            variable = Variable.objects.get(pk=int(request_dict.get('variable')[0]))
        except:
            return JsonResponse([], safe=False)
        with connection.cursor() as c:
            c.execute('select distinct year from data_values where fk_var_id = %s;',
                      [variable.pk])
        years = [int(year[0]) for year in c.fetchall()]
        years = sorted(years)
        return JsonResponse(years, safe=False)
    else:
        return JsonResponse([], safe=False)


def get_entities(request: HttpRequest):
    request_dict = get_query_as_dict(request)
    if request_dict.get('variable'):
        try:
            variable = Variable.objects.get(pk=int(request_dict.get('variable')[0]))
        except:
            return JsonResponse([], safe=False)
        with connection.cursor() as c:
            c.execute('select distinct fk_ent_id from data_values where fk_var_id = %s;',
                      [variable.pk])
        entities = [int(entity[0]) for entity in c.fetchall()]
        entities = list(Entity.objects.filter(pk__in=entities).values('id', 'name'))
        entities = sorted(entities, key=lambda k: k['name'])
        return JsonResponse(entities, safe=False)
    else:
        return JsonResponse([], safe=False)


@csrf_exempt
def get_data(request: HttpRequest):
    request_dict = get_query_as_dict(request)
    countries = []
    years = []

    if request_dict.get('variable'):
        try:
            variable = Variable.objects.get(pk=int(request_dict.get('variable')[0]))
        except:
            return JsonResponse([], safe=False)

    if request_dict.get('years'):
        try:
            years = [int(each) for each in json.loads(request_dict.get('years')[0])]
        except:
            pass
    if request_dict.get('entities'):
        try:
            countries = [int(each) for each in request_dict.get('entities')]
        except:
            pass
    format_years = ','.join(['%s'] * len(years))
    format_entities = ','.join(['%s'] * len(countries))
    with connection.cursor() as c:
        c.execute('SELECT `value`, `year`, data_values.`fk_ent_id` as entity_id ' \
                  ' from data_values ' \
                  ' WHERE ' \
                  'data_values.`fk_var_id` = %s AND data_values.`fk_ent_id` in (%s) AND data_values.`year` in (%s) ORDER BY fk_ent_id, year, fk_var_id;' %
                  (variable.pk, format_entities, format_years),
                  tuple(countries + years))

    entity_id_names = Entity.objects.filter(pk__in=countries).values('id', 'name')
    entity_id_names = {each['id']: each['name'] for each in entity_id_names}

    data = []
    for each in c.fetchall():
        data.append({'value': each[0], 'year': each[1], 'entity': entity_id_names[each[2]]})

    return JsonResponse(data, safe=False)


def get_metadata(request: HttpRequest):
    metadata = {}
    var_entities = {}
    entity_id_name = {}
    with connection.cursor() as c:
        c.execute('select distinct fk_var_id, fk_ent_id from data_values;')
        for each in c.fetchall():
            if each[0] not in var_entities:
                var_entities[each[0]] = []
                var_entities[each[0]].append(each[1])
            else:
                var_entities[each[0]].append(each[1])
    category_names = DatasetCategory.objects.all().values('id', 'name').iterator()
    for each in category_names:
        metadata[each['id']] = {'name': each['name'], 'subcategories': {}}

    subcategory_names = DatasetSubcategory.objects.all().values('id', 'name', 'fk_dst_cat_id').iterator()
    for each in subcategory_names:
        metadata[each['fk_dst_cat_id']]['subcategories'][each['id']] = {'name': each['name'], 'variables': {}}

    variable_names = Variable.objects.all().select_related('fk_dst_id__fk_dst_cat_id', 'fk_dst_id__fk_dst_subcat_id').values('id', 'name', 'fk_dst_id__fk_dst_cat_id', 'fk_dst_id__fk_dst_subcat_id').iterator()
    for each in variable_names:
        metadata[each['fk_dst_id__fk_dst_cat_id']]['subcategories'][each['fk_dst_id__fk_dst_subcat_id']]['variables'][each['id']] = {'name': each['name'], 'entities': var_entities.get(each['id'], [])}
        if var_entities.get(each['id'], []):
            var_entities[each['id']] = None

    all_entities = Entity.objects.all().values('id', 'name').iterator()
    for each in all_entities:
        entity_id_name[each['id']] = each['name']

    return JsonResponse({'metadata': metadata, 'entity_info': entity_id_name}, safe=False)


def get_js_config(request: HttpRequest, varid: str):
    try:
        variable = Variable.objects.get(pk=int(varid))
    except Variable.DoesNotExist:
        return HttpResponseNotFound('Variable does not exist.')

    initial_js_str = r"""{"subtitle": "Projections from 2020 through to 2100 based on UN median estimates. Population figures have been categorised based on those under and over fifteen years old", "note": "", "chart-time": [null, null], "cache": true, "selected-countries": [{"name": "Under 15", "code": null, "id": 34676, "color": "#3360a9"}, {"name": "Total", "code": null, "id": 34678, "color": "#818282"}], "tabs": ["chart", "data", "sources"], "default-tab": "chart", "line-type": 0, "line-tolerance": 1, "chart-description": "", "sourceDesc": "UN Population Division (2015 Revision)", "chart-dimensions": [{"id": 1511, "order": 0, "property": "y", "unit": "", "displayName": "", "targetYear": null, "isProjection": false, "tolerance": 5, "color": "", "chartId": 693, "variableId": 2020}, {"id": 1512, "order": 1, "property": "y", "unit": "", "displayName": "", "targetYear": null, "isProjection": true, "tolerance": 5, "color": "", "chartId": 693, "variableId": 2021}], "logos": ["OWD"], "y-axis": {"axis-label-distance": "-1", "axis-min": "0", "axis-scale": "linear", "axis-label": ""}, "x-axis": {"axis-scale": "linear"}, "margins": {"top": 10, "left": 60, "bottom": 10, "right": 10}, "units": "[{\"property\":\"y\",\"visible\":true,\"title\":\"\",\"unit\":\"\",\"format\":\"2\"}]", "hide-legend": false, "hide-toggle": false, "entity-type": "age group", "group-by-variables": false, "add-country-mode": "disabled", "x-axis-scale-selector": false, "y-axis-scale-selector": false, "activeLegendKeys": null, "currentStackMode": null, "timeline": null, "identityLine": false, "map-config": {"variableId": 2020, "targetYear": 1980, "targetYearMode": "latest", "defaultYear": 1980, "mode": "specific", "timeTolerance": 1, "timeRanges": [], "timelineMode": "slider", "baseColorScheme": "BuGn", "colorSchemeInterval": 10, "colorSchemeMinValue": "", "colorSchemeValues": [], "colorSchemeLabels": [], "colorSchemeValuesAutomatic": true, "colorSchemeInvert": false, "customColorsActive": false, "customNumericColors": [], "customCategoryColors": {}, "customCategoryLabels": {}, "customHiddenCategories": {}, "isColorblind": false, "projection": "World", "defaultProjection": "World", "legendDescription": "", "legendStepSize": 20, "legendOrientation": "portrait"}, "form-config": {"entities-collection": []}, "id": 693, "data-entry-url": "", "title": "World population and projected growth to 2100, by age group", "chart-type": "LineChart", "internalNotes": "", "slug": "world-population-and-projected-growth-to-2100-by-age-group", "published": true, "logosSVG": ["<svg><a xmlns:xlink=\"http://www.w3.org/1999/xlink\" xlink:href=\"https://ourworldindata.org\" target=\"_blank\"><g><rect x=\"0\" y=\"0\" fill=\"#1B2543\" width=\"211.2\" height=\"130.1\"/><rect x=\"0\" y=\"112.2\" fill=\"#E63912\" width=\"211.2\" height=\"17.9\"/><g><rect x=\"1.2\" y=\"12.3\" fill=\"none\" width=\"210.2\" height=\"99.2\"/><path fill=\"#FFFFFF\" d=\"M37.8,32.2c0,12.7-7.7,19.4-17.1,19.4c-9.7,0-16.6-7.5-16.6-18.7c0-11.7,7.3-19.3,17.1-19.3 C31.3,13.7,37.8,21.3,37.8,32.2z M9.2,32.8c0,7.9,4.3,14.9,11.8,14.9c7.5,0,11.8-6.9,11.8-15.3c0-7.3-3.8-14.9-11.8-14.9 C13.1,17.5,9.2,24.8,9.2,32.8z\"/><path fill=\"#FFFFFF\" d=\"M62.7,43.8c0,2.7,0.1,5.1,0.2,7.2h-4.3l-0.3-4.3h-0.1c-1.3,2.1-4,4.9-8.8,4.9c-4.2,0-9.1-2.3-9.1-11.6 V24.6h4.8v14.6c0,5,1.5,8.4,5.9,8.4c3.2,0,5.5-2.2,6.3-4.4c0.3-0.7,0.4-1.6,0.4-2.5V24.6h4.8V43.8z\"/><path fill=\"#FFFFFF\" d=\"M67.3,32.8c0-3.1-0.1-5.8-0.2-8.2h4.2l0.2,5.2h0.2c1.2-3.5,4.1-5.8,7.3-5.8c0.5,0,0.9,0.1,1.4,0.2v4.5 c-0.5-0.1-1-0.2-1.6-0.2c-3.4,0-5.8,2.6-6.5,6.2c-0.1,0.7-0.2,1.4-0.2,2.2V51h-4.8V32.8z\"/><path fill=\"#FFFFFF\" d=\"M95.4,51l-9.4-36.8h5l4.4,18.6c1.1,4.6,2.1,9.2,2.7,12.7h0.1c0.6-3.7,1.8-8,3-12.8l4.9-18.5h5l4.5,18.7 c1,4.4,2,8.7,2.6,12.6h0.1c0.8-4,1.8-8.1,3-12.7l4.9-18.5h4.9L120.6,51h-5L111,31.9c-1.1-4.7-1.9-8.3-2.4-12h-0.1 c-0.7,3.7-1.5,7.3-2.8,12L100.4,51H95.4z\"/><path fill=\"#FFFFFF\" d=\"M154.9,37.6c0,9.8-6.8,14-13.2,14c-7.2,0-12.7-5.2-12.7-13.6c0-8.8,5.8-14,13.1-14 C149.8,24,154.9,29.5,154.9,37.6z M133.9,37.9c0,5.8,3.3,10.1,8,10.1c4.6,0,8-4.3,8-10.3c0-4.5-2.2-10.1-7.9-10.1 C136.3,27.6,133.9,32.8,133.9,37.9z\"/><path fill=\"#FFFFFF\" d=\"M158.2,32.8c0-3.1-0.1-5.8-0.2-8.2h4.2l0.2,5.2h0.2c1.2-3.5,4.1-5.8,7.3-5.8c0.5,0,0.9,0.1,1.4,0.2v4.5 c-0.5-0.1-1-0.2-1.6-0.2c-3.4,0-5.8,2.6-6.5,6.2c-0.1,0.7-0.2,1.4-0.2,2.2V51h-4.8V32.8z\"/><path fill=\"#FFFFFF\" d=\"M173.5,12.3h4.8V51h-4.8V12.3z\"/><path fill=\"#FFFFFF\" d=\"M206.5,12.3v31.9c0,2.3,0.1,5,0.2,6.8h-4.3l-0.2-4.6H202c-1.5,2.9-4.7,5.2-9,5.2 c-6.4,0-11.3-5.4-11.3-13.4c-0.1-8.8,5.4-14.2,11.9-14.2c4,0,6.8,1.9,8,4h0.1V12.3H206.5z M201.7,35.4c0-0.6-0.1-1.4-0.2-2 c-0.7-3.1-3.3-5.6-6.9-5.6c-5,0-7.9,4.4-7.9,10.2c0,5.3,2.6,9.8,7.8,9.8c3.2,0,6.2-2.1,7.1-5.7c0.2-0.7,0.2-1.3,0.2-2.1V35.4z\"/><path fill=\"#FFFFFF\" d=\"M42.8,64c0.1,1.6-1.1,2.9-3.1,2.9c-1.7,0-2.9-1.3-2.9-2.9c0-1.7,1.3-3,3-3C41.7,61,42.8,62.3,42.8,64z M37.4,97.8V71.4h4.8v26.4H37.4z\"/><path fill=\"#FFFFFF\" d=\"M47.5,78.6c0-2.7-0.1-5-0.2-7.1h4.3l0.3,4.4h0.1c1.3-2.5,4.4-5,8.8-5c3.7,0,9.4,2.2,9.4,11.2v15.8h-4.8 V82.6c0-4.3-1.6-7.8-6.1-7.8c-3.2,0-5.6,2.2-6.5,4.9c-0.2,0.6-0.3,1.4-0.3,2.2v15.9h-4.8V78.6z\"/><path fill=\"#FFFFFF\" d=\"M84,61.6c2.9-0.4,6.3-0.8,10.1-0.8c6.8,0,11.7,1.6,14.9,4.6c3.3,3,5.2,7.3,5.2,13.2c0,6-1.9,10.9-5.3,14.3 c-3.4,3.4-9.1,5.3-16.3,5.3c-3.4,0-6.2-0.2-8.6-0.4V61.6z M88.8,94.1c1.2,0.2,3,0.3,4.8,0.3c10.2,0,15.7-5.7,15.7-15.6 c0.1-8.7-4.9-14.2-14.9-14.2c-2.5,0-4.3,0.2-5.6,0.5V94.1z\"/><path fill=\"#FFFFFF\" d=\"M132.1,97.8l-0.4-3.3h-0.2c-1.5,2.1-4.3,3.9-8.1,3.9c-5.4,0-8.1-3.8-8.1-7.6c0-6.4,5.7-9.9,15.9-9.8v-0.5 c0-2.2-0.6-6.1-6-6.1c-2.5,0-5,0.8-6.9,2l-1.1-3.2c2.2-1.4,5.4-2.3,8.7-2.3c8.1,0,10.1,5.5,10.1,10.8v9.9c0,2.3,0.1,4.5,0.4,6.3 H132.1z M131.4,84.4c-5.3-0.1-11.2,0.8-11.2,5.9c0,3.1,2.1,4.6,4.5,4.6c3.4,0,5.6-2.2,6.4-4.4c0.2-0.5,0.3-1,0.3-1.5V84.4z\"/><path fill=\"#FFFFFF\" d=\"M146.6,63.9v7.6h6.9v3.7h-6.9v14.2c0,3.3,0.9,5.1,3.6,5.1c1.3,0,2.2-0.2,2.8-0.3l0.2,3.6 c-0.9,0.4-2.4,0.7-4.3,0.7c-2.2,0-4-0.7-5.2-2c-1.4-1.4-1.9-3.8-1.9-6.9V75.1h-4.1v-3.7h4.1v-6.3L146.6,63.9z\"/><path fill=\"#FFFFFF\" d=\"M171.4,97.8l-0.4-3.3h-0.2c-1.5,2.1-4.3,3.9-8.1,3.9c-5.4,0-8.1-3.8-8.1-7.6c0-6.4,5.7-9.9,15.9-9.8v-0.5 c0-2.2-0.6-6.1-6-6.1c-2.5,0-5,0.8-6.9,2l-1.1-3.2c2.2-1.4,5.4-2.3,8.7-2.3c8.1,0,10.1,5.5,10.1,10.8v9.9c0,2.3,0.1,4.5,0.4,6.3 H171.4z M170.7,84.4c-5.3-0.1-11.2,0.8-11.2,5.9c0,3.1,2.1,4.6,4.5,4.6c3.4,0,5.6-2.2,6.4-4.4c0.2-0.5,0.3-1,0.3-1.5V84.4z\"/></g></g></a></svg>"], "variableCacheTag": "0dc40d54f7c7b5fce4e0827169f5e7eb"}"""
    configjs = json.loads(initial_js_str)
    configjs['subtitle'] = variable.name

    entity_id_name = {}
    all_entities = Entity.objects.all().values('id', 'name').iterator()
    for each in all_entities:
        entity_id_name[each['id']] = each['name']

    entities_list = []
    with connection.cursor() as c:
        c.execute('select distinct fk_ent_id from data_values where fk_var_id = %s;', [int(varid)])

    for each in c.fetchall():
        entities_list.append(each[0])
    if len(entities_list) > 5:
        randomly_selected = random.sample(entities_list, 5)
    else:
        randomly_selected = entities_list

    configjs['selected-countries'] = [{'name': entity_id_name[item], 'id': item} for item in randomly_selected]
    configjs['tabs'] = ['chart']
    configjs['chart-dimensions'] = [{'id': None, 'order': 0, 'property': 'y', 'unit': variable.unit, 'displayName': '',
                                     'targetYear': None, 'isProjection': False, 'tolerance': 5,
                                     'color': '', 'chartId': None, 'variableId': int(varid)}]
    configjs['entity-type'] = ''
    configjs['map-config'] = ''
    configjs['id'] = int(varid)
    configjs['title'] = variable.name
    configjs['slug'] = ''
    configjs['variableCacheTag'] = 'dashapptag'

    return HttpResponse('App.loadChart(%s)' % json.dumps(configjs))


@xframe_options_exempt  # Allow embedding
def serve_graph(request: HttpRequest, varid: str):
    canonicalurl = request.build_absolute_uri('/dataviewer/graph/') + varid
    baseurl = request.build_absolute_uri('/dataiewer/graph/') + varid

    chartmeta = {}

    title = ''
    title = re.sub("/, \*time\*/", "", title)
    title = re.sub("/\*time\*/", "", title)
    chartmeta['title'] = title

    configpath = "%s/dataviewer/vardata/%s.js" % (settings.BASE_URL, varid)

    return render(request, 'show_chart.html',
                                context={'chartmeta': chartmeta, 'configpath': configpath,
                                         'query': '',
                                         })
