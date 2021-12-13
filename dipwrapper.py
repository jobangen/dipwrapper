#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dipwrapper
~~~~~~~~~~

Python wrapper for the API of the 'Dokumentations- und Informationssystems
für Parlamentsmaterialien (DIP)' of the german Bundestag:
https://dip.bundestag.de/%C3%BCber-dip/hilfe/api

An API key ist provided on the web site.

For implementation details of the API see the documentation:
https://dip.bundestag.de/documents/informationsblatt_zur_dip_api.pdf

"""

import requests
import xml.etree.ElementTree as ET


class DIP():
    """
    Represents the API.
    """
    base_url = 'https://search.dip.bundestag.de/api/v1'
    doctypes = ['aktivitaet', 'drucksache', 'drucksache-text',
                'person', 'plenarprotokoll', 'plenarprotokoll-text'
                'vorgang', 'vorgangsposition']
    fformats = ['xml', 'json']
    parameters = ['cursor', 'format', 'f.id', 'f.datum.start', 'f.datum.end',
                  'f.drucksache', 'f.plenarprotokoll', 'f.vorgang',
                  'f.aktivitaet', 'f.zuordnung']

    def __init__(self, apikey, fformat='json',):
        self.apikey = apikey
        self.fformat = fformat

    def _validate_resource_type(self, resource):
        "Make sure provided resource type is valid."
        if resource not in self.doctypes:
            raise ValueError(f"Invalid doctype '{resource}'. "
                             f"Expected one of: {self.doctypes}")

    def _validate_parameter_types(self, parameters):
        """
        Make sure provided parameters are valid.
        Not all types of resources support all query parameters;
        see DIP documentation.
        This is NOT enforced by this wrapper.
        """
        for key in parameters:
            if key not in self.parameters:
                raise ValueError(f"Invalid doctype '{key}'. "
                                 f"Expected one of: {self.parameters}")

    def _extract_documents(self, response):
        """
        Extract meta data of documents from response.
        """
        if self.fformat == 'json':
            return response.get('documents')
        else:
            return response.findall('document')

    def _extract_cursor(self, response):
        """
        Extract cursor from response.
        """
        if self.fformat == 'json':
            return response.get('cursor')
        else:
            return response.find('cursor').text

    def _get_and_handle_response(self, url, params):
        """
        Get response using url and params.
        Raise HTTPerror if error occurs.
        Parse response based on format.
        """
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        if self.fformat == 'json':
            return response.json()
        else:
            return ET.fromstring(response.content)

        return response

    def get_resource_all(self, res_type):
        """
        Get all documents of res_type.
        Returns generator that yields a list of
        json: dict
        xml: xml.etree.ElementTree.Element
        """
        self._validate_resource_type(res_type)
        url = f'{self.base_url}/{res_type}'
        params = {'format': self.fformat,
                  'apikey': self.apikey}
        while True:
            response = self._get_and_handle_response(url, params=params)
            cursor = self._extract_cursor()

            if params.get('cursor') != cursor:
                params['cursor'] = cursor
                yield self._extract_documents(response)
            else:
                break

    def get_resource_id(self, res_type, dpi_id):
        """
        Get meta data for document with dpi_id.
        Returns:
        json ->  dct
        xml  ->  xml.etree.ElementTree.Element
        """
        self._validate_resource_type(res_type)
        url = f'{self.base_url}/{res_type}/{dpi_id}'
        params = {'format': self.fformat,
                  'apikey': self.apikey}
        response = self._get_and_handle_response(url, params)
        return response

    def get_resource_multiple(self, res_type, parameters):
        """
        Get all documents that match query defined by parameters.
        The API accepts duplication of key which can be provided as follows:
        {'f.id': ['258442', '84394']}
        Returns generator that yields a list of
        json: dict
        xml: xml.etree.ElementTree.Element
        """
        self._validate_resource_type(res_type)
        url = f'{self.base_url}/{res_type}'
        params = {'format': self.fformat,
                  'apikey': self.apikey}
        params.update(parameters)
        self._validate_parameter_types(parameters)
        while True:
            response = self._get_and_handle_response(url, params=params)
            cursor = self._extract_cursor(response)

            if params.get('cursor') != cursor:
                params['cursor'] = cursor
                yield self._extract_documents(response)
            else:
                break
