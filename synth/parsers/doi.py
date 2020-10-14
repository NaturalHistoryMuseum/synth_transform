import re
from urllib.parse import unquote_plus

import requests
import untangle


class DOIExtractor(object):
    @classmethod
    def dois(cls, string, fix=False):
        """
        Run through all the stages to try and find a DOI in the given string. Does not check if the DOI is valid; the
        calling function must do this and break at the appropriate point.
        """
        stages = [
            cls.doi_regex,
            cls.doi_regex_strict,
            cls.mapress_regex,
            cls.nature_regex,
            cls.cambridge_regex,
            cls.elsevier_api,
            cls.cambridge_bibtex,
            cls.ingenta_bibtex,
            cls.pensoft_bibtex,
            cls.pubmed
        ]

        if fix:
            string = unquote_plus(string)
            string = string.replace(' ', '')

        for s in stages:
            try:
                extracted_doi = s(string)
            except Exception as e:
                extracted_doi = None
            if extracted_doi is not None:
                yield extracted_doi, s.__name__

    @classmethod
    def doi_regex(cls, string):
        """
        Search for a DOI in a standard format.

        :param string: a string to search
        :return: the DOI string or None if nothing was found
        """
        # regex source: https://www.crossref.org/blog/dois-and-matching-regular-expressions/
        rgx = re.compile(r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', re.I)
        extras_regex = re.compile(r'[./](e?pdf|abstract|full|short)', re.I)
        doi_match = rgx.search(string)
        if doi_match:
            doi = doi_match.group()
            # DOIs are case insensitive so convert everything to uppercase
            # https://www.doi.org/doi_handbook/2_Numbering.html#2.4
            doi = doi.upper()
            # strip trailing full stops
            doi = doi.rstrip('.')
            # remove any URL artifacts
            doi = extras_regex.split(doi)[0]
            return doi

    @classmethod
    def doi_regex_strict(cls, string):
        """
        Similar to doi_regex, but excluding some of the less common characters in order to avoid
        picking up non-DOI artefacts.

        :param string: a string to search
        :return: the DOI string or None if nothing was found
        """
        rgx = re.compile(r'10.\d{4,9}/[-._A-Z0-9]+', re.I)
        extras_regex = re.compile(r'[./](e?pdf|abstract|full|short)', re.I)
        doi_match = rgx.search(string)
        if doi_match:
            doi = doi_match.group()
            # DOIs are case insensitive so convert everything to uppercase
            # https://www.doi.org/doi_handbook/2_Numbering.html#2.4
            doi = doi.upper()
            # strip trailing full stops
            doi = doi.rstrip('.')
            # remove any URL artifacts
            doi = extras_regex.split(doi)[0]
            return doi

    @classmethod
    def mapress_regex(cls, string):
        """
        Search for a DOI in mapress.com/biotaxa.org URLs.
        :param string: a string to search
        """
        rgx = re.compile(r'(\w+taxa\.\d{1,4}\.\d+\.\d+)')
        mapress = rgx.search(string)
        if mapress is not None:
            doi = '10.11646/' + mapress.groups()[0]
            return doi

    @classmethod
    def nature_regex(cls, string):
        rgx = re.compile(r'(s\d{5}-\d{3}-\d{5}-.)')
        nature = rgx.search(string)
        if nature is not None:
            doi = '10.1038/' + nature.groups()[0]
            return doi
        rgx = re.compile(r'nature\.com/articles/([^/]+)')
        nature = rgx.search(string)
        if nature is not None:
            doi = '10.1038/' + nature.groups()[0]
            return doi

    @classmethod
    def cambridge_regex(cls, string):
        rgx = re.compile(r'fileId=(S[A-Z0-9]+)')
        cambridge = rgx.search(string)
        if cambridge is not None:
            doi = '10.1017/' + cambridge.groups()[0]
            return doi

    @classmethod
    def elsevier_api(cls, string):
        rgx = re.compile(r'([SB][A-Z0-9]{16})')
        elsevier_id = rgx.search(string)
        if elsevier_id is not None:
            pii = elsevier_id.groups()[0]
            r = requests.get(f'https://api.elsevier.com/content/article/pii/{pii}')
            if r.ok:
                xml_tree = untangle.parse(r.text)
                doi = xml_tree.full_text_retrieval_response.coredata.prism_doi.cdata
                return doi

    @classmethod
    def cambridge_bibtex(cls, string):
        if 'cambridge.org' not in string:
            return
        original_url = ('http://' if not string.startswith('http') else '') + string
        r = requests.get(original_url)
        redirect_url = r.url
        cambridge_id = redirect_url.split('/')[-1]
        url = 'https://www.cambridge.org/core/services/aop-easybib/export?exportType=bibtex&productIds=' + \
              cambridge_id + '&citationStyle=bibtex'
        r = requests.get(url)
        doi = cls.doi_regex(r.text)
        return doi

    @classmethod
    def ingenta_bibtex(cls, string):
        rgx = re.compile(r'(ingentaconnect\.com/.+/\d{4}/\d+/\d+/art\d+)')
        ingenta = rgx.search(string)
        if ingenta is not None:
            url = 'http://www.' + ingenta.groups()[0] + '?format=bib'
            r = requests.get(url)
            doi = cls.doi_regex(r.text)
            return doi

    @classmethod
    def pensoft_bibtex(cls, string, use_regex_2=False):
        if 'pensoft' not in string and 'zookeys' not in string:
            return
        rgx_1 = r'articles.php\?.*id=(\d+)'
        rgx_2 = r'(?<!_)(?:article_)?id=(\d+)|articles?/(\d+)'  # only use if the other format doesn't work
        id_rgx = re.compile(rgx_2 if use_regex_2 else rgx_1)
        journal_rgx = re.compile(r'([a-z]+)\.pensoft|journals/([a-z]+)')
        pensoft = id_rgx.search(string)
        if pensoft is not None:
            journal = journal_rgx.search(string)
            if journal is None or all([j is None or j == 'www' for j in journal.groups()]):
                journal = 'zookeys'
            else:
                journal = [j for j in journal.groups() if j != 'www' and j is not None][0]
            pensoft_id = [i for i in pensoft.groups() if i is not None][0]
            url = 'https://' + journal + '.pensoft.net/article/' + pensoft_id + '/download/bibtex'
            r = requests.get(url)
            doi = cls.doi_regex(r.text)
            return doi
        elif not use_regex_2:
            r = requests.get(string)
            if r.ok and r.url != string:
                doi = cls.pensoft_bibtex(r.url)
                if doi is not None:
                    return doi
                else:
                    return cls.pensoft_bibtex(string, use_regex_2=True)

    @classmethod
    def pubmed(cls, string):
        if 'ncbi.nlm.nih.gov' not in string:
            return
        rgx = re.compile(r'(\d{7})')
        pmcid = rgx.search(string)
        if pmcid is not None:
            url = 'https://refinder.org/find?search=simple&db=pubmed&limit=1&text=PMC' + pmcid.groups()[0]
            r = requests.get(url)
            if r.ok:
                doi = r.json()[0]['doi']
                return doi
