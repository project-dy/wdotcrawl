import requests
import random
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse

# Implements various queries to Wikidot engine through its AJAX facilities


class Wikidot:
    def __init__(self, site):
        self.site = site        # Wikidot site to query
        self.sitename = urlparse(site).hostname.lower()
        self.delay = 200        # Delay between requests in msec
        self.debug = False      # Print debug messages
        self.next_timeslot = time.process_time()   # Can call immediately
        self.max_retries = 5


    # To honor usage rules, we wait for self.delay between requests.
    # Low-level query functions call this before every request to Wikidot./
    def _wait_request_slot(self):
        tm = time.process_time()
        if self.next_timeslot - tm > 0:
            time.sleep(self.next_timeslot - tm)
        self.next_timeslot = tm + self.delay / 1000
        pass

    # Makes a Wikidot AJAX query. Returns the response+title or throws an error.
    def queryex(self, params, urlAppend = None):
        token = "".join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for i in range(8))
        cookies = {"wikidot_token7": token}
        params['wikidot_token7'] = token

        if self.debug:
            print(params)
            print(cookies)

        url = self.site+'/ajax-module-connector.php'
        if urlAppend is not None:
            url += urlAppend

        # In case of e. g. 500 errors
        retries = 0
        while retries < self.max_retries:
            self._wait_request_slot()

            req = requests.request('POST', url, data=params, cookies=cookies)

            # Usually a 502 error, recovers immediately
            if req.status_code >= 500:
                retries += 1
                print('500 error for ' + url + ', retries ' + str(retries) + '/' + str(self.max_retries))

                # In case of debug enabled, we already printed this above
                if not self.debug:
                    print(req, params)

                # Be nice, double wait delay for errors
                self._wait_request_slot()

                continue

            try:
                # In case of 404 errors or other stuff that indicates
                # some bug in how we handle or request things
                req.raise_for_status()
                json = req.json()
            except Exception as e:
                print('Failed to get response from wikidot', e, req, url, params)
                raise e

            if json['status'] == 'ok':
                return json['body'], (json['title'] if 'title' in json else '')
            else:
                raise Exception(req.text)

        print('Failed too many times', url, params, cookies)
        raise Exception('Failed too many times for ' + url)

    # Same but only returns the body, most responses don't have titles
    def query(self, params, urlAppend = None):
        return self.queryex(params, urlAppend)[0]


    # List all pages for the site.

    # Raw version
    # For the supported formats (module_body) see:
    # See https://github.com/gabrys/wikidot/blob/master/php/modules/list/ListPagesModule.php
    def list_pages_raw(self, limit, offset, category, tags):
        op = {
          'moduleName': 'list/ListPagesModule',
          'limit': limit if limit else '10000',
          'perPage': limit if (limit and limit>0) else '10000',
          'module_body': '%%page_unix_name%%',
          'separate': 'false',
          'p': str(offset),
          'category': category if category else '.',
          'tags': tags if tags else None,
          'order': 'dateCreatedDesc',  # This way limit makes sense. This is also the default
        }
        if limit<=0:
            del op['limit']
        if not tags:
            del op['tags']
        res = self.query(op, '/p/' + str(offset))
        return res

    # Client version
    def list_pages(self, limit, category, tags):
        offset = 1
        pages = []

        while True:
            raw = self.list_pages_raw(limit, offset, category, tags).replace('<br/>',"\n")
            soup = BeautifulSoup(raw, 'html.parser')


            for entry in soup.div.p.text.split('\n'):
                pages.append(entry)

            if self.debug:
                print('Pages found:', len(pages))

            targets = soup.find_all('span','target')
            if len(targets) < 2:
                print("Unable to find next listing page, not enough target spans")
                break

            next_url = targets[-1].a.get('href').split('/')
            if len(next_url) > 0 and next_url[-1].isnumeric():
                next_page = int(next_url[-1])

                if self.debug:
                    print('Next listing page', next_page)

            else:
                print("invalid next url", next_url)
                break

            #next_page = int(targets[0].a.text)

            current_spans = soup.find_all('span','current')
            if len(current_spans) > 0:
                current_page = int(current_spans[0].text)

                if self.debug:
                    print('Current listing page', current_page)

            else:
                print("unable to find current page")
                break;

            if next_page != offset + 1:
                if self.debug:
                    print('Next page is wrong', next_page, 'hopefully at the end')
                break

            offset += 1

            print("Fetching listing page", offset)

        return pages


    # Retrieves internal page_id by page unix_name.
    # Page IDs are required for most of page functions.

    def get_page_id(self, page_unix_name):
        # The only freaking way to get page ID is to load the page! Wikidot!
        self._wait_request_slot()
        req = requests.request('GET', self.site+'/'+page_unix_name + '/noredirect/true')
        soup = BeautifulSoup(req.text, 'html.parser')
        for item in soup.head.find_all('script'):
            text = item.text
            pos = text.find("WIKIREQUEST.info.pageId = ")
            if pos >= 0:
                pos += len("WIKIREQUEST.info.pageId = ")
                crlf = text.find(";", pos)
                if crlf >= 0:
                    return int(text[pos:crlf])
                else:
                    return int(text[pos:])
        return None


    # Retrieves and returns page tags by page unix_name.

    def get_page_tags(self, page_unix_name):
        # You can only go page scraping if the tags are never changed and the cli isn't logged into wikidot...
        self._wait_request_slot()
        req = requests.request('GET', self.site+'/'+page_unix_name + '/noredirect/true')
        soup = BeautifulSoup(req.text, 'html.parser')
        taglist = []
        for item in soup.body.select('.page-tags span a'):
            taglist.append(item.text)
        return ' '.join(taglist)


    # Retrieves a list of revisions for a page.
    # See https://github.com/gabrys/wikidot/blob/master/php/modules/history/PageRevisionListModule.php

    # Raw version
    def get_revisions_raw(self, page_id, limit):
        res = self.query({
          'moduleName': 'history/PageRevisionListModule',
          'page_id': page_id,
          'page': '1',
          'perpage': limit if limit else '10000',
          'options': '{"all":true}'
        })

        soup = BeautifulSoup(res, 'html.parser')
        return soup.table.contents

    # Client version
    def get_revisions(self, page_id, limit):
        revs = []
        for tr in self.get_revisions_raw(page_id, limit):
            if tr.name != 'tr': continue # there's a header + various junk

            # RevID is stored as a value of an INPUT field
            rev_id = tr.input['value'] if tr.input else None
            if rev_id is None: continue # can't parse

            # Flag in <span class="spantip">
            rev_flag = None
            flag_span = tr.find("span", attrs={"class": "spantip"})
            rev_flag = flag_span.getText() if flag_span else None

            # Unixtime is stored as a CSS class time_*
            rev_date = 0
            date_span = tr.find("span", attrs={"class": "odate"})
            if date_span is not None:
                for cls in date_span['class']:
                    if cls.startswith('time_'):
                        rev_date = int(cls[5:])

            # Username in a last <a> under <span class="printuser">
            user_span = tr.find("span", attrs={"class": "printuser"})
            last_a = None
            for last_a in user_span.find_all('a'): pass
            rev_user = last_a.getText() if last_a else None


            # Comment is in the last TD of the row
            last_td = None
            for last_td in tr.find_all('td'): pass
            rev_comment = last_td.getText() if last_td else ""

            revs.append({
                'id': rev_id,
                'flag': rev_flag,
                'date': rev_date,
                'user': rev_user,
                'comment': rev_comment,
            })
        return revs

    # Retrieves revision differences for two revisions.
    # The from and to ids cannot be the same.
    def get_revision_diff_raw(self, from_rev_id, to_rev_id):
        res = self.query({
          'moduleName': 'history/PageDiffModule',
          'from_revision_id': from_rev_id,
          'to_revision_id': to_rev_id,
          'show_type': 'inline',
        }).replace('</a>','') # the tags in the <td> from PageDiffModule response strangely have </a> appended

        soup = BeautifulSoup(res, 'html.parser')
        return soup.table.contents

    # Returns only the tag changes since that's the only thing we care about at the moment.
    # Why does wikidot not put tags in the response from page version module?
    def get_tags_from_diff(self, from_rev_id, to_rev_id):
        from_tags = None
        to_tags = None
        for tr in self.get_revision_diff_raw(from_rev_id, to_rev_id):
            if tr.name != 'tr': continue # there's a header + various junk
            tds = tr.find_all('td')
            if len(tds) < 3: continue
            if tds[0].get_text().strip() == 'Tags:':
                from_tags = tds[1].get_text().strip()
                to_tags = tds[2].get_text().strip()

        return from_tags

    # Retrieves revision source for a revision.
    # There's no raw version because there's nothing else in raw.
    def get_revision_source(self, rev_id):
        res = self.query({
          'moduleName': 'history/PageSourceModule',
          'revision_id': rev_id,
          # We don't need page id
        })
        # The source is HTMLified but BeautifulSoup's getText() will decode that
        # - htmlentities
        # - <br/>s in place of linebreaks
        # - random real linebreaks (have to be ignored)
        soup = BeautifulSoup(res, 'html.parser')
        return soup.div.getText().lstrip(' \r\n')

    # Retrieves the rendered version + additional info unavailable in get_revision_source:
    # * Title
    # * Unixname at the time
    def get_revision_version_raw(self, rev_id):
        res = self.queryex({
          'moduleName': 'history/PageVersionModule',
          'revision_id': rev_id,
        })
        return res

    def get_revision_version(self, rev_id):
        res = self.get_revision_version_raw(rev_id) # this has title!
        soup = BeautifulSoup(res[0], 'html.parser')

        # First table is a flyout with revision details. Remove and study it.
        unixname = None
        details = soup.find("div", attrs={"id": "page-version-info"}).extract()
        for tr in details.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) < 2: continue
            if tds[0].getText().strip() == 'Page name:':
                unixname = tds[1].getText().strip()

        return {
          'rev_id': rev_id,
          'unixname': unixname,
          'title': res[1],
          'content': str(soup), # only content remains
        }


    # Retrieves the rating for the page
    def get_rates_raw(self, page_id):
        res = self.query({
          'moduleName': 'pagerate/WhoRatedPageModule',
          'pageId': page_id,
        })

        return res

    def get_rates(self, page_id):
        res = self.get_rates_raw(page_id).replace("<br/>", "</div><div>") # put every vote in their own divs
        soup = BeautifulSoup(res, 'html.parser')
        voters_raw = soup.select("div")
        voters_raw.pop(-1) # remove empty last div
        voters = {"normal":{}, "deleted":{}}
        for voter in voters_raw:
            vote = voter.span.find_next_sibling("span").string.strip()
            if voter.find("span", class_="deleted"):
                user = voter.find("span", class_="deleted")["data-id"]
                voters["deleted"][user] = vote
            else:
                user = voter.span.a["onclick"].replace('WIKIDOT.page.listeners.userInfo(','').replace('); return false;','') # why does wikidot only put existing user id in the onclick attribute bruh
                voters["normal"][user] = vote
        return voters
