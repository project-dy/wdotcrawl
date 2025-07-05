import argparse
import sys
import locale
import codecs
import os
from wikidot import Wikidot
from rmaint import RepoMaintainer

# TODO: Files.
# TODO: Forum and comment pages.
# TODO: Ability to download new transactions since last dump.
#   We'll probably check the last revision time, then query all transactions and select those with greater revision time (not equal, since we would have downloaded equals at the previous dump)

def get_crawl_parser():
    parser = argparse.ArgumentParser(description='Queries Wikidot')
    parser.add_argument('site', help='URL of Wikidot site')
    # Actions
    parser.add_argument('--list-pages', action='store_true', help='List all pages on this site')
    parser.add_argument('--list-pages-no', action='store_true', help='List number of total pages on this site')
    parser.add_argument('--rates', action='store_true', help='Print page rates (requires --page)')
    parser.add_argument('--max-page-count', type=int, default='10000', help='Only list/fetch up to this amount of pages')

    parser.add_argument('--source', action='store_true', help='Print page source (requires --page)')
    parser.add_argument('--content', action='store_true', help='Print page content (requires --page)')
    parser.add_argument('--log', action='store_true', help='Print page revision log (requires --page)')
    parser.add_argument('--dump', type=str, help='Download page revisions to this directory')
    # parser.add_argument('--convert-format', action='store_true', help='Convert save files format between legacy .txt and new .ftml')
    # Debug actions
    parser.add_argument('--list-pages-raw', action='store_true')
    parser.add_argument('--log-raw', action='store_true')
    parser.add_argument('--rates-raw', action='store_true')
    # Action settings
    parser.add_argument('--page', type=str, help='Query only this page')
    parser.add_argument('--depth', type=int, default='10000', help='Query only last N revisions')
    parser.add_argument('--revids', action='store_true', help='Store last revision ids in the repository')
    parser.add_argument('--category', type=str, help='Selecting categories to query (use "" to contain)')
    parser.add_argument('--tags', type=str, default=None, help='Selecting tags to query (use "" to contain)')
    parser.add_argument('--creator', type=str, default=None, help='Selecting page creator to query (use "" to contain)')
    parser.add_argument('--skip', type=str, help='Skip the specified revision')
    parser.add_argument('--skip-pages', type=str, help='Skip the specified pages')
    parser.add_argument('--cleanup', action='store_true', help='Clean up after downloading repo')
    parser.add_argument('--use-ftml', action='store_true', help='Use the new .ftml format for saving crawled pages instead of the legacy .txt format')
    # Common settings
    parser.add_argument('--debug', action='store_true', help='Print debug info')
    parser.add_argument('--delay', type=int, default='200', help='Delay between consequent calls to Wikidot')
    return parser


def force_dirs(path):
    os.makedirs(path, exist_ok=True)

def run_crawl_job(args, progress_callback=None):
    wd = Wikidot(args.site)
    wd.debug = args.debug
    wd.delay = args.delay

    if args.list_pages_raw:
        print((wd.list_pages_raw(args.max_page_count, 1, args.category, args.tags, args.creator)))

    elif args.list_pages:
        for page in wd.list_pages(args.max_page_count, args.category, args.tags, args.creator):
            print(page)

    elif args.list_pages_no:
        print(len(wd.list_pages(-1, args.category, args.tags, args.creator)))

    elif args.source:
        if not args.page:
            raise Exception("Please specify --page for --source.")

        page_id, _ = wd.get_page_id(page_unix_name=args.page)
        if not page_id:
            raise Exception("Page not found: "+args.page)

        revs = wd.get_revisions(page_id, 1) # last revision
        print((wd.get_revision_source(revs[0]['id'])))

    elif args.content:
        if not args.page:
            raise Exception("Please specify --page for --source.")

        page_id, _ = wd.get_page_id(page_unix_name=args.page)
        if not page_id:
            raise Exception("Page not found: "+args.page)

        revs = wd.get_revisions(page_id, 1) # last revision
        print((wd.get_revision_version(revs[0]['id'])))

    elif args.log_raw:
        if not args.page:
            raise Exception("Please specify --page for --log.")

        page_id, _ = wd.get_page_id(page_unix_name=args.page)
        if not page_id:
            raise Exception("Page not found: "+args.page)

        print((wd.get_revisions_raw(page_id, args.depth)))

    elif args.log:
        if not args.page:
            raise Exception("Please specify --page for --log.")

        page_id, _ = wd.get_page_id(page_unix_name=args.page)
        if not page_id:
            raise Exception("Page not found: "+args.page)
        for rev in wd.get_revisions(page_id, args.depth):
            print((str(rev)))

    elif args.rates_raw:
        if not args.page:
            raise Exception("Please specify --page for --rates-raw.")

        page_id, _ = wd.get_page_id(args.page)
        if not page_id:
            raise Exception("Page not found: "+args.page)

        print((wd.get_rates_raw(page_id)))

    elif args.rates:
        if not args.page:
            raise Exception("Please specify --page for --rates.")

        page_id, _ = wd.get_page_id(args.page)
        if not page_id:
            raise Exception("Page not found: "+args.page)

        print((wd.get_rates(page_id)))

    elif args.dump:
        print(("Downloading pages to "+args.dump))
        force_dirs(args.dump)

        rm = RepoMaintainer(wd, args.dump, progress_callback=progress_callback)
        rm.debug = args.debug
        rm.storeRevIds = args.revids
        rm.max_depth = args.depth
        rm.max_page_count = args.max_page_count
        rm.use_ftml = args.use_ftml
        rm.buildRevisionList([args.page] if args.page else None, args.category, args.tags, args.creator)
        rm.openRepo()

        if args.skip_pages:
            rm.pages_to_skip = args.skip_pages.split(",")
        if args.skip:
            rm.revs_to_skip = args.skip.split(",")
        
        if (rm.use_ftml and (rm.use_ftml != rm.prev_use_ftml)):
            rm.convertFormat()

        print("Downloading revisions")
        rm.fetchAll()

        if args.cleanup:
            rm.cleanup()

        print("Done.")

if __name__ == "__main__":
    _args = parser.parse_args()
    run_crawl_job(_args)
