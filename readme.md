*This is a fork to make a permanent backup of the SCP wiki.*

This is a Python command line client for relatively popular wiki hosting
http://www.wikidot.com which lets you:

* List all pages on a site
* See all revisions of a page
* Query page source
* List number of pages on a site
* Query condition selection by category and/or tags

Most interestingly, it allows you to download the whole site as a Git repository, with proper commit dates, author and comments!

##### Dependencies

You can install the dependencies using 
```bash
pip install -r requirements.txt
```

##### Examples:

    crawl.py http://example.wikidot.com --dump ExampleRepo --use-ftml
    crawl.py http://example.wikidot.com --log --page example-page

It uses internal Wikidot AJAX requests to do its job. If you're from Wikidot, please don't break it. Thank you! We'll try to be nice and not put a load on your servers.

Downloading of large sites might take a while. If anything breaks, just restart the same command, it'll continue from where it crashed.

##### Useful links:

Wikidot code (very old) which simplifies things a bit:

* https://github.com/gabrys/wikidot/blob/master/php/modules/history/PageRevisionListModule.php

The descriptions for on-site modules are heavily correlated with AJAX ones:

* http://www.wikidot.com/doc-modules:listpages-module

Someone else did Wikidot AJAX:

* https://github.com/kerel-fs/ogn-rdb/blob/master/wikidotcrawler.py

The FTML format tailored for Wikidot-styled metadata and page source code:

* https://gist.github.com/Zokhoi/06dbc890a4f2fab3eadcd7d2ed0d8698

#### TODO

 - Handle deleted images. Probably need to check the diff and check all pages for references if removed from one page.
