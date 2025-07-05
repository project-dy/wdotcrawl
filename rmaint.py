import wikidot

# Basic python stuff
import os
import codecs
import pickle as pickle
import json

# git stuff
from git import Repo, Actor
import time # For parsing unix epoch timestamps from wikidot and convert to normal timestamps
import re # For sanitizing usernames to fake email addresses

from tqdm import tqdm # for progress bar

# Repository builder and maintainer
# Contains logic for actual loading and maintaining the repository over the course of its construction.

# Usage:
#   rm = RepoMaintainer(wikidot, path)
#   rm.buildRevisionList(pages, category, tags)
#   rm.openRepo()
#   while rm.commitNext():
#       pass
#   rm.cleanup()

# Talkative.

class RepoMaintainer:
    def __init__(self, wikidot, path, progress_callback=None):
        # Settings
        self.wd = wikidot           # Wikidot instance
        self.path = path            # Path to repository
        self.debug = False          # = True to enable more printing
        self.storeRevIds = True     # = True to store .revid with each commit
        self.use_ftml = True        # Shether to use the new FTML format instead of plain text
        self.prev_use_ftml = None   # Whether we have used FTML format in previous runs
        self.progress_callback = progress_callback

        # Internal state
        self.wrevs = None           # Compiled wikidot revision list (history)

        self.rev_no = 0             # Next revision to process
        self.last_names = {}        # Tracks page renames: name atm -> last name in repo
        self.last_parents = {}      # Tracks page parent names: name atm -> last parent in repo
        self.category = None        # Tracks category(s) to get form
        self.tags = None            # Tracks tag(s) to get form
        self.created_by = None      # Tracks creator to get from

        self.repo = None            # Git repo object
        self.index = None           # Git current index object
        self.max_depth = 10000      # download at most this number of revisions
        self.max_page_count = 10000 # download at most this number of pages

        self.pbar = None
        self.first_fetched = 0      # For progress bar
        self.fetched_revids = set()

        self.revs_to_skip = []
        self.pages_to_skip = []


    #
    # Saves and loads revision list from file
    #
    def saveWRevs(self):
        fp = open(self.path+'/.wrevs', 'wb')
        pickle.dump(self.wrevs, fp)
        fp.close()

    def loadWRevs(self):
        fp = open(self.path+'/.wrevs', 'rb')
        self.wrevs = pickle.load(fp)
        fp.close()

    def savePages(self, pages):
        fp = open(self.path+'/.pages', 'wb')
        pickle.dump(pages, fp)
        fp.close()

    def appendFetchedRevid(self, revid):
        fp = open(self.path+'/.fetched.txt', 'a')
        fp.write(revid + '\n')
        fp.close()

    def loadFetchedRevids(self):
        self.fetched_revids = set([line.rstrip() for line in open(self.path+'/.fetched.txt', 'r')])

    def saveFailedImages(self):
        file_path = self.path + '/.failed-images.txt'
        fp = open(file_path, 'w')
        for failed in self.wd.failed_images:
            fp.write(failed + '\n')
        fp.close()

    def loadFailedImages(self):
        file_path = self.path + '/.failed-images.txt'
        if not os.path.isfile(file_path):
            return
        self.wd.failed_images = set([line.rstrip() for line in open(file_path, 'r')])

    # Persistent metadata about the repo:
    #  - Tracks page renames: name atm -> last name in repo
    #  - Tracks page parent names: name atm -> last parent in repo
    # Variable metadata about the repo:
    #  - Tracks category: settings for category(s) to get from
    #  - Tracks tags: settings for tag(s) to get from
    def saveMetadata(self):
        metadata = {
            'category': self.category,
            'tags': self.tags,
            'created_by': self.created_by,
            'names': self.last_names,
            'parents': self.last_parents,
            'use_ftml': self.use_ftml
        }
        fp = open(self.path+'/.metadata.json', 'w')
        json.dump(metadata, fp)
        fp.close()

    def loadMetadata(self):
        fp = open(self.path+'/.metadata.json', 'r')
        metadata = json.load(fp)
        self.category = metadata['category']
        self.tags = metadata['tags']
        self.created_by = metadata['created_by']
        self.last_names = metadata['names']
        self.last_parents = metadata['parents']
        self.prev_use_ftml = metadata['use_ftml'] if 'use_ftml' in metadata else None
        fp.close()

        self.loadFetchedRevids()
    #
    # Compiles a combined revision list for a given set of pages, or all pages on the site.
    #  pages: compile history for these pages
    #  category: get from these category(s)
    #  tags: get from these tag(s)
    #
    # If there exists a cached revision list at the repository destination,
    # it is loaded and no requests are made.
    #
    def buildRevisionList(self, pages = None, category = None, tags = None, created_by = None):
        self.category = category if category else (self.category if self.category else '.')
        self.tags = tags if tags else (self.tags if self.tags else None)
        self.created_by = created_by if created_by else (self.created_by if self.created_by else None)
        self.use_ftml = self.use_ftml if self.use_ftml is not None else (self.prev_use_ftml if self.prev_use_ftml is not None else True)

        if os.path.isfile(self.path+'/.wrevs'):
            tqdm.write("Loading cached revision list...")
            if self.progress_callback:
                self.progress_callback("Loading cached revision list...")
            self.loadWRevs()
        else:
            self.wrevs = []
            if self.debug:
                tqdm.write('No existing wrevs')
                if self.progress_callback:
                    self.progress_callback('No existing wrevs')

        if os.path.isfile(self.path+'/.fetched.txt'):
            self.loadFetchedRevids()
            tqdm.write(f"{len(self.fetched_revids)} revisions already fetched")
            if self.progress_callback:
                self.progress_callback(f"{len(self.fetched_revids)} revisions already fetched")
        else:
            self.fetched_revids = set()

        if self.debug:
            tqdm.write("Building revision list...")
            if self.progress_callback:
                self.progress_callback("Building revision list...")

        fetched_pages = set()

        if not pages:
            if os.path.isfile(self.path+'/.pages'):
                tqdm.write('Loading fetched pages')
                if self.progress_callback:
                    self.progress_callback('Loading fetched pages')
                fp = open(self.path+'/.pages', 'rb')
                pages = pickle.load(fp)
                fp.close()


            if not pages or len(pages) < self.max_page_count:
                if self.debug:
                    tqdm.write('Need to fetch pages')
                    if self.progress_callback:
                        self.progress_callback('Need to fetch pages')
                pages = self.wd.list_pages(self.max_page_count, self.category, self.tags, self.created_by)
                self.savePages(pages)
            elif self.debug:
                tqdm.write(f"{len(pages)} pages loaded")
                if self.progress_callback:
                    self.progress_callback(f"{len(pages)} pages loaded")

        total_wrevs_collect = len(self.wrevs)
        for i, wrev in enumerate(tqdm(self.wrevs, desc='Collecting pages we already got revisions for', disable=self.progress_callback is not None)):
            if self.progress_callback:
                self.progress_callback(f"Collecting pages with revisions: {i+1}/{total_wrevs_collect}")
            page_name = wrev['page_name']

            if page_name in fetched_pages:
                continue

            fetched_pages.add(page_name)

        if self.debug:
            tqdm.write(f"Already fetched revisions for {len(fetched_pages)} of {len(pages)}")
            if self.progress_callback:
                self.progress_callback(f"Already fetched revisions for {len(fetched_pages)} of {len(pages)}")

        fetched = 0
        total_pages_to_fetch = len(pages)
        for i, page in enumerate(tqdm(pages, desc='Updating list of revisions to fetch', disable=self.progress_callback is not None)):
            if self.progress_callback:
                self.progress_callback(f"Updating list of revisions to fetch: {i+1}/{total_pages_to_fetch}")
            if page in fetched_pages:
                continue

            # TODO: more generic blacklisting
            if page == "sandbox":
                if self.debug:
                    tqdm.write(f"Skipping {page}")
                    if self.progress_callback:
                        self.progress_callback(f"Skipping {page}")
                continue

            fetched += 1
            page_id = self.wd.get_page_id(page)

            if self.debug:
                tqdm.write(f"ID: {page_id}")
                if self.progress_callback:
                    self.progress_callback(f"ID: {page_id}")

            if page_id is None:
                tqdm.write(f'Page gone? {page}')
                if self.progress_callback:
                    self.progress_callback(f'Page gone? {page}')
                continue

            revs = self.wd.get_revisions(page_id=page_id, limit=self.max_depth)
            for rev in revs:
                if rev['id'] in self.fetched_revids:
                    continue

                self.wrevs.append({
                  'page_id' : page_id,
                  'page_name' : page, # current name, not at revision time (revisions can rename them)
                  'rev_id' : rev['id'],
                  'flag' : rev['flag'],
                  'date' : rev['date'],
                  'user' : rev['user'],
                  'comment' : rev['comment'],
                })
            self.saveWRevs() # Save a cached copy

        tqdm.write(f"Number of revisions already fetched {len(self.fetched_revids)} {len(self.wrevs)}")
        if self.progress_callback:
            self.progress_callback(f"Number of revisions already fetched {len(self.fetched_revids)} {len(self.wrevs)}")

        if os.path.isfile(self.path+'/.metadata.json'):
            self.loadMetadata()

        tqdm.write("")
        if self.progress_callback:
            self.progress_callback("")

        tqdm.write(f"Total revisions: {len(self.wrevs)}")
        if self.progress_callback:
            self.progress_callback(f"Total revisions: {len(self.wrevs)}")

        if self.debug:
            tqdm.write("Sorting revisions...")
            if self.progress_callback:
                self.progress_callback("Sorting revisions...")

        self.wrevs.sort(key=lambda rev: rev['date'])

        if self.debug:
            if len(self.wrevs) < 100:
                tqdm.write("")
                tqdm.write("Revision list: ")
                if self.progress_callback:
                    self.progress_callback("")
                    self.progress_callback("Revision list: ")
                for rev in self.wrevs:
                    tqdm.write(str(rev)+"\n")
                    if self.progress_callback:
                        self.progress_callback(str(rev)+"\n")
                tqdm.write("")
                if self.progress_callback:
                    self.progress_callback("")
            else:
                tqdm.write("Too many revisions, not printing everything")
                if self.progress_callback:
                    self.progress_callback("Too many revisions, not printing everything")


    #
    # Saves and loads operational state from file
    #
    def saveState(self):
        fp = open(self.path+'/.wstate', 'wb')
        pickle.dump(self.rev_no, fp)
        fp.close()

    def loadState(self):
        if not os.path.isfile(self.path+'/.wstate'):
            return
        fp = open(self.path+'/.wstate', 'rb')
        self.rev_no = pickle.load(fp)
        fp.close()


    #
    # Initializes the construction process, after the revision list has been compiled.
    # Either creates a new repo, or loads the existing one at the target path
    # and restores its construction state.
    #
    def openRepo(self):
        # Create a new repository or continue from aborted dump
        self.last_names = {} # Tracks page renames: name atm -> last name in repo
        self.last_parents = {} # Tracks page parent names: name atm -> last parent in repo
        self.loadFailedImages()

        if os.path.isdir(self.path+'/.git'):
            tqdm.write("Continuing from aborted dump state...")
            if self.progress_callback:
                self.progress_callback("Continuing from aborted dump state...")
            self.loadState()
            self.repo = Repo(self.path)
            assert not self.repo.bare

        else: # create a new repository (will fail if one exists)
            tqdm.write("Initializing repository...")
            if self.progress_callback:
                self.progress_callback("Initializing repository...")
            self.repo = Repo.init(self.path)
            self.rev_no = 0

            if self.storeRevIds:
                # Add revision id file to the new repo
                fname = '.revid'
                codecs.open(self.path + '/' + fname, "w", "UTF-8").close()
                self.repo.index.add([fname])
                self.index.commit("Initial creation of repo")
        self.index = self.repo.index

    #
    # Takes an unprocessed revision from a revision log, fetches its data and commits it.
    # Returns false if no unprocessed revisions remain.
    #
    def commitNext(self, rev):
        if self.rev_no >= len(self.wrevs):
            return False

        rev = self.wrevs[self.rev_no]
        pagerev = [val for idx,val in enumerate(self.wrevs) if (val['page_id']==rev['page_id'])]
        tagrev = [val for idx,val in enumerate(pagerev) if (val['flag']=='A')]
        unixname = rev['page_name']

        if rev['rev_id'] in self.fetched_revids:
            self.rev_no += 1

            self.saveState() # Update operation state
            return True

        if rev['rev_id'] in self.revs_to_skip:
            tqdm.write(f"Skipping {rev}")
            if self.progress_callback:
                self.progress_callback(f"Skipping {rev}")
            return True

        unixname = rev['page_name']
        if unixname in self.pages_to_skip:
            tqdm.write(f"Skipping {rev}")
            if self.progress_callback:
                self.progress_callback(f"Skipping {rev}")
            return True

        source = self.wd.get_revision_source(rev['rev_id'])
        # Page title and unix_name changes are only available through another request:
        details = self.wd.get_revision_version(rev['rev_id'])
        # Page tags changes are only available through a third request:
        if tagrev:
            new_rev_id = ""
            for id in [v['rev_id'] for k in [enumerate(tagrev), enumerate(pagerev)] for j, v in k]:
                if rev['rev_id']!=id:
                    new_rev_id = id
                    break
                else:
                    continue
            if not new_rev_id:
                # Page scraping for tags
                # This is only done where the only new revision for a single page
                # in the to-be-fetched list is a tag change, and we'll be better off
                # scraping for tags than to get another revision id for comparison
                tags = self.wd.get_page_tags(unixname)
            else:
                tags = self.wd.get_tags_from_diff(rev['rev_id'], new_rev_id)
        else:
            # Page scraping for tags
            # This has to be done because we don't know if the page tags are added along
            # with a new page creation (and possibly persist throughout revision history)
            tags = self.wd.get_page_tags(unixname)

        # Store revision_id for last commit
        # Without this, empty commits (e.g. file uploads) will be skipped by Git
        if self.storeRevIds:
            fname = self.path+'/.revid'
            outp = codecs.open(fname, "w", "UTF-8")
            outp.write(rev['rev_id']) # rev_ids are unique amongst all pages, and only one page changes in each commit anyway
            outp.close()

        winsafename = unixname.replace(':','~') # windows does not allow ':' in file name, this makes pages with colon in unix name safe on windows
        rev_unixname = details['unixname'] if details['unixname'] else unixname # may be different in revision than atm
        rev_winsafename = rev_unixname.replace(':','~') # windows-safe name in revision

        # Unfortunately, there's no exposed way in Wikidot to see page breadcrumbs at any point in history.
        # One way to know they were changed is by comparing revisions, but that require a request for each revision.
        # Another way is revision comments, though evil people may trick us.
        if rev['comment'].startswith('Parent page set to: "'):
            # This is a parenting revision, remember the new parent
            parent_unixname = rev['comment'][21:-2]
            if self.debug:
                tqdm.write(f'Parent changed {parent_unixname}')
                if self.progress_callback:
                    self.progress_callback(f'Parent changed {parent_unixname}')
            self.last_parents[unixname] = parent_unixname
        else:
            # Else use last parent_unixname we've recorded
            parent_unixname =  self.last_parents[unixname] if unixname in self.last_parents else None

        ## TODO: test#APIs
        #if rev['comment'].startswith('Removed tags: ') or rev['comment'].startswith('Added tags: '):
        #    self.updateTags(rev['comment'], rev_unixname)

        # There are also problems when parent page gets renamed -- see updateChildren

        # If the page is tracked and its name just changed, tell Git
        fname = str(rev_winsafename)
        if (self.use_ftml): fname = fname + '.ftml'
        else: fname = fname + '.txt' # legacy format
        rename = (unixname in self.last_names) and (self.last_names[unixname] != rev_unixname)

        commit_msg = ""

        added_file_paths = []

        if rename:
            name_rename_from = str(self.last_names[unixname]).replace(':','~')
            if (self.use_ftml): name_rename_from = name_rename_from + '.ftml'
            else: name_rename_from = name_rename_from + '.txt' # legacy format

            if self.debug:
                tqdm.write(f"Moving renamed {name_rename_from} to {fname}")
                if self.progress_callback:
                    self.progress_callback(f"Moving renamed {name_rename_from} to {fname}")

            self.updateChildren(self.last_names[unixname], rev_unixname) # Update children which reference us -- see comments there

            # Try to do the best we can, these situations usually stem from vandalism people have cleaned up
            if os.path.isfile(self.path + '/' + name_rename_from):
                self.index.move([name_rename_from, fname], force=True)
                commit_msg += "Renamed from " + str(self.last_names[unixname]) + ' to ' + str(rev_unixname) + ' '
            else:
                tqdm.write(f"Source file does not exist, probably deleted or renamed from already? {name_rename_from}")
                if self.progress_callback:
                    self.progress_callback(f"Source file does not exist, probably deleted or renamed from already? {name_rename_from}")

        # Add new page
        elif not os.path.isfile(self.path + '/' + fname): # never before seen
            commit_msg += "Created "
            if self.debug:
                tqdm.write(f"Adding {fname}")
                if self.progress_callback:
                    self.progress_callback(f"Adding {fname}")
        elif rev['comment'] == '':
            commit_msg += "Updated "

        self.last_names[unixname] = rev_unixname

        # Ouput contents
        outp = codecs.open(self.path + '/' + fname, "w", "UTF-8")
        if self.use_ftml:
            outp.write('---\n')
            outp.write('site: ' + self.wd.site+'\n')
            outp.write('page: ' + details['unixname']+'\n')
        if details['title']:
            outp.write('title: ' + details['title']+'\n')
        if tags:
            outp.write('tags: '+' '.join(tags)+'\n')
        if parent_unixname:
            outp.write('parent: '+parent_unixname+'\n')
        if self.use_ftml:
            outp.write('---\n')
        outp.write(source)
        outp.close()

        added_file_paths.append(str(fname))

        commit_msg += rev_unixname

        # Commit
        if rev['comment'] != '':
            commit_msg += ': ' + rev['comment']
        else:
            commit_msg += ' (no message)'
        if rev['date']:
            parsed_time = time.gmtime(int(rev['date'])) # TODO: assumes GMT
            commit_date = time.strftime('%Y-%m-%d %H:%M:%S', parsed_time)
        else:
            commit_date = None

        got_images = False

        # Add some spacing in the commit message
        if len(details['images']) > 0:
            commit_msg += '\n'

        for image in details['images']:
            if self.wd.maybe_download_file(image['src'], self.path + '/' + image['filepath']):
                commit_msg += '\nAdded image: ' + image['src']
                got_images = True
                # If we do this gitpython barfs on itself
                #added_file_paths.append(image['filepath'])
            else:
                self.saveFailedImages()


        if got_images:
            added_file_paths.append("images")
        tqdm.write(f"Committing: {self.rev_no}. {commit_msg}")
        if self.progress_callback:
            self.progress_callback(f"Committing: {self.rev_no}. {commit_msg}")

        # Include metadata in the commit (if changed)
        self.appendFetchedRevid(rev['rev_id'])
        self.saveMetadata()
        added_file_paths.append('.metadata.json')
        self.index.add(added_file_paths)

        username = str(rev['user'])
        email = re.sub(pattern = r'[^a-zA-Z0-9\-.+]', repl='', string=username).lower() + '@' + self.wd.sitename
        author = Actor(username, email)

        commit = self.index.commit(commit_msg, author=author, author_date=commit_date)

        if self.debug:
            tqdm.write(f'Committed {commit.name_rev} by {author}')
            if self.progress_callback:
                self.progress_callback(f'Committed {commit.name_rev} by {author}')

        self.fetched_revids.add(rev['rev_id'])

        self.rev_no += 1
        self.saveState() # Update operation state

        return True

    def convertFormat(self, use_ftml = True):
        # This method support conversion both ways
        # but in practice we only allow migrating to new format and not backwards
        pages = None
        added_file_paths = []
        commit_msg = "Convert from " + ("txt" if use_ftml else "ftml") + " to " + ("ftml" if use_ftml else "txt")
        if os.path.isfile(self.path+'/.pages'):
            tqdm.write('Loading fetched pages')
            if self.progress_callback:
                self.progress_callback('Loading fetched pages')
            fp = open(self.path+'/.pages', 'rb')
            pages = pickle.load(fp)
            fp.close()
        total_pages_to_convert = len(pages)
        for i, page in enumerate(tqdm(pages, desc='Loading list of pages to convert format', disable=self.progress_callback is not None)):
            if self.progress_callback:
                self.progress_callback(f"Converting format: {i+1}/{total_pages_to_convert}")
            if use_ftml:
                if os.path.isfile(self.path+'/'+page+'.txt'):
                    fname = self.path+'/'+page+'.txt'
                    with codecs.open(fname, "r", "UTF-8") as f:
                        content = f.readlines()
                    if not content[0].startswith('---'):
                        # Extracting all metadata and enclose them in YAML FrontMatter format
                        idx = 0
                        while (content[idx].startswith('site:') or content[idx].startswith('page:') or content[idx].startswith('title:') or
                            content[idx].startswith('tags:') or content[idx].startswith('parent:')):
                            idx+=1
                        content.insert(idx, '---\n')
                        for i in range(idx):
                            if content[content[i].index(':')+1] != ' ':
                                split = content[i].split(':')
                                split[0] = split[0]+' '
                                content[i] = ':'.join(split)
                        content.insert(0, '---\n')
                        with codecs.open(self.path+'/'+page+'.txt', "w", "UTF-8") as f:
                            f.writelines(content)
                    self.index.move([page+'.txt', page+'.ftml'], force=True)
                    added_file_paths.append(str(page+'.ftml'))
            else:
                if os.path.isfile(self.path+'/'+page+'.ftml'):
                    self.index.move([page+'.ftml', page+'.txt'], force=True)
                    added_file_paths.append(str(page+'.txt'))
        self.index.add(added_file_paths)

        commit = self.index.commit(commit_msg)

        if self.debug:
            tqdm.write(f'Committed {commit.name_rev} for format conversion')
            if self.progress_callback:
                self.progress_callback(f'Committed {commit.name_rev} for format conversion')

    def fetchAll(self):
        to_fetch = []
        total_wrevs = len(self.wrevs)
        for i, rev in enumerate(tqdm(self.wrevs, desc='Creating list of revisions to fetch', disable=self.progress_callback is not None)):
            if self.progress_callback:
                self.progress_callback(f"Creating list of revisions to fetch: {i+1}/{total_wrevs}")
            if rev['rev_id'] not in self.fetched_revids:
                to_fetch.append(rev)
        
        total_to_fetch = len(to_fetch)
        for i, rev in enumerate(tqdm(to_fetch, desc='Downloading', disable=self.progress_callback is not None)):
            if self.progress_callback:
                self.progress_callback(f"Downloading revisions: {i+1}/{total_to_fetch}")
            self.commitNext(rev)
            if self.progress_callback:
                self.progress_callback(f"Downloaded revision {rev['rev_id']}")

    #
    # Updates all children of the page to reflect parent's unixname change.
    #
    # Any page may be assigned a parent, which adds entry to revision log. We store this as parent:unixname in the page body.
    # A parent may then be renamed.
    # Wikidot logs no additional changes for child pages, yet they stay linked to the parent.
    #
    # Therefore, on every rename we must update all linked children in the same revision.
    #
    def updateChildren(self, oldunixname, newunixname):
        if self.debug:
            tqdm.write(f'Updating parents for {oldunixname} {newunixname}')
            if self.progress_callback:
                self.progress_callback(f'Updating parents for {oldunixname} {newunixname}')

        for child in list(self.last_parents.keys()):
            if self.last_parents[child] == oldunixname and self.last_parents[child] != newunixname:
                self.updateParentField(child, self.last_parents[child], newunixname)

    def updateTags(self, comment, unixname):
        file_name = self.path+'/'+unixname+'.txt'
        removed = []
        removed_match = re.search(pattern = r'Removed tags: ([^.]+,?)\.')
        if removed_match is not None:
            removed = removed_match.group(1).split(', ')

        tags = []

        with codecs.open(file_name, "r", "UTF-8") as f:
            content = f.readlines()

        tagsline = None
        for line in content:
            if line.startswith('tags:'):
                tagsline = line
                break

        # Father forgive me for the indentation depth
        idx = -1
        if tagsline is not None:
            idx = content.index(tagsline)
            for tag in tagsline.split(','):
                if not tag in removed:
                    tags.append(tag)


        added_match = re.search(pattern = r'Added tags: ([^.]+,?)\.')
        if added_match is not None:
            tags += added_match.group(1).split(', ')

        tags.sort()

        newtagsline = 'tags: ' + ','.join(tags) + '\n'
        if idx != -1:
            contents[idx] = newtagsline
        else:
            contents = newtagsline + contents

        with codecs.open(file_name, "w", "UTF-8") as f:
            f.writelines(content)

    #
    # Processes a page file and updates "parent: ..." string to reflect a change in parent's unixname.
    # The rest of the file is preserved.
    #
    def updateParentField(self, child_unixname, parent_oldunixname, parent_newunixname):
        child_winsafename = child_unixname.replace(':','~')
        parent_winsafename = parent_oldunixname.replace(':','~')
        child_path = self.path+'/'+child_winsafename+'.txt'
        if not os.path.isfile(child_path):
            tqdm.write(f'Failed to find child file! {child_path}')
            if self.progress_callback:
                self.progress_callback(f'Failed to find child file! {child_path}')
            return
        with codecs.open(child_path, "r", "UTF-8") as f:
            content = f.readlines()
        # Since this is all tracked by us, we KNOW there's a line in standard format somewhere
        idx = content.index('parent: '+parent_oldunixname+'\n')
        if idx < 0:
            idx = content.index('parent:'+parent_oldunixname+'\n')
        if idx < 0:
            raise Exception("Cannot update child page "+child_unixname+": "
                +"it is expected to have parent set to "+parent_oldunixname+", but there seems to be no such record in it.")
        content[idx] = 'parent: '+parent_newunixname+'\n'
        with codecs.open(self.path+'/'+child_winsafename+'.txt', "w", "UTF-8") as f:
            f.writelines(content)


    #
    # Finalizes the construction process and deletes any temporary files.
    #
    def cleanup(self):
        if os.path.exists(self.path+'/.wstate'):
            os.remove(self.path+'/.wstate')
        else:
            tqdm.write("wstate does not exist?")
            if self.progress_callback:
                self.progress_callback("wstate does not exist?")

        if os.path.exists(self.path+'/.wrevs'):
            os.remove(self.path+'/.wrevs')
        else:
            tqdm.write("wrevs does not exist?")
            if self.progress_callback:
                self.progress_callback("wrevs does not exist?")

        if os.path.exists(self.path+'/.pages'):
            os.remove(self.path+'/.pages')

        if self.rev_no > 0:
            self.index.add(['.fetched.txt'])
            self.index.commit('Updating fetched revisions')
