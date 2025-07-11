# -*- coding: utf-8 -*-
#
# Copyright (C) 2021-2022 Yehui Wang, Shengbao Li
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Yehui Wang <yehui.wang.mdh@gmail.com>
#   Shengbao Li <lishengbao147@gmail.com>

import logging
import re
import time

import requests

from dateutil.relativedelta import relativedelta
from datetime import datetime

from grimoire_elk.elastic import ElasticSearch
from grimoire_elk.errors import ELKError
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)

from elasticsearch import Elasticsearch as ES, RequestsHttpConnection

from grimoire_elk.enriched.utils import get_time_diff_days

from grimoire_elk.enriched.enrich import Enrich, metadata
from grimoire_elk.elastic_mapping import Mapping as BaseMapping


CATEGORY_ISSUE = "issue"
CATEGORY_PULL_REQUEST = "pull_request"
CATEGORY_REPO = 'repository'
CATEGORY_EVENT = "event"
CATEGORY_STARGAZER = "stargazer"
CATEGORY_FORK = "fork"
CATEGORY_WATCH = "watch"

GITCODE = 'https://gitcode.com/'
GITCODE_ISSUES = "gitcode_issues"
GITCODE_MERGES = "gitcode_pulls"

logger = logging.getLogger(__name__)


def deep_get(dictionary, keys, default=None):
    """递归获取字典深层的值"""
    for key in keys:
        if dictionary is None:
            return default
        dictionary = dictionary.get(key)
    return dictionary or default


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.
        geopoints type is not created in dynamic mapping
        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
               "merge_author_geolocation": {
                   "type": "geo_point"
               },
               "assignee_geolocation": {
                   "type": "geo_point"
               },
               "state": {
                   "type": "keyword"
               },
               "user_geolocation": {
                   "type": "geo_point"
               },
               "title_analyzed": {
                 "type": "text",
                 "index": true
               }
            }
        }
        """

        return {"items": mapping}


class GitCodeEnrich(Enrich):

    mapping = Mapping

    issue_roles = ['assignee_data', 'user_data']
    pr_roles = ['merged_by_data', 'user_data']
    roles = ['assignee_data', 'merged_by_data', 'user_data']
    event_roles = ['actor', 'reporter']

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_onion)
        # self.studies.append(self.enrich_pull_requests)
        # self.studies.append(self.enrich_geolocation)
        # self.studies.append(self.enrich_extra_data)
        # self.studies.append(self.enrich_backlog_analysis)

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_author(self):
        return "user_data"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_identities(self, item):
        """Return the identities from an item"""

        category = item['category']
        item = item['data']

        if category == "issue":
            identity_types = ['user', 'assignee']
        elif category == "pull_request":
            identity_types = ['user', 'merged_by']
        else:
            identity_types = []

        for identity in identity_types:
            identity_attr = identity + "_data"
            if item[identity] and identity_attr in item:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity_attr])
                if user:
                    yield user

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]

        if not user:
            return identity

        identity['username'] = user['login']
        identity['email'] = None
        identity['name'] = None
        if 'email' in user:
            identity['email'] = user['email']
        if 'name' in user:
            identity['name'] = user['name']
        return identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    def get_time_to_first_attention(self, item):
        """Get the first date at which a comment was made to the issue by someone
        other than the user who created the issue
        """
        comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']
                         if 'user' in comment and 'user' in item and item['user']['login'] != comment['user']['login']]
        if comment_dates:
            return min(comment_dates)
        return None
    #get comments and exclude bot  
    def get_num_of_comments_without_bot(self, item):
        """Get the num of comment was made to the issue by someone
        other than the user who created the issue and bot
        """
        comments = [comment for comment in item['comments_data']
                         if 'user' in comment and 'user' in item and item['user']['login'] != comment['user']['login'] \
                             and not (comment['user'].get('name', '').endswith("bot"))]
        return len(comments)
      
    #get first attendtion without bot
    def get_time_to_first_attention_without_bot(self, item):
        """Get the first date at which a comment was made to the issue by someone
        other than the user who created the issue and bot
        """
        comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']
                         if 'user' in comment and 'user' in item and item['user']['login'] != comment['user']['login'] \
                             and not (comment['user'].get('name', '').endswith("bot"))]
        if comment_dates:
            return min(comment_dates)
        return None
    
    def get_num_of_reviews_without_bot(self, item):
        """Get the num of comment was made to the issue by someone
        other than the user who created the issue and bot
        """
        comments = [comment for comment in item['review_comments_data']
                         if 'user' in comment and 'user' in item and item['user']['login'] != comment['user']['login'] \
                             and not (comment['user'].get('name', '').endswith("bot")) \
                                 and not (comment['user'].get('name', '').endswith("ci"))]
        return len(comments) 

    def get_time_to_merge_request_response(self, item):
        """Get the first date at which a review was made on the PR by someone
        other than the user who created the PR
        """
        review_dates = []
        for comment in item['review_comments_data']:
            # skip comments of ghost users
            if 'user' not in comment or not comment['user']:
                continue
                
            if 'user' not in item or not item['user']:
                continue

            # skip comments of the pull request creator
            if item['user']['login'] == comment['user']['login']:
                continue

            review_dates.append(str_to_datetime(comment['created_at']))

        if review_dates:
            return min(review_dates)

        return None

    #get first attendtion without bot
    def get_time_to_first_review_attention_without_bot(self, item):
        """Get the first date at which a comment was made to the pr by someone
        other than the user who created the pr and bot
        """
        comment_dates = [str_to_datetime(comment['created_at']) for comment in item['review_comments_data']
                         if 'user' in comment and 'user' in item and item['user']['login'] != comment['user']['login'] \
                             and not (comment['user'].get('name', '').endswith("bot"))]
        if comment_dates:
            return min(comment_dates)
        return None

    def get_latest_comment_date(self, item):
        """Get the date of the latest comment on the issue/pr"""

        comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']]
        if comment_dates:
            return max(comment_dates)
        return None

    def get_num_commenters(self, item):
        """Get the number of unique people who commented on the issue/pr"""

        commenters = [comment['user']['login'] for comment in item['comments_data'] if 'user' in comment]
        return len(set(commenters))
    
    def get_CVE_message(self, item):
        """Get the first date at which a comment was made to the issue by someone
        other than the user who created the issue and bot
        """
        if item["body"] and "漏洞公开时间" in item["body"] :
            issue_body = item["body"].splitlines()
            cve_body = {}
            for message in issue_body:
                try:
                    [key,val] = message.split('：')
                    cve_body[key.strip()] = val.strip()
                except Exception as e:
                    pass
            return cve_body
        else:
            return None
    


    @metadata
    def get_rich_item(self, item):

        rich_category_switch = {
            CATEGORY_ISSUE: lambda: self.__get_rich_issue(item),
            CATEGORY_PULL_REQUEST: lambda: self.__get_rich_pull(item),
            CATEGORY_REPO: lambda: self.__get_rich_repo(item),
            CATEGORY_EVENT: lambda: self.__get_rich_event(item),
            CATEGORY_STARGAZER: lambda: self.__get_rich_stargazer(item),
            CATEGORY_FORK: lambda: self.__get_rich_fork(item),
            CATEGORY_WATCH: lambda: self.__get_rich_watch(item)
        }
        if item['category'] in rich_category_switch:
            rich_item = rich_category_switch[item['category']]()
        else:
            logger.error("[gitcode] rich item not defined for GitCode category {}".format(
                         item['category']))

        self.add_repository_labels(rich_item)
        self.add_metadata_filter_raw(rich_item)
        return rich_item

    def __get_rich_pull(self, item):
        rich_pr = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_pr[f] = item[f]
            else:
                rich_pr[f] = None
        # The real data
        pull_request = item['data']

        if pull_request['closed_at'] == '':
            pull_request['closed_at'] = None
        if pull_request['merged_at'] == '':
            pull_request['merged_at'] = None

        #close and merge in gitcode are two different status
        if pull_request['state'] == 'merged':
            rich_pr['time_to_close_days'] = \
                get_time_diff_days(pull_request['created_at'], pull_request['merged_at'])
        else:
            rich_pr['time_to_close_days'] = \
                get_time_diff_days(pull_request['created_at'], pull_request['closed_at'])
        
        # merged is not equal to closed in gitcode, pr state have open, merged, closed
        if pull_request['state'] == 'open':
            rich_pr['time_open_days'] = \
                get_time_diff_days(pull_request['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_pr['time_open_days'] = rich_pr['time_to_close_days']

        rich_pr['user_login'] = pull_request.get('user', {}).get('login')

        user = pull_request.get('user_data', None)
        if user is not None and user:
            rich_pr['user_name'] = user['name']
            rich_pr['author_name'] = user['name']
            rich_pr['user_email'] = user.get('email', None)
            rich_pr["user_domain"] = self.get_email_domain(user['email']) if user.get('email', None) else None
            rich_pr['user_org'] = user.get('company', None)
            rich_pr['user_location'] = user.get('location', None)
            rich_pr['user_geolocation'] = None
        else:
            rich_pr['user_name'] = None
            rich_pr['user_email'] = None
            rich_pr["user_domain"] = None
            rich_pr['user_org'] = None
            rich_pr['user_location'] = None
            rich_pr['user_geolocation'] = None
            rich_pr['author_name'] = None

        merged_by = pull_request.get('merged_by_data', None)
        if merged_by and merged_by is not None:
            rich_pr['merge_author_login'] = merged_by['login']
            rich_pr['merge_author_name'] = merged_by['name']
            rich_pr["merge_author_domain"] = self.get_email_domain(merged_by['email']) if merged_by.get('email', None) else None
            rich_pr['merge_author_org'] = merged_by.get('company', None)
            rich_pr['merge_author_location'] = merged_by.get('location', None)
            rich_pr['merge_author_geolocation'] = None
        else:
            rich_pr['merge_author_name'] = None
            rich_pr['merge_author_login'] = None
            rich_pr["merge_author_domain"] = None
            rich_pr['merge_author_org'] = None
            rich_pr['merge_author_location'] = None
            rich_pr['merge_author_geolocation'] = None
        
        testers_login = set()
        [testers_login.add(tester.get('login')) for tester in pull_request['testers'] if 'testers' in pull_request]
        rich_pr['testers_login'] = list(testers_login)
        requested_reviewers_login = set()
        [requested_reviewers_login.add(requested_reviewer.get('login')) for requested_reviewer in pull_request['assignees'] if 'assignees' in pull_request]
        rich_pr['requested_reviewers_login'] = list(requested_reviewers_login)

        rich_pr['id'] = pull_request['id']
        rich_pr['id_in_repo'] = pull_request['html_url'].split("/")[-1]
        rich_pr['repository'] = self.get_project_repository(rich_pr)
        rich_pr['title'] = pull_request['title']
        rich_pr['title_analyzed'] = pull_request['title']
        rich_pr['state'] = pull_request['state']
        rich_pr['created_at'] = pull_request['created_at']
        rich_pr['updated_at'] = pull_request['updated_at']
        rich_pr['merged'] = pull_request['state'] == 'merged'
        rich_pr['merged_at'] = pull_request['merged_at']
        rich_pr['closed_at'] = pull_request['closed_at']
        rich_pr['url'] = pull_request['html_url']
        rich_pr['review_mode'] = pull_request.get('review_mode')
        rich_pr['labels'] = [label['name'] for label in pull_request.get('labels', [])]
        rich_pr['assignees_accept_count'] = sum(1 for a in pull_request['assignees'] if a.get('accept') is True and not a.get('login', '').lower().endswith('bot'))

        rich_pr['pull_request'] = True
        rich_pr['item_type'] = 'pull request'

        rich_pr['gitcode_repo'] = rich_pr['repository'].replace(GITCODE, '')
        rich_pr['gitcode_repo'] = re.sub('.git$', '', rich_pr['gitcode_repo'])
        rich_pr["url_id"] = rich_pr['gitcode_repo'] + "/pull/" + rich_pr['id_in_repo']

        # GMD code development metrics
        rich_pr['forks'] = None
        rich_pr['code_merge_duration'] = get_time_diff_days(pull_request['created_at'],
                                                            pull_request['merged_at'])
        rich_pr['num_review_comments'] = len(pull_request['review_comments_data'])

        rich_pr['time_to_merge_request_response'] = None
        if rich_pr['num_review_comments'] != 0:
            min_review_date = self.get_time_to_merge_request_response(pull_request)
            rich_pr['time_to_merge_request_response'] = \
                get_time_diff_days(str_to_datetime(pull_request['created_at']), min_review_date)
            rich_pr['num_review_comments_without_bot'] = \
                                   self.get_num_of_reviews_without_bot(pull_request)
            rich_pr['time_to_first_attention_without_bot'] = \
                get_time_diff_days(str_to_datetime(pull_request['created_at']),
                                    self.get_time_to_first_review_attention_without_bot(pull_request))
                                    
        rich_pr['commits_data'] = pull_request['commits_data']

        
        if 'linked_issues' in pull_request:
            rich_pr['linked_issues_count'] = len(pull_request['linked_issues'])
            rich_pr['linked_issues'] = pull_request['linked_issues']
        
        if self.prjs_map:
            rich_pr.update(self.get_item_project(rich_pr))

        if 'project' in item:
            rich_pr['project'] = item['project']

        rich_pr.update(self.get_grimoire_fields(pull_request['created_at'], "pull_request"))

        item[self.get_field_date()] = rich_pr[self.get_field_date()]
        rich_pr.update(self.get_item_sh(item, self.pr_roles))

        return rich_pr

    def __get_rich_issue(self, item):
        rich_issue = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_issue[f] = item[f]
            else:
                rich_issue[f] = None
        # The real data
        issue = item['data']

        if 'finished_at' not in issue or issue['finished_at'] == '':
            issue['finished_at'] = None

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['finished_at'])

        #issue have four status: open, closed.
        if issue['state'] == 'open':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime_utcnow().replace(tzinfo=None))
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        rich_issue['user_login'] = issue.get('user', {}).get('login')

        user = issue.get('user_data', None)
        if user is not None and user:
            rich_issue['user_name'] = user['name']
            rich_issue['author_name'] = user['name']
            rich_issue['user_email'] = user.get('email', None)
            rich_issue["user_domain"] = self.get_email_domain(user['email']) if user.get('email', None) else None
            rich_issue['user_org'] = user.get('company', None)
            rich_issue['user_location'] = user.get('location', None)
            rich_issue['user_geolocation'] = None
        else:
            rich_issue['user_name'] = None
            rich_issue['user_email'] = None
            rich_issue["user_domain"] = None
            rich_issue['user_org'] = None
            rich_issue['user_location'] = None
            rich_issue['user_geolocation'] = None
            rich_issue['author_name'] = None

        assignee = issue.get('assignee_data', None)
        if assignee and assignee is not None:
            rich_issue['assignee_login'] = assignee['login']
            rich_issue['assignee_name'] = assignee['name']
            rich_issue["assignee_domain"] = self.get_email_domain(assignee['email']) if assignee.get('email', None) else None
            rich_issue['assignee_org'] = assignee.get('company', None)
            rich_issue['assignee_location'] = assignee.get('location', None)
            rich_issue['assignee_geolocation'] = None
        else:
            rich_issue['assignee_name'] = None
            rich_issue['assignee_login'] = None
            rich_issue["assignee_domain"] = None
            rich_issue['assignee_org'] = None
            rich_issue['assignee_location'] = None
            rich_issue['assignee_geolocation'] = None

        rich_issue['id'] = issue['id']
        rich_issue['id_in_repo'] = issue['html_url'].split("/")[-1]
        rich_issue['repository'] = self.get_project_repository(rich_issue)
        rich_issue['title'] = issue['title']
        rich_issue['title_analyzed'] = issue['title']
        rich_issue['state'] = issue['state']
        rich_issue['created_at'] = issue['created_at']
        rich_issue['updated_at'] = issue['updated_at']
        rich_issue['closed_at'] = issue['finished_at']
        rich_issue['url'] = issue['html_url']
        rich_issue['issue_type'] = issue.get('issue_type')
        labels = []
        [labels.append(label['name']) for label in issue['labels'] if 'labels' in issue]
        rich_issue['labels'] = labels

        rich_issue['pull_request'] = False
        rich_issue['item_type'] = 'issue'

        rich_issue['gitcode_repo'] = rich_issue['repository'].replace(GITCODE, '')
        rich_issue['gitcode_repo'] = re.sub('.git$', '', rich_issue['gitcode_repo'])
        rich_issue["url_id"] = rich_issue['gitcode_repo'] + "/issues/" + rich_issue['id_in_repo']

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        if 'project' in item:
            rich_issue['project'] = item['project']

        rich_issue['time_to_first_attention'] = None
        if issue['comments'] != 0:
            rich_issue['time_to_first_attention'] = \
                get_time_diff_days(str_to_datetime(issue['created_at']),
                                   self.get_time_to_first_attention(issue))
            rich_issue['num_of_comments_without_bot'] = \
                                   self.get_num_of_comments_without_bot(issue)
            rich_issue['time_to_first_attention_without_bot'] = \
                get_time_diff_days(str_to_datetime(issue['created_at']),
                                    self.get_time_to_first_attention_without_bot(issue))

        cve_message = self.get_CVE_message(issue)
        if cve_message :
            try:
                scores = cve_message['BaseScore'].split(' ')
                rich_issue['cve_public_time'] = cve_message['漏洞公开时间']
                rich_issue['cve_create_time'] = rich_issue['created_at']          
                rich_issue['cve_percerving_time'] = rich_issue['time_to_first_attention_without_bot'] if 'time_to_first_attention_without_bot' in rich_issue else None
                rich_issue['cve_handling_time'] = rich_issue['time_open_days']
                if len(scores) == 2:
                    rich_issue['cve_base_score'] = scores[0]
                    rich_issue['cve_level'] = scores[1]
                else:
                    rich_issue['cve_base_score'] = None
                    rich_issue['cve_level'] = None
            except Exception as error:
                logger.error("CVE messgae is not complete: %s", error)
        else:
            rich_issue['cve_public_time'] = None
            rich_issue['cve_create_time'] = None
            rich_issue['cve_base_score'] = None
            rich_issue['cve_level'] = None
            rich_issue['cve_percerving_time'] = None
            rich_issue['cve_handling_time'] = None
        
                    
        rich_issue.update(self.get_grimoire_fields(issue['created_at'], "issue"))

        
        item[self.get_field_date()] = rich_issue[self.get_field_date()]
        rich_issue.update(self.get_item_sh(item, self.issue_roles))

        return rich_issue

    def __get_rich_repo(self, item):
        rich_repo = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_repo[f] = item[f]
            else:
                rich_repo[f] = None

        repo = item['data']

        rich_repo['forks_count'] = repo['forks_count']
        rich_repo['subscribers_count'] = repo['watchers_count']
        rich_repo['stargazers_count'] = repo['stargazers_count']
        rich_repo['fetched_on'] = repo['fetched_on']
        rich_repo['url'] = repo['web_url']
        rich_repo['status'] = repo['status']
        if repo["status"] in "关闭":
            rich_repo['archived'] = True
            rich_repo['archivedAt'] = repo['updated_at']
        else:
            rich_repo['archived'] = False
            rich_repo['archivedAt'] = None
        rich_repo['created_at'] = repo['created_at']
        rich_repo['updated_at'] = repo['updated_at']
        
        rich_releases = []
        for release in repo['releases'] :
            rich_releases_dict = {}
            rich_releases_dict['id'] = release['target_commitish']
            rich_releases_dict['tag_name'] = release['tag_name']
            rich_releases_dict['target_commitish'] = release['target_commitish']
            rich_releases_dict['prerelease'] = release['prerelease']
            rich_releases_dict['name'] = release['name']
            rich_releases_dict['body'] = release['body']
            rich_releases_dict['created_at'] = release['created_at']
            release_author = release['author']
            rich_releases_author_dict = {}
            rich_releases_author_dict['login'] = release_author['login']
            rich_releases_author_dict['name'] = release_author['name']
            rich_releases_dict['author'] = rich_releases_author_dict
            rich_releases.append(rich_releases_dict)
        rich_repo['releases'] = rich_releases
        rich_repo['releases_count'] = len(rich_releases)
        
        
        rich_branches = []
        for branch in repo.get('branches', []):
            rich_branches_item = {}
            rich_branches_item["name"] = deep_get(branch, ["name"])
            rich_branches_item["author_name"] = deep_get(branch, ["commit", "commit", "author", "name"])
            rich_branches_item["author_date"] = deep_get(branch, ["commit", "commit", "author", "date"])
            rich_branches_item["author_email"] = deep_get(branch, ["commit", "commit", "author", "email"])
            rich_branches_item["committer_name"] = deep_get(branch, ["commit", "commit", "committer", "name"])
            rich_branches_item["committer_date"] = deep_get(branch, ["commit", "commit", "committer", "date"])
            rich_branches_item["committer_email"] = deep_get(branch, ["commit", "commit", "committer", "email"])
            rich_branches_item["message"] = deep_get(branch, ["commit", "commit", "message"])
            rich_branches_item["sha"] = deep_get(branch, ["commit", "sha"])
            rich_branches_item["url"] = deep_get(branch, ["commit", "url"])
            rich_branches_item["protected"] = deep_get(branch, ["protected"])
            rich_branches_item["developers_can_push"] = branch.get("developers_can_push")
            rich_branches_item["developers_can_merge"] = branch.get("developers_can_merge")
            rich_branches.append(rich_branches_item)
        rich_repo['branches'] = rich_branches
        rich_repo['branches_count'] = len(rich_branches)
        

        rich_repo["topics"] = repo.get('project_labels', [])

        if self.prjs_map:
            rich_repo.update(self.get_item_project(rich_repo))

        rich_repo.update(self.get_grimoire_fields(item['metadata__updated_on'], "repository"))

        return rich_repo
    
    def get_event_type(self, action_type, content):
        if action_type is None:
            return None
        
        rules = [
            (lambda: action_type == "label" and "add" in content, "LabeledEvent"),
            (lambda: action_type == "label" and "delete" in content, "UnlabeledEvent"),
            (lambda: action_type == "closed", "ClosedEvent"),
            (lambda: action_type == "opened", "ReopenedEvent"),
            (lambda: action_type == "milestone" and "changed" in content, "MilestonedEvent"),
            (lambda: action_type == "milestone" and "removed" in content, "DemilestonedEvent"),
            (lambda: action_type == "locked", "LockedEvent"),
            (lambda: action_type == "unlocked", "UnlockedEvent"),
            (lambda: action_type == "title", "RenamedTitleEvent"),
            (lambda: action_type == "merged", "MergedEvent"),
            (lambda: action_type == "description", "ChangeDescriptionEvent"),
            (lambda: action_type == "add_mr_issue_link", "LinkIssueEvent"),
            (lambda: action_type == "delete_mr_issue_link", "UnlinkIssueEvent"),
            (lambda: action_type == "add_issue_mr_link", "LinkPullRequestEvent"),
            (lambda: action_type == "delete_issue_mr_link", "UnlinkPullRequestEvent"),
            (lambda: action_type == "add_issue_branch_link", "SettingBranchEvent"),
            (lambda: action_type == "delete_issue_branch_link", "ChangeBranchEvent"),
            (lambda: action_type == "discussion", "DiscussionEvent"),
            (lambda: action_type == "confidential", "ConfidentialEvent"),
            (lambda: (action_type == "assignee" and "assigned" in content) or (action_type == "mr_change" and ("Add assignees" in content or "Add approvers" in content)),"AssignedEvent"),
            (lambda: (action_type == "assignee" and "unassigned" in content) or (action_type == "mr_change" and ("Delete assignees" in content or "Delete approvers" in content)),"UnassignedEvent"),
            (lambda: action_type == "mr_change" and "Add testers" in content, "SetTesterEvent"),
            (lambda: action_type == "mr_change" and "deleted testers" in content, "UnsetTesterEvent"),
            (lambda: action_type == "mr_change" and "Add reviewers" in content ,"SetReviewerEvent"),
            (lambda: action_type == "mr_change" and "Delete reviewers" in content ,"UnsetReviewerEvent"),
            (lambda: action_type == "mr_change" and "Approval Gate : pass" in content, "CheckPassEvent"),
            (lambda: action_type == "mr_change" and "Test Gate : pass" in content, "TestPassEvent"),
            (lambda: action_type == "mr_change" and "Review Gate : pass" in content, "ReviewPassEvent"),
            (lambda: action_type == "mr_change" and "Approval Gate : reset" in content, "ResetAssignResultEvent"),
            (lambda: action_type == "mr_change" and "Test Gate : reset" in content, "ResetTestResultEvent"),
            (lambda: action_type == "mr_change" and "Review Gate : reset" in content, "ResetReviewResultEvent"),
            (lambda: action_type == "mr_change" and "Approval Gate : reject" in content, "CheckRejectEvent"),
            (lambda: action_type == "mr_change" and "Test Gate : reject" in content, "TestRejectEvent"),
            (lambda: action_type == "mr_change" and "Review Gate : reject" in content, "ReviewRejectEvent"),    
        ]

        for condition, event_type in rules:
            if condition():
                return event_type
        return None


    def __get_rich_event(self, item):
        rich_event = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_event[f] = item[f]
            else:
                rich_event[f] = None
        # The real data
        event = item['data']
        main_content = item['data']['issue'] if 'issue' in event else item['data']['pull']
        actor = item['data']['user']

        # move the issue reporter to level of actor. This is needed to
        # allow `get_item_sh` adding SortingHat identities
        reporter = main_content.get('user', {})
        item['data']['reporter'] = reporter
        item['data']['actor'] = actor

        rich_event['id'] = event['id']
        rich_event['icon'] = event.get('icon')
        rich_event['actor_username'] = actor['login']
        rich_event['user_login'] = rich_event['actor_username']
        rich_event['content'] = event['content']
        rich_event['created_at'] = event['created_at']
        rich_event['action_type'] = event.get('action_type')
        rich_event['event_type'] = self.get_event_type(event.get('action_type'), event['content'])
        rich_event['repository'] = item["tag"]
        rich_event['pull_request'] = False if 'issue' in event else True
        rich_event['item_type'] = 'issue' if 'issue' in event else 'pull request'

        rich_event['gitcode_repo'] = rich_event['repository'].replace(GITCODE, '')
        rich_event['gitcode_repo'] = re.sub('.git$', '', rich_event['gitcode_repo'])
        if rich_event['pull_request']:
            rich_event['pull_id'] = main_content['id']
            rich_event['pull_id_in_repo'] = main_content['html_url'].split("/")[-1]
            rich_event['pull_title'] = main_content['title']
            rich_event['pull_title_analyzed'] = main_content['title']
            rich_event['pull_state'] = main_content['state']
            rich_event['pull_created_at'] = main_content['created_at']
            rich_event['pull_updated_at'] = main_content['updated_at']
            rich_event['pull_closed_at'] = None if main_content['closed_at'] == '' else main_content['closed_at']
            rich_event['pull_url'] = main_content['html_url']
            rich_event['pull_labels'] = [label['name'] for label in main_content['labels']]
            rich_event["pull_url_id"] = rich_event['gitcode_repo'] + "/pull/" + rich_event['pull_id_in_repo']
        else:
            rich_event['issue_id'] = main_content['id']
            rich_event['issue_id_in_repo'] = main_content['html_url'].split("/")[-1]
            rich_event['issue_title'] = main_content['title']
            rich_event['issue_title_analyzed'] = main_content['title']
            rich_event['issue_state'] = main_content['state']
            rich_event['issue_created_at'] = main_content['created_at']
            rich_event['issue_updated_at'] = main_content['updated_at']
            rich_event['issue_closed_at'] = None if main_content['finished_at'] == '' else main_content['finished_at']
            rich_event['issue_finished_at'] = rich_event['issue_closed_at']
            rich_event['issue_url'] = main_content['html_url']  
            rich_event['issue_labels'] = [label['name'] for label in main_content['labels']]
            rich_event["issue_url_id"] = rich_event['gitcode_repo'] + "/issues/" + rich_event['issue_id_in_repo']
        
        user = event.get('user_data', None)
        if user is not None and user:
            rich_event['user_name'] = user['name']
            rich_event['author_name'] = user['name']
            rich_event['user_email'] = user.get('email', None)
            rich_event["user_domain"] = self.get_email_domain(user['email']) if user.get('email', None) else None
            rich_event['user_org'] = user.get('company', None)
            rich_event['user_location'] = user.get('location', None)
            rich_event['user_geolocation'] = None
        else:
            rich_event['user_name'] = None
            rich_event['user_email'] = None
            rich_event["user_domain"] = None
            rich_event['user_org'] = None
            rich_event['user_location'] = None
            rich_event['user_geolocation'] = None
            rich_event['author_name'] = None


        if self.prjs_map:
            rich_event.update(self.get_item_project(rich_event))

        if 'project' in item:
            rich_event['project'] = item['project']

        rich_event.update(self.get_grimoire_fields(event['created_at'], "event"))
        item[self.get_field_date()] = rich_event[self.get_field_date()]
        rich_event.update(self.get_item_sh(item, self.event_roles))

        return rich_event

    def __get_rich_stargazer(self, item):
        rich_stargazer = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_stargazer[f] = item[f]
            else:
                rich_stargazer[f] = None
        # The real data
        stargazer = item['data']
        user = stargazer.get('user_data', None)
        if user is not None and user:
            rich_stargazer['user_id'] = user['id']
            rich_stargazer["user_login"] = user["login"]
            rich_stargazer["user_name"] = user["name"]
            rich_stargazer["auhtor_name"] = user["name"]
            rich_stargazer["user_html_url"] = user["html_url"]
            rich_stargazer['user_email'] = user.get('email', None)
            rich_stargazer["user_domain"] = self.get_email_domain(user['email']) if user.get('email', None) else None
            rich_stargazer['user_company'] = user.get('company', None)
            rich_stargazer['user_location'] = user.get('location', None)
            rich_stargazer["user_remark"] = user.get("remark", None)
            rich_stargazer["user_type"] = user["type"]
            
        else:
            rich_stargazer['user_id'] = None
            rich_stargazer["user_login"] = stargazer['login']
            rich_stargazer["user_name"] = stargazer['name']
            rich_stargazer["auhtor_name"] = None
            rich_stargazer["user_html_url"] = None
            rich_stargazer['user_email'] = None
            rich_stargazer["user_domain"] = None
            rich_stargazer['user_company'] = None
            rich_stargazer['user_location'] = None
            rich_stargazer["user_remark"] = None
            rich_stargazer["user_type"] = None

        rich_stargazer["star_at"] = stargazer["starred_at"]
        rich_stargazer["created_at"] = stargazer["starred_at"]
                  
        if self.prjs_map:
            rich_stargazer.update(self.get_item_project(rich_stargazer))                  
        rich_stargazer.update(self.get_grimoire_fields(stargazer['starred_at'], "stargazer"))

        return rich_stargazer

    def __get_rich_fork(self, item):
        rich_fork = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_fork[f] = item[f]
            else:
                rich_fork[f] = None
        # The real data
        fork = item['data']
        user = fork.get('user_data', None)
        if user is not None and user:
            rich_fork['user_id'] = user['id']
            rich_fork["user_login"] = user["login"]
            rich_fork["user_name"] = user["name"]
            rich_fork["auhtor_name"] = user["name"]
            rich_fork["user_html_url"] = user["html_url"]
            rich_fork['user_email'] = user.get('email', None)
            rich_fork["user_domain"] = self.get_email_domain(user['email']) if user.get('email', None) else None
            rich_fork['user_company'] = user.get('company', None)
            rich_fork['user_location'] = user.get('location', None)
            rich_fork["user_remark"] = user.get("remark", None)
            rich_fork["user_type"] = user["type"]
            
        else:
            rich_fork['user_id'] = None
            rich_fork["user_login"] = fork["owner"]['login']
            rich_fork["user_name"] = fork["owner"]['name']
            rich_fork["auhtor_name"] = None
            rich_fork["user_html_url"] = None
            rich_fork['user_email'] = None
            rich_fork['user_domain'] = None
            rich_fork['user_company'] = None
            rich_fork['user_location'] = None
            rich_fork["user_remark"] = None
            rich_fork["user_type"] = None

        rich_fork["fork_at"] = fork["created_at"]
        rich_fork["created_at"] = fork["created_at"]
                  
        if self.prjs_map:
            rich_fork.update(self.get_item_project(rich_fork))                  
        rich_fork.update(self.get_grimoire_fields(fork['created_at'], "fork"))

        return rich_fork

    def __get_rich_watch(self, item):
        rich_watch = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_watch[f] = item[f]
            else:
                rich_watch[f] = None
        # The real data
        watch = item['data']
        user = watch.get('user_data', None)
        if user is not None and user:
            rich_watch['user_id'] = user['id']
            rich_watch["user_login"] = user["login"]
            rich_watch["user_name"] = user["name"]
            rich_watch["auhtor_name"] = user["name"]
            rich_watch["user_html_url"] = user["html_url"]
            rich_watch['user_email'] = user.get('email', None)
            rich_watch["user_domain"] = self.get_email_domain(user['email']) if user.get('email', None) else None
            rich_watch['user_company'] = user.get('company', None)
            rich_watch['user_location'] = user.get('location', None)
            rich_watch["user_remark"] = user.get("remark", None)
            rich_watch["user_type"] = user["type"]
            
        else:
            rich_watch['user_id'] = None
            rich_watch["user_login"] = watch['login']
            rich_watch["user_name"] = watch['name']
            rich_watch["auhtor_name"] = None
            rich_watch["user_html_url"] = None
            rich_watch['user_email'] = None
            rich_watch['user_demain'] = None
            rich_watch['user_company'] = None
            rich_watch['user_location'] = None
            rich_watch["user_remark"] = None
            rich_watch["user_type"] = None

        rich_watch["watch_at"] = watch["watch_at"]
        rich_watch["created_at"] = watch["watch_at"]
                  
        if self.prjs_map:
            rich_watch.update(self.get_item_project(rich_watch))                  
        rich_watch.update(self.get_grimoire_fields(watch['watch_at'], "watch"))

        return rich_watch

    def enrich_onion(self, ocean_backend, enrich_backend,
                     in_index, out_index, data_source=None, no_incremental=False,
                     contribs_field='uuid',
                     timeframe_field='grimoire_creation_date',
                     sort_on_field='metadata__timestamp',
                     seconds=Enrich.ONION_INTERVAL):

        if not data_source:
            raise ELKError(cause="Missing data_source attribute")

        if data_source not in [GITCODE_ISSUES, GITCODE_MERGES, ]:
            logger.warning("[gitcode] data source value {} should be: {} or {}".format(
                data_source, GITCODE_ISSUES, GITCODE_MERGES))

        super().enrich_onion(enrich_backend=enrich_backend,
                             in_index=in_index,
                             out_index=out_index,
                             data_source=data_source,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental,
                             seconds=seconds)
