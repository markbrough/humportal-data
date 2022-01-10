import csv
import os
import json
import datetime

import requests

BASE_URL = "https://analytics.codeforiati.org/{}"
STATS_URL = "https://stats.codeforiati.org/{}"
HUMANITARIAN_ANALYTICS = BASE_URL.format("humanitarian.csv")
VERSIONS = STATS_URL.format("current/inverted-publisher/versions.json")
CODELIST_VALUES = STATS_URL.format("current/inverted-publisher/codelist_values.json")
ELEMENTS = STATS_URL.format("current/inverted-publisher/elements.json")
ACTIVITIES = STATS_URL.format("current/inverted-publisher/activities.json")
HUMANITARIAN = STATS_URL.format("current/inverted-publisher/humanitarian.json")
FREQUENCY = BASE_URL.format("timeliness_frequency.csv")

def get_source_data():
    r = requests.get(HUMANITARIAN_ANALYTICS)
    with open("cache/humanitarian_analytics.csv", 'w') as humanitarian_analytics_csv:
        humanitarian_analytics_csv.write(r.text)

    r = requests.get(FREQUENCY)
    frequency = {}
    with open("cache/frequency.json", 'w') as humanitarian_frequency_json:
        csvreader = csv.DictReader(r.content.decode('utf-8').splitlines(), delimiter=',')
        for row in csvreader:
            frequency[row['Publisher Registry Id']] = row['Frequency']
        json.dump(frequency, humanitarian_frequency_json)

    r = requests.get(VERSIONS)
    publishers = {}
    with open("cache/versions.json", 'w') as versions_json:
        versions = json.loads(r.text)
        for version, _pubs in versions.items():
            for _pub in _pubs.keys():
                publishers[_pub] = version
        json.dump(publishers, versions_json)

    r = requests.get(CODELIST_VALUES)
    with open("cache/codelist_values.json", 'w') as codelists_json:
        codelists_json.write(r.text)

    r = requests.get(ELEMENTS)
    with open("cache/elements.json", 'w') as elements_json:
        elements_json.write(r.text)

    r = requests.get(ACTIVITIES)
    with open("cache/activities.json", 'w') as activities_json:
        activities_json.write(r.text)

    r = requests.get(HUMANITARIAN)
    with open("cache/humanitarian.json", 'w') as humanitarian_json:
        humanitarian_json.write(r.text)


def generate_homepage_stats(analytics_publishers):
    signatories = set()
    signatories_data = {}
    with open('data/signatories.csv', 'r') as signatories_csv:
        csvreader = csv.DictReader(signatories_csv)
        for row in csvreader:
            signatories.add(row['GB signatory'])
            if row['GB signatory'] not in signatories_data:
                signatories_data[row['GB signatory']] = {
                    'iati': False,
                    'humanitarian': False
                }
            publisher_id = row['Registred Pub. ID']
            if publisher_id in analytics_publishers:
                if int(analytics_publishers[publisher_id]['Number of Activities']) > 0:
                    signatories_data[row['GB signatory']]['iati'] = True
                if int(analytics_publishers[publisher_id]['Publishing Humanitarian']) > 0:
                    signatories_data[row['GB signatory']]['humanitarian'] = True

    with open('output/homepage.json', 'w') as jsonfile:
        json.dump({
            'signatories': len(signatories),
            'iati': len(list(filter(
                lambda signatory_data: signatory_data['iati'] == True, signatories_data.values()))),
            'humanitarian': len(list(filter(
                lambda signatory_data: signatory_data['humanitarian'] == True, signatories_data.values())))
        },
        jsonfile)


def generate_signatory_data(analytics_publishers):
    publishers = []
    with open('cache/versions.json', 'r') as versions_json:
        versions = json.load(versions_json)
    with open('cache/codelist_values.json', 'r') as codelists_json:
        codelist_values = json.load(codelists_json)
    with open('cache/elements.json', 'r') as elements_json:
        elements = json.load(elements_json)
    with open('cache/frequency.json', 'r') as frequency_json:
        frequency = json.load(frequency_json)
    with open('cache/activities.json', 'r') as activities_json:
        activities = json.load(activities_json)
    with open('cache/humanitarian.json', 'r') as humanitarian_json:
        humanitarian = json.load(humanitarian_json)
    with open('data/signatories.csv', 'r') as signatories_csv:
        csvreader = csv.DictReader(signatories_csv)
        for row in csvreader:
            publisher_id = row['Registred Pub. ID']
            pub = analytics_publishers.get(publisher_id, {})
            hum_data = float(pub.get('Publishing Humanitarian', 0)) > 0
            num_activities = activities.get(publisher_id, 0)
            num_humanitarian_activities = humanitarian['is_humanitarian'].get(publisher_id, 0)
            _202_hum_data = (humanitarian['is_humanitarian_by_attrib'].get(publisher_id, 0) > 0) or \
                (humanitarian['contains_humanitarian_scope_without_humanitarian'].get(publisher_id, 0) > 0)
            _203_hum_data = ((publisher_id in codelist_values['.//transaction/transaction-type/@code']['12'].keys()) or
                (publisher_id in codelist_values['.//transaction/transaction-type/@code']['13'].keys()) or
                (publisher_id in codelist_values['.//default-aid-type/@vocabulary']['2'].keys()) or
                (publisher_id in codelist_values['.//default-aid-type/@vocabulary']['3'].keys()) or
                (publisher_id in codelist_values['.//participating-org/@type']['24'].keys()) or
                (publisher_id in codelist_values['.//transaction/provider-org/@type']['24'].keys()) or
                (publisher_id in codelist_values['.//transaction/receiver-org/@type']['24'].keys())
                )
            monthly = frequency.get(publisher_id) == "Monthly"
            traceability = publisher_id in elements['iati-activity/transaction/provider-org/@provider-activity-id'].keys()
            publishers.append({
                'publisherID': publisher_id,
                'iatiOrganisationID': row['IATI organisation reference'],
                'name': row['Publisher'],
                'gbSignatory': row['GB signatory'],
                'organisationType': row['Organisation type'],
                'iatiVersion': versions.get(publisher_id, None),
                'humData': hum_data,
                'humanitarianActivities': num_humanitarian_activities,
                'activities': num_activities,
                '202HumData': _202_hum_data,
                '203HumData': _203_hum_data,
                'traceability': traceability,
                'monthly': monthly,
                'frequency': frequency.get(publisher_id)
            })
    with open('output/signatories.json', 'w') as jsonfile:
        json.dump(publishers, jsonfile)
    with open('output/signatories.csv', 'w') as csvfile:
        fieldnames = publishers[0].keys()
        csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csvwriter.writeheader()
        for row in publishers:
            csvwriter.writerow(row)


def generate_signatories_progress():
    with open('output/homepage.json', 'r') as homepage_stats_jsonfile:
        homepage_stats = json.load(homepage_stats_jsonfile)
        num_signatories = homepage_stats['signatories']

    signatories = {}
    with open('output/signatories.json', 'r') as signatories_json_file:
        signatories_stats = json.load(signatories_json_file)
        for sig in signatories_stats:
            if sig['gbSignatory'] not in signatories:
                signatories[sig['gbSignatory']] = {
                    '202': False,
                    'granular_202': False,
                    'granular_203': False,
                    'traceability': False,
                    'iati': False,
                    'humanitarian': False
                }
            if sig['iatiVersion'] in ['2.02', '2.03']:
                signatories[sig['gbSignatory']]['202'] = True
            if sig['202HumData'] == True:
                signatories[sig['gbSignatory']]['granular_202'] = True
            if sig['203HumData'] == True:
                signatories[sig['gbSignatory']]['granular_203'] = True
            if sig['traceability'] == True:
                signatories[sig['gbSignatory']]['traceability'] = True
            if sig['activities'] > 0:
                signatories[sig['gbSignatory']]['iati'] = True
            if sig['humanitarianActivities'] > 0:
                signatories[sig['gbSignatory']]['humanitarian'] = True

    num_202 = len(list(filter(lambda signatory: signatory['202'] == True, signatories.values())))
    num_granular_202 = len(list(filter(lambda signatory: signatory['granular_202'] == True, signatories.values())))
    num_granular_203 = len(list(filter(lambda signatory: signatory['granular_203'] == True, signatories.values())))
    num_traceability = len(list(filter(lambda signatory: signatory['traceability'] == True, signatories.values())))
    num_iati = len(list(filter(lambda signatory: signatory['iati'] == True, signatories.values())))
    num_hum = len(list(filter(lambda signatory: signatory['humanitarian'] == True, signatories.values())))

    today = {
        'id': '',
        'Date': datetime.datetime.utcnow().date().isoformat(),
        'Total Signatories': num_signatories,
        'Publishing open data using IATI': num_iati,
        'Publishing data on their humanitarian activities': num_hum,
        'Using v2.02 of the IATI standard or later': num_202,
        'Providing more granular v2.02': num_granular_202,
        'Providing more granular v2.03': num_granular_203,
        'Publishing Traceability Information': num_traceability

    }
    with open('data/signatories-progress.csv', 'r') as signatories_progress_csv:
        csvreader = csv.DictReader(signatories_progress_csv)
        with open('output/signatories-progress.json', 'w') as signatories_progress_json:
            out = []
            for row in csvreader:
                out.append(row)
            out.append(today)
            json.dump(out, signatories_progress_json)

    with open('data/signatories-progress.csv', 'r') as signatories_progress_csv:
        csvreader = csv.DictReader(signatories_progress_csv)
        with open('output/signatories-progress.csv', 'w') as csvfile:
            fieldnames = csvreader.fieldnames
            csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
            csvwriter.writeheader()
            for row in csvreader:
                csvwriter.writerow(row)
            csvwriter.writerow(today)


def generate_analytics_publishers():
    analytics_publishers = {}
    with open('cache/humanitarian_analytics.csv', 'r') as humanitarian_analytics_csv:
        csvreader = csv.DictReader(humanitarian_analytics_csv)
        for row in csvreader:
            analytics_publishers[row['Publisher Registry Id']] = row
    return analytics_publishers


def generate_metadata(started, finished):
    with open('output/metadata.json', 'w') as metadatafile:
        json.dump({
            'started': started.isoformat(),
            'finished': finished.isoformat()
        }, metadatafile)


def generate_stats():
    started = datetime.datetime.utcnow()
    os.makedirs('data/', exist_ok=True)
    os.makedirs('cache/', exist_ok=True)
    os.makedirs('output/', exist_ok=True)
    get_source_data()
    analytics_publishers = generate_analytics_publishers()
    generate_homepage_stats(analytics_publishers)
    generate_signatory_data(analytics_publishers)
    generate_signatories_progress()
    finished = datetime.datetime.utcnow()
    generate_metadata(started, finished)


if __name__=='__main__':
    generate_stats()
