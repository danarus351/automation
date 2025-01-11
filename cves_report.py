import requests as re
import csv
import pandas as pd


def update_report(package_name='n/a', cve_link='n/a', cve_description='n/a', cve_status='n/a'):
    report[package_name] = {}
    report[package_name]['cve_description'] = cve_description
    report[package_name]['cve_link'] = cve_link
    report[package_name]['cve_status'] = cve_status


def export_to_excel(rep):
    report_pd = pd.DataFrame(rep)
    print(report_pd)


report = {}

with open('<exported package list>', 'r') as csv_file:
    packages_csv = csv.reader(csv_file, delimiter=',')
    for line in packages_csv:
        load = re.get('https://ubuntu.com/security/cves.json?package={}&version=xenial'.format(line[4]),
                      headers={'accept': 'application/json'}).json()['cves']
        if len(load) > 0:
            for distro in load[0]['packages'][0]['statuses']:
                if distro['release_codename'] == 'xenial':
                    package_name = load[0]['packages'][0]['name']
                    cve_link = load[0]['packages'][0]['source']
                    cve_description = load[0]['description']
                    cve_status = distro['status']
                    update_report(package_name=package_name, cve_link=cve_link, cve_description=cve_description,
                                  cve_status=cve_status)
                    break
        else:
            pass
export_to_excel(report)
