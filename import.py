import requests, json
from datetime import datetime
from tqdm import tqdm

# Byt nyckel-variablerna till dina egna API-nycklar.
# https://app.hubspot.com/integrations-settings/xxxxxxx/api-key
# https://xxxx-xxxxxx.pipedrive.com/settings/personal/api
pipedrive_key = "9be17c794f401c454862d225cf77be4b9053a53c"
hubspot_key = "06a5be9d-0582-442b-a0f7-13083ba50c89"

if __name__ == "__main__":
    error = 0

    headers = {
        'Host': 'api.hubapi.com',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'pythonrequests/2.10.0',
        'Cache-Control': 'no-cache'
    }
    
    # Funkar Pipedrive-nyckeln?
    if json.loads(requests.get('https://api.pipedrive.com/v1/currencies?limit=500&api_token=' + pipedrive_key).text)['success'] != False:

        # Funkar Hubspot-nyckeln?
        if "status" not in json.loads(requests.get('https://api.hubapi.com/contacts/v1/lists/all/contacts/all?limit=250&hapikey=' + hubspot_key).text).keys():

            # Alla contacts
            resp = requests.get('https://api.pipedrive.com/v1/persons?limit=500&api_token=' + pipedrive_key)
            if resp.status_code != 200:

                # Något gick fel
                print(resp.status_code)
                error += 1

            else:
                contacts = json.loads(resp.text)
                for contact in tqdm(contacts['data'], dynamic_ncols=50, desc="Importing contacts..."):

                    customer = {
                        'email':        '',
                        'firstname':    '',
                        'lastname':     '',
                        'organization': '',
                        'epochDate':     0,
                        'address':      '',
                        'phone':        ''
                    }

                    try:
                        customer['email'] = contact['email'][0]['value']
                    except TypeError:
                        pass

                    try:
                        customer['firstname'] = contact['name'].split()[0]
                        customer['lastname'] = contact['name'].split()[1]
                    except IndexError:
                        customer['firstname'] = contact['name']

                    try:
                        customer['organization'] = contact['org_name']
                    except TypeError:
                        pass
                    
                    try:
                        customer['epoch'] = datetime.strptime(str(contact['add_time']), '%Y-%m-%d %H:%M:%S').timestamp()
                    except TypeError as e:
                        pass

                    try:
                        customer['address'] = contact['org_id']['address']
                    except TypeError:
                        pass
                    
                    try:
                        customer['phone'] = contact['phone'][0]['value']
                    except TypeError:
                        pass

                    # Stoppa in kontakt i Hubspot.
                    # Alternativt byta till /contacts/v1/contact/batch/ ifall /contacts/v1/contact
                    # blir rate-limited.
                    post = {"properties":[{"property":"email","value":customer['email']},{"property":"firstname","value":customer['firstname']},{"property":"lastname","value":customer['lastname']},{"property":"company","value":customer['organization']},{"property":"phone","value":customer['phone']},{"property":"address","value":customer['address']}]}
                    resp = requests.post('https://api.hubapi.com/contacts/v1/contact/?limit=250&hapikey=' + hubspot_key, data=json.dumps(post), headers=headers)
            
            # Alla companies
            resp = requests.get('https://api.pipedrive.com/v1/organizations?limit=500&api_token=' + pipedrive_key)
            if resp.status_code != 200:

                # Något gick fel
                print(resp.status_code)
                error += 1

            else:
                companies = json.loads(resp.text)
                for company in tqdm(companies['data'], dynamic_ncols=50, desc="Importing companies..."):
                    companyAttr = {'name': ''}

                    if "name" in company.keys():
                        if str(company['name']) != "None":
                            try:
                                companyAttr['name'] = company['name']
                            except TypeError:
                                pass

                    post = {"properties": [{"name": "name","value": companyAttr['name']},{"name": "description","value": ""}]}
                    requests.post('https://api.hubapi.com/companies/v2/companies?limit=250&hapikey=' + hubspot_key, data=json.dumps(post), headers=headers)
            
            # Alla deals
            pipeResp = requests.get('https://api.pipedrive.com/v1/deals?limit=500&api_token=' + pipedrive_key)
            hubResp = requests.get('https://api.hubapi.com/deals/v1/deal/paged?includeAssociations=true&properties=dealname&limit=250&hapikey=' + hubspot_key)

            if pipeResp.status_code != 200:
                # Något gick fel
                print(pipeResp.status_code)
                error += 1
            else:
                deals = json.loads(pipeResp.text)
                for deal in tqdm(deals['data'], dynamic_ncols=50, desc="Importing deals..."):
                    dealAttr = {
                        'title':                '',
                        'value':                '',
                        'add_time':              0,
                        'expected_close_date':   0,
                        'epochDate':             0,
                        'address':              '',
                        'phone':                ''
                    }

                    if "title" in deal.keys():
                        if str(deal['title']) != "None":
                            try:
                                dealAttr['title'] = deal['title']
                            except TypeError:
                                pass
                    
                    if "value" in deal.keys():
                        if str(deal['value'] != "None"):
                            try: 
                                dealAttr['value'] = deal['value']
                            except TypeError:
                                pass
                    
                    if "add_time" in deal.keys():
                        if str(deal['add_time'] != "None"):
                            try:
                                dealAttr['add_time'] = datetime.strptime(str(deal['add_time']), '%Y-%m-%d %H:%M:%S').timestamp()
                            except TypeError:
                                pass
                    
                    if "expected_close_date" in deal.keys():
                        if str(deal['expected_close_date']) != "None":
                            try:
                                dealAttr['expected_close_date'] = datetime.strptime(str(deal['expected_close_date']), '%Y-%m-%d').timestamp()
                            except TypeError:
                                pass

                    # Add deal
                    post = {"properties": [{"name": "dealname","value": dealAttr['title']},{"name": "amount","value": dealAttr['value']},{"name": "closedate","value": int(int(dealAttr['expected_close_date'])*1000)},{"name": "createdate","value": int(int(dealAttr['add_time'])*1000)}]}
                    resp = requests.post('https://api.hubapi.com/deals/v1/deal?limit=250&hapikey=' + hubspot_key, data=json.dumps(post), headers=headers)
            
            pipeResp = requests.get('https://api.pipedrive.com/v1/deals?limit=500&api_token=' + pipedrive_key)
            hubResp = requests.get('https://api.hubapi.com/deals/v1/deal/paged?includeAssociations=true&properties=dealname&limit=250&offset=0&hapikey=' + hubspot_key)
            
            # Associera deals
            hubDeals = json.loads(hubResp.text)
            pipeDeals = json.loads(pipeResp.text)
            offset = 0

            while hubDeals["hasMore"] == True:
                hubAscResp = requests.get('https://api.hubapi.com/deals/v1/deal/paged?includeAssociations=true&properties=dealname&limit=250&offset=' + str(offset) + '&hapikey=' + hubspot_key)
                hubDeals = json.loads(hubAscResp.text)

                offset = hubDeals['offset']
                print(offset)
                for hubDeal in tqdm(hubDeals['deals'], dynamic_ncols=50, desc="Associating deals..."):
                    for pipeDeal in pipeDeals['data']:
                        if hubDeal['properties']['dealname']['versions'][0]['value'] == pipeDeal['title']:
                            hubDealId = hubDeal['dealId']
                            try:
                                personVid = json.loads(requests.get('https://api.hubapi.com/contacts/v1/contact/email/' + str(pipeDeal['person_id']['email'][0]['value']) + '/profile?limit=250&hapikey=' + hubspot_key).text)['vid']
                                resp = requests.put('https://api.hubapi.com/deals/v1/deal/' + str(hubDealId) + '/associations/CONTACT?id=' + str(personVid) + '&offset=' + str(offset) + '&limit=250&hapikey=' + hubspot_key)
                                print(resp.text)
                            except (KeyError, json.decoder.JSONDecodeError, TypeError) as e:
                                print(e)
                                pass
                        else:
                            continue

            # Assigna owners
            pipeResp = requests.get('https://api.pipedrive.com/v1/users?api_token=' + pipedrive_key)
            pipeOwners = json.loads(pipeResp.text)
            owners = {'data': []}

            # Skapa owners
            '''
            for pipeOwner in pipeOwners['data']:
                propertyData = {
                    'portalId': 5299456,
                    'type': 'PERSON',
                    'firstname': pipeOwner['name'],
                    'lastname': '',
                    'email': pipeOwner['email'],
                    'quota': 10000,
                    'remoteList': [
                        'portalId': 5299456,
                        'remoteType': 'EMAIL',
                        'remoteId': pipeOwner['email'],
                        'active': true
                    ]
                }

                requests.put('http://api.hubapi.com/owners/v2/owners?hapikey=' + hubspot_key, data=propertyData)
            '''

            # Lokal JSON-dict
            resp = requests.get('http://api.hubapi.com/owners/v2/owners?hapikey=' + hubspot_key)
            hubOwners = json.loads(resp.text)
            for pipeOwner in pipeOwners['data']:
                for hubOwner in hubOwners:
                    owners['data'].append({'pipeId': pipeOwner['id'], 'hubId': hubOwner['ownerId'], 'name': pipeOwner['name']})
            
            print(owners)
            resp = requests.get('https://api.pipedrive.com/v1/deals?status=all_not_deleted&start=0&limit=500&api_token=' + pipedrive_key)
            pipeDeals = json.loads(resp.text)

            for pipeDeal in pipeDeals['data']:
                #hubspot_owner_id
                pipeOwnerId = pipeDeal['creator_user_id']['id'] # 1000

                if pipeOwnerId in owners:
                    propertyData = {'name': 'hubspot_owner_id', 'value': owners['data']['id']}
                    requests.put('https://api.hubapi.com/properties/v1/deals/properties/named/hubspot_owner_id', data=propertyData)
            if error > 0:
                print("ERROR: " + str(error) + " errors occured")

        else:
            print("Hubspot API key doesn't exist! (" + hubspot_key + ")")
    else:
        print("Pipedrive API key doesn't exist! (" + pipedrive_key + ")")

